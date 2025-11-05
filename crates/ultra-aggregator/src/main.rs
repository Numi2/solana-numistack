// Numan Thabit 2025
// crates/ultra-aggregator/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use bytes::{Buf, BytesMut};
use faststreams::{decode_record_from_slice, Record};
#[cfg(feature = "rkyv")]
use faststreams::{decode_record_archived_from_slice, ArchivedRecord, FLAG_LZ4, FLAG_RKYV};
#[cfg(feature = "rkyv")]
use rkyv;
#[cfg(all(feature = "rkyv", feature = "kafka"))]
use rkyv::de::deserializers::SharedDeserializeMap;
#[cfg(all(feature = "rkyv", feature = "kafka"))]
use rkyv::Deserialize;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::ser::{SerializeMap, Serializer};
use socket2::SockRef;
use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::signal;
use tokio::time::{self, Duration};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, serde::Deserialize)]
struct KafkaCfg {
    brokers: String,
    topic_accounts: String,
    topic_txs: String,
    topic_blocks: String,
    topic_slots: String,
}

// json_view removed: replaced with JsonEvent pipeline
#[derive(Debug, Clone, serde::Deserialize)]
struct SocketCfg {
    uds_path: String,
    // Optional tuning knob: requested socket recv buffer size
    uds_recv_buf_bytes: Option<usize>,
    // Optional safety bound: drop frames larger than this many bytes to avoid OOM
    max_frame_bytes: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct Cfg {
    // Back-compat: single-listener config
    uds_path: String,
    stdout_json: bool,
    metrics_addr: Option<String>,
    // Optional tuning knob: requested socket recv buffer size
    uds_recv_buf_bytes: Option<usize>,
    // Optional safety bound: drop frames larger than this many bytes to avoid OOM (back-compat)
    max_frame_bytes: Option<usize>,
    // New: multi-listener with per-socket overrides
    listeners: Option<Vec<SocketCfg>>,
    #[cfg(feature = "kafka")]
    kafka: Option<KafkaCfg>,
}

#[cfg(feature = "kafka")]
#[derive(Clone)]
struct KafkaSink {
    tx: tokio::sync::mpsc::Sender<Record>,
}
#[cfg(feature = "kafka")]
impl KafkaSink {
    fn new(cfg: KafkaCfg) -> Result<Self> {
        use rdkafka::client::DefaultClientContext;
        use rdkafka::producer::{FutureProducer, FutureRecord};
        use rdkafka::util::TokioRuntime;
        use rdkafka::ClientConfig;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(65_536);
        tokio::spawn(async move {
            let prod: FutureProducer<DefaultClientContext, TokioRuntime> = match ClientConfig::new()
                .set("bootstrap.servers", &cfg.brokers)
                .set("queue.buffering.max.messages", "2000000")
                .set("queue.buffering.max.kbytes", "1048576")
                .set("message.timeout.ms", "5000")
                .create::<FutureProducer<DefaultClientContext, TokioRuntime>>()
            {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("kafka producer init failed: {e}");
                    return;
                }
            };
            while let Some(rec) = rx.recv().await {
                let (topic, key) = match &rec {
                    Record::Account(a) => {
                        (&cfg.topic_accounts, bs58::encode(&a.pubkey).into_string())
                    }
                    Record::Tx(t) => (&cfg.topic_txs, bs58::encode(&t.signature).into_string()),
                    Record::Block(b) => {
                        let k = b
                            .blockhash
                            .map(|h| bs58::encode(h).into_string())
                            .unwrap_or_default();
                        (&cfg.topic_blocks, k)
                    }
                    Record::Slot { slot, .. } => (&cfg.topic_slots, slot.to_string()),
                    Record::EndOfStartup => (&cfg.topic_slots, "eos".to_string()),
                };
                if let Ok(payload) = bincode::serialize(&rec) {
                    let _ = prod
                        .send(
                            FutureRecord::to(topic).key(&key).payload(&payload),
                            std::time::Duration::from_secs(1),
                        )
                        .await;
                }
            }
        });
        Ok(Self { tx })
    }

    fn try_send(&self, rec: Record) {
        let _ = self.tx.try_send(rec);
    }
}

#[derive(Clone)]
struct JsonSink {
    tx: tokio::sync::mpsc::Sender<JsonEvent>,
}

impl JsonSink {
    fn new() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<JsonEvent>(65_536);
        std::thread::spawn(move || {
            let stdout = std::io::stdout();
            let mut w = std::io::LineWriter::new(stdout.lock());
            let cache_cap = std::env::var("ULTRA_JSON_B58_CACHE_CAP")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(16_384);
            let mut cache32 = Base58Cache::<32>::new(cache_cap);
            let mut cache64 = Base58Cache::<64>::new(cache_cap / 2);
            while let Some(evt) = rx.blocking_recv() {
                gauge!("ultra_json_queue_depth").set(rx.len() as f64);
                if write_json_event(&evt, &mut w, &mut cache32, &mut cache64).is_ok() {
                    let _ = w.write_all(b"\n");
                }
            }
        });
        Self { tx }
    }

    fn try_send(&self, evt: JsonEvent) -> bool {
        self.tx.try_send(evt).is_ok()
    }
}

static INGEST_SEQ: AtomicU64 = AtomicU64::new(0);
const INGEST_SAMPLE_MASK: u64 = 0xFF; // sample ~1/256
const INGEST_SAMPLE_WEIGHT: u64 = 256;

#[derive(Clone, Debug)]
enum JsonEvent {
    Account {
        slot: u64,
        is_startup: bool,
        pubkey: [u8; 32],
        lamports: u64,
        owner: [u8; 32],
        executable: bool,
        rent_epoch: u64,
        data_len: usize,
    },
    Tx {
        slot: u64,
        signature: [u8; 64],
        err: Option<String>,
        vote: bool,
    },
    Block {
        slot: u64,
        blockhash: Option<[u8; 32]>,
        parent_slot: Option<u64>,
        rewards_len: u32,
        block_time_unix: Option<i64>,
        leader: Option<[u8; 32]>,
    },
    Slot {
        slot: u64,
        parent: Option<u64>,
        status: u8,
    },
    EndOfStartup,
}

fn json_event_owned_from_record(rec: &Record) -> JsonEvent {
    match rec {
        Record::Account(a) => JsonEvent::Account {
            slot: a.slot,
            is_startup: a.is_startup,
            pubkey: a.pubkey,
            lamports: a.lamports,
            owner: a.owner,
            executable: a.executable,
            rent_epoch: a.rent_epoch,
            data_len: a.data.len(),
        },
        Record::Tx(t) => JsonEvent::Tx {
            slot: t.slot,
            signature: t.signature,
            err: t.err.clone(),
            vote: t.vote,
        },
        Record::Block(b) => JsonEvent::Block {
            slot: b.slot,
            blockhash: b.blockhash,
            parent_slot: b.parent_slot,
            rewards_len: b.rewards_len,
            block_time_unix: b.block_time_unix,
            leader: b.leader,
        },
        Record::Slot {
            slot,
            parent,
            status,
        } => JsonEvent::Slot {
            slot: *slot,
            parent: *parent,
            status: *status,
        },
        Record::EndOfStartup => JsonEvent::EndOfStartup,
    }
}

#[cfg(feature = "rkyv")]
fn json_event_from_archived_record(rec: &ArchivedRecord) -> JsonEvent {
    match rec {
        ArchivedRecord::Account(a) => JsonEvent::Account {
            slot: a.slot,
            is_startup: a.is_startup,
            pubkey: a.pubkey,
            lamports: a.lamports,
            owner: a.owner,
            executable: a.executable,
            rent_epoch: a.rent_epoch,
            data_len: a.data.len() as usize,
        },
        ArchivedRecord::Tx(t) => {
            let err = match &t.err {
                rkyv::option::ArchivedOption::Some(s) => Some(s.as_str().to_owned()),
                rkyv::option::ArchivedOption::None => None,
            };
            JsonEvent::Tx {
                slot: t.slot,
                signature: t.signature,
                err,
                vote: t.vote,
            }
        }
        ArchivedRecord::Block(b) => {
            let blockhash = match &b.blockhash {
                rkyv::option::ArchivedOption::Some(h) => Some(*h),
                rkyv::option::ArchivedOption::None => None,
            };
            let parent_slot = match &b.parent_slot {
                rkyv::option::ArchivedOption::Some(x) => Some(*x),
                rkyv::option::ArchivedOption::None => None,
            };
            let block_time_unix = match &b.block_time_unix {
                rkyv::option::ArchivedOption::Some(x) => Some(*x),
                rkyv::option::ArchivedOption::None => None,
            };
            let leader = match &b.leader {
                rkyv::option::ArchivedOption::Some(p) => Some(*p),
                rkyv::option::ArchivedOption::None => None,
            };
            JsonEvent::Block {
                slot: b.slot,
                blockhash,
                parent_slot,
                rewards_len: b.rewards_len,
                block_time_unix,
                leader,
            }
        }
        ArchivedRecord::Slot { slot, parent, status } => {
            let parent = match parent {
                rkyv::option::ArchivedOption::Some(p) => Some(*p),
                rkyv::option::ArchivedOption::None => None,
            };
            JsonEvent::Slot {
                slot: *slot,
                parent,
                status: *status,
            }
        }
        ArchivedRecord::EndOfStartup => JsonEvent::EndOfStartup,
    }
}

#[derive(Debug)]
struct Base58Cache<const N: usize> {
    map: std::collections::HashMap<[u8; N], Arc<str>>,
    order: VecDeque<[u8; N]>,
    capacity: usize,
}

impl<const N: usize> Base58Cache<N> {
    fn new(capacity: usize) -> Self {
        Self {
            map: std::collections::HashMap::new(),
            order: VecDeque::with_capacity(capacity.min(1024)),
            capacity,
        }
    }

    fn encode(&mut self, bytes: &[u8; N]) -> Arc<str> {
        if self.capacity == 0 {
            return Arc::from(bs58::encode(bytes).into_string().into_boxed_str());
        }
        if let Some(existing) = self.map.get(bytes) {
            return Arc::clone(existing);
        }
        let encoded: Arc<str> = Arc::from(bs58::encode(bytes).into_string().into_boxed_str());
        if self.map.len() == self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
        self.order.push_back(*bytes);
        self.map.insert(*bytes, Arc::clone(&encoded));
        encoded
    }
}

// removed json_value_from_event: replaced with write_json_event for direct serialization

fn write_json_event<W: Write>(
    evt: &JsonEvent,
    w: &mut W,
    cache32: &mut Base58Cache<32>,
    cache64: &mut Base58Cache<64>,
) -> serde_json::Result<()> {
    let mut ser = serde_json::Serializer::new(w);
    match evt {
        JsonEvent::Account {
            slot,
            is_startup,
            pubkey,
            lamports,
            owner,
            executable,
            rent_epoch,
            data_len,
        } => {
            let pubkey_b58 = cache32.encode(pubkey);
            let owner_b58 = cache32.encode(owner);
            let mut m = ser.serialize_map(Some(9))?;
            m.serialize_entry("type", "account")?;
            m.serialize_entry("slot", slot)?;
            m.serialize_entry("is_startup", is_startup)?;
            m.serialize_entry("pubkey", pubkey_b58.as_ref())?;
            m.serialize_entry("lamports", lamports)?;
            m.serialize_entry("owner", owner_b58.as_ref())?;
            m.serialize_entry("executable", executable)?;
            m.serialize_entry("rent_epoch", rent_epoch)?;
            m.serialize_entry("data_len", data_len)?;
            m.end()
        }
        JsonEvent::Tx {
            slot,
            signature,
            err,
            vote,
        } => {
            let sig_b58 = cache64.encode(signature);
            let mut m = ser.serialize_map(Some(5))?;
            m.serialize_entry("type", "tx")?;
            m.serialize_entry("slot", slot)?;
            m.serialize_entry("signature", sig_b58.as_ref())?;
            m.serialize_entry("err", err)?;
            m.serialize_entry("vote", vote)?;
            m.end()
        }
        JsonEvent::Block {
            slot,
            blockhash,
            parent_slot,
            rewards_len,
            block_time_unix,
            leader,
        } => {
            let blockhash_b58 = blockhash.as_ref().map(|h| cache32.encode(h));
            let leader_b58 = leader.as_ref().map(|l| cache32.encode(l));
            let mut m = ser.serialize_map(Some(7))?;
            m.serialize_entry("type", "block")?;
            m.serialize_entry("slot", slot)?;
            m.serialize_entry("blockhash", &blockhash_b58.as_ref().map(|s| s.as_ref()))?;
            m.serialize_entry("parent_slot", parent_slot)?;
            m.serialize_entry("rewards_len", rewards_len)?;
            m.serialize_entry("block_time_unix", block_time_unix)?;
            m.serialize_entry("leader", &leader_b58.as_ref().map(|s| s.as_ref()))?;
            m.end()
        }
        JsonEvent::Slot {
            slot,
            parent,
            status,
        } => {
            let mut m = ser.serialize_map(Some(4))?;
            m.serialize_entry("type", "slot")?;
            m.serialize_entry("slot", slot)?;
            m.serialize_entry("parent", parent)?;
            m.serialize_entry("status", status)?;
            m.end()
        }
        JsonEvent::EndOfStartup => {
            let mut m = ser.serialize_map(Some(1))?;
            m.serialize_entry("type", "end_of_startup")?;
            m.end()
        }
    }
}

static RESYNC_EVENTS_THIS_MINUTE: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/aggregator.json".to_string());
    let cfg: Cfg = {
        let raw = std::fs::read_to_string(&cfg_path)?;
        serde_json::from_str(&raw)?
    };

    if let Some(addr) = &cfg.metrics_addr {
        let _ = PrometheusBuilder::new()
            .with_http_listener(addr.parse::<std::net::SocketAddr>().unwrap())
            .install();
    }

    // Export a per-minute gauge for resync events
    tokio::spawn(async move {
        let mut tick = time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            let n = RESYNC_EVENTS_THIS_MINUTE.swap(0, Ordering::Relaxed);
            gauge!("ultra_resync_events_per_minute").set(n as f64);
        }
    });

    // Construct listeners list (multi-listener support with per-socket overrides)
    let listeners_cfg: Vec<SocketCfg> = if let Some(list) = cfg.listeners.clone() {
        list
    } else {
        vec![SocketCfg {
            uds_path: cfg.uds_path.clone(),
            uds_recv_buf_bytes: cfg.uds_recv_buf_bytes,
            max_frame_bytes: cfg.max_frame_bytes,
        }]
    };

    #[cfg(feature = "kafka")]
    let kafka_sink = if let Some(k) = cfg.kafka.clone() {
        Some(KafkaSink::new(k)?)
    } else {
        None
    };

    let json_sink = if cfg.stdout_json {
        Some(JsonSink::new())
    } else {
        None
    };

    let shutdown = signal::ctrl_c();
    tokio::pin!(shutdown);

    // Spawn one accept loop per listener
    for s in listeners_cfg {
        let json_clone = json_sink.clone();
        let default_recv = cfg.uds_recv_buf_bytes;
        let default_mfb = cfg.max_frame_bytes;
        #[cfg(feature = "kafka")]
        let ks = kafka_sink.clone();
        tokio::spawn(async move {
            let uds_path = s.uds_path.clone();
            if Path::new(&uds_path).exists() {
                let _ = std::fs::remove_file(&uds_path);
            }
            let listener = match UnixListener::bind(&uds_path) {
                Ok(l) => l,
                Err(e) => {
                    error!("failed to bind {}: {e}", uds_path);
                    return;
                }
            };
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(_meta) = std::fs::metadata(&uds_path) {
                    let _ =
                        std::fs::set_permissions(&uds_path, std::fs::Permissions::from_mode(0o660));
                }
            }
            info!("listening UDS {}", uds_path);

            let recv_req = s
                .uds_recv_buf_bytes
                .or(default_recv)
                .unwrap_or(32 * 1024 * 1024);
            let max_frame_bytes = s
                .max_frame_bytes
                .or(default_mfb)
                .unwrap_or(16 * 1024 * 1024);

            loop {
                tokio::select! {
                    Ok((sock, _)) = listener.accept() => {
                        #[cfg(unix)] {
                            let sr = SockRef::from(&sock);
                            let _ = sr.set_recv_buffer_size(recv_req);
                            if let Ok(actual) = sr.recv_buffer_size() {
                                info!("UDS recv buffer set: requested={} actual={}", recv_req, actual);
                                gauge!("ultra_uds_recv_buf_bytes").set(actual as f64);
                            }
                        }
                        let json_clone2 = json_clone.clone();
                        #[cfg(feature = "kafka")] {
                            let ks2 = ks.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_client(sock, json_clone2, max_frame_bytes, ks2).await {
                                    error!("client error: {e:?}");
                                }
                            });
                        }
                        #[cfg(not(feature = "kafka"))] {
                            tokio::spawn(async move {
                                if let Err(e) = handle_client(sock, json_clone2, max_frame_bytes).await {
                                    error!("client error: {e:?}");
                                }
                            });
                        }
                    }
                }
            }
        });
    }

    // Wait for shutdown signal
    let _ = shutdown.as_mut().await;
    info!("shutting down");
    Ok(())
}

async fn handle_client(
    mut sock: UnixStream,
    json: Option<JsonSink>,
    max_frame_bytes: usize,
    #[cfg(feature = "kafka")] ks: Option<KafkaSink>,
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(1 << 20);
    let mut scratch: Vec<u8> = Vec::with_capacity(8 * 1024);
    loop {
        // read available bytes directly into the growable buffer
        let n = sock.read_buf(&mut buf).await?;
        if n == 0 {
            break;
        }

        // Try to peel records out
        loop {
            // Safety pre-check: if header present and declared frame size is excessive, resync
            if buf.len() >= 12 {
                let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                let ver = u16::from_be_bytes([buf[4], buf[5]]);
                if magic != faststreams::FRAME_MAGIC || ver != faststreams::FRAME_VERSION {
                    counter!("ultra_decode_bad_header_total").increment(1);
                    counter!("ultra_resync_events_total").increment(1);
                    RESYNC_EVENTS_THIS_MINUTE.fetch_add(1, Ordering::Relaxed);
                    let magic_bytes = faststreams::FRAME_MAGIC.to_be_bytes();
                    if let Some(idx) = memchr::memmem::find(&buf[..], &magic_bytes) {
                        if idx > 0 {
                            buf.advance(idx);
                        }
                    } else {
                        buf.clear();
                    }
                    break;
                }
                let len = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
                if len > max_frame_bytes {
                    counter!("ultra_frame_too_large_total").increment(1);
                    counter!("ultra_resync_events_total").increment(1);
                    RESYNC_EVENTS_THIS_MINUTE.fetch_add(1, Ordering::Relaxed);
                    // Resync by searching for the next magic after the current one to avoid re-parsing the same header
                    let magic_bytes = faststreams::FRAME_MAGIC.to_be_bytes();
                    if let Some(rel) = memchr::memmem::find(&buf[4..], &magic_bytes) {
                        buf.advance(rel + 4);
                    } else {
                        buf.clear();
                    }
                    break;
                }
            }
            #[cfg(feature = "rkyv")]
            {
                if buf.len() >= 12 {
                    let flags = u16::from_be_bytes([buf[6], buf[7]]);
                    if (flags & FLAG_RKYV) != 0 && (flags & FLAG_LZ4) == 0 {
                        match decode_record_archived_from_slice(&buf[..]) {
                            Ok((arec, consumed)) => {
                                if let Some(js) = &json {
                                    let evt = json_event_from_archived_record(arec);
                                    if !js.try_send(evt) {
                                        counter!("ultra_json_dropped_total").increment(1);
                                    }
                                }
                                #[cfg(feature = "kafka")]
                                if let Some(k) = &ks {
                                    // Deserialize only for Kafka sink (selective conversion)
                                    let mut map = SharedDeserializeMap::new();
                                    match arec.deserialize(&mut map) {
                                        Ok(rec) => k.try_send(rec),
                                        Err(_) => {
                                            counter!("ultra_kafka_deser_errors_total").increment(1);
                                        }
                                    }
                                }
                                let v = INGEST_SEQ.fetch_add(1, Ordering::Relaxed);
                                if (v & INGEST_SAMPLE_MASK) == 0 {
                                    counter!("ultra_records_ingested_total").increment(INGEST_SAMPLE_WEIGHT);
                                }
                                buf.advance(consumed);
                                continue;
                            }
                            Err(_) => { /* fall through to bincode path */ }
                        }
                    }
                }
            }
            match decode_record_from_slice(&buf[..], &mut scratch) {
                Ok(rec_and_len) => {
                    let (rec, consumed) = rec_and_len;
                    if let Some(js) = &json {
                        let evt = json_event_owned_from_record(&rec);
                        if !js.try_send(evt) {
                            counter!("ultra_json_dropped_total").increment(1);
                        }
                    }
                    let v = INGEST_SEQ.fetch_add(1, Ordering::Relaxed);
                    if (v & INGEST_SAMPLE_MASK) == 0 {
                        counter!("ultra_records_ingested_total").increment(INGEST_SAMPLE_WEIGHT);
                    }
                    #[cfg(feature = "kafka")]
                    if let Some(k) = &ks {
                        k.try_send(rec);
                    }
                    buf.advance(consumed);
                }
                Err(faststreams::StreamError::BadHeader) => {
                    counter!("ultra_decode_bad_header_total").increment(1);
                    counter!("ultra_resync_events_total").increment(1);
                    RESYNC_EVENTS_THIS_MINUTE.fetch_add(1, Ordering::Relaxed);
                    // scan for next magic to resync
                    let magic = faststreams::FRAME_MAGIC.to_be_bytes();
                    if let Some(idx) = memchr::memmem::find(&buf[..], &magic) {
                        if idx > 0 {
                            buf.advance(idx);
                        }
                    } else {
                        buf.clear();
                    }
                    break;
                }
                Err(faststreams::StreamError::De(_)) => {
                    counter!("ultra_decode_need_more_total").increment(1);
                    break;
                } // need more bytes
                Err(faststreams::StreamError::Io(_)) => {
                    counter!("ultra_decode_io_total").increment(1);
                    break;
                }
                Err(faststreams::StreamError::Ser(_)) => {
                    counter!("ultra_decode_ser_total").increment(1);
                    break;
                }
            }
        }
    }
    Ok(())
}
