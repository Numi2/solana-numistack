// crates/ultra-aggregator/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use faststreams::{Record, decode_record_from_slice};
use bytes::{Buf, BytesMut};
use metrics::{counter};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use socket2::SockRef;

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct KafkaCfg {
    brokers: String,
    topic_accounts: String,
    topic_txs: String,
    topic_blocks: String,
    topic_slots: String,
}

fn json_view(rec: &Record) -> serde_json::Value {
    match rec {
        Record::Account(a) => serde_json::json!({
            "type": "account",
            "slot": a.slot,
            "is_startup": a.is_startup,
            "pubkey": bs58::encode(&a.pubkey).into_string(),
            "lamports": a.lamports,
            "owner": bs58::encode(&a.owner).into_string(),
            "executable": a.executable,
            "rent_epoch": a.rent_epoch,
            "data_len": a.data.len(),
        }),
        Record::Tx(t) => serde_json::json!({
            "type": "tx",
            "slot": t.slot,
            "signature": bs58::encode(&t.signature).into_string(),
            "err": t.err,
            "vote": t.vote,
        }),
        Record::Block(b) => serde_json::json!({
            "type": "block",
            "slot": b.slot,
            "blockhash": b.blockhash.map(|h| bs58::encode(&h).into_string()),
            "parent_slot": b.parent_slot,
            "rewards_len": b.rewards_len,
            "block_time_unix": b.block_time_unix,
            "leader": b.leader.map(|l| bs58::encode(&l).into_string()),
        }),
        Record::Slot { slot, parent, status } => serde_json::json!({
            "type": "slot",
            "slot": slot,
            "parent": parent,
            "status": status,
        }),
        Record::EndOfStartup => serde_json::json!({"type": "end_of_startup"}),
    }
}
#[derive(Debug, Clone, serde::Deserialize)]
struct Cfg {
    uds_path: String,
    stdout_json: bool,
    metrics_addr: Option<String>,
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
        use rdkafka::ClientConfig;
        use rdkafka::producer::{FutureProducer, FutureRecord};
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Record>(65_536);
        tokio::spawn(async move {
            let prod: FutureProducer = match ClientConfig::new()
                .set("bootstrap.servers", &cfg.brokers)
                .set("queue.buffering.max.messages", "2000000")
                .set("queue.buffering.max.kbytes", "1048576")
                .set("message.timeout.ms", "5000")
                .create()
            {
                Ok(p) => p,
                Err(e) => { eprintln!("kafka producer init failed: {e}"); return; }
            };
            while let Some(rec) = rx.recv().await {
                let (topic, key) = match &rec {
                    Record::Account(a) => (&cfg.topic_accounts, bs58::encode(&a.pubkey).into_string()),
                    Record::Tx(t) => (&cfg.topic_txs, bs58::encode(&t.signature).into_string()),
                    Record::Block(b) => {
                        let k = b.blockhash.map(|h| bs58::encode(h).into_string()).unwrap_or_default();
                        (&cfg.topic_blocks, k)
                    }
                    Record::Slot { slot, .. } => (&cfg.topic_slots, slot.to_string()),
                    Record::EndOfStartup => (&cfg.topic_slots, "eos".to_string()),
                };
                if let Ok(payload) = bincode::serialize(&rec) {
                    let _ = prod
                        .send(FutureRecord::to(topic).key(&key).payload(&payload), std::time::Duration::from_secs(1))
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg_path = std::env::args().nth(1).unwrap_or_else(|| "configs/aggregator.json".to_string());
    let cfg: Cfg = {
        let raw = std::fs::read_to_string(&cfg_path)?;
        serde_json::from_str(&raw)?
    };

    if let Some(addr) = &cfg.metrics_addr {
        let _ = PrometheusBuilder::new().with_http_listener(addr.parse::<std::net::SocketAddr>().unwrap()).install();
    }

    if Path::new(&cfg.uds_path).exists() {
        let _ = std::fs::remove_file(&cfg.uds_path);
    }
    let listener = UnixListener::bind(&cfg.uds_path)?;
    // best-effort: set perms to 0660 for controlled access
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(_meta) = std::fs::metadata(&cfg.uds_path) {
            let _ = std::fs::set_permissions(&cfg.uds_path, std::fs::Permissions::from_mode(0o660));
        }
    }
    info!("listening UDS {}", cfg.uds_path);

    #[cfg(feature = "kafka")]
    let kafka_sink = if let Some(k) = cfg.kafka.clone() {
        Some(KafkaSink::new(k)?)
    } else { None };

    let shutdown = signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutting down");
                break;
            }
            Ok((sock, _)) = listener.accept() => {
                // Best-effort: enlarge recv buffer on accepted socket
                #[cfg(unix)] {
                    let _ = SockRef::from(&sock).set_recv_buffer_size(4 * 1024 * 1024);
                }
                let cfg_clone = cfg.clone();
                #[cfg(feature = "kafka")] {
                    let ks = kafka_sink.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(sock, cfg_clone, ks).await {
                            error!("client error: {e:?}");
                        }
                    });
                }
                #[cfg(not(feature = "kafka"))] {
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(sock, cfg_clone).await {
                            error!("client error: {e:?}");
                        }
                    });
                }
            }
        }
    }
    Ok(())
}

async fn handle_client(mut sock: UnixStream, cfg: Cfg, #[cfg(feature = "kafka")] ks: Option<KafkaSink>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(1 << 20);
    let mut scratch: Vec<u8> = Vec::with_capacity(8 * 1024);
    loop {
        // read available bytes directly into the growable buffer
        let n = sock.read_buf(&mut buf).await?;
        if n == 0 { break; }

        // Try to peel records out
        loop {
            match decode_record_from_slice(&buf[..], &mut scratch) {
                Ok(rec_and_len) => {
                    let (rec, consumed) = rec_and_len;
                    if cfg.stdout_json { println!("{}", serde_json::to_string(&json_view(&rec))?); }
                    counter!("ultra_records_ingested_total").increment(1);
                    #[cfg(feature = "kafka")]
                    if let Some(k) = &ks { k.try_send(rec); }
                    buf.advance(consumed);
                }
                Err(faststreams::StreamError::BadHeader) => {
                    // scan for next magic to resync
                    let magic = faststreams::FRAME_MAGIC.to_be_bytes();
                    if let Some(idx) = buf[..].windows(4).position(|w| w == magic) {
                        if idx > 0 { buf.advance(idx); }
                    } else {
                        buf.clear();
                    }
                    break;
                }
                Err(faststreams::StreamError::De(_)) => break, // need more bytes
                Err(faststreams::StreamError::Io(_)) => break,
                Err(faststreams::StreamError::Ser(_)) => break,
            }
        }
    }
    Ok(())
}