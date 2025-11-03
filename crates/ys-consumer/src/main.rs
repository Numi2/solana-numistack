// crates/ys-consumer/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use crossbeam_channel::{bounded, Receiver};
use crossbeam_queue::ArrayQueue;
use faststreams::{encode_into_with, encode_record_ref_into_with, EncodeOptions, write_all_vectored, Record, TxUpdate, AccountUpdateRef, BlockMeta, RecordRef};
use futures::{SinkExt, StreamExt};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::{Duration, Instant};
use event_listener::{Event, Listener};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::{error, info};
use tokio::signal;
use tracing_subscriber::EnvFilter;
use yellowstone_grpc_client::GeyserGrpcClient;
use std::collections::HashMap;
use yellowstone_grpc_proto::prelude::{
    subscribe_update, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

fn uds_connect(path: &str) -> std::io::Result<UnixStream> {
    let s = UnixStream::connect(path)?;
    s.set_nonblocking(false)?;
    s.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
    Ok(s)
}

trait BatchSource {
    fn blocking_pop(&self, flush_interval: Duration) -> Option<Vec<u8>>;
    fn try_pop(&self) -> Option<Vec<u8>>;
}

impl BatchSource for Receiver<Vec<u8>> {
    #[inline]
    fn blocking_pop(&self, _flush_interval: Duration) -> Option<Vec<u8>> {
        // Use fully blocking recv for first item to avoid artificial latency.
        match self.recv() {
            Ok(v) => Some(v),
            Err(_) => None,
        }
    }
    #[inline]
    fn try_pop(&self) -> Option<Vec<u8>> { self.try_recv().ok() }
}

// Lightweight buffer pool to reuse Vec<u8> allocations in the fast path.
#[derive(Debug)]
struct BufPool {
    q: ArrayQueue<Vec<u8>>,
    default_capacity: usize,
}

impl BufPool {
    fn new(max_items: usize, default_capacity: usize) -> Self {
        Self { q: ArrayQueue::new(max_items), default_capacity }
    }
    fn get(&self) -> Vec<u8> {
        self.q.pop().unwrap_or_else(|| Vec::with_capacity(self.default_capacity))
    }
    fn put(&self, mut buf: Vec<u8>) {
        buf.clear();
        let _ = self.q.push(buf);
    }
}

// Event-driven SPSC queue wrapper: producers notify, consumer blocks on event.
#[derive(Clone)]
struct SpscSender {
    q: std::sync::Arc<ArrayQueue<Vec<u8>>>,
    ev: std::sync::Arc<Event>,
}

impl SpscSender {
    fn push(&self, v: Vec<u8>) -> Result<(), Vec<u8>> {
        match self.q.push(v) {
            Ok(()) => {
                self.ev.notify(1);
                Ok(())
            }
            Err(v) => Err(v),
        }
    }
    fn len(&self) -> usize { self.q.len() }
}

struct SpscQueue {
    q: std::sync::Arc<ArrayQueue<Vec<u8>>>,
    ev: std::sync::Arc<Event>,
}

impl BatchSource for SpscQueue {
    #[inline]
    fn blocking_pop(&self, flush_interval: Duration) -> Option<Vec<u8>> {
        // Double-checked wait with timeout to support flush cadence.
        loop {
            if let Some(v) = self.q.pop() { return Some(v); }
            let listener = self.ev.listen();
            if let Some(v) = self.q.pop() { return Some(v); }
            let _ = listener.wait_timeout(flush_interval);
        }
    }
    #[inline]
    fn try_pop(&self) -> Option<Vec<u8>> { self.q.pop() }
}

fn writer_loop_generic<S: BatchSource>(uds_path: String, src: S, shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>, batch_max: usize, batch_bytes_max: usize, flush_interval: Duration, buf_pool: std::sync::Arc<BufPool>) {
    let mut backoff = Duration::from_millis(50);
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
        match uds_connect(&uds_path) {
            Ok(mut stream) => {
                // Best-effort: enlarge send buffer
                let _ = socket2::SockRef::from(&stream).set_send_buffer_size(batch_bytes_max);
                let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_max);
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
                    // Blocking wait for first frame (no artificial timeout)
                    let first = match src.blocking_pop(flush_interval) { Some(v) => v, None => break };
                    let mut size = first.len();
                    batch.push(first);
                    let start = Instant::now();
                    while batch.len() < batch_max && size < batch_bytes_max {
                        if start.elapsed() >= flush_interval { break; }
                        if let Some(m) = src.try_pop() {
                            size += m.len();
                            if size > batch_bytes_max { break; }
                            batch.push(m);
                        } else {
                            break;
                        }
                    }
                    // pre-metrics for the batch
                    let batch_frames = batch.len();
                    let batch_bytes: usize = batch.iter().map(|b| b.len()).sum();
                    if let Err(e) = write_all_vectored(&mut stream, &batch) {
                        eprintln!("ys-consumer: write error: {e}");
                        break;
                    }
                    counter!("ys_consumer_write_batches_total").increment(1);
                    counter!("ys_consumer_write_bytes_total").increment(batch_bytes as u64);
                    histogram!("ys_consumer_write_batch_frames").record(batch_frames as f64);
                    for b in batch.drain(..) { buf_pool.put(b); }
                }
                thread::sleep(Duration::from_millis(100));
                backoff = Duration::from_millis(50);
            }
            Err(err) => {
                eprintln!("ys-consumer: connect {} failed: {err}", uds_path);
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let endpoint = std::env::var("YS_ENDPOINT").expect("YS_ENDPOINT");
    let x_token = std::env::var("YS_X_TOKEN").ok();
    let uds_path = std::env::var("ULTRA_UDS").unwrap_or_else(|_| "/var/run/ultra-geyser.sock".to_string());
    let metrics_addr = std::env::var("YS_METRICS_ADDR").ok();

    if let Some(addr) = metrics_addr.as_deref() {
        let _ = PrometheusBuilder::new().with_http_listener(addr.parse::<std::net::SocketAddr>().unwrap()).install();
    }

    let endpoint_static = Box::leak(endpoint.into_boxed_str());
    fn env_bool(name: &str, default: bool) -> bool {
        match std::env::var(name) {
            Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "y"),
            Err(_) => default,
        }
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name).ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(default)
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(default)
    }

    let sub_slots = env_bool("YS_SUB_SLOTS", true);
    let sub_accounts = env_bool("YS_SUB_ACCOUNTS", true);
    let sub_transactions = env_bool("YS_SUB_TRANSACTIONS", true);
    let sub_blocks = env_bool("YS_SUB_BLOCKS", true);
    let sub_blocks_meta = env_bool("YS_SUB_BLOCKS_META", true);

    let mut slots = HashMap::new();
    if sub_slots { slots.insert("".to_string(), SubscribeRequestFilterSlots::default()); }
    let mut accounts = HashMap::new();
    if sub_accounts { accounts.insert("".to_string(), SubscribeRequestFilterAccounts::default()); }
    let mut transactions = HashMap::new();
    if sub_transactions { transactions.insert("".to_string(), SubscribeRequestFilterTransactions::default()); }
    let mut blocks = HashMap::new();
    if sub_blocks { blocks.insert("".to_string(), SubscribeRequestFilterBlocks::default()); }
    let mut blocks_meta = HashMap::new();
    if sub_blocks_meta { blocks_meta.insert("".to_string(), SubscribeRequestFilterBlocksMeta::default()); }
    
    let req = SubscribeRequest {
        slots,
        accounts,
        transactions,
        blocks,
        blocks_meta,
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: Some(SubscribeRequestPing { id: 0 }),
        ..Default::default()
    };
    let backoff_min = Duration::from_millis(env_u64("YS_BACKOFF_MIN_MS", 250));
    let backoff_max = Duration::from_millis(env_u64("YS_BACKOFF_MAX_MS", 10_000));
    let idle_timeout = Duration::from_millis(env_u64("YS_IDLE_TIMEOUT_MS", 3_000));
    let mut reconnect_backoff = backoff_min;

    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let queue_cap = env_usize("YS_QUEUE_CAP", 65_536);
    let batch_max = env_usize("YS_BATCH_MAX", 1024);
    let batch_bytes_max = env_usize("YS_BATCH_BYTES_MAX", 2 * 1024 * 1024);
    // Ensure non-zero flush interval to avoid busy-wait in SPSC mode when queue is empty.
    let flush_interval_ms = env_u64("YS_FLUSH_INTERVAL_MS", 1);
    let flush_interval = Duration::from_millis(std::cmp::max(1, flush_interval_ms));
    let use_spsc = env_bool("YS_SPSC", false);

    // Buffer pool config
    let buf_pool_cap = env_usize("YS_BUF_POOL_CAP", queue_cap);
    let buf_default_cap = env_usize("YS_BUF_DEFAULT_CAP", 4096);
    let buf_pool = std::sync::Arc::new(BufPool::new(buf_pool_cap, buf_default_cap));

    // queue and writer
    let mut txq_opt: Option<crossbeam_channel::Sender<Vec<u8>>> = None;
    let mut spsc_send_opt: Option<SpscSender> = None;
    if use_spsc {
        let inner_q = std::sync::Arc::new(ArrayQueue::<Vec<u8>>::new(queue_cap));
        let ev = std::sync::Arc::new(Event::new());
        spsc_send_opt = Some(SpscSender { q: inner_q.clone(), ev: ev.clone() });
        let uds_path_clone = uds_path.clone();
        let sd = shutdown.clone();
        let src = SpscQueue { q: inner_q, ev };
        let pool = buf_pool.clone();
        thread::Builder::new().name("ys-writer".into()).spawn(move || {
            writer_loop_generic(uds_path_clone, src, &sd, batch_max, batch_bytes_max, flush_interval, pool);
        })?;
    } else {
        let (txq, rxq) = bounded::<Vec<u8>>(queue_cap);
        let uds_path_clone = uds_path.clone();
        let sd = shutdown.clone();
        let pool = buf_pool.clone();
        thread::Builder::new().name("ys-writer".into()).spawn(move || {
            writer_loop_generic(uds_path_clone, rxq, &sd, batch_max, batch_bytes_max, flush_interval, pool);
        })?;
        txq_opt = Some(txq);
    }

    // writer spawned above in both branches
    // info is logged after a successful subscribe in the loop below

    // metrics: queue depth sampler
    if metrics_addr.is_some() {
        if let Some(txq) = &txq_opt {
            let txq = txq.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(250));
                loop {
                    tick.tick().await;
                    gauge!("ys_consumer_queue_depth").set(txq.len() as f64);
                }
            });
        }
        if let Some(s) = &spsc_send_opt {
            let s = s.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(250));
                loop {
                    tick.tick().await;
                    gauge!("ys_consumer_queue_depth").set(s.len() as f64);
                }
            });
        }
    }

    // Simple time-based jitter without external RNG
    fn jitter(d: Duration) -> Duration {
        use std::time::{SystemTime, UNIX_EPOCH};
        let base_ms = d.as_millis() as u64;
        if base_ms <= 1 { return d; }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_else(|_| Duration::from_millis(0));
        let r = (now.as_nanos() as u64) ^ (base_ms.rotate_left(13));
        let half = base_ms / 2;
        let jitter_ms = half + (r % (half.max(1)));
        Duration::from_millis(jitter_ms.max(1))
    }

    let shutdown_sig = signal::ctrl_c();
    tokio::pin!(shutdown_sig);

    'outer: loop {
        // connect + subscribe (with shutdown support)
        let mut builder = GeyserGrpcClient::build_from_static(endpoint_static);
        if let Some(tok) = x_token.clone() {
            builder = match builder.x_token(Some(tok)) { Ok(b) => b, Err(e) => { error!("token set error: {e}"); tokio::time::sleep(reconnect_backoff).await; continue; } };
        }
        let mut client = match builder.connect().await { Ok(c) => c, Err(e) => { error!("connect error: {e}"); counter!("ys_connect_fail_total").increment(1); tokio::time::sleep(jitter(reconnect_backoff)).await; reconnect_backoff = (reconnect_backoff * 2).min(backoff_max); continue; } };
        let (mut tx, mut rx) = match client.subscribe().await { Ok(sr) => sr, Err(e) => { error!("subscribe error: {e}"); counter!("ys_subscribe_fail_total").increment(1); tokio::time::sleep(jitter(reconnect_backoff)).await; reconnect_backoff = (reconnect_backoff * 2).min(backoff_max); continue; } };
        if let Err(e) = tx.send(req.clone()).await { error!("send subscribe request failed: {e}"); counter!("ys_send_fail_total").increment(1); tokio::time::sleep(jitter(reconnect_backoff)).await; reconnect_backoff = (reconnect_backoff * 2).min(backoff_max); continue; }
        reconnect_backoff = backoff_min;
        info!("connected to Yellowstone; forwarding to {}", uds_path);

        loop {
            let next_fut = rx.next();
            let idle_timer = tokio::time::sleep(idle_timeout);
            tokio::pin!(idle_timer);
            tokio::select! {
                _ = &mut shutdown_sig => { info!("shutting down"); break 'outer; }
                _ = &mut idle_timer => { counter!("ys_idle_timeouts_total").increment(1); error!("idle timeout (no updates for {:?})", idle_timeout); break; }
                res = next_fut => {
                    match res {
                        Some(Ok(upd)) => {
                            match upd.update_oneof {
            Some(subscribe_update::UpdateOneof::Transaction(t)) => {
                let mut sig = [0u8; 64];
                // Extract signature from transaction if available
                if let Some(tx_data) = &t.transaction {
                    if tx_data.signature.len() == 64 {
                        sig.copy_from_slice(&tx_data.signature);
                    }
                }
                let rec = Record::Tx(TxUpdate {
                    slot: t.slot,
                    signature: sig,
                    err: t.transaction.as_ref().and_then(|tx| tx.meta.as_ref()).and_then(|m| m.err.as_ref().cloned()).map(|e| format!("{:?}", e)),
                    vote: false, // is_vote not available in new structure
                });
                let mut buf = buf_pool.get();
                let t0 = Instant::now();
                if encode_into_with(&rec, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                    histogram!("ys_consumer_encode_us", "kind" => "tx").record(t0.elapsed().as_secs_f64() * 1e6);
                    if let Some(txq) = &txq_opt {
                        match txq.try_send(buf) {
                            Ok(()) => {}
                            Err(e) => { counter!("ys_consumer_dropped_total").increment(1); let buf = e.into_inner(); buf_pool.put(buf); }
                        }
                    } else if let Some(s) = &spsc_send_opt {
                        match s.push(buf) { Ok(()) => {}, Err(buf) => { counter!("ys_consumer_dropped_total").increment(1); buf_pool.put(buf); } }
                    } else {
                        buf_pool.put(buf);
                    }
                } else {
                    buf_pool.put(buf);
                }
            }
            Some(subscribe_update::UpdateOneof::Account(a)) => {
                if let Some(acc) = &a.account {
                    // Decode base58 into fixed arrays
                    let pubkey = bs58::decode(&acc.pubkey)
                        .into_vec()
                        .ok()
                        .and_then(|v| v.try_into().ok())
                        .unwrap_or([0; 32]);
                    let owner = bs58::decode(&acc.owner)
                        .into_vec()
                        .ok()
                        .and_then(|v| v.try_into().ok())
                        .unwrap_or([0; 32]);
                    let aref = RecordRef::Account(AccountUpdateRef {
                        slot: a.slot,
                        is_startup: a.is_startup,
                        pubkey,
                        lamports: acc.lamports,
                        owner,
                        executable: acc.executable,
                        rent_epoch: acc.rent_epoch,
                        data: &acc.data,
                    });
                    let mut buf = buf_pool.get();
                    let t0 = Instant::now();
                    if encode_record_ref_into_with(&aref, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                        histogram!("ys_consumer_encode_us", "kind" => "account").record(t0.elapsed().as_secs_f64() * 1e6);
                        if let Some(txq) = &txq_opt {
                            match txq.try_send(buf) { Ok(()) => {}, Err(e) => { counter!("ys_consumer_dropped_total").increment(1); let buf = e.into_inner(); buf_pool.put(buf); } }
                        } else if let Some(s) = &spsc_send_opt {
                            match s.push(buf) { Ok(()) => {}, Err(buf) => { counter!("ys_consumer_dropped_total").increment(1); buf_pool.put(buf); } }
                        } else {
                            buf_pool.put(buf);
                        }
                    } else {
                        buf_pool.put(buf);
                    }
                }
            }
            Some(subscribe_update::UpdateOneof::Block(b)) => {
                let bh = if !b.blockhash.is_empty() {
                    bs58::decode(&b.blockhash).into_vec().ok().and_then(|v| v.try_into().ok())
                } else { None };
                // leader field not available in new proto version, set to None
                let ld = None;
                let block_time = b.block_time.as_ref().and_then(|ts| if ts.timestamp != 0 { Some(ts.timestamp) } else { None });
                let rec = Record::Block(BlockMeta {
                    slot: b.slot,
                    blockhash: bh,
                    parent_slot: if b.parent_slot > 0 { b.parent_slot - 1 } else { 0 },
                    rewards_len: b.rewards.as_ref().map(|r| r.rewards.len()).unwrap_or(0) as u32,
                    block_time_unix: block_time,
                    leader: ld,
                });
                let mut buf = buf_pool.get();
                let t0 = Instant::now();
                if encode_into_with(&rec, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                    histogram!("ys_consumer_encode_us", "kind" => "block").record(t0.elapsed().as_secs_f64() * 1e6);
                    if let Some(txq) = &txq_opt {
                        match txq.try_send(buf) { Ok(()) => {}, Err(e) => { counter!("ys_consumer_dropped_total").increment(1); let buf = e.into_inner(); buf_pool.put(buf); } }
                    } else if let Some(s) = &spsc_send_opt {
                        match s.push(buf) { Ok(()) => {}, Err(buf) => { counter!("ys_consumer_dropped_total").increment(1); buf_pool.put(buf); } }
                    } else {
                        buf_pool.put(buf);
                    }
                } else {
                    buf_pool.put(buf);
                }
            }
            Some(subscribe_update::UpdateOneof::Slot(s)) => {
                let rec = Record::Slot { slot: s.slot, parent: s.parent, status: s.status as u8 };
                let mut buf = buf_pool.get();
                let t0 = Instant::now();
                if encode_into_with(&rec, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                    histogram!("ys_consumer_encode_us", "kind" => "slot").record(t0.elapsed().as_secs_f64() * 1e6);
                    if let Some(txq) = &txq_opt {
                        match txq.try_send(buf) { Ok(()) => {}, Err(e) => { counter!("ys_consumer_dropped_total").increment(1); let buf = e.into_inner(); buf_pool.put(buf); } }
                    } else if let Some(s) = &spsc_send_opt {
                        match s.push(buf) { Ok(()) => {}, Err(buf) => { counter!("ys_consumer_dropped_total").increment(1); buf_pool.put(buf); } }
                    } else {
                        buf_pool.put(buf);
                    }
                } else {
                    buf_pool.put(buf);
                }
            }
            _ => {}
                            }
                        }
                        Some(Err(e)) => { error!("stream error: {e}"); break; }
                        None => { error!("stream closed by server"); break; }
                    }
                }
            }
        }
        counter!("ys_reconnects_total").increment(1);
        tokio::time::sleep(jitter(reconnect_backoff)).await;
        reconnect_backoff = (reconnect_backoff * 2).min(backoff_max);
    }
    Ok(())
}