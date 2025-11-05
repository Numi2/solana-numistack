// Numan Thabit 2025
// crates/ys-consumer/src/main.rs
#![forbid(unsafe_code)]
mod shm_ring;
use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TrySendError};
use crossbeam_queue::ArrayQueue;
use event_listener::{Event, Listener};
use faststreams::{
    decode_record_from_slice, encode_into_with, encode_record_ref_into_with, write_all_vectored,
    AccountUpdateRef, BlockMeta, EncodeOptions, Record, RecordRef, TxUpdate,
};
use futures::{SinkExt, StreamExt};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::collections::{HashMap, VecDeque};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use yellowstone_grpc_client::GeyserGrpcClient;
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
    fn approx_len(&self) -> usize;
}

impl BatchSource for Receiver<Vec<u8>> {
    #[inline]
    fn blocking_pop(&self, flush_interval: Duration) -> Option<Vec<u8>> {
        // Block with timeout to support adaptive flush cadence; keep waiting on timeouts.
        loop {
            match self.recv_timeout(flush_interval) {
                Ok(v) => return Some(v),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return None,
            }
        }
    }
    #[inline]
    fn try_pop(&self) -> Option<Vec<u8>> {
        self.try_recv().ok()
    }
    #[inline]
    fn approx_len(&self) -> usize {
        self.len()
    }
}

// Lightweight buffer pool to reuse Vec<u8> allocations in the fast path.
#[derive(Debug)]
struct BufPool {
    q: ArrayQueue<Vec<u8>>,
    default_capacity: usize,
}

impl BufPool {
    fn new(max_items: usize, default_capacity: usize) -> Self {
        Self {
            q: ArrayQueue::new(max_items),
            default_capacity,
        }
    }
    fn get(&self) -> Vec<u8> {
        self.q
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.default_capacity))
    }
    fn put(&self, mut buf: Vec<u8>) {
        buf.clear();
        let _ = self.q.push(buf);
    }
}

#[derive(Debug)]
struct AddressCache {
    map: HashMap<Box<str>, [u8; 32]>,
    order: VecDeque<Box<str>>,
    capacity: usize,
}

impl AddressCache {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::with_capacity(capacity.min(1024)),
            capacity,
        }
    }

    #[inline]
    fn decode(&mut self, key_bytes: &[u8]) -> [u8; 32] {
        if key_bytes.len() == 32 {
            let mut decoded = [0u8; 32];
            decoded.copy_from_slice(key_bytes);
            return decoded;
        }

        let key = match std::str::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(_) => return [0u8; 32],
        };

        self.decode_str(key)
    }

    #[inline]
    fn decode_str(&mut self, key: &str) -> [u8; 32] {
        if self.capacity == 0 {
            return decode_base58(key);
        }
        if let Some(existing) = self.map.get(key) {
            return *existing;
        }
        let decoded = decode_base58(key);
        if self.map.len() == self.capacity {
            if let Some(old_key) = self.order.pop_front() {
                self.map.remove(old_key.as_ref());
            }
        }
        let key_box: Box<str> = key.into();
        self.order.push_back(key_box.clone());
        self.map.insert(key_box, decoded);
        decoded
    }
}

#[inline]
fn decode_base58(input: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    match bs58::decode(input).onto(&mut out) {
        Ok(len) if len == out.len() => out,
        _ => [0u8; 32],
    }
}

const BACKPRESSURE_SPIN_LIMIT: usize = 32;
const BACKPRESSURE_SLEEP_MICROS: u64 = 50;

fn enqueue_with_backpressure(
    tx: &Sender<Vec<u8>>,
    mut buf: Vec<u8>,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    pool: &std::sync::Arc<BufPool>,
) -> bool {
    for _ in 0..BACKPRESSURE_SPIN_LIMIT {
        match tx.try_send(buf) {
            Ok(()) => return true,
            Err(TrySendError::Full(b)) => {
                if shutdown.load(Ordering::Relaxed) {
                    pool.put(b);
                    return false;
                }
                std::thread::yield_now();
                buf = b;
            }
            Err(TrySendError::Disconnected(b)) => {
                pool.put(b);
                return false;
            }
        }
    }

    loop {
        match tx.try_send(buf) {
            Ok(()) => return true,
            Err(TrySendError::Full(b)) => {
                if shutdown.load(Ordering::Relaxed) {
                    pool.put(b);
                    return false;
                }
                std::thread::sleep(Duration::from_micros(BACKPRESSURE_SLEEP_MICROS));
                buf = b;
            }
            Err(TrySendError::Disconnected(b)) => {
                pool.put(b);
                return false;
            }
        }
    }
}

fn forward_frame(
    buf: Vec<u8>,
    txq_opt: &Option<Sender<Vec<u8>>>,
    spsc_opt: &Option<SpscSender>,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    pool: &std::sync::Arc<BufPool>,
) -> bool {
    if let Some(tx) = txq_opt {
        return enqueue_with_backpressure(tx, buf, shutdown, pool);
    }
    if let Some(sender) = spsc_opt {
        match sender.push_with_backpressure(buf, shutdown) {
            Ok(()) => true,
            Err(b) => {
                pool.put(b);
                false
            }
        }
    } else {
        pool.put(buf);
        false
    }
}

static FRAMES_PROCESSED: AtomicU64 = AtomicU64::new(0);
static FRAMES_DROPPED_OVERSIZE: AtomicU64 = AtomicU64::new(0);
static FRAMES_DLQ: AtomicU64 = AtomicU64::new(0);
static SAMPLE_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct DlqSink {
    inner: std::sync::Arc<DlqInner>,
}

struct DlqInner {
    tx: Sender<DlqRecord>,
}

struct DlqRecord {
    data: Vec<u8>,
    reason: &'static str,
    kind: &'static str,
    frame_len: usize,
    frame_limit: usize,
    timestamp: SystemTime,
}

impl DlqSink {
    fn new(dir: PathBuf, capacity: usize) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let (tx, rx) = crossbeam_channel::bounded::<DlqRecord>(capacity);
        let worker_dir = dir.clone();
        thread::Builder::new()
            .name("ys-dlq".into())
            .spawn(move || Self::run(worker_dir, rx))
            .map_err(std::io::Error::other)?;
        Ok(Self {
            inner: std::sync::Arc::new(DlqInner { tx }),
        })
    }

    fn enqueue(&self, record: DlqRecord) -> Result<(), DlqRecord> {
        match self.inner.tx.try_send(record) {
            Ok(()) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(record)) => Err(record),
            Err(crossbeam_channel::TrySendError::Disconnected(record)) => Err(record),
        }
    }

    fn run(dir: PathBuf, rx: Receiver<DlqRecord>) {
        let mut seq: u64 = 0;
        while let Ok(record) = rx.recv() {
            if let Err(e) = Self::write_record(dir.as_path(), &record, seq) {
                counter!("ys_consumer_dlq_write_error_total").increment(1);
                warn!(
                    target = "ys.consumer",
                    "dlq write failed: {} (reason={}, kind={})", e, record.reason, record.kind
                );
            }
            seq = seq.wrapping_add(1);
        }
    }

    fn write_record(dir: &Path, record: &DlqRecord, seq: u64) -> std::io::Result<()> {
        let elapsed = record
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0));
        let base = format!(
            "dlq-{}-{:09}-{:06}",
            elapsed.as_secs(),
            elapsed.subsec_nanos(),
            seq
        );
        let data_path = dir.join(format!("{}.fstr", base));
        let meta_path = dir.join(format!("{}.meta", base));
        std::fs::write(&data_path, &record.data)?;
        let meta = format!(
            "ts_unix={}\nreason={}\nkind={}\nframe_len={}\nframe_limit={}\n",
            elapsed.as_secs_f64(),
            record.reason,
            record.kind,
            record.frame_len,
            record.frame_limit
        );
        std::fs::write(meta_path, meta)?;
        Ok(())
    }
}

fn update_ratios() {
    let processed = FRAMES_PROCESSED.load(Ordering::Relaxed);
    let dropped = FRAMES_DROPPED_OVERSIZE.load(Ordering::Relaxed);
    let total = processed + dropped;
    if total == 0 {
        gauge!("ys_consumer_frame_oversize_drop_ratio").set(0.0);
        gauge!("ys_consumer_dlq_ratio").set(0.0);
        return;
    }
    gauge!("ys_consumer_frame_oversize_drop_ratio").set(dropped as f64 / total as f64);
    let dlq = FRAMES_DLQ.load(Ordering::Relaxed);
    gauge!("ys_consumer_dlq_ratio").set(dlq as f64 / total as f64);
}

fn frame_kind_from_bytes(frame: &[u8], scratch: &mut Vec<u8>) -> &'static str {
    match decode_record_from_slice(frame, scratch) {
        Ok((record, _)) => {
            scratch.clear();
            match record {
                Record::Account(_) => "account",
                Record::Tx(_) => "tx",
                Record::Block(_) => "block",
                Record::Slot { .. } => "slot",
                Record::EndOfStartup => "end_of_startup",
            }
        }
        Err(_) => {
            scratch.clear();
            "unknown"
        }
    }
}

fn drop_to_dlq(
    frame: Vec<u8>,
    reason: &'static str,
    frame_limit: usize,
    buf_pool: &std::sync::Arc<BufPool>,
    scratch: &mut Vec<u8>,
    dlq: Option<&DlqSink>,
) {
    counter!("ys_consumer_frame_oversize_drop_count").increment(1);
    FRAMES_DROPPED_OVERSIZE.fetch_add(1, Ordering::Relaxed);
    let frame_len = frame.len();
    let kind = frame_kind_from_bytes(&frame, scratch);
    // Oversize histograms to help determine sensible caps and batch windowing
    histogram!("ys_consumer_oversize_frame_len", "kind" => kind).record(frame_len as f64);
    if frame_len > frame_limit {
        histogram!(
            "ys_consumer_oversize_frame_excess_bytes",
            "kind" => kind
        )
        .record((frame_len - frame_limit) as f64);
    }
    warn!(
        target = "ys.consumer",
        reason = reason,
        frame_kind = kind,
        frame_len = frame_len,
        frame_limit = frame_limit,
        "dropping frame"
    );
    let timestamp = SystemTime::now();
    let mut frame_opt = Some(frame);
    if let Some(sink) = dlq {
        let record = DlqRecord {
            data: frame_opt.take().unwrap(),
            reason,
            kind,
            frame_len,
            frame_limit,
            timestamp,
        };
        match sink.enqueue(record) {
            Ok(()) => {
                counter!("ys_consumer_dlq_enqueue_count").increment(1);
                FRAMES_DLQ.fetch_add(1, Ordering::Relaxed);
                update_ratios();
                return;
            }
            Err(record) => {
                counter!("ys_consumer_dlq_enqueue_fail_total").increment(1);
                frame_opt = Some(record.data);
            }
        }
    }
    if let Some(frame) = frame_opt {
        buf_pool.put(frame);
    }
    update_ratios();
}

// Event-driven SPSC queue wrapper: producers notify, consumer blocks on event.
#[derive(Clone)]
struct SpscSender {
    q: std::sync::Arc<ArrayQueue<Vec<u8>>>,
    ev: std::sync::Arc<Event>,
}

impl SpscSender {
    fn push_with_backpressure(
        &self,
        mut v: Vec<u8>,
        shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), Vec<u8>> {
        for _ in 0..BACKPRESSURE_SPIN_LIMIT {
            match self.q.push(v) {
                Ok(()) => {
                    self.ev.notify(1);
                    return Ok(());
                }
                Err(buf) => {
                    if shutdown.load(Ordering::Relaxed) {
                        return Err(buf);
                    }
                    std::thread::yield_now();
                    v = buf;
                }
            }
        }

        while !shutdown.load(Ordering::Relaxed) {
            match self.q.push(v) {
                Ok(()) => {
                    self.ev.notify(1);
                    return Ok(());
                }
                Err(buf) => {
                    std::thread::sleep(Duration::from_micros(BACKPRESSURE_SLEEP_MICROS));
                    v = buf;
                }
            }
        }

        Err(v)
    }
    fn len(&self) -> usize {
        self.q.len()
    }
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
            if let Some(v) = self.q.pop() {
                return Some(v);
            }
            let listener = self.ev.listen();
            if let Some(v) = self.q.pop() {
                return Some(v);
            }
            let _ = listener.wait_timeout(flush_interval);
        }
    }
    #[inline]
    fn try_pop(&self) -> Option<Vec<u8>> {
        self.q.pop()
    }
    #[inline]
    fn approx_len(&self) -> usize {
        self.q.len()
    }
}

#[derive(Clone, Copy)]
struct WriterLimits {
    batch_max: usize,
    batch_bytes_max: usize,
    frame_bytes_max: usize,
}

#[inline]
fn scale_duration(base: Duration, denom: u32) -> Duration {
    if denom <= 1 {
        return base;
    }
    let micros: u128 = base.as_micros();
    let scaled = (micros / denom as u128).max(1);
    Duration::from_micros(scaled as u64)
}

#[inline]
fn effective_flush_interval(base: Duration, prev_len: usize, curr_len: usize) -> Duration {
    let mut denom: u32 = 1;
    if curr_len > prev_len {
        let growth = curr_len - prev_len;
        if growth >= 32_768 {
            denom = denom.max(8);
        } else if growth >= 16_384 {
            denom = denom.max(4);
        } else if growth >= 4_096 {
            denom = denom.max(2);
        }
    }
    if curr_len >= 32_768 {
        denom = denom.max(8);
    } else if curr_len >= 16_384 {
        denom = denom.max(4);
    } else if curr_len >= 8_192 {
        denom = denom.max(2);
    }
    // Keep at least 1ms to avoid busy loops while still bounding tail
    let eff = scale_duration(base, denom);
    if eff < Duration::from_millis(1) {
        Duration::from_millis(1)
    } else {
        eff
    }
}

fn writer_loop_generic<S: BatchSource>(
    uds_path: String,
    src: S,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    limits: WriterLimits,
    flush_interval: Duration,
    buf_pool: std::sync::Arc<BufPool>,
    dlq: Option<DlqSink>,
) {
    let WriterLimits {
        batch_max,
        batch_bytes_max,
        frame_bytes_max,
    } = limits;
    let mut backoff = Duration::from_millis(50);
    let mut pending_frame: Option<Vec<u8>> = None;
    let mut scratch: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut prev_queue_len: usize = 0;
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        match uds_connect(&uds_path) {
            Ok(mut stream) => {
                let _ = socket2::SockRef::from(&stream).set_send_buffer_size(batch_bytes_max);
                let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_max);
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    // Adapt flush interval when queue depth is rising to bound tail
                    let curr_len = src.approx_len();
                    let eff_flush =
                        effective_flush_interval(flush_interval, prev_queue_len, curr_len);
                    prev_queue_len = curr_len;

                    let (first, retried_singleton) = match pending_frame.take() {
                        Some(frame) => (frame, true),
                        None => match src.blocking_pop(eff_flush) {
                            Some(frame) => (frame, false),
                            None => break,
                        },
                    };
                    if first.len() > frame_bytes_max {
                        drop_to_dlq(
                            first,
                            "frame_oversize",
                            frame_bytes_max,
                            &buf_pool,
                            &mut scratch,
                            dlq.as_ref(),
                        );
                        continue;
                    }
                    batch.push(first);
                    let mut batch_bytes = batch.last().map(|b| b.len()).unwrap_or(0);
                    let salvaged_singleton = retried_singleton;
                    let start = Instant::now();
                    while batch.len() < batch_max {
                        if start.elapsed() >= eff_flush {
                            break;
                        }
                        match src.try_pop() {
                            Some(next) => {
                                if next.len() > frame_bytes_max {
                                    drop_to_dlq(
                                        next,
                                        "frame_oversize",
                                        frame_bytes_max,
                                        &buf_pool,
                                        &mut scratch,
                                        dlq.as_ref(),
                                    );
                                    continue;
                                }
                                if batch_bytes + next.len() > batch_bytes_max {
                                    pending_frame = Some(next);
                                    counter!("ys_consumer_oversized_batch_split_count")
                                        .increment(1);
                                    break;
                                }
                                batch_bytes += next.len();
                                batch.push(next);
                            }
                            None => break,
                        }
                    }

                    let batch_frames = batch.len();
                    if batch_frames == 0 {
                        continue;
                    }
                    let batch_bytes_total: usize = batch.iter().map(|b| b.len()).sum();
                    debug_assert!(
                        batch_bytes_total <= batch_bytes_max,
                        "batch_bytes_total={} > max={}",
                        batch_bytes_total,
                        batch_bytes_max
                    );
                    match write_all_vectored(&mut stream, &batch) {
                        Ok(()) => {
                            counter!("ys_consumer_write_batches_total").increment(1);
                            counter!("ys_consumer_write_bytes_total")
                                .increment(batch_bytes_total as u64);
                            histogram!("ys_consumer_write_batch_frames")
                                .record(batch_frames as f64);
                            histogram!("ys_consumer_write_batch_bytes")
                                .record(batch_bytes_total as f64);
                            FRAMES_PROCESSED.fetch_add(batch_frames as u64, Ordering::Relaxed);
                            update_ratios();
                            if salvaged_singleton {
                                counter!("ys_consumer_singleton_salvaged_count").increment(1);
                            }
                        }
                        Err(e) => {
                            error!(target = "ys.consumer", "write error: {}", e);
                            for frame in batch.drain(..) {
                                buf_pool.put(frame);
                            }
                            break;
                        }
                    }
                    for frame in batch.drain(..) {
                        buf_pool.put(frame);
                    }
                }
                thread::sleep(Duration::from_millis(100));
                backoff = Duration::from_millis(50);
            }
            Err(err) => {
                error!(
                    target = "ys.consumer",
                    "connect {} failed: {}", uds_path, err
                );
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }
        }
    }
    if let Some(frame) = pending_frame.take() {
        buf_pool.put(frame);
    }
}

fn writer_loop_shm<S: BatchSource>(
    mut ring: shm_ring::ShmRingWriter,
    src: S,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    limits: WriterLimits,
    flush_interval: Duration,
    buf_pool: std::sync::Arc<BufPool>,
    dlq: Option<DlqSink>,
) {
    let WriterLimits {
        batch_max,
        batch_bytes_max,
        frame_bytes_max,
    } = limits;
    let mut pending_frame: Option<Vec<u8>> = None;
    let mut scratch: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut prev_queue_len: usize = 0;
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_max);
        // Similar batching to UDS to amortize per-iteration overhead
        let curr_len = src.approx_len();
        let eff_flush = effective_flush_interval(flush_interval, prev_queue_len, curr_len);
        prev_queue_len = curr_len;

        let (first, retried_singleton) = match pending_frame.take() {
            Some(frame) => (frame, true),
            None => match src.blocking_pop(eff_flush) {
                Some(frame) => (frame, false),
                None => break,
            },
        };
        if first.len() > frame_bytes_max {
            drop_to_dlq(
                first,
                "frame_oversize",
                frame_bytes_max,
                &buf_pool,
                &mut scratch,
                dlq.as_ref(),
            );
            continue;
        }
        batch.push(first);
        let mut batch_bytes = batch.last().map(|b| b.len()).unwrap_or(0);
        let salvaged_singleton = retried_singleton;
        let start = Instant::now();
        while batch.len() < batch_max {
            if start.elapsed() >= eff_flush {
                break;
            }
            match src.try_pop() {
                Some(next) => {
                    if next.len() > frame_bytes_max {
                        drop_to_dlq(
                            next,
                            "frame_oversize",
                            frame_bytes_max,
                            &buf_pool,
                            &mut scratch,
                            dlq.as_ref(),
                        );
                        continue;
                    }
                    if batch_bytes + next.len() > batch_bytes_max {
                        pending_frame = Some(next);
                        counter!("ys_consumer_oversized_batch_split_count").increment(1);
                        break;
                    }
                    batch_bytes += next.len();
                    batch.push(next);
                }
                None => break,
            }
        }
        if batch.is_empty() {
            continue;
        }
        let mut wrote = 0usize;
        for frame in &batch {
            if ring.try_push(frame) {
                wrote += 1;
            } else {
                counter!("ys_consumer_shm_backpressure_drops_total").increment(1);
                // On failure, stop and requeue remaining into pool to avoid reordering
                break;
            }
        }
        if wrote == batch.len() {
            counter!("ys_consumer_write_batches_total").increment(1);
            counter!("ys_consumer_write_bytes_total").increment(batch_bytes as u64);
            histogram!("ys_consumer_write_batch_frames").record(batch.len() as f64);
            histogram!("ys_consumer_write_batch_bytes").record(batch_bytes as f64);
            FRAMES_PROCESSED.fetch_add(batch.len() as u64, Ordering::Relaxed);
            update_ratios();
            if salvaged_singleton {
                counter!("ys_consumer_singleton_salvaged_count").increment(1);
            }
            for frame in batch.drain(..) {
                buf_pool.put(frame);
            }
        } else {
            // Put back un-sent frames (including the first failure and any remaining)
            for frame in batch.drain(..) {
                buf_pool.put(frame);
            }
            // Back off very briefly to allow reader to advance
            std::thread::sleep(Duration::from_micros(100));
        }
    }
    if let Some(frame) = pending_frame.take() {
        buf_pool.put(frame);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let endpoint = std::env::var("YS_ENDPOINT").expect("YS_ENDPOINT");
    let x_token = std::env::var("YS_X_TOKEN").ok();
    let uds_path =
        std::env::var("ULTRA_UDS").unwrap_or_else(|_| "/var/run/ultra-geyser.sock".to_string());
    let metrics_addr = std::env::var("YS_METRICS_ADDR").ok();

    if let Some(addr) = metrics_addr.as_deref() {
        let _ = PrometheusBuilder::new()
            .with_http_listener(addr.parse::<std::net::SocketAddr>().unwrap())
            .install();
    }

    let endpoint_static = Box::leak(endpoint.into_boxed_str());
    fn env_bool(name: &str, default: bool) -> bool {
        match std::env::var(name) {
            Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "y"),
            Err(_) => default,
        }
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(default)
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default)
    }

    let sub_slots = env_bool("YS_SUB_SLOTS", true);
    let sub_accounts = env_bool("YS_SUB_ACCOUNTS", true);
    let sub_transactions = env_bool("YS_SUB_TRANSACTIONS", true);
    let sub_blocks = env_bool("YS_SUB_BLOCKS", true);
    let sub_blocks_meta = env_bool("YS_SUB_BLOCKS_META", true);

    let mut slots = HashMap::new();
    if sub_slots {
        slots.insert("".to_string(), SubscribeRequestFilterSlots::default());
    }
    let mut accounts = HashMap::new();
    if sub_accounts {
        accounts.insert("".to_string(), SubscribeRequestFilterAccounts::default());
    }
    let mut transactions = HashMap::new();
    if sub_transactions {
        transactions.insert(
            "".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
    }
    let mut blocks = HashMap::new();
    if sub_blocks {
        blocks.insert("".to_string(), SubscribeRequestFilterBlocks::default());
    }
    let mut blocks_meta = HashMap::new();
    if sub_blocks_meta {
        blocks_meta.insert("".to_string(), SubscribeRequestFilterBlocksMeta::default());
    }

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
    let frame_bytes_max = env_usize("YS_FRAME_BYTES_MAX", batch_bytes_max);
    let writer_limits = WriterLimits {
        batch_max,
        batch_bytes_max,
        frame_bytes_max,
    };
    // Ensure non-zero flush interval to avoid busy-wait in SPSC mode when queue is empty.
    let flush_interval_ms = env_u64("YS_FLUSH_INTERVAL_MS", 1);
    let flush_interval = Duration::from_millis(std::cmp::max(1, flush_interval_ms));
    let use_spsc = env_bool("YS_SPSC", false);
    let output_mode = std::env::var("YS_OUTPUT").unwrap_or_else(|_| "uds".to_string());
    let use_shm = matches!(output_mode.as_str(), "shm" | "ring" | "shmem");
    let shm_path_default = if cfg!(target_os = "linux") {
        "/dev/shm/ultra-faststreams.ring"
    } else {
        "/tmp/ultra-faststreams.ring"
    };
    let shm_path = std::env::var("YS_SHM_PATH").unwrap_or_else(|_| shm_path_default.to_string());
    let shm_cap_bytes = env_usize("YS_SHM_CAP_BYTES", 64 * 1024 * 1024);

    // Buffer pool config
    let buf_pool_cap = env_usize("YS_BUF_POOL_CAP", queue_cap);
    let buf_default_cap = env_usize("YS_BUF_DEFAULT_CAP", 4096);
    let buf_pool = std::sync::Arc::new(BufPool::new(buf_pool_cap, buf_default_cap));

    let pubkey_cache_cap = env_usize("YS_PUBKEY_CACHE_CAP", 8_192);
    let mut address_cache = AddressCache::new(pubkey_cache_cap);

    let dlq_sink = match std::env::var("YS_DLQ_DIR").ok().filter(|s| !s.is_empty()) {
        Some(path) => {
            let capacity = env_usize("YS_DLQ_QUEUE_CAP", 1024);
            let dir = PathBuf::from(&path);
            Some(
                DlqSink::new(dir, capacity)
                    .with_context(|| format!("init DLQ sink at {}", path))?,
            )
        }
        None => None,
    };

    // queue and writer
    let mut txq_opt: Option<crossbeam_channel::Sender<Vec<u8>>> = None;
    let mut spsc_send_opt: Option<SpscSender> = None;
    if use_spsc {
        let inner_q = std::sync::Arc::new(ArrayQueue::<Vec<u8>>::new(queue_cap));
        let ev = std::sync::Arc::new(Event::new());
        spsc_send_opt = Some(SpscSender {
            q: inner_q.clone(),
            ev: ev.clone(),
        });
        let uds_path_clone = uds_path.clone();
        let sd = shutdown.clone();
        let src = SpscQueue { q: inner_q, ev };
        let pool = buf_pool.clone();
        let dlq_clone = dlq_sink.clone();
        if use_shm {
            let sd2 = shutdown.clone();
            let pool2 = buf_pool.clone();
            let dlq2 = dlq_sink.clone();
            let shm_path2 = shm_path.clone();
            thread::Builder::new()
                .name("ys-writer".into())
                .spawn(move || {
                    let mut backoff = Duration::from_millis(50);
                    loop {
                        if sd2.load(Ordering::Relaxed) { break; }
                        match shm_ring::ShmRingWriter::open_or_create(&shm_path2, shm_cap_bytes) {
                            Ok(ring) => {
                                info!("writing to SHM ring {}", shm_path2);
                                writer_loop_shm(
                                    ring,
                                    src,
                                    &sd,
                                    writer_limits,
                                    flush_interval,
                                    pool2.clone(),
                                    dlq2.clone(),
                                );
                                break;
                            }
                            Err(e) => {
                                error!("shm open {} failed: {}", shm_path2, e);
                                std::thread::sleep(backoff);
                                backoff = (backoff * 2).min(Duration::from_secs(2));
                            }
                        }
                    }
                })?;
        } else {
            thread::Builder::new()
                .name("ys-writer".into())
                .spawn(move || {
                    writer_loop_generic(
                        uds_path_clone,
                        src,
                        &sd,
                        writer_limits,
                        flush_interval,
                        pool,
                        dlq_clone,
                    );
                })?;
        }
    } else {
        let (txq, rxq) = bounded::<Vec<u8>>(queue_cap);
        let uds_path_clone = uds_path.clone();
        let sd = shutdown.clone();
        let pool = buf_pool.clone();
        let dlq_clone = dlq_sink.clone();
        if use_shm {
            let sd2 = shutdown.clone();
            let pool2 = buf_pool.clone();
            let dlq2 = dlq_sink.clone();
            let shm_path2 = shm_path.clone();
            thread::Builder::new()
                .name("ys-writer".into())
                .spawn(move || {
                    let mut backoff = Duration::from_millis(50);
                    loop {
                        if sd2.load(Ordering::Relaxed) { break; }
                        match shm_ring::ShmRingWriter::open_or_create(&shm_path2, shm_cap_bytes) {
                            Ok(ring) => {
                                info!("writing to SHM ring {}", shm_path2);
                                writer_loop_shm(
                                    ring,
                                    rxq,
                                    &sd,
                                    writer_limits,
                                    flush_interval,
                                    pool2.clone(),
                                    dlq2.clone(),
                                );
                                break;
                            }
                            Err(e) => {
                                error!("shm open {} failed: {}", shm_path2, e);
                                std::thread::sleep(backoff);
                                backoff = (backoff * 2).min(Duration::from_secs(2));
                            }
                        }
                    }
                })?;
        } else {
            thread::Builder::new()
                .name("ys-writer".into())
                .spawn(move || {
                    writer_loop_generic(
                        uds_path_clone,
                        rxq,
                        &sd,
                        writer_limits,
                        flush_interval,
                        pool,
                        dlq_clone,
                    );
                })?;
        }
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
        if base_ms <= 1 {
            return d;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_millis(0));
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
            builder = match builder.x_token(Some(tok)) {
                Ok(b) => b,
                Err(e) => {
                    error!("token set error: {e}");
                    tokio::time::sleep(reconnect_backoff).await;
                    continue;
                }
            };
        }
        // gRPC tuning knobs
        let init_conn_window = env_u64("YS_INIT_CONN_WINDOW", 32 * 1024 * 1024) as u32;
        let init_stream_window = env_u64("YS_INIT_STREAM_WINDOW", 16 * 1024 * 1024) as u32;
        let keepalive_interval_ms = env_u64("YS_HTTP2_KEEPALIVE_INTERVAL_MS", 1_000);
        let keepalive_timeout_ms = env_u64("YS_HTTP2_KEEPALIVE_TIMEOUT_MS", 3_000);
        let tcp_keepalive_secs = env_u64("YS_TCP_KEEPALIVE_SECS", 30);
        let concurrency_limit = env_usize("YS_GRPC_CONCURRENCY_LIMIT", 256);
        let connect_timeout_ms = env_u64("YS_CONNECT_TIMEOUT_MS", 3_000);
        // Best-effort: these builder methods may not exist in older deps; ignore errors by chaining options only when available.
        builder = builder
            .initial_connection_window_size(init_conn_window)
            .initial_stream_window_size(init_stream_window)
            .http2_keep_alive_interval(Duration::from_millis(keepalive_interval_ms))
            .http2_keep_alive_timeout(Duration::from_millis(keepalive_timeout_ms))
            .keepalive_while_idle(true)
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive_secs)))
            .concurrency_limit(concurrency_limit)
            .connect_timeout(Duration::from_millis(connect_timeout_ms));
        let mut client = match builder.connect().await {
            Ok(c) => c,
            Err(e) => {
                error!("connect error: {e}");
                counter!("ys_connect_fail_total").increment(1);
                tokio::time::sleep(jitter(reconnect_backoff)).await;
                reconnect_backoff = (reconnect_backoff * 2).min(backoff_max);
                continue;
            }
        };
        let (mut tx, mut rx) = match client.subscribe().await {
            Ok(sr) => sr,
            Err(e) => {
                error!("subscribe error: {e}");
                counter!("ys_subscribe_fail_total").increment(1);
                tokio::time::sleep(jitter(reconnect_backoff)).await;
                reconnect_backoff = (reconnect_backoff * 2).min(backoff_max);
                continue;
            }
        };
        if let Err(e) = tx.send(req.clone()).await {
            error!("send subscribe request failed: {e}");
            counter!("ys_send_fail_total").increment(1);
            tokio::time::sleep(jitter(reconnect_backoff)).await;
            reconnect_backoff = (reconnect_backoff * 2).min(backoff_max);
            continue;
        }
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
                let v = SAMPLE_SEQ.fetch_add(1, Ordering::Relaxed);
                let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                if encode_into_with(&rec, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                    if let Some(t0) = maybe_t0 {
                        histogram!("ys_consumer_encode_us", "kind" => "tx").record(t0.elapsed().as_secs_f64() * 1e6);
                    }
                    if !forward_frame(buf, &txq_opt, &spsc_send_opt, &shutdown, &buf_pool) {
                        counter!("ys_consumer_dropped_total").increment(1);
                    }
                } else {
                    buf_pool.put(buf);
                }
            }
            Some(subscribe_update::UpdateOneof::Account(a)) => {
                if let Some(acc) = &a.account {
                    let pubkey = address_cache.decode(&acc.pubkey);
                    let owner = address_cache.decode(&acc.owner);
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
                    let v = SAMPLE_SEQ.fetch_add(1, Ordering::Relaxed);
                    let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                    if encode_record_ref_into_with(&aref, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                        if let Some(t0) = maybe_t0 {
                            histogram!("ys_consumer_encode_us", "kind" => "account").record(t0.elapsed().as_secs_f64() * 1e6);
                        }
                        if !forward_frame(buf, &txq_opt, &spsc_send_opt, &shutdown, &buf_pool) {
                            counter!("ys_consumer_dropped_total").increment(1);
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
                    parent_slot: Some(b.parent_slot),
                    rewards_len: b.rewards.as_ref().map(|r| r.rewards.len()).unwrap_or(0) as u32,
                    block_time_unix: block_time,
                    leader: ld,
                });
                let mut buf = buf_pool.get();
                let v = SAMPLE_SEQ.fetch_add(1, Ordering::Relaxed);
                let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                if encode_into_with(&rec, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                    if let Some(t0) = maybe_t0 { histogram!("ys_consumer_encode_us", "kind" => "block").record(t0.elapsed().as_secs_f64() * 1e6); }
                    if !forward_frame(buf, &txq_opt, &spsc_send_opt, &shutdown, &buf_pool) {
                        counter!("ys_consumer_dropped_total").increment(1);
                    }
                } else {
                    buf_pool.put(buf);
                }
            }
            Some(subscribe_update::UpdateOneof::Slot(s)) => {
                let rec = Record::Slot { slot: s.slot, parent: s.parent, status: s.status as u8 };
                let mut buf = buf_pool.get();
                let v = SAMPLE_SEQ.fetch_add(1, Ordering::Relaxed);
                let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                if encode_into_with(&rec, &mut buf, EncodeOptions::latency_uds()).is_ok() {
                    if let Some(t0) = maybe_t0 { histogram!("ys_consumer_encode_us", "kind" => "slot").record(t0.elapsed().as_secs_f64() * 1e6); }
                    if !forward_frame(buf, &txq_opt, &spsc_send_opt, &shutdown, &buf_pool) {
                        counter!("ys_consumer_dropped_total").increment(1);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_base58_roundtrip() {
        let input: [u8; 32] = [42u8; 32];
        let encoded = bs58::encode(input).into_string();
        let decoded = decode_base58(&encoded);
        assert_eq!(decoded, input);
    }

    #[test]
    fn decode_base58_invalid_returns_zeroes() {
        let decoded = decode_base58("not-base58");
        assert_eq!(decoded, [0u8; 32]);
    }

    #[test]
    fn address_cache_caches_and_evicts() {
        let mut cache = AddressCache::new(2);
        let key_a = "11111111111111111111111111111112"; // bs58 for mostly zeros with trailing 2
        let key_b = "11111111111111111111111111111113";
        let key_c = "11111111111111111111111111111114";
        let a = cache.decode_str(key_a);
        let _b = cache.decode_str(key_b);
        let a_cached = cache.decode_str(key_a);
        assert_eq!(a, a_cached);
        let _c = cache.decode_str(key_c);
        // key_a should be evicted once capacity exceeded, causing re-decode
        let a_new = cache.decode_str(key_a);
        assert_eq!(a_new, a);
    }

    #[test]
    fn buf_pool_recycles_large_buffers() {
        let pool = std::sync::Arc::new(BufPool::new(2, 16));
        let mut buf = pool.get();
        buf.extend_from_slice(&[1, 2, 3, 4]);
        pool.put(buf);
        let reused = pool.get();
        assert_eq!(reused.len(), 0);
        assert!(reused.capacity() >= 16);
    }

    #[test]
    fn frame_kind_detection_matches_variant() {
        let record = Record::Slot {
            slot: 1,
            parent: Some(0),
            status: 2,
        };
        let encoded = faststreams::encode_record(&record).expect("encode");
        let mut scratch = Vec::new();
        let kind = frame_kind_from_bytes(&encoded, &mut scratch);
        assert_eq!(kind, "slot");
    }
}
