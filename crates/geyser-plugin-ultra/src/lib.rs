#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::unwrap_used, clippy::expect_used)]
mod config;
mod writer;
mod pool;

use config::{Config, ValidatedConfig, Streams};
use crossbeam_channel::{bounded, Sender};
use faststreams::{encode_into_with, encode_record_ref_into_with, EncodeOptions, AccountUpdateRef, BlockMeta, Record, RecordRef, TxUpdate};
use metrics::{counter, histogram, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use tracing::debug;
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result as GeyserResult, SlotStatus,
};
// no direct imports
use std::fs::File;
use std::io::Read;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

struct Ultra {
    cfg: Option<ValidatedConfig>,
    tx: Option<Sender<pool::PooledBuf>>,
    shutdown: Arc<AtomicBool>,
    streams: Streams,
    logger_set: Mutex<bool>,
    pool: Option<Arc<pool::BufferPool>>,
    metrics_seq: AtomicU64,
    writer: Option<thread::JoinHandle<()>>,
    metrics_handle: Option<PrometheusHandle>,
    enqueued_total: AtomicU64,
    dropped_total: AtomicU64,
    encode_error_total: AtomicU64,
    queue_depth_max: Arc<AtomicU64>,
    processed_total: Arc<AtomicU64>,
}

#[derive(Debug)]
struct PluginError(String);

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

impl std::error::Error for PluginError {}

impl std::fmt::Debug for Ultra {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ultra").finish()
    }
}

impl Ultra {
    fn new() -> Self {
        Self {
            cfg: None,
            tx: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            streams: Streams { accounts: true, transactions: true, blocks: true, slots: true },
            logger_set: Mutex::new(false),
            pool: None,
            metrics_seq: AtomicU64::new(0),
            writer: None,
            metrics_handle: None,
            enqueued_total: AtomicU64::new(0),
            dropped_total: AtomicU64::new(0),
            encode_error_total: AtomicU64::new(0),
            queue_depth_max: Arc::new(AtomicU64::new(0)),
            processed_total: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for Ultra {
    fn default() -> Self { Self::new() }
}

impl GeyserPlugin for Ultra {
    fn name(&self) -> &'static str { "ultra" }

    fn setup_logger(&self, logger: &'static dyn log::Log, level: log::LevelFilter) -> GeyserResult<()> {
        let mut set = self.logger_set.lock();
        if !*set {
            log::set_max_level(level);
            log::set_logger(logger).map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
            *set = true;
        }
        Ok(())
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> GeyserResult<()> {
        // Read JSON config
        let mut f = File::open(config_file).map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
        let mut s = String::new();
        f.read_to_string(&mut s).map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
        let cfg_raw: Config = serde_json::from_str(&s).map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
        let cfg = cfg_raw.validate().map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;

        // Metrics
        if let Some(m) = &cfg.metrics {
            if let Some(addr) = &m.listen_addr {
                match addr.parse::<std::net::SocketAddr>() {
                    Ok(sock) => {
                        match PrometheusBuilder::new().with_http_listener(sock).install_recorder() {
                            Ok(h) => { self.metrics_handle = Some(h); }
                            Err(e) => { log::error!("failed to install metrics exporter: {}", e); }
                        }
                    }
                    Err(e) => {
                        log::error!("invalid metrics listen_addr '{}': {}", addr, e);
                    }
                }
            }
        }

        // Initialize reusable buffer pool sized for bursts
        let pool_default_cap = cfg.pool_default_cap;
        let pool = pool::BufferPool::new(cfg.pool_items_max, pool_default_cap);
        let (tx, rx) = bounded::<pool::PooledBuf>(cfg.queue_capacity);
        gauge!("ultra_pool_cap_bytes").set(pool_default_cap as f64);

        // Writer thread
        let wcfg = cfg.clone();
        let shutdown = self.shutdown.clone();
        let qmax = Arc::clone(&self.queue_depth_max);
        let processed = Arc::clone(&self.processed_total);
        let jh = thread::Builder::new().name("ultra-writer".into()).spawn(move || {
            writer::run_writer(wcfg, rx, &shutdown, qmax, processed)
        }).map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;

        self.streams = cfg.streams.clone();
        self.tx = Some(tx);
        self.cfg = Some(cfg);
        self.pool = Some(pool);
        self.writer = Some(jh);

        Ok(())
    }

    fn on_unload(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Close channel to wake writer
        self.tx = None;
        // Join writer with timeout
        if let Some(jh) = self.writer.take() {
            if !join_with_timeout(jh, std::time::Duration::from_secs(3)) {
                log::error!("ultra: writer did not terminate within timeout");
            }
        }
        let enq = self.enqueued_total.load(Ordering::Relaxed);
        let drp = self.dropped_total.load(Ordering::Relaxed);
        let enc_err = self.encode_error_total.load(Ordering::Relaxed);
        let qmax = self.queue_depth_max.load(Ordering::Relaxed);
        let processed = self.processed_total.load(Ordering::Relaxed);
        log::info!("ultra: unload summary processed={} enqueued={} dropped={} encode_errors={} max_queue_len={}", processed, enq, drp, enc_err, qmax);
    }

    fn account_data_notifications_enabled(&self) -> bool { self.streams.accounts }
    fn transaction_notifications_enabled(&self) -> bool { self.streams.transactions }
    fn entry_notifications_enabled(&self) -> bool { false }

    fn update_account(&self, account: ReplicaAccountInfoVersions<'_>, slot: u64, is_startup: bool) -> GeyserResult<()> {
        if !self.streams.accounts { return Ok(()); }
        let tx = match &self.tx { Some(t) => t, None => return Ok(()), };
        let (pubkey, lamports, owner, executable, rent_epoch, data) = match account {
            ReplicaAccountInfoVersions::V0_0_1(a) => (
                a.pubkey, a.lamports, a.owner, a.executable, a.rent_epoch, a.data
            ),
            _ => return Ok(()),
        };
        let pk_bytes = {
            let s: &[u8] = AsRef::<[u8]>::as_ref(&pubkey);
            if s.len() == 32 { let mut a = [0u8;32]; a.copy_from_slice(s); a } else { [0u8;32] }
        };
        let owner_bytes = {
            let s: &[u8] = AsRef::<[u8]>::as_ref(&owner);
            if s.len() == 32 { let mut a = [0u8;32]; a.copy_from_slice(s); a } else { [0u8;32] }
        };
        let aref = RecordRef::Account(AccountUpdateRef {
            slot,
            is_startup,
            pubkey: pk_bytes,
            lamports,
            owner: owner_bytes,
            executable,
            rent_epoch,
            data,
        });
        if let Some(pool) = &self.pool {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let t0 = Instant::now();
                    match encode_record_ref_into_with(&aref, buf, EncodeOptions::latency_uds()) {
                        Ok(()) => {
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 {
                                histogram!("ultra_encode_ns", "kind" => "account").record(t0.elapsed().as_nanos() as f64);
                                if let Some(sz) = pb.as_slice().map(|s| s.len()) { histogram!("ultra_record_bytes", "kind" => "account").record(sz as f64); }
                            }
                            if tx.try_send(pb).is_err() {
                                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 { counter!("ultra_dropped_total", "reason" => "queue_full").increment(256); }
                            } else {
                                self.enqueued_total.fetch_add(1, Ordering::Relaxed);
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 { counter!("ultra_enqueued_total").increment(256); }
                            }
                        }
                        Err(e) => {
                            self.encode_error_total.fetch_add(1, Ordering::Relaxed);
                            counter!("ultra_encode_error_total", "kind" => "account").increment(1);
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 { debug!(target = "ultra.encode", "account encode failed: {e}"); }
                        }
                    }
                }
            } else {
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                counter!("ultra_dropped_total", "reason" => "no_buf").increment(1);
            }
        }
        Ok(())
    }

    fn notify_transaction(&self, transaction: ReplicaTransactionInfoVersions<'_>, slot: u64) -> GeyserResult<()> {
        if !self.streams.transactions { return Ok(()); }
        let tx = match &self.tx { Some(t) => t, None => return Ok(()), };
        let (sig, is_vote, err_opt) = match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(t) => {
                let sig = t.signature;
                let vote = t.is_vote;
                let err = Some(&t.transaction_status_meta).and_then(|m| m.status.clone().err()).map(|e| format!("{:?}", e));
                (sig, vote, err)
            }
            _ => return Ok(())
        };
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(sig.as_ref());
        let rec = Record::Tx(TxUpdate {
            slot,
            signature: sig_bytes,
            err: err_opt,
            vote: is_vote,
        });
        if let Some(pool) = &self.pool {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let t0 = Instant::now();
                    match encode_into_with(&rec, buf, EncodeOptions::latency_uds()) {
                        Ok(()) => {
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 {
                                histogram!("ultra_encode_ns", "kind" => "tx").record(t0.elapsed().as_nanos() as f64);
                                if let Some(sz) = pb.as_slice().map(|s| s.len()) { histogram!("ultra_record_bytes", "kind" => "tx").record(sz as f64); }
                            }
                            if tx.try_send(pb).is_err() {
                                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 { counter!("ultra_dropped_total", "reason" => "queue_full").increment(256); }
                            } else {
                                self.enqueued_total.fetch_add(1, Ordering::Relaxed);
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 { counter!("ultra_enqueued_total").increment(256); }
                            }
                        }
                        Err(e) => {
                            self.encode_error_total.fetch_add(1, Ordering::Relaxed);
                            counter!("ultra_encode_error_total", "kind" => "tx").increment(1);
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 { debug!(target = "ultra.encode", "tx encode failed: {e}"); }
                        }
                    }
                }
            } else {
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                counter!("ultra_dropped_total", "reason" => "no_buf").increment(1);
            }
        }
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> GeyserResult<()> {
        if !self.streams.blocks { return Ok(()); }
        let tx = match &self.tx { Some(t) => t, None => return Ok(()), };
        if let ReplicaBlockInfoVersions::V0_0_1(b) = blockinfo {
            let rec = Record::Block(BlockMeta {
                slot: b.slot,
                blockhash: None, // Avoid per-event base58 allocation; upstream bytes not available
                parent_slot: None, // Unknown from this API; avoid guessing
                rewards_len: b.rewards.len() as u32,
                block_time_unix: b.block_time,
                leader: None, // Leader info not available in new API
            });
            if let Some(pool) = &self.pool {
                if let Some(mut pb) = pool.try_get() {
                    if let Some(buf) = pb.inner_mut() {
                        let t0 = Instant::now();
                        match encode_into_with(&rec, buf, EncodeOptions::latency_uds()) {
                            Ok(()) => {
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 {
                                    histogram!("ultra_encode_ns", "kind" => "block").record(t0.elapsed().as_nanos() as f64);
                                    if let Some(sz) = pb.as_slice().map(|s| s.len()) { histogram!("ultra_record_bytes", "kind" => "block").record(sz as f64); }
                                }
                                if tx.try_send(pb).is_err() {
                                    self.dropped_total.fetch_add(1, Ordering::Relaxed);
                                    let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                    if (v & 0xFF) == 0 { counter!("ultra_dropped_total", "reason" => "queue_full").increment(256); }
                                } else {
                                    self.enqueued_total.fetch_add(1, Ordering::Relaxed);
                                    let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                    if (v & 0xFF) == 0 { counter!("ultra_enqueued_total").increment(256); }
                                }
                            }
                            Err(e) => {
                                self.encode_error_total.fetch_add(1, Ordering::Relaxed);
                                counter!("ultra_encode_error_total", "kind" => "block").increment(1);
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 { debug!(target = "ultra.encode", "block encode failed: {e}"); }
                            }
                        }
                    }
                } else {
                    self.dropped_total.fetch_add(1, Ordering::Relaxed);
                    counter!("ultra_dropped_total", "reason" => "no_buf").increment(1);
                }
            }
        }
        Ok(())
    }

    fn update_slot_status(&self, slot: u64, parent: Option<u64>, status: &SlotStatus) -> GeyserResult<()> {
        if !self.streams.slots { return Ok(()); }
        let tx = match &self.tx { Some(t) => t, None => return Ok(()), };
        let st = match status {
            SlotStatus::Processed => 0u8,
            SlotStatus::Confirmed => 1,
            SlotStatus::Rooted => 2,
            SlotStatus::FirstShredReceived => 3,
            SlotStatus::Completed => 4,
            SlotStatus::CreatedBank => 5,
            SlotStatus::Dead(_) => 6,
        };
        let rec = Record::Slot { slot, parent, status: st };
        if let Some(pool) = &self.pool {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let t0 = Instant::now();
                    match encode_into_with(&rec, buf, EncodeOptions::latency_uds()) {
                        Ok(()) => {
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 {
                                histogram!("ultra_encode_ns", "kind" => "slot").record(t0.elapsed().as_nanos() as f64);
                                if let Some(sz) = pb.as_slice().map(|s| s.len()) { histogram!("ultra_record_bytes", "kind" => "slot").record(sz as f64); }
                            }
                            if tx.try_send(pb).is_err() {
                                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 { counter!("ultra_dropped_total", "reason" => "queue_full").increment(256); }
                            } else {
                                self.enqueued_total.fetch_add(1, Ordering::Relaxed);
                                let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                                if (v & 0xFF) == 0 { counter!("ultra_enqueued_total").increment(256); }
                            }
                        }
                        Err(e) => {
                            self.encode_error_total.fetch_add(1, Ordering::Relaxed);
                            counter!("ultra_encode_error_total", "kind" => "slot").increment(1);
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 { debug!(target = "ultra.encode", "slot encode failed: {e}"); }
                        }
                    }
                }
            } else {
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                counter!("ultra_dropped_total", "reason" => "no_buf").increment(1);
            }
        }
        Ok(())
    }

    fn notify_end_of_startup(&self) -> GeyserResult<()> {
        let tx = match &self.tx { Some(t) => t, None => return Ok(()), };
        if let Some(pool) = &self.pool {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let t0 = Instant::now();
                    match encode_into_with(&Record::EndOfStartup, buf, EncodeOptions::latency_uds()) {
                        Ok(()) => {
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 { histogram!("ultra_encode_ns", "kind" => "eos").record(t0.elapsed().as_nanos() as f64); if let Some(sz) = pb.as_slice().map(|s| s.len()) { histogram!("ultra_record_bytes", "kind" => "eos").record(sz as f64); } }
                            let _ = tx.try_send(pb);
                        }
                        Err(e) => {
                            self.encode_error_total.fetch_add(1, Ordering::Relaxed);
                            counter!("ultra_encode_error_total", "kind" => "eos").increment(1);
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 { debug!(target = "ultra.encode", "eos encode failed: {e}"); }
                        }
                    }
                }
            } else {
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                counter!("ultra_dropped_total", "reason" => "no_buf").increment(1);
            }
        }
        Ok(())
    }
}

// Required export symbol by Solana Geyser plugin manager.
#[no_mangle]
pub extern "C" fn _create_plugin() -> *mut c_void {
    let plugin = Ultra::new();
    // Box<dyn GeyserPlugin> erased as raw pointer to satisfy the loader
    let boxed: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(boxed) as *mut c_void
}


fn join_with_timeout(jh: thread::JoinHandle<()>, timeout: std::time::Duration) -> bool {
    use std::sync::mpsc;
    let (tx, rx) = mpsc::sync_channel::<()>(1);
    thread::spawn(move || {
        let _ = jh.join();
        let _ = tx.send(());
    });
    rx.recv_timeout(timeout).is_ok()
}

