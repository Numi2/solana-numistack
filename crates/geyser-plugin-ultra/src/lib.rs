// Numan Thabit 2025
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::unwrap_used, clippy::expect_used)]
mod affinity;
mod config;
mod meter;
mod pool;
mod queue;
mod writer;

use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result as GeyserResult, SlotStatus,
};
use config::{Config, DropPolicy, Streams, ValidatedConfig};
use faststreams::{
    encode_into_with, encode_record_ref_into_with, AccountUpdateRef, BlockMeta, EncodeOptions,
    Record, RecordRef, TxUpdate,
};
use metrics::{counter, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use queue::{Producer, SpscRing};
use tracing::debug;
// no direct imports
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::{hint::spin_loop, num::Wrapping};

struct Ultra {
    cfg: Option<ValidatedConfig>,
    producers: Vec<Producer<pool::PooledBuf>>,
    shutdown: Arc<AtomicBool>,
    streams: Streams,
    logger_set: Mutex<bool>,
    pools: Vec<Arc<pool::BufferPool>>,
    metrics_seq: AtomicU64,
    writer_handles: Vec<thread::JoinHandle<()>>,
    metrics_handle: Option<PrometheusHandle>,
    meter: Arc<meter::Meter>,
    metrics_flusher: Option<thread::JoinHandle<()>>,
    shed_accounts_until: Mutex<HashMap<[u8; 32], std::time::Instant>>,
}

#[derive(Debug)]
struct PluginError(String);

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
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
            producers: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            streams: Streams {
                accounts: true,
                transactions: true,
                blocks: true,
                slots: true,
            },
            logger_set: Mutex::new(false),
            pools: Vec::new(),
            metrics_seq: AtomicU64::new(0),
            writer_handles: Vec::new(),
            metrics_handle: None,
            meter: Arc::new(meter::Meter::default()),
            metrics_flusher: None,
            shed_accounts_until: Mutex::new(HashMap::new()),
        }
    }

    fn writer_count(&self) -> usize {
        self.producers.len()
    }

    fn queue_policy(&self) -> DropPolicy {
        self.cfg
            .as_ref()
            .map(|cfg| cfg.queue_drop_policy)
            .unwrap_or(DropPolicy::DropNewest)
    }

    fn writer_index_for_bytes(&self, bytes: &[u8]) -> Option<usize> {
        let count = self.writer_count();
        if count == 0 {
            None
        } else {
            Some(shard_index(bytes, count))
        }
    }

    fn writer_index_for_u64(&self, value: u64) -> Option<usize> {
        let count = self.writer_count();
        if count == 0 {
            None
        } else {
            Some(shard_from_u64(value, count))
        }
    }

    fn try_enqueue(&self, idx: usize, buffer: pool::PooledBuf) -> Result<(), pool::PooledBuf> {
        let policy = self.queue_policy();
        let producer = match self.producers.get(idx) {
            Some(p) => p,
            None => return Err(buffer),
        };
        match policy {
            DropPolicy::DropNewest => producer.try_push(buffer),
            DropPolicy::DropOldest => producer.push_drop_oldest(buffer),
            DropPolicy::Block => {
                let mut current = buffer;
                loop {
                    match producer.try_push(current) {
                        Ok(()) => return Ok(()),
                        Err(buf) => {
                            if self.shutdown.load(Ordering::Relaxed) {
                                return Err(buf);
                            }
                            spin_loop();
                            current = buf;
                        }
                    }
                }
            }
        }
    }

    fn record_enqueue_success(&self) {
        self.meter.inc_enqueued(1);
    }

    fn record_drop_shard(&self, reason: &'static str, shard: usize, by: u64) {
        match reason {
            "backpressure" | "queue_full" => self.meter.inc_dropped_queue_full(by),
            "no_buf" => self.meter.inc_dropped_no_buf(by),
            "oversize" | "serialization_error" | "write_blocked" => {},
            _ => {},
        }
        counter!("ultra_dropped_total", "reason" => reason, "shard" => shard.to_string())
            .increment(by);
    }

    fn record_queue_depth(&self, idx: usize) {
        if let Some(producer) = self.producers.get(idx) {
            let depth = producer.len() as u64;
            self.meter.observe_queue_depth_max(depth);
        }
    }

    #[inline]
    fn shed_accounts_ttl_ms(&self) -> u64 {
        self.cfg.as_ref().map(|c| c.shed_throttle_ms).unwrap_or(500)
    }

    #[inline]
    fn mark_shed_account(&self, pk: [u8; 32]) {
        let ttl = self.shed_accounts_ttl_ms();
        if ttl == 0 {
            return;
        }
        let until = std::time::Instant::now() + std::time::Duration::from_millis(ttl);
        let mut map = self.shed_accounts_until.lock();
        map.insert(pk, until);
        counter!("ultra_shed_total", "action" => "mark").increment(1);
    }

    #[inline]
    fn is_account_shed(&self, pk: &[u8; 32]) -> bool {
        let now = std::time::Instant::now();
        let mut map = self.shed_accounts_until.lock();
        if let Some(until) = map.get(pk).cloned() {
            if now >= until {
                map.remove(pk);
                return false;
            }
            return true;
        }
        false
    }
}

impl Default for Ultra {
    fn default() -> Self {
        Self::new()
    }
}

impl GeyserPlugin for Ultra {
    fn name(&self) -> &'static str {
        "ultra"
    }

    fn setup_logger(
        &self,
        logger: &'static dyn log::Log,
        level: log::LevelFilter,
    ) -> GeyserResult<()> {
        let mut set = self.logger_set.lock();
        if !*set {
            log::set_max_level(level);
            log::set_logger(logger)
                .map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
            *set = true;
        }
        Ok(())
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> GeyserResult<()> {
        // Read JSON config
        self.shutdown.store(false, Ordering::Relaxed);
        let mut f = File::open(config_file)
            .map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
        let mut s = String::new();
        f.read_to_string(&mut s)
            .map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
        let cfg_raw: Config = serde_json::from_str(&s)
            .map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
        let cfg = cfg_raw
            .validate()
            .map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;

        // Metrics
        if let Some(m) = &cfg.metrics {
            if let Some(addr) = &m.listen_addr {
                match addr.parse::<std::net::SocketAddr>() {
                    Ok(sock) => {
                        match PrometheusBuilder::new()
                            .with_http_listener(sock)
                            .install_recorder()
                        {
                            Ok(h) => {
                                self.metrics_handle = Some(h);
                            }
                            Err(e) => {
                                log::error!("failed to install metrics exporter: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("invalid metrics listen_addr '{}': {}", addr, e);
                    }
                }
            }
        }

        // Initialize per-writer reusable buffer pools sized for bursts
        let pool_default_cap = cfg.pool_default_cap;
        let mut pools: Vec<Arc<pool::BufferPool>> = Vec::with_capacity(cfg.writer_threads);
        for _ in 0..cfg.writer_threads {
            pools.push(pool::BufferPool::new(cfg.pool_items_max, pool_default_cap));
        }

        let mut producers = Vec::with_capacity(cfg.writer_threads);
        let mut handles = Vec::with_capacity(cfg.writer_threads);
        let core_ids = affinity::select_writer_core_ids(&cfg, cfg.writer_threads);
        for writer_idx in 0..cfg.writer_threads {
            let ring = SpscRing::with_capacity(cfg.queue_capacity);
            let (producer, consumer) = ring.split();
            let writer_cfg = cfg.clone();
            let shutdown = Arc::clone(&self.shutdown);
            let meter = Arc::clone(&self.meter);
            let core_aff = core_ids.get(writer_idx).cloned();
            let handle = thread::Builder::new()
                .name(format!("ultra-writer-{writer_idx}"))
                .spawn(move || {
                    writer::run_writer(writer_idx, writer_cfg, consumer, &shutdown, meter, core_aff)
                })
                .map_err(|e| GeyserPluginError::Custom(Box::new(PluginError(e.to_string()))))?;
            producers.push(producer);
            handles.push(handle);
        }

        self.streams = cfg.streams.clone();
        self.producers = producers;
        self.cfg = Some(cfg);
        self.pools = pools;
        self.writer_handles = handles;

        // Spawn low-priority metrics flusher if metrics exporter enabled
        if self.metrics_handle.is_some() {
            if let Some(flusher) = meter::spawn_flusher(
                Arc::clone(&self.meter),
                self.producers.clone(),
                Arc::clone(&self.shutdown),
            ) {
                self.metrics_flusher = Some(flusher);
            }
        }

        Ok(())
    }

    fn on_unload(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.metrics_flusher.take() {
            let _ = join_with_timeout(handle, std::time::Duration::from_secs(2));
        }
        self.producers.clear();
        let mut handles = Vec::new();
        std::mem::swap(&mut handles, &mut self.writer_handles);
        for (idx, handle) in handles.into_iter().enumerate() {
            if !join_with_timeout(handle, std::time::Duration::from_secs(3)) {
                log::error!("ultra: writer {idx} did not terminate within timeout");
            }
        }
        let enq = self.meter.enqueued_total.load(Ordering::Relaxed);
        let drp = self.meter.dropped_queue_full_total.load(Ordering::Relaxed)
            + self.meter.dropped_no_buf_total.load(Ordering::Relaxed);
        let enc_err = self
            .meter
            .encode_error_account_total
            .load(Ordering::Relaxed)
            + self.meter.encode_error_tx_total.load(Ordering::Relaxed)
            + self.meter.encode_error_block_total.load(Ordering::Relaxed)
            + self.meter.encode_error_slot_total.load(Ordering::Relaxed)
            + self.meter.encode_error_eos_total.load(Ordering::Relaxed);
        let qmax = self.meter.queue_depth_max.load(Ordering::Relaxed);
        let processed = self.meter.processed_total.load(Ordering::Relaxed);
        log::info!(
            "ultra: unload summary processed={} enqueued={} dropped={} encode_errors={} max_queue_len={}",
            processed, enq, drp, enc_err, qmax
        );
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.streams.accounts
    }
    fn transaction_notifications_enabled(&self) -> bool {
        self.streams.transactions
    }
    fn entry_notifications_enabled(&self) -> bool {
        false
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions<'_>,
        slot: u64,
        is_startup: bool,
    ) -> GeyserResult<()> {
        if !self.streams.accounts {
            return Ok(());
        }
        let (pubkey, lamports, owner, executable, rent_epoch, data) = match account {
            ReplicaAccountInfoVersions::V0_0_1(a) => (
                a.pubkey,
                a.lamports,
                a.owner,
                a.executable,
                a.rent_epoch,
                a.data,
            ),
            _ => return Ok(()),
        };
        let pk_bytes = {
            let s: &[u8] = AsRef::<[u8]>::as_ref(&pubkey);
            if s.len() == 32 {
                let mut a = [0u8; 32];
                a.copy_from_slice(s);
                a
            } else {
                [0u8; 32]
            }
        };
        // If this account pubkey is currently shed, skip early to throttle upstream work.
        if self.is_account_shed(&pk_bytes) {
            counter!("ultra_shed_total", "action" => "skip").increment(1);
            return Ok(());
        }
        let owner_bytes = {
            let s: &[u8] = AsRef::<[u8]>::as_ref(&owner);
            if s.len() == 32 {
                let mut a = [0u8; 32];
                a.copy_from_slice(s);
                a
            } else {
                [0u8; 32]
            }
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
        let idx = match self.writer_index_for_bytes(&pk_bytes) {
            Some(i) => i,
            None => {
                // No writers; shed this key temporarily to reduce encode pressure.
                self.mark_shed_account(pk_bytes);
                return Ok(());
            }
        };
        if let Some(pool) = self.pools.get(idx) {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                    let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                    let cap_hint = self
                        .cfg
                        .as_ref()
                        .map(|c| c.pool_default_cap)
                        .unwrap_or(64 * 1024)
                        .saturating_sub(12);
                    let mut opts = EncodeOptions::latency_uds();
                    opts.payload_hint = Some(cap_hint);
                    match encode_record_ref_into_with(&aref, buf, opts) {
                        Ok(()) => {
                            if let Some(t0) = maybe_t0 {
                                histogram!("ultra_encode_ns", "kind" => "account")
                                    .record(t0.elapsed().as_nanos() as f64);
                                if let Some(sz) = pb.as_slice().map(|s| s.len()) {
                                    histogram!("ultra_record_bytes", "kind" => "account")
                                        .record(sz as f64);
                                    if let Some(cfg) = &self.cfg {
                                        if sz > cfg.pool_default_cap {
                                            // Oversize frame; drop
                                            drop(pb);
                                            self.record_drop_shard("oversize", idx, 1);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            match self.try_enqueue(idx, pb) {
                                Ok(()) => {
                                    self.record_queue_depth(idx);
                                    self.record_enqueue_success();
                                }
                                Err(buf) => {
                                    drop(buf);
                                    self.record_drop_shard("backpressure", idx, 1);
                                }
                            }
                        }
                        Err(e) => {
                            self.meter.inc_encode_error_account(1);
                            self.record_drop_shard("serialization_error", idx, 1);
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 {
                                debug!(target = "ultra.encode", "account encode failed: {e}");
                            }
                        }
                    }
                }
            } else {
                self.record_drop_shard("no_buf", idx, 1);
            }
        }
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> GeyserResult<()> {
        if !self.streams.transactions {
            return Ok(());
        }
        let (sig, is_vote, err_opt) = match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(t) => {
                let sig = t.signature;
                let vote = t.is_vote;
                let err = Some(&t.transaction_status_meta)
                    .and_then(|m| m.status.clone().err())
                    .map(|e| format!("{:?}", e));
                (sig, vote, err)
            }
            _ => return Ok(()),
        };
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(sig.as_ref());
        let rec = Record::Tx(TxUpdate {
            slot,
            signature: sig_bytes,
            err: err_opt,
            vote: is_vote,
        });
        let idx = match self.writer_index_for_bytes(&sig_bytes) {
            Some(i) => i,
            None => return Ok(()),
        };
        if let Some(pool) = self.pools.get(idx) {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                    let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                    let cap_hint = self
                        .cfg
                        .as_ref()
                        .map(|c| c.pool_default_cap)
                        .unwrap_or(64 * 1024)
                        .saturating_sub(12);
                    let mut opts = EncodeOptions::latency_uds();
                    opts.payload_hint = Some(cap_hint);
                    match encode_into_with(&rec, buf, opts) {
                        Ok(()) => {
                            if let Some(t0) = maybe_t0 {
                                histogram!("ultra_encode_ns", "kind" => "tx")
                                    .record(t0.elapsed().as_nanos() as f64);
                                if let Some(sz) = pb.as_slice().map(|s| s.len()) {
                                    histogram!("ultra_record_bytes", "kind" => "tx")
                                        .record(sz as f64);
                                    if let Some(cfg) = &self.cfg {
                                        if sz > cfg.pool_default_cap {
                                            drop(pb);
                                            self.record_drop_shard("oversize", idx, 1);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            match self.try_enqueue(idx, pb) {
                                Ok(()) => {
                                    self.record_queue_depth(idx);
                                    self.record_enqueue_success();
                                }
                                Err(buf) => {
                                    drop(buf);
                                    self.record_drop_shard("backpressure", idx, 1);
                                }
                            }
                        }
                        Err(e) => {
                            self.meter.inc_encode_error_tx(1);
                            self.record_drop_shard("serialization_error", idx, 1);
                            let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                            if (v & 0xFF) == 0 {
                                debug!(target = "ultra.encode", "tx encode failed: {e}");
                            }
                        }
                    }
                }
            } else {
                self.record_drop_shard("no_buf", idx, 1);
            }
        }
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> GeyserResult<()> {
        if !self.streams.blocks {
            return Ok(());
        }
        if let ReplicaBlockInfoVersions::V0_0_1(b) = blockinfo {
            let rec = Record::Block(BlockMeta {
                slot: b.slot,
                blockhash: None, // Avoid per-event base58 allocation; upstream bytes not available
                parent_slot: None, // Unknown from this API; avoid guessing
                rewards_len: b.rewards.len() as u32,
                block_time_unix: b.block_time,
                leader: None, // Leader info not available in new API
            });
            let idx = match self.writer_index_for_u64(b.slot) { Some(i) => i, None => return Ok(()) };
            if let Some(pool) = self.pools.get(idx) {
                if let Some(mut pb) = pool.try_get() {
                    if let Some(buf) = pb.inner_mut() {
                        let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                        let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                        let cap_hint = self
                            .cfg
                            .as_ref()
                            .map(|c| c.pool_default_cap)
                            .unwrap_or(64 * 1024)
                            .saturating_sub(12);
                        let mut opts = EncodeOptions::latency_uds();
                        opts.payload_hint = Some(cap_hint);
                        match encode_into_with(&rec, buf, opts) {
                            Ok(()) => {
                                if let Some(t0) = maybe_t0 {
                                    histogram!("ultra_encode_ns", "kind" => "block")
                                        .record(t0.elapsed().as_nanos() as f64);
                                    if let Some(sz) = pb.as_slice().map(|s| s.len()) {
                                        histogram!("ultra_record_bytes", "kind" => "block")
                                            .record(sz as f64);
                                        if let Some(cfg) = &self.cfg {
                                            if sz > cfg.pool_default_cap {
                                                drop(pb);
                                                self.record_drop_shard("oversize", idx, 1);
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                                match self.try_enqueue(idx, pb) {
                                    Ok(()) => {
                                        self.record_queue_depth(idx);
                                        self.record_enqueue_success();
                                    }
                                    Err(buf) => {
                                        drop(buf);
                                        self.record_drop_shard("backpressure", idx, 1);
                                    }
                                }
                            }
                            Err(e) => {
                                self.meter.inc_encode_error_block(1);
                                self.record_drop_shard("serialization_error", idx, 1);
                                if maybe_t0.is_some() {
                                    debug!(target = "ultra.encode", "block encode failed: {e}");
                                }
                            }
                        }
                    }
                } else {
                    self.record_drop_shard("no_buf", idx, 1);
                }
            }
        }
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> GeyserResult<()> {
        if !self.streams.slots {
            return Ok(());
        }
        let st = match status {
            SlotStatus::Processed => 0u8,
            SlotStatus::Confirmed => 1,
            SlotStatus::Rooted => 2,
            SlotStatus::FirstShredReceived => 3,
            SlotStatus::Completed => 4,
            SlotStatus::CreatedBank => 5,
            SlotStatus::Dead(_) => 6,
        };
        let rec = Record::Slot {
            slot,
            parent,
            status: st,
        };
        let idx = match self.writer_index_for_u64(slot) { Some(i) => i, None => return Ok(()) };
        if let Some(pool) = self.pools.get(idx) {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                    let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                    let cap_hint = self
                        .cfg
                        .as_ref()
                        .map(|c| c.pool_default_cap)
                        .unwrap_or(64 * 1024)
                        .saturating_sub(12);
                    let mut opts = EncodeOptions::latency_uds();
                    opts.payload_hint = Some(cap_hint);
                    match encode_into_with(&rec, buf, opts) {
                        Ok(()) => {
                            if let Some(t0) = maybe_t0 {
                                histogram!("ultra_encode_ns", "kind" => "slot")
                                    .record(t0.elapsed().as_nanos() as f64);
                                if let Some(sz) = pb.as_slice().map(|s| s.len()) {
                                    histogram!("ultra_record_bytes", "kind" => "slot")
                                        .record(sz as f64);
                                    if let Some(cfg) = &self.cfg {
                                        if sz > cfg.pool_default_cap {
                                            drop(pb);
                                            self.record_drop_shard("oversize", idx, 1);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            match self.try_enqueue(idx, pb) {
                                Ok(()) => {
                                    self.record_queue_depth(idx);
                                    self.record_enqueue_success();
                                }
                                Err(buf) => {
                                    drop(buf);
                                    self.record_drop_shard("backpressure", idx, 1);
                                }
                            }
                        }
                        Err(e) => {
                            self.meter.inc_encode_error_slot(1);
                            self.record_drop_shard("serialization_error", idx, 1);
                            if maybe_t0.is_some() {
                                debug!(target = "ultra.encode", "slot encode failed: {e}");
                            }
                        }
                    }
                }
            } else {
                self.record_drop_shard("no_buf", idx, 1);
            }
        }
        Ok(())
    }

    fn notify_end_of_startup(&self) -> GeyserResult<()> {
        let idx = self.writer_index_for_u64(0).unwrap_or(0);
        if let Some(pool) = self.pools.get(idx) {
            if let Some(mut pb) = pool.try_get() {
                if let Some(buf) = pb.inner_mut() {
                    let v = self.metrics_seq.fetch_add(1, Ordering::Relaxed);
                    let maybe_t0 = if (v & 0xFF) == 0 { Some(Instant::now()) } else { None };
                    let cap_hint = self
                        .cfg
                        .as_ref()
                        .map(|c| c.pool_default_cap)
                        .unwrap_or(64 * 1024)
                        .saturating_sub(12);
                    let mut opts = EncodeOptions::latency_uds();
                    opts.payload_hint = Some(cap_hint);
                    match encode_into_with(&Record::EndOfStartup, buf, opts) {
                        Ok(()) => {
                            if let Some(t0) = maybe_t0 {
                                histogram!("ultra_encode_ns", "kind" => "eos")
                                    .record(t0.elapsed().as_nanos() as f64);
                                if let Some(sz) = pb.as_slice().map(|s| s.len()) {
                                    histogram!("ultra_record_bytes", "kind" => "eos")
                                        .record(sz as f64);
                                    if let Some(cfg) = &self.cfg {
                                        if sz > cfg.pool_default_cap {
                                            drop(pb);
                                            self.record_drop_shard("oversize", idx, 1);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            match self.try_enqueue(idx, pb) {
                                Ok(()) => {
                                    self.record_queue_depth(idx);
                                    self.record_enqueue_success();
                                }
                                Err(buf) => {
                                    drop(buf);
                                    self.record_drop_shard("backpressure", idx, 1);
                                }
                            }
                        }
                        Err(e) => {
                            self.meter.inc_encode_error_eos(1);
                            self.record_drop_shard("serialization_error", idx, 1);
                            if maybe_t0.is_some() {
                                debug!(target = "ultra.encode", "eos encode failed: {e}");
                            }
                        }
                    }
                }
            } else {
                self.record_drop_shard("no_buf", idx, 1);
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

fn shard_index(bytes: &[u8], modulo: usize) -> usize {
    if modulo <= 1 {
        return 0;
    }
    let mut hash = Wrapping(0xcbf29ce484222325u64);
    for byte in bytes {
        hash ^= Wrapping(*byte as u64);
        hash *= Wrapping(0x100000001b3);
    }
    (hash.0 as usize) % modulo
}

fn shard_from_u64(value: u64, modulo: usize) -> usize {
    if modulo <= 1 {
        return 0;
    }
    shard_index(&value.to_le_bytes(), modulo)
}

#[cfg(test)]
mod tests {
    use super::{config, shard_from_u64, shard_index, DropPolicy, Streams, Ultra};
    use std::{thread, time::Duration};
    use tempfile::tempdir;

    fn build_config(socket_path: String) -> config::Config {
        config::Config {
            socket_path,
            queue_capacity: 4096,
            queue_drop_policy: DropPolicy::DropNewest,
            batch_max: 512,
            batch_bytes_max: 64 * 1024,
            flush_after_ms: 0,
            write_timeout_ms: 200,
            pin_core: None,
            rt_priority: None,
            sched_policy: None,
            histogram_sample_log2: 8,
            streams: Streams {
                accounts: true,
                transactions: true,
                blocks: true,
                slots: true,
            },
            metrics: None,
            pool_items_max: Some(256),
            memory_budget_bytes: Some(256 * 64 * 1024),
            writer_threads: 4,
            shed_throttle_ms: 25,
            write_spin_cap_us: 300,
            write_sleep_backoff_us: 750,
            use_seqpacket: cfg!(target_os = "linux"),
            lock_memory: false,
        }
    }

    #[test]
    fn config_validate_populates_defaults() {
        let dir = tempdir().expect("tempdir");
        let sock = dir.path().join("ultra.sock");
        let cfg = build_config(sock.to_string_lossy().to_string());
        let validated = cfg.validate().expect("config should validate");
        assert_eq!(validated.queue_capacity, 4096);
        assert_eq!(validated.writer_threads, 4);
        assert_eq!(validated.pool_items_max, 256);
        assert_eq!(validated.pool_default_cap, 64 * 1024);
        assert_eq!(validated.queue_drop_policy, DropPolicy::DropNewest);
    }

    #[test]
    fn config_validate_rejects_relative_socket_path() {
        let cfg = build_config("relative.sock".to_string());
        let err = cfg.validate().expect_err("relative paths must fail");
        assert!(err.to_string().contains("socket_path must be absolute"));
    }

    #[test]
    fn config_validate_rejects_small_batch_bytes() {
        let dir = tempdir().expect("tempdir");
        let sock = dir.path().join("ultra.sock");
        let mut cfg = build_config(sock.to_string_lossy().to_string());
        cfg.batch_bytes_max = 512; // below 1 KiB minimum
        let err = cfg
            .validate()
            .expect_err("batch_bytes_max below minimum should fail");
        assert!(err.to_string().contains("batch_bytes_max out of range"));
    }

    #[test]
    fn shard_index_consistent_with_u64_variant() {
        for modulo in [1usize, 2, 8, 16, 1024] {
            for value in [0u64, 1, 42, u64::MAX - 1] {
                let idx_from_u64 = shard_from_u64(value, modulo);
                let idx_from_bytes = shard_index(&value.to_le_bytes(), modulo);
                assert_eq!(idx_from_u64, idx_from_bytes);
                assert!(idx_from_u64 < modulo.max(1));
            }
        }
    }

    #[test]
    fn ultra_mark_shed_account_clears_after_ttl() {
        let dir = tempdir().expect("tempdir");
        let sock = dir.path().join("ultra.sock");
        let mut cfg = build_config(sock.to_string_lossy().to_string());
        cfg.shed_throttle_ms = 1;
        let validated = cfg.validate().expect("config should validate");

        let mut ultra = Ultra::new();
        ultra.cfg = Some(validated);
        let key = [9u8; 32];
        ultra.mark_shed_account(key);
        assert!(ultra.is_account_shed(&key));
        thread::sleep(Duration::from_millis(2));
        assert!(!ultra.is_account_shed(&key));
    }
}
