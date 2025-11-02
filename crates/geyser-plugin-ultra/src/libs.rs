// crates/geyser-plugin-ultra/src/lib.rs
#![forbid(unsafe_code)]
mod config;
mod writer;

use config::{Config, Streams};
use crossbeam_channel::{bounded, Sender};
use faststreams::{encode_record, AccountUpdate, BlockMeta, Record, TxUpdate};
use metrics::{counter};
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::Mutex;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as GeyserResult, SlotStatus,
};
use solana_sdk::pubkey::Pubkey;
use std::fs::File;
use std::io::Read;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

#[derive(Debug)]
struct Ultra {
    cfg: Option<Config>,
    tx: Option<Sender<Vec<u8>>>,
    shutdown: Arc<AtomicBool>,
    streams: Streams,
    logger_set: Mutex<bool>,
}

impl Ultra {
    fn new() -> Self {
        Self {
            cfg: None,
            tx: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            streams: Streams { accounts: true, transactions: true, blocks: true, slots: true },
            logger_set: Mutex::new(false),
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
            log::set_logger(logger).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
            *set = true;
        }
        Ok(())
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> GeyserResult<()> {
        // Read JSON config
        let mut f = File::open(config_file).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        let mut s = String::new();
        f.read_to_string(&mut s).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        let cfg: Config = serde_json::from_str(&s).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;

        // Metrics
        if let Some(m) = &cfg.metrics {
            if let Some(addr) = &m.listen_addr {
                let addr = addr.clone();
                std::thread::spawn(move || {
                    let _ = PrometheusBuilder::new().with_http_listener(addr.parse().unwrap()).install();
                });
            }
        }

        let (tx, rx) = bounded::<Vec<u8>>(cfg.queue_capacity);

        // Writer thread
        let wcfg = cfg.clone();
        let shutdown = self.shutdown.clone();
        thread::Builder::new().name("ultra-writer".into()).spawn(move || {
            writer::run_writer(wcfg, rx, &shutdown)
        }).map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;

        self.streams = cfg.streams.clone();
        self.tx = Some(tx);
        self.cfg = Some(cfg);

        Ok(())
    }

    fn on_unload(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.tx = None;
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
        let try_pk = Pubkey::try_from(pubkey).ok();
        let try_owner = Pubkey::try_from(owner).ok();
        let rec = Record::Account(AccountUpdate {
            slot,
            is_startup,
            pubkey: try_pk.map(|p| p.to_bytes()).unwrap_or([0;32]),
            lamports,
            owner: try_owner.map(|p| p.to_bytes()).unwrap_or([0;32]),
            executable,
            rent_epoch,
            data: data.to_vec(),
        });
        if let Ok(buf) = encode_record(&rec) {
            if tx.try_send(buf).is_err() {
                counter!("ultra_dropped_total", "kind" => "account").increment(1);
            } else {
                counter!("ultra_enqueued_total", "kind" => "account").increment(1);
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
                let err = t.transaction_status_meta.and_then(|m| m.status.clone().err()).map(|e| format!("{:?}", e));
                (sig, vote, err)
            }
            _ => return Ok(())
        };
        let rec = Record::Tx(TxUpdate {
            slot,
            signature_b58: sig.to_string(),
            err: err_opt,
            vote: is_vote,
        });
        if let Ok(buf) = encode_record(&rec) {
            if tx.try_send(buf).is_err() {
                counter!("ultra_dropped_total", "kind" => "tx").increment(1);
            } else {
                counter!("ultra_enqueued_total", "kind" => "tx").increment(1);
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
                blockhash_b58: b.blockhash.map(|h| h.to_string()),
                parent_slot: b.parent_slot.unwrap_or(0),
                rewards_len: b.rewards.as_deref().map(|r| r.len()).unwrap_or(0) as u32,
                block_time_unix: b.block_time,
                leader: b.leader.as_ref().map(|p| p.to_string()),
            });
            if let Ok(buf) = encode_record(&rec) {
                if tx.try_send(buf).is_err() {
                    counter!("ultra_dropped_total", "kind" => "block").increment(1);
                } else {
                    counter!("ultra_enqueued_total", "kind" => "block").increment(1);
                }
            }
        }
        Ok(())
    }

    fn update_slot_status(&self, slot: u64, parent: Option<u64>, status: SlotStatus) -> GeyserResult<()> {
        if !self.streams.slots { return Ok(()); }
        let tx = match &self.tx { Some(t) => t, None => return Ok(()), };
        let st = match status {
            SlotStatus::Processed => 0u8,
            SlotStatus::Confirmed => 1,
            SlotStatus::Rooted => 2,
        };
        let rec = Record::Slot { slot, parent, status: st };
        if let Ok(buf) = encode_record(&rec) {
            if tx.try_send(buf).is_err() {
                counter!("ultra_dropped_total", "kind" => "slot").increment(1);
            } else {
                counter!("ultra_enqueued_total", "kind" => "slot").increment(1);
            }
        }
        Ok(())
    }

    fn notify_end_of_startup(&self) -> GeyserResult<()> {
        let tx = match &self.tx { Some(t) => t, None => return Ok(()), };
        if let Ok(buf) = encode_record(&Record::EndOfStartup) {
            let _ = tx.try_send(buf);
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