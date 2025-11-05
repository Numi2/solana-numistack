use metrics::counter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::error;

#[derive(Debug, Default)]
pub struct Meter {
    pub enqueued_total: AtomicU64,
    pub dropped_queue_full_total: AtomicU64,
    pub dropped_no_buf_total: AtomicU64,
    pub encode_error_account_total: AtomicU64,
    pub encode_error_tx_total: AtomicU64,
    pub encode_error_block_total: AtomicU64,
    pub encode_error_slot_total: AtomicU64,
    pub encode_error_eos_total: AtomicU64,
    pub processed_total: AtomicU64,
    pub reconnects_total: AtomicU64,
    pub queue_depth_max: AtomicU64,
}

impl Meter {
    #[inline]
    pub fn inc_enqueued(&self, by: u64) {
        self.enqueued_total.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_dropped_queue_full(&self, by: u64) {
        self.dropped_queue_full_total
            .fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_dropped_no_buf(&self, by: u64) {
        self.dropped_no_buf_total.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_encode_error_account(&self, by: u64) {
        self.encode_error_account_total
            .fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_encode_error_tx(&self, by: u64) {
        self.encode_error_tx_total.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_encode_error_block(&self, by: u64) {
        self.encode_error_block_total
            .fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_encode_error_slot(&self, by: u64) {
        self.encode_error_slot_total
            .fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_encode_error_eos(&self, by: u64) {
        self.encode_error_eos_total.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_processed(&self, by: u64) {
        self.processed_total.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_reconnects(&self, by: u64) {
        self.reconnects_total.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    pub fn observe_queue_depth_max(&self, depth: u64) {
        let mut cur = self.queue_depth_max.load(Ordering::Relaxed);
        while depth > cur {
            match self.queue_depth_max.compare_exchange(
                cur,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

pub fn spawn_flusher(
    meter: Arc<Meter>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Option<thread::JoinHandle<()>> {
    match thread::Builder::new()
        .name("ultra-metrics-flusher".to_string())
        .spawn(move || {
            #[cfg(target_os = "linux")]
            unsafe {
                let _ = libc::setpriority(libc::PRIO_PROCESS, 0, 19);
            }
            let mut prev_enq = 0u64;
            let mut prev_drp_qf = 0u64;
            let mut prev_drp_nb = 0u64;
            let mut prev_enc_acc = 0u64;
            let mut prev_enc_tx = 0u64;
            let mut prev_enc_blk = 0u64;
            let mut prev_enc_slt = 0u64;
            let mut prev_enc_eos = 0u64;
            let mut prev_proc = 0u64;
            let mut prev_reco = 0u64;
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let cur_enq = meter.enqueued_total.load(Ordering::Relaxed);
                let cur_drp_qf = meter.dropped_queue_full_total.load(Ordering::Relaxed);
                let cur_drp_nb = meter.dropped_no_buf_total.load(Ordering::Relaxed);
                let cur_enc_acc = meter.encode_error_account_total.load(Ordering::Relaxed);
                let cur_enc_tx = meter.encode_error_tx_total.load(Ordering::Relaxed);
                let cur_enc_blk = meter.encode_error_block_total.load(Ordering::Relaxed);
                let cur_enc_slt = meter.encode_error_slot_total.load(Ordering::Relaxed);
                let cur_enc_eos = meter.encode_error_eos_total.load(Ordering::Relaxed);
                let cur_proc = meter.processed_total.load(Ordering::Relaxed);
                let cur_reco = meter.reconnects_total.load(Ordering::Relaxed);

                let de = cur_enq.saturating_sub(prev_enq);
                let ddqf = cur_drp_qf.saturating_sub(prev_drp_qf);
                let ddnb = cur_drp_nb.saturating_sub(prev_drp_nb);
                let dea = cur_enc_acc.saturating_sub(prev_enc_acc);
                let det = cur_enc_tx.saturating_sub(prev_enc_tx);
                let deb = cur_enc_blk.saturating_sub(prev_enc_blk);
                let des = cur_enc_slt.saturating_sub(prev_enc_slt);
                let dee = cur_enc_eos.saturating_sub(prev_enc_eos);
                let dp = cur_proc.saturating_sub(prev_proc);
                let dr = cur_reco.saturating_sub(prev_reco);

                if de > 0 {
                    counter!("ultra_enqueued_total").increment(de);
                }
                if ddqf > 0 {
                    counter!("ultra_dropped_total", "reason" => "queue_full").increment(ddqf);
                }
                if ddnb > 0 {
                    counter!("ultra_dropped_total", "reason" => "no_buf").increment(ddnb);
                }
                if dea > 0 {
                    counter!("ultra_encode_error_total", "kind" => "account").increment(dea);
                }
                if det > 0 {
                    counter!("ultra_encode_error_total", "kind" => "tx").increment(det);
                }
                if deb > 0 {
                    counter!("ultra_encode_error_total", "kind" => "block").increment(deb);
                }
                if des > 0 {
                    counter!("ultra_encode_error_total", "kind" => "slot").increment(des);
                }
                if dee > 0 {
                    counter!("ultra_encode_error_total", "kind" => "eos").increment(dee);
                }
                if dp > 0 {
                    counter!("ultra_processed_total").increment(dp);
                }
                if dr > 0 {
                    counter!("ultra_reconnects_total").increment(dr);
                }

                prev_enq = cur_enq;
                prev_drp_qf = cur_drp_qf;
                prev_drp_nb = cur_drp_nb;
                prev_enc_acc = cur_enc_acc;
                prev_enc_tx = cur_enc_tx;
                prev_enc_blk = cur_enc_blk;
                prev_enc_slt = cur_enc_slt;
                prev_enc_eos = cur_enc_eos;
                prev_proc = cur_proc;
                prev_reco = cur_reco;

                thread::sleep(Duration::from_millis(200));
            }
        }) {
        Ok(handle) => Some(handle),
        Err(err) => {
            error!(%err, "failed to spawn metrics flusher thread");
            None
        }
    }
}
