// Numan Thabit 2025
// crates/geyser-plugin-ultra/src/writer.rs
use crate::config::ValidatedConfig;
use crate::meter::Meter;
use crate::pool::PooledBuf;
use crate::queue::Consumer;
use faststreams::write_all_vectored_slices;
#[cfg(target_os = "linux")]
use libc;
use metrics::{counter, gauge, histogram};
use smallvec::SmallVec;
use socket2::SockRef;
use std::cell::Cell;
use std::io::IoSlice;
use std::os::unix::net::UnixStream;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info};

enum PopOutcome<T> {
    Item(T),
    Timeout,
    Shutdown,
}

/// Writer thread: drains frames from the channel and writes to the UDS with minimal latency.
/// NOTE: For best results pin this thread to an isolated CPU core (see comment below).
pub fn run_writer(
    _writer_index: usize,
    cfg: ValidatedConfig,
    queue: Consumer<PooledBuf>,
    shutdown: &Arc<AtomicBool>,
    meter: Arc<Meter>,
    core_affinity: Option<core_affinity::CoreId>,
) {
    // NOTE: For lowest tail latency in production, consider isolating the pinned core from the
    // general scheduler using kernel boot parameters, e.g. isolcpus=nohz,managed_irq,domain,1
    // and moving other background daemons off that core. This complements RT scheduling below.
    {
        if let Some(core_id) = core_affinity {
            let _ = core_affinity::set_for_current(core_id);
        }
        #[cfg(target_os = "linux")]
        if core_affinity.is_none() {
            if let Some(pc) = cfg.pin_core {
                let _ = core_affinity::set_for_current(core_affinity::CoreId { id: pc });
            }
        }
        #[cfg(target_os = "linux")]
        if let Some(prio) = cfg.rt_priority {
            let policy_str = cfg.sched_policy.as_deref().unwrap_or("fifo");
            let policy = if policy_str.eq_ignore_ascii_case("rr") {
                libc::SCHED_RR
            } else if policy_str.eq_ignore_ascii_case("fifo") {
                libc::SCHED_FIFO
            } else {
                error!(
                    target = "ultra.writer",
                    "unknown sched_policy '{policy_str}', falling back to FIFO"
                );
                libc::SCHED_FIFO
            };
            let param = libc::sched_param {
                sched_priority: prio,
            };
            unsafe {
                if libc::sched_setscheduler(0, policy, &param) != 0 {
                    let err = std::io::Error::last_os_error();
                    error!(
                        target = "ultra.writer",
                        "failed to set RT scheduler ({policy_str}, prio {prio}): {err}"
                    );
                }
            }
        }
    }
    thread_local! {
        static HISTO_SEQ: Cell<u64> = const { Cell::new(0) };
    }
    const SPIN_SLEEP: Duration = Duration::from_micros(50);
    const BUSY_SPINS: usize = 256;

    fn pop_with_timeout(
        queue: &Consumer<PooledBuf>,
        timeout: Duration,
        shutdown: &AtomicBool,
    ) -> PopOutcome<PooledBuf> {
        let start = Instant::now();
        let mut spins = 0usize;
        loop {
            if let Some(item) = queue.pop() {
                return PopOutcome::Item(item);
            }
            if shutdown.load(Ordering::Relaxed) {
                return PopOutcome::Shutdown;
            }
            if timeout != Duration::ZERO && start.elapsed() >= timeout {
                return PopOutcome::Timeout;
            }
            if spins < BUSY_SPINS {
                spins = spins.wrapping_add(1);
                std::hint::spin_loop();
                continue;
            }
            thread::sleep(SPIN_SLEEP);
        }
    }
    // Histogram sampling mask: (2^log2 - 1). Default ~1/256.
    let histo_mask: u64 = (1u64 << (cfg.histogram_sample_log2 as u32)) - 1;
    let mut backoff = Duration::from_millis(50);
    let mut backoff_seq: u64 = 0;
    let mut last_connect_log: Option<Instant> = None;
    let mut last_logged_backoff: Duration = Duration::from_millis(0);
    gauge!("ultra_writer_alive").set(1.0);
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        counter!("ultra_connect_attempts_total").increment(1);
        match UnixStream::connect(&cfg.socket_path) {
            Ok(mut stream) => {
                counter!("ultra_connect_success_total").increment(1);
                stream.set_nonblocking(false).ok();
                stream
                    .set_write_timeout(Some(Duration::from_millis(cfg.write_timeout_ms)))
                    .ok();
                // Best-effort: increase send buffer to accommodate large batches
                let sockref = SockRef::from(&stream);
                let _ = sockref.set_send_buffer_size(cfg.batch_bytes_max);
                #[cfg(any(target_os = "macos", target_os = "ios"))]
                {
                    let _ = sockref.set_nosigpipe(true);
                }
                if let Ok(effective) = sockref.send_buffer_size() {
                    info!(
                        target = "ultra.writer",
                        "send buffer size ~{} bytes", effective
                    );
                }
                // Batch & drain loop
                let mut batch: Vec<PooledBuf> = Vec::with_capacity(cfg.batch_max);
                let mut cur_flush_after_ms = cfg.flush_after_ms;
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    // Shutdown-responsive first receive
                    match pop_with_timeout(&queue, Duration::from_millis(50), shutdown) {
                        PopOutcome::Item(first) => {
                            let mut size = first.as_slice().map(|s| s.len()).unwrap_or(0);
                            batch.push(first);
                            let start = Instant::now();
                            let deadline = if cfg.flush_after_ms > 0 {
                                Some(start + Duration::from_millis(cfg.flush_after_ms))
                            } else {
                                None
                            };
                            while batch.len() < cfg.batch_max && size < cfg.batch_bytes_max {
                                if let Some(dl) = deadline {
                                    if Instant::now() >= dl {
                                        break;
                                    }
                                }
                                match queue.pop() {
                                    Some(m) => {
                                        let mlen = m.as_slice().map(|s| s.len()).unwrap_or(0);
                                        if size + mlen > cfg.batch_bytes_max {
                                            break;
                                        }
                                        size += mlen;
                                        batch.push(m);
                                        continue;
                                    }
                                    None => {
                                        if let Some(dl) = deadline {
                                            // Wait until the deadline for another message.
                                            let now = Instant::now();
                                            if now >= dl {
                                                break;
                                            }
                                            let remaining = dl.saturating_duration_since(now);
                                            match pop_with_timeout(&queue, remaining, shutdown) {
                                                PopOutcome::Item(m) => {
                                                    let mlen =
                                                        m.as_slice().map(|s| s.len()).unwrap_or(0);
                                                    if size + mlen > cfg.batch_bytes_max {
                                                        break;
                                                    }
                                                    size += mlen;
                                                    batch.push(m);
                                                    continue;
                                                }
                                                PopOutcome::Timeout => break,
                                                PopOutcome::Shutdown => return,
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                }
                            }
                            // Adaptive flush tuning: shrink delay under pressure, restore slowly
                            {
                                let depth = queue.len();
                                let cap = cfg.queue_capacity;
                                if depth * 100 / cap >= 75 {
                                    if cur_flush_after_ms > 0 {
                                        cur_flush_after_ms = (cur_flush_after_ms / 2).max(0);
                                    }
                                } else if cur_flush_after_ms < cfg.flush_after_ms {
                                    // restore slowly
                                    cur_flush_after_ms = (cur_flush_after_ms + 1).min(cfg.flush_after_ms);
                                }
                            }

                            let mut send_batch = std::mem::take(&mut batch);
                            let write_start = Instant::now();
                            let mut stall_ns: u128 = 0;
                            let mut write_ok = false;
                            {
                                let mut ios: SmallVec<[IoSlice<'_>; 64]> =
                                    SmallVec::with_capacity(send_batch.len().min(64));
                                for buf in &send_batch {
                                    if let Some(slice) = buf.as_slice() {
                                        ios.push(IoSlice::new(slice));
                                    }
                                }
                                let mut block_start: Option<Instant> = None;
                                let mut spun = false;
                                loop {
                                    match write_all_vectored_slices(&mut stream, ios.as_mut_slice()) {
                                        Ok(()) => {
                                            if let Some(start) = block_start.take() {
                                                stall_ns += start.elapsed().as_nanos();
                                            }
                                            write_ok = true;
                                            break;
                                        }
                                        Err(ref e)
                                            if e.kind() == std::io::ErrorKind::WouldBlock
                                                || e.kind() == std::io::ErrorKind::TimedOut =>
                                        {
                                            counter!("ultra_write_timeouts_total").increment(1);
                                            if block_start.is_none() {
                                                block_start = Some(Instant::now());
                                            }
                                            if !spun {
                                                counter!("ultra_write_backoff_total", "phase" => "spin")
                                                    .increment(1);
                                                let spin_until = Instant::now()
                                                    + Duration::from_micros(cfg.write_spin_cap_us);
                                                while Instant::now() < spin_until {
                                                    std::hint::spin_loop();
                                                }
                                                spun = true;
                                            } else {
                                                counter!("ultra_write_backoff_total", "phase" => "sleep")
                                                    .increment(1);
                                                thread::sleep(Duration::from_micros(
                                                    cfg.write_sleep_backoff_us,
                                                ));
                                            }
                                            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                                                break;
                                            }
                                            continue;
                                        }
                                        Err(e) => {
                                            if let Some(start) = block_start.take() {
                                                stall_ns += start.elapsed().as_nanos();
                                            }
                                            error!(target = "ultra.writer", "write error: {e}");
                                            counter!("ultra_write_errors_total").increment(1);
                                            // attribute drops by batch size
                                            counter!("ultra_write_error_drops_total").increment(send_batch.len() as u64);
                                            break;
                                        }
                                    }
                                }
                            }
                            let elapsed = write_start.elapsed();
                            if stall_ns > 0 && write_ok {
                                histogram!("ultra_write_block_us")
                                    .record(stall_ns as f64 / 1_000.0);
                            }
                            let elapsed_ms = elapsed.as_secs_f64() * 1_000.0;
                            if write_ok {
                                counter!("ultra_bytes_sent_total").increment(size as u64);
                                counter!("ultra_batches_sent_total").increment(1);
                                histogram!("ultra_batch_len").record(send_batch.len() as f64);
                                histogram!("ultra_batch_bytes").record(size as f64);
                                HISTO_SEQ.with(|seq| {
                                    let v = seq.get();
                                    seq.set(v.wrapping_add(1));
                                    if (v & histo_mask) == 0 {
                                        histogram!("ultra_batch_ms").record(elapsed_ms);
                                    }
                                });
                                let sent_count = send_batch.len() as u64;
                                meter.inc_processed(sent_count);
                            }
                            // Return frames to pool by dropping items in place
                            send_batch.clear();
                            batch = send_batch;
                        }
                        PopOutcome::Timeout => {
                            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                                break;
                            } else {
                                continue;
                            }
                        }
                        PopOutcome::Shutdown => return,
                    }
                }
                // Broken pipe; reconnect
                backoff = backoff
                    .max(Duration::from_millis(200))
                    .min(Duration::from_secs(2));
                meter.inc_reconnects(1);
                backoff_seq = backoff_seq.wrapping_add(1);
                let jitter = Duration::from_millis(backoff_seq & 0x1F).min(backoff / 2);
                let sleep_for = backoff + jitter;
                gauge!("ultra_reconnect_backoff_ms").set(sleep_for.as_millis() as f64);
                thread::sleep(sleep_for);
            }
            Err(err) => {
                let now = Instant::now();
                let should_log = last_connect_log.is_none()
                    || backoff != last_logged_backoff
                    || last_connect_log
                        .map(|t| now.duration_since(t) >= Duration::from_secs(30))
                        .unwrap_or(true);
                if should_log {
                    error!(
                        target = "ultra.writer",
                        "connect {} failed: {err} (backoff {:?})",
                        cfg.socket_path.display(),
                        backoff
                    );
                    last_connect_log = Some(now);
                    last_logged_backoff = backoff;
                }
                counter!("ultra_connect_errors_total").increment(1);
                backoff_seq = backoff_seq.wrapping_add(1);
                let jitter = Duration::from_millis(backoff_seq & 0x1F).min(backoff / 2);
                let sleep_for = backoff + jitter;
                gauge!("ultra_reconnect_backoff_ms").set(sleep_for.as_millis() as f64);
                thread::sleep(sleep_for);
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }
        };
    }
    gauge!("ultra_writer_alive").set(0.0);
}
