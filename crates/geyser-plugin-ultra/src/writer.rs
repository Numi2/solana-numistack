// crates/geyser-plugin-ultra/src/writer.rs
use crate::Config;
use crossbeam_channel::Receiver;
use faststreams::write_all_vectored;
use metrics::{counter, histogram};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};
use socket2::SockRef;

pub fn run_writer(cfg: Config, rx: Receiver<Vec<u8>>, shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>) {
    let mut backoff = Duration::from_millis(50);
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        // Ensure stale socket path is gone
        if Path::new(&cfg.socket_path).exists() {
            // If it's a socket created by server, we just connect; no unlink here.
        }

        match UnixStream::connect(&cfg.socket_path) {
            Ok(mut stream) => {
                stream.set_nonblocking(false).ok();
                stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
                // Best-effort: increase send buffer to accommodate large batches
                let sockref = SockRef::from(&stream);
                let _ = sockref.set_send_buffer_size(cfg.batch_bytes_max);
                // Batch & drain loop
                let mut batch: Vec<Vec<u8>> = Vec::with_capacity(cfg.batch_max);
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    // blocking recv
                    match rx.recv_timeout(Duration::from_millis(1)) {
                        Ok(first) => {
                            let mut size = first.len();
                            batch.push(first);
                            let start = Instant::now();
                            while batch.len() < cfg.batch_max && size < cfg.batch_bytes_max {
                                if start.elapsed() >= Duration::from_millis(cfg.flush_after_ms) { break; }
                                match rx.try_recv() {
                                    Ok(m) => {
                                        size += m.len();
                                        if size > cfg.batch_bytes_max { break; }
                                        batch.push(m);
                                    }
                                    Err(_) => break,
                                }
                            }
                            let start = Instant::now();
                            if let Err(e) = write_all_vectored(&mut stream, &batch) {
                                eprintln!("geyser-plugin-ultra: write error: {e}");
                                break;
                            }
                            let elapsed = start.elapsed().as_nanos() as f64 / 1_000_000.0;
                            counter!("ultra_bytes_sent_total").increment(size as u64);
                            counter!("ultra_batches_sent_total").increment(1);
                            histogram!("ultra_batch_ms").record(elapsed);
                            batch.clear();
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                    }
                }
                // Broken pipe; reconnect
                thread::sleep(Duration::from_millis(100));
                backoff = Duration::from_millis(50);
            }
            Err(err) => {
                eprintln!("geyser-plugin-ultra: connect {} failed: {err}", cfg.socket_path);
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }
        };
    }
}
        