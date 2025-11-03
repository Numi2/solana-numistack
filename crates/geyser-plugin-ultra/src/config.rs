// crates/geyser-plugin-ultra/src/config.rs
use serde::Deserialize;
use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use std::fs;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

const ONE_MIB: usize = 1 << 20;
#[cfg(target_os = "linux")]
const UDS_PATH_MAX: usize = 108;
#[cfg(not(target_os = "linux"))]
const UDS_PATH_MAX: usize = 104; // conservative default for BSD/macOS

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Streams {
    pub accounts: bool,
    pub transactions: bool,
    pub blocks: bool,
    pub slots: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub socket_path: String,
    #[serde(default = "default_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_batch")]
    pub batch_max: usize,
    #[serde(default = "default_batch_bytes")]
    pub batch_bytes_max: usize,
    #[serde(default = "default_flush_after_ms")]
    pub flush_after_ms: u64,
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    #[serde(default)]
    pub pin_core: Option<usize>,
    #[serde(default)]
    pub rt_priority: Option<i32>,
    #[serde(default)]
    pub sched_policy: Option<String>,
    #[serde(default = "default_histogram_sample_log2")]
    pub histogram_sample_log2: u8,
    #[serde(default = "default_streams")]
    pub streams: Streams,
    #[serde(default)]
    pub metrics: Option<Metrics>,
    #[serde(default)]
    pub pool_items_max: Option<usize>,
    #[serde(default)]
    pub memory_budget_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub listen_addr: Option<String>, // e.g. "0.0.0.0:9977"
}

fn default_capacity() -> usize {
    8192
}
fn default_batch() -> usize {
    512
}
fn default_batch_bytes() -> usize {
    2 * 1024 * 1024
}
fn default_flush_after_ms() -> u64 {
    0
}
fn default_write_timeout_ms() -> u64 {
    200
}
fn default_histogram_sample_log2() -> u8 {
    8
}
fn default_streams() -> Streams {
    Streams { accounts: true, transactions: true, blocks: true, slots: true }
}

#[derive(Debug, Clone)]
pub struct ValidatedConfig {
    pub socket_path: PathBuf,
    pub queue_capacity: usize,
    pub batch_max: usize,
    pub batch_bytes_max: usize,
    pub flush_after_ms: u64,
    pub write_timeout_ms: u64,
    pub pin_core: Option<usize>,
    pub rt_priority: Option<i32>,
    pub sched_policy: Option<String>,
    pub histogram_sample_log2: u8,
    pub streams: Streams,
    pub metrics: Option<Metrics>,
    pub pool_items_max: usize,
    pub pool_default_cap: usize,
    pub memory_budget_bytes: Option<usize>,
}

impl Config {
    pub fn validate(&self) -> Result<ValidatedConfig> {
        // socket_path: absolute, parent exists or creatable, length limit
        let socket_path = PathBuf::from(&self.socket_path);
        if !socket_path.is_absolute() {
            return Err(anyhow!("socket_path must be absolute: {}", self.socket_path));
        }
        let parent = socket_path.parent().ok_or_else(|| anyhow!("socket_path has no parent"))?;
        if !parent.exists() {
            fs::create_dir_all(parent).map_err(|e| anyhow!("failed to create parent dir {:?}: {}", parent, e))?;
        }
        let path_len = socket_path.as_os_str().as_bytes().len();
        if path_len > UDS_PATH_MAX {
            return Err(anyhow!("socket_path length {} exceeds platform max {}", path_len, UDS_PATH_MAX));
        }

        // queue_capacity: 1..=1_000_000
        let queue_capacity = self.queue_capacity.clamp(1, 1_000_000);
        if !(1..=1_000_000).contains(&self.queue_capacity) {
            return Err(anyhow!("queue_capacity out of range: {} (allowed 1..=1_000_000)", self.queue_capacity));
        }

        // batch_bytes_max: 1 KiB..=64 MiB
        let min_b = 1024usize;
        let max_b = 64 * 1024 * 1024usize;
        if self.batch_bytes_max < min_b || self.batch_bytes_max > max_b {
            return Err(anyhow!("batch_bytes_max out of range: {} (allowed 1KiB..=64MiB)", self.batch_bytes_max));
        }

        // pool sizing
        let logical_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let default_pool_items = std::cmp::min(queue_capacity, 4 * logical_cpus);
        let pool_items_max = self.pool_items_max.unwrap_or(default_pool_items);
        if pool_items_max == 0 || pool_items_max > queue_capacity {
            return Err(anyhow!("pool_items_max must be in 1..=queue_capacity ({}), got {}", queue_capacity, pool_items_max));
        }
        let pool_default_cap = std::cmp::min(self.batch_bytes_max, ONE_MIB);

        // optional memory budget
        if let Some(budget) = self.memory_budget_bytes {
            let ceiling = pool_items_max.saturating_mul(pool_default_cap);
            if ceiling > budget {
                return Err(anyhow!("memory ceiling {} exceeds memory_budget_bytes {} (items={} * cap={})", ceiling, budget, pool_items_max, pool_default_cap));
            }
        }

        Ok(ValidatedConfig {
            socket_path,
            queue_capacity,
            batch_max: self.batch_max,
            batch_bytes_max: self.batch_bytes_max,
            flush_after_ms: self.flush_after_ms,
            write_timeout_ms: self.write_timeout_ms,
            pin_core: self.pin_core,
            rt_priority: self.rt_priority,
            sched_policy: self.sched_policy.clone(),
            histogram_sample_log2: self.histogram_sample_log2,
            streams: self.streams.clone(),
            metrics: self.metrics.clone(),
            pool_items_max,
            pool_default_cap,
            memory_budget_bytes: self.memory_budget_bytes,
        })
    }
}