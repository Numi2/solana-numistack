// crates/geyser-plugin-ultra/src/config.rs
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Streams {
    pub accounts: bool,
    pub transactions: bool,
    pub blocks: bool,
    pub slots: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Config {
    pub libpath: String,
    pub socket_path: String,
    #[serde(default = "default_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_batch")]
    pub batch_max: usize,
    #[serde(default = "default_batch_bytes")]
    pub batch_bytes_max: usize,
    #[serde(default = "default_flush_after_ms")]
    pub flush_after_ms: u64,
    #[serde(default = "default_streams")]
    pub streams: Streams,
    #[serde(default)]
    pub metrics: Option<Metrics>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Metrics {
    pub listen_addr: Option<String>, // e.g. "0.0.0.0:9977"
}

fn default_capacity() -> usize {
    65_536
}
fn default_batch() -> usize {
    512
}
fn default_batch_bytes() -> usize {
    1 * 1024 * 1024
}
fn default_flush_after_ms() -> u64 {
    1
}
fn default_streams() -> Streams {
    Streams { accounts: true, transactions: true, blocks: true, slots: true }
}