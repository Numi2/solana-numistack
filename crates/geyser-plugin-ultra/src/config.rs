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
pub struct Config {
    pub libpath: String,
    pub socket_path: String,
    #[serde(default = "default_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_batch")]
    pub batch_max: usize,
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
    262_144
}
fn default_batch() -> usize {
    4096
}
fn default_streams() -> Streams {
    Streams { accounts: true, transactions: true, blocks: true, slots: true }
}