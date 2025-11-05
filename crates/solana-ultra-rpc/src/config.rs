// Numan Thabit 2021
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the ultra RPC server.
#[derive(Clone, Debug)]
pub struct UltraRpcConfig {
    /// QUIC socket to listen on for JSON-RPC requests.
    pub rpc_bind: SocketAddr,
    /// Prometheus metrics endpoint bind address.
    pub metrics_bind: SocketAddr,
    /// Path to the Unix domain socket exposed by `ultra-aggregator` for live deltas.
    pub aggregator_socket: PathBuf,
    /// Path to the snapshot stream (usually another UDS) for bootstrap.
    pub snapshot_socket: PathBuf,
    /// Size of the hot cache shard vector (power of two, e.g. 64).
    pub shard_count: usize,
    /// Maximum number of inflight QUIC bi-directional streams per connection.
    pub max_streams: u32,
    /// Deadline for adaptive batching windows.
    pub max_batch_delay: Duration,
    /// Target maximum batch size per RPC method.
    pub max_batch_size: usize,
    /// Maximum number of buffered requests per method queue.
    pub queue_depth: usize,
    /// Optional upstream HTTP endpoint for cache misses.
    pub fallback_url: Option<String>,
}

impl Default for UltraRpcConfig {
    fn default() -> Self {
        Self {
            rpc_bind: "0.0.0.0:8899".parse().expect("valid listen addr"),
            metrics_bind: "127.0.0.1:9898".parse().expect("valid metrics addr"),
            aggregator_socket: PathBuf::from("/tmp/ultra-aggregator.sock"),
            snapshot_socket: PathBuf::from("/tmp/ultra-aggregator.snapshot.sock"),
            shard_count: 128,
            max_streams: 4_096,
            max_batch_delay: Duration::from_micros(150),
            max_batch_size: 128,
            queue_depth: 16_384,
            fallback_url: None,
        }
    }
}

impl UltraRpcConfig {
    /// Ensure derived values remain within sane bounds.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.shard_count.is_power_of_two(),
            "shard_count must be a power of two"
        );
        anyhow::ensure!(self.max_batch_size > 0, "max_batch_size must be > 0");
        anyhow::ensure!(
            self.queue_depth >= self.max_batch_size,
            "queue depth should cover at least one batch"
        );
        anyhow::ensure!(
            self.max_streams > 0,
            "must allow at least one concurrent stream"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> UltraRpcConfig {
        UltraRpcConfig::default()
    }

    #[test]
    fn validate_accepts_default_config() {
        base_config()
            .validate()
            .expect("default config should validate");
    }

    #[test]
    fn validate_rejects_non_power_of_two_shards() {
        let mut cfg = base_config();
        cfg.shard_count = 3;
        let err = cfg
            .validate()
            .expect_err("non power-of-two shard count must fail");
        assert!(err.to_string().contains("shard_count"));
    }

    #[test]
    fn validate_requires_queue_depth_covering_batches() {
        let mut cfg = base_config();
        cfg.max_batch_size = 512;
        cfg.queue_depth = 128;
        let err = cfg
            .validate()
            .expect_err("queue depth smaller than batch should fail");
        assert!(err
            .to_string()
            .contains("queue depth should cover at least one batch"));
    }

    #[test]
    fn validate_requires_nonzero_streams() {
        let mut cfg = base_config();
        cfg.max_streams = 0;
        let err = cfg
            .validate()
            .expect_err("max_streams == 0 must fail validation");
        assert!(err
            .to_string()
            .contains("must allow at least one concurrent stream"));
    }

    #[test]
    fn validate_allows_customized_parameters() {
        let mut cfg = base_config();
        cfg.shard_count = 32;
        cfg.max_batch_size = 64;
        cfg.queue_depth = 4_096;
        cfg.max_streams = 1_024;
        cfg.validate().expect("custom config should validate");
    }
}
