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
