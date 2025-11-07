// Numan Thabit 2025
// crates/solana-ultra-rpc/src/bin/ultra_rpc_server.rs
use anyhow::Result;
use solana_ultra_rpc::config::UltraRpcConfig;
use solana_ultra_rpc::launch_server;
use std::path::PathBuf;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let rpc_bind = std::env::var("ULTRA_RPC_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8899".to_string())
        .parse()?;
    let metrics_bind = std::env::var("ULTRA_RPC_METRICS")
        .unwrap_or_else(|_| "127.0.0.1:9898".to_string())
        .parse()?;
    let aggregator_socket = PathBuf::from(
        std::env::var("ULTRA_RPC_DELTA")
            .unwrap_or_else(|_| "/tmp/ultra-aggregator.sock".to_string()),
    );
    let snapshot_socket = PathBuf::from(
        std::env::var("ULTRA_RPC_SNAPSHOT")
            .unwrap_or_else(|_| "/tmp/ultra-aggregator.snapshot.sock".to_string()),
    );
    let shard_count: usize = std::env::var("ULTRA_RPC_SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);
    let max_streams: u32 = std::env::var("ULTRA_RPC_MAX_STREAMS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4096);
    let quic_stream_recv_window: u64 = std::env::var("ULTRA_RPC_STREAM_RECV_WINDOW")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4 * 1024 * 1024);
    let quic_conn_recv_window: u64 = std::env::var("ULTRA_RPC_CONN_RECV_WINDOW")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024 * 1024);
    let quic_idle_ms: u64 = std::env::var("ULTRA_RPC_IDLE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30_000);
    let max_batch_delay_micros: u64 = std::env::var("ULTRA_RPC_BATCH_DELAY_US")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(150);
    let max_batch_size: usize = std::env::var("ULTRA_RPC_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);
    let queue_depth: usize = std::env::var("ULTRA_RPC_QUEUE_DEPTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16_384);
    let fallback_url = std::env::var("ULTRA_RPC_FALLBACK").ok();

    let cfg = UltraRpcConfig {
        rpc_bind,
        metrics_bind,
        aggregator_socket,
        snapshot_socket,
        shard_count,
        max_streams,
        max_batch_delay: std::time::Duration::from_micros(max_batch_delay_micros),
        max_batch_size,
        queue_depth,
        fallback_url,
        quic_stream_recv_window,
        quic_conn_recv_window,
        quic_max_idle_timeout: if quic_idle_ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(quic_idle_ms))
        },
    };
    let handle = launch_server(cfg).await?;
    info!("solana-ultra-rpc started");
    let _ = signal::ctrl_c().await;
    handle.shutdown().await?;
    Ok(())
}
