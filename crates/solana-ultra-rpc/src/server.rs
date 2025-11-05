// Numan Thabit
//! Top-level orchestration for the ultra RPC server.

use std::sync::Arc;
use std::thread::JoinHandle as ThreadJoinHandle;

use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, routing::get, Router};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::cache::AccountCache;
use crate::config::UltraRpcConfig;
use crate::ingest;
use crate::ingest::geyser;
use crate::rpc::{RpcRouter, SlotTracker};
use crate::telemetry::Telemetry;
use crate::transport::QuicRpcServer;

/// Running server handle, used to initiate shutdown.
pub struct UltraRpcServerHandle {
    quic: Option<QuicRpcServer>,
    tasks: Vec<JoinHandle<anyhow::Result<()>>>,
    canceller: CancellationToken,
    metrics_thread: Option<ThreadJoinHandle<()>>,
}

impl UltraRpcServerHandle {
    /// Gracefully stop the server and wait for background tasks.
    pub async fn shutdown(mut self) -> Result<()> {
        self.canceller.cancel();
        if let Some(quic) = self.quic.take() {
            quic.close().await;
        }
        for handle in self.tasks.drain(..) {
            handle.abort();
        }
        if let Some(thread) = self.metrics_thread.take() {
            let _ = thread.join();
        }
        Ok(())
    }
}

/// Spawn the RPC server according to the provided configuration.
pub async fn launch_server(config: UltraRpcConfig) -> Result<UltraRpcServerHandle> {
    config.validate()?;

    let cache = Arc::new(AccountCache::new(config.shard_count));
    let telemetry = Arc::new(Telemetry::init("solana-ultra-rpc")?);
    let metrics = telemetry.rpc_metrics();
    let slot_tracker = Arc::new(SlotTracker::new());

    info!(addr = %config.snapshot_socket.display(), "hydrating cache from snapshot");
    let snapshot_stream = geyser::connect_snapshot_stream(&config.snapshot_socket).await?;
    ingest::prewarm_from_snapshot(&cache, &slot_tracker, snapshot_stream)
        .await
        .context("failed to hydrate cache from snapshot")?;

    info!(addr = %config.aggregator_socket.display(), "connecting delta stream");
    let delta_stream = geyser::connect_delta_stream(&config.aggregator_socket).await?;

    let router = Arc::new(RpcRouter::new(
        cache.clone(),
        metrics.clone(),
        slot_tracker.clone(),
    ));
    let quic = QuicRpcServer::bind(&config, router.clone()).await?;

    let canceller = CancellationToken::new();
    let mut tasks = Vec::new();

    // Delta application task.
    let delta_cancel = canceller.clone();
    tasks.push(tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = delta_cancel.cancelled() => Ok(()),
            res = ingest::apply_deltas(cache, slot_tracker, delta_stream) => res,
        }
    }));

    // Metrics endpoint on a dedicated thread with its own runtime.
    let telemetry_state = telemetry.clone();
    let metrics_addr = config.metrics_bind;
    let metrics_cancel = canceller.clone();
    let metrics_thread = std::thread::Builder::new()
        .name("metrics".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("metrics runtime");
            rt.block_on(async move {
                match tokio::net::TcpListener::bind(metrics_addr).await {
                    Ok(listener) => {
                        info!(addr = %metrics_addr, "metrics endpoint ready");
                        let app = Router::new()
                            .route("/metrics", get(metrics_handler))
                            .with_state(telemetry_state);
                        let serve = axum::serve(listener, app.into_make_service());
                        tokio::select! {
                            _ = metrics_cancel.cancelled() => {},
                            _ = serve => {},
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "failed to bind metrics endpoint");
                    }
                }
            });
        })
        .ok();

    Ok(UltraRpcServerHandle {
        quic: Some(quic),
        tasks,
        canceller,
        metrics_thread,
    })
}

async fn metrics_handler(State(telemetry): State<Arc<Telemetry>>) -> (StatusCode, String) {
    match telemetry.render_prometheus() {
        Ok(body) => (StatusCode::OK, body),
        Err(err) => {
            warn!(error = %err, "failed to gather metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
        }
    }
}
