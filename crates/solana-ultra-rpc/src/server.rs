// Numan Thabit
//! Top-level orchestration for the ultra RPC server.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
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

    // Metrics endpoint.
    let telemetry_state = telemetry.clone();
    let metrics_addr = config.metrics_bind;
    let metrics_cancel = canceller.clone();
    tasks.push(tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(metrics_addr).await?;
        info!(addr = %metrics_addr, "metrics endpoint ready");
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(telemetry_state);
        tokio::select! {
            _ = metrics_cancel.cancelled() => Ok(()),
            res = axum::serve(listener, app.into_make_service()) => {
                res.map_err(|err| anyhow!(err))
            }
        }
    }));

    Ok(UltraRpcServerHandle {
        quic: Some(quic),
        tasks,
        canceller,
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
