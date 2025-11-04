// Numan Thabit 2025
use std::net::SocketAddr;

use anyhow::Result;
use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::{
    flamegraph::FlamegraphService,
    metrics::ObserverMetrics,
    state::{ObserverState, ValidatorSnapshot},
};

#[derive(Clone)]
struct AppState {
    metrics: ObserverMetrics,
    observers: ObserverState,
    flamegraph: Option<FlamegraphService>,
}

pub async fn serve(
    bind: SocketAddr,
    metrics: ObserverMetrics,
    observers: ObserverState,
    flamegraph: Option<FlamegraphService>,
) -> Result<()> {
    let state = AppState {
        metrics,
        observers,
        flamegraph,
    };

    let router = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/validators", get(validators_handler))
        .route("/healthz", get(health_handler))
        .route("/debug/flamegraph", get(flamegraph_handler))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    info!(bind = %bind, "HTTP server listening");
    let listener = TcpListener::bind(bind).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            if let Err(err) = tokio::signal::ctrl_c().await {
                tracing::error!(error = %err, "failed to listen for shutdown signal");
            }
            tracing::info!("shutdown signal received; terminating http server");
        })
        .await?;

    Ok(())
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.metrics.gather() {
        Ok(body) => (StatusCode::OK, body).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to render metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
    }
}

async fn validators_handler(State(state): State<AppState>) -> impl IntoResponse {
    let snapshots: Vec<ValidatorSnapshot> = state.observers.snapshots();
    Json(snapshots)
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn flamegraph_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.flamegraph {
        Some(ref service) => match service.snapshot_svg() {
            Ok(svg) => Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "image/svg+xml")
                .body(Body::from(svg))
                .unwrap(),
            Err(err) => {
                tracing::error!(error = %err, "failed to export flamegraph");
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(err.to_string()))
                    .unwrap()
            }
        },
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("flamegraph disabled"))
            .unwrap(),
    }
}
