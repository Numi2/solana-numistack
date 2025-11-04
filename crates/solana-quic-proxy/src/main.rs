// Numan Thabit 2022
use std::sync::Arc;

use anyhow::Context;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header::CONTENT_TYPE, StatusCode},
    response::Response,
    routing::{get, post},
    Router,
};
use clap::Parser;
use solana_quic_proxy::{
    client::{ProxyError, QuicRpcClient},
    config::{CliArgs, Config},
    metrics::ProxyMetrics,
};
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

#[derive(Clone)]
struct AppState {
    client: Arc<QuicRpcClient>,
    metrics: Arc<ProxyMetrics>,
    max_request_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = CliArgs::parse();
    let config = Arc::new(Config::from_cli(&cli)?);
    let metrics = Arc::new(ProxyMetrics::new()?);
    let client = Arc::new(QuicRpcClient::new(config.clone(), metrics.clone())?);

    if !config.lazy_connect {
        if let Err(err) = client.warmup().await {
            warn!(error = %err, "upstream preconnect failed; continuing with lazy dial");
        }
    }

    let state = AppState {
        client,
        metrics: metrics.clone(),
        max_request_bytes: config.max_request_bytes,
    };

    let mut app = Router::new()
        .route("/", post(proxy_handler))
        .route("/rpc", post(proxy_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state);
    if config.http_trace {
        app = app.layer(TraceLayer::new_for_http());
    }

    info!(listen = %config.listen, upstream = %config.upstream, lazy_connect = config.lazy_connect, "solana-quic-proxy listening");

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .context("failed to bind listen socket")?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum server exited with error")?;

    Ok(())
}

async fn shutdown_signal() {
    if let Err(err) = signal::ctrl_c().await {
        warn!(%err, "failed to install shutdown handler");
    }
    info!("shutdown signal received");
}

async fn proxy_handler(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "empty request body");
    }

    if body.len() > state.max_request_bytes {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request exceeds configured limit",
        );
    }

    state.metrics.in_flight_inc();
    let start = tokio::time::Instant::now();
    let result = state.client.request(body.as_ref()).await;
    state.metrics.in_flight_dec();

    match result {
        Ok(response) => {
            state.metrics.record_success(
                start.elapsed(),
                response.latency,
                body.len(),
                response.payload.len(),
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(response.payload))
                .unwrap_or_else(|err| {
                    error_response(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string())
                })
        }
        Err(err) => {
            state.metrics.record_failure();
            error!(error = %err, "upstream request failed");
            let status = status_for_error(&err);
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32000,
                    "message": err.to_string(),
                },
                "id": null,
            });
            Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap_or_else(|builder_err| {
                    error_response(StatusCode::INTERNAL_SERVER_ERROR, &builder_err.to_string())
                })
        }
    }
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    match state.metrics.render() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain; version=0.0.4")
            .body(Body::from(body))
            .unwrap_or_else(|err| {
                error_response(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string())
            }),
        Err(err) => {
            error!(error = %err, "failed to render metrics");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "metrics encoder failure")
        }
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(message.to_string()))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("internal error"))
                .unwrap()
        })
}

fn status_for_error(err: &ProxyError) -> StatusCode {
    match err {
        ProxyError::ResponseTooLarge { .. } => StatusCode::BAD_GATEWAY,
        ProxyError::Connect(_) => StatusCode::BAD_GATEWAY,
        ProxyError::Connection(_) => StatusCode::BAD_GATEWAY,
        ProxyError::Write(_) | ProxyError::Read(_) => StatusCode::BAD_GATEWAY,
        ProxyError::Protocol(_) => StatusCode::BAD_GATEWAY,
    }
}
