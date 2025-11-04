// Numan Thabit 2025
use std::{io::ErrorKind, net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::future::OptionFuture;
use once_cell::sync::Lazy;
use reqwest::{header::CONTENT_TYPE, Client, Url};
use serde::Deserialize;
use tokio::{
    net::UdpSocket,
    task::JoinHandle,
    time::{interval_at, timeout, Instant, MissedTickBehavior},
};

use crate::{
    alert::AlertingService, config::ValidatorConfig, metrics::ObserverMetrics, state::ObserverState,
};

pub fn spawn_scrapers(
    validators: Vec<ValidatorConfig>,
    state: ObserverState,
    metrics: ObserverMetrics,
    scrape_interval: Duration,
    alerting: Option<AlertingService>,
) -> Vec<JoinHandle<()>> {
    validators
        .into_iter()
        .map(|validator| {
            let state = state.clone();
            let metrics = metrics.clone();
            let alerting = alerting.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    run_validator(validator, state, metrics, scrape_interval, alerting).await
                {
                    tracing::error!(%err, "validator scrape loop terminated");
                }
            })
        })
        .collect()
}

async fn run_validator(
    validator: ValidatorConfig,
    state: ObserverState,
    metrics: ObserverMetrics,
    scrape_interval: Duration,
    alerting: Option<AlertingService>,
) -> Result<()> {
    let ValidatorConfig {
        name,
        gossip_addr,
        quic_addr,
        rpc_url,
        expected_slot_lookback,
        max_slot_interval,
    } = validator;

    let rpc_client = if rpc_url.is_some() {
        Some(
            Client::builder()
                .timeout(Duration::from_secs(2))
                .tcp_nodelay(true)
                .pool_idle_timeout(Some(Duration::from_secs(10)))
                .pool_max_idle_per_host(2)
                .build()
                .context("failed to construct rpc client")?,
        )
    } else {
        None
    };

    if rpc_client.is_none() {
        tracing::warn!(validator = %name, "no rpc_url configured; slot tracking metrics will be unavailable");
    }

    let gossip_socket = create_probe_socket(gossip_addr).await?;
    let quic_socket = match quic_addr {
        Some(addr) => Some(create_probe_socket(addr).await?),
        None => None,
    };

    let mut ticker = interval_at(Instant::now(), scrape_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_slot = state.get(&name).and_then(|s| s.last_slot);
    let lag_threshold = expected_slot_lookback as f64;
    let mut lag_exceeded = false;

    loop {
        ticker.tick().await;

        let rpc_probe = OptionFuture::from(rpc_url.as_ref().and_then(|url| {
            rpc_client
                .as_ref()
                .map(|client| measure_rpc_health(client, url))
        }));
        let gossip_probe = measure_udp_roundtrip(&gossip_socket);
        let quic_probe = OptionFuture::from(quic_socket.as_ref().map(measure_udp_roundtrip));

        let (rpc_result, gossip_result, quic_result) =
            tokio::join!(rpc_probe, gossip_probe, quic_probe);

        let mut propagation = None;

        if let Some(result) = rpc_result {
            match result {
                Ok(sample) => {
                    let bounded = max_slot_interval
                        .map(|limit| sample.latency.min(limit))
                        .unwrap_or(sample.latency);
                    propagation = Some(bounded);
                    last_slot = Some(sample.slot);
                    state.update_cluster_highest_slot(sample.slot);
                    metrics.record_rpc_latency(&name, sample.latency.as_secs_f64());
                    state.update_rpc_latency(&name, sample.latency);
                }
                Err(err) => {
                    tracing::debug!(validator = %name, error = %err, "rpc health check failed");
                    metrics.inc_scrape_error(&name, "rpc");
                }
            }
        }

        match gossip_result {
            Ok(Some(latency)) => {
                metrics.record_gossip_latency(&name, latency.as_secs_f64());
                state.update_gossip_latency(&name, latency);
                propagation = propagation.or(Some(latency));
            }
            Ok(None) => {
                metrics.inc_scrape_error(&name, "gossip");
            }
            Err(err) => {
                tracing::debug!(validator = %name, error = %err, "failed to measure gossip latency");
                metrics.inc_scrape_error(&name, "gossip");
            }
        }

        if let Some(result) = quic_result {
            match result {
                Ok(Some(latency)) => {
                    metrics.record_quic_latency(&name, latency.as_secs_f64());
                    state.update_quic_latency(&name, latency);
                }
                Ok(None) => metrics.inc_scrape_error(&name, "quic"),
                Err(err) => {
                    tracing::debug!(validator = %name, error = %err, "failed to measure quic latency");
                    metrics.inc_scrape_error(&name, "quic");
                }
            }
        }

        if let (Some(slot), Some(propagation)) = (last_slot, propagation) {
            metrics.record_slot_propagation(&name, propagation.as_secs_f64());
            state.update_slot(&name, slot, propagation);

            if let Some(highest) = state.highest_slot() {
                let lag = highest.saturating_sub(slot) as f64;
                metrics.set_slot_lag(&name, lag);
                if lag > lag_threshold {
                    if !lag_exceeded {
                        tracing::warn!(
                            validator = %name,
                            lag,
                            threshold = lag_threshold,
                            "slot lag exceeded expected lookback"
                        );
                        lag_exceeded = true;
                    }
                } else if lag_exceeded {
                    tracing::info!(
                        validator = %name,
                        lag,
                        threshold = lag_threshold,
                        "slot lag recovered within expected lookback"
                    );
                    lag_exceeded = false;
                }
            }
        }

        if let Some(alerting) = &alerting {
            if let Some(snapshot) = state.get(&name) {
                if let Err(err) = alerting.maybe_trigger(&snapshot).await {
                    tracing::warn!(validator = %name, error = %err, "failed to trigger alert");
                }
            }
        }
    }
}

const UDP_TIMEOUT_MS: u64 = 150;

static GET_SLOT_PAYLOAD: Lazy<Bytes> =
    Lazy::new(|| Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#));

#[derive(Debug, Deserialize)]
struct JsonRpcGetSlot {
    result: u64,
}

#[derive(Debug)]
struct RpcHealthSample {
    slot: u64,
    latency: Duration,
}

async fn measure_rpc_health(client: &Client, url: &Url) -> Result<RpcHealthSample> {
    let start = Instant::now();
    let response = client
        .post(url.clone())
        .header(CONTENT_TYPE, "application/json")
        .body(GET_SLOT_PAYLOAD.clone())
        .send()
        .await
        .context("rpc request failed")?;
    let latency = start.elapsed();

    if !response.status().is_success() {
        anyhow::bail!("rpc endpoint returned status {}", response.status());
    }

    let slot = response
        .json::<JsonRpcGetSlot>()
        .await
        .context("failed to decode rpc body")?
        .result;

    Ok(RpcHealthSample { slot, latency })
}

async fn create_probe_socket(addr: SocketAddr) -> Result<UdpSocket> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("failed to bind UDP socket")?;
    socket
        .connect(addr)
        .await
        .with_context(|| format!("failed to connect UDP socket to {addr}"))?;
    Ok(socket)
}

async fn measure_udp_roundtrip(socket: &UdpSocket) -> Result<Option<Duration>> {
    let mut buf = [0u8; 32];
    loop {
        match socket.try_recv(&mut buf) {
            Ok(_) => continue,
            Err(err) if err.kind() == ErrorKind::WouldBlock => break,
            Err(err) => return Err(err.into()),
        }
    }

    let start = Instant::now();
    socket
        .send(&buf)
        .await
        .context("failed to send UDP probe")?;

    let timeout_duration = Duration::from_millis(UDP_TIMEOUT_MS);

    match timeout(timeout_duration, socket.recv(&mut buf)).await {
        Ok(Ok(_)) => Ok(Some(start.elapsed())),
        Ok(Err(err)) => Err(err.into()),
        Err(_) => Ok(None),
    }
}
