// Numii
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use prometheus::{
    exponential_buckets, opts, Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry,
    TextEncoder,
};

pub struct ProxyMetrics {
    registry: Registry,
    requests: IntCounter,
    failures: IntCounter,
    inflight: IntGauge,
    request_latency: Histogram,
    upstream_latency: Histogram,
    bytes_in: Histogram,
    bytes_out: Histogram,
    connection_resets: IntCounter,
}

impl ProxyMetrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new_custom(Some("solana_quic_proxy".into()), None)
            .context("failed to create metrics registry")?;

        let requests = IntCounter::with_opts(opts!("requests_total", "Total requests forwarded"))
            .context("failed to build requests counter")?;
        let failures =
            IntCounter::with_opts(opts!("requests_failed_total", "Total failed requests"))
                .context("failed to build failures counter")?;
        let connection_resets = IntCounter::with_opts(opts!(
            "upstream_connection_resets_total",
            "Total upstream QUIC connection resets"
        ))
        .context("failed to build connection resets counter")?;
        let inflight = IntGauge::with_opts(opts!(
            "inflight_requests",
            "Number of in-flight proxy requests"
        ))
        .context("failed to build inflight gauge")?;
        let latency_buckets =
            exponential_buckets(5e-5, 1.8, 14).context("failed to build latency buckets")?;
        let request_latency = Histogram::with_opts(
            HistogramOpts::new("request_latency_seconds", "Total request latency")
                .buckets(latency_buckets.clone()),
        )
        .context("failed to build request latency histogram")?;
        let upstream_latency = Histogram::with_opts(
            HistogramOpts::new(
                "upstream_latency_seconds",
                "Upstream QUIC round-trip latency",
            )
            .buckets(latency_buckets),
        )
        .context("failed to build upstream latency histogram")?;
        let bytes_in = Histogram::with_opts(HistogramOpts::new(
            "request_bytes",
            "Size of incoming JSON-RPC payloads",
        ))
        .context("failed to build request bytes histogram")?;
        let bytes_out = Histogram::with_opts(HistogramOpts::new(
            "response_bytes",
            "Size of upstream JSON-RPC responses",
        ))
        .context("failed to build response bytes histogram")?;

        registry
            .register(Box::new(requests.clone()))
            .context("register requests")?;
        registry
            .register(Box::new(failures.clone()))
            .context("register failures")?;
        registry
            .register(Box::new(connection_resets.clone()))
            .context("register connection resets")?;
        registry
            .register(Box::new(inflight.clone()))
            .context("register inflight")?;
        registry
            .register(Box::new(request_latency.clone()))
            .context("register request latency")?;
        registry
            .register(Box::new(upstream_latency.clone()))
            .context("register upstream latency")?;
        registry
            .register(Box::new(bytes_in.clone()))
            .context("register request bytes")?;
        registry
            .register(Box::new(bytes_out.clone()))
            .context("register response bytes")?;

        Ok(Self {
            registry,
            requests,
            failures,
            inflight,
            request_latency,
            upstream_latency,
            bytes_in,
            bytes_out,
            connection_resets,
        })
    }

    pub fn in_flight_inc(&self) {
        self.inflight.inc();
    }

    pub fn in_flight_dec(&self) {
        self.inflight.dec();
    }

    pub fn record_success(
        &self,
        total: Duration,
        upstream: Duration,
        bytes_in: usize,
        bytes_out: usize,
    ) {
        self.requests.inc();
        self.request_latency.observe(total.as_secs_f64());
        self.upstream_latency.observe(upstream.as_secs_f64());
        self.bytes_in.observe(bytes_in as f64);
        self.bytes_out.observe(bytes_out as f64);
    }

    pub fn record_failure(&self) {
        self.failures.inc();
    }

    pub fn record_connection_reset(&self) {
        self.connection_resets.inc();
    }

    pub fn render(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::with_capacity(4096);
        encoder
            .encode(&metric_families, &mut buf)
            .context("failed to encode metrics")?;
        String::from_utf8(buf).map_err(|err| anyhow!("metrics output not utf8: {err}"))
    }
}
