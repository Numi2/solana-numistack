// Numan Thabit 2029
//! OpenTelemetry → Prometheus exporter setup and instrument handles.

use anyhow::Context;
use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider as _};
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::metrics::MeterProvider as SdkMeterProvider;
use prometheus::{Encoder, TextEncoder};

/// Telemetry context initialised for the RPC server.
pub struct Telemetry {
    registry: prometheus::Registry,
    _provider: SdkMeterProvider,
    meter: Meter,
}

impl Telemetry {
    /// Construct a Prometheus-backed OpenTelemetry provider for the service.
    pub fn init(service_name: impl Into<String>) -> anyhow::Result<Self> {
        let service_name = service_name.into();
        let registry = prometheus::Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .context("failed to build prometheus exporter")?;

        let provider = SdkMeterProvider::builder().with_reader(exporter).build();
        let meter = provider.meter(service_name);
        global::set_meter_provider(provider.clone());

        Ok(Self {
            registry,
            _provider: provider,
            meter,
        })
    }

    /// Telemetry meter handle for creating instruments.
    pub fn meter(&self) -> Meter {
        self.meter.clone()
    }

    /// Gather the current Prometheus exposition text.
    pub fn render_prometheus(&self) -> anyhow::Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .context("failed encoding prometheus metrics")?;
        String::from_utf8(buffer).context("prometheus output not utf8")
    }

    /// Create the standard RPC metrics instruments.
    pub fn rpc_metrics(&self) -> RpcMetrics {
        let meter = self.meter();
        RpcMetrics::new(meter)
    }
}

/// Common RPC instrumentation handles.
#[derive(Clone)]
pub struct RpcMetrics {
    requests: Counter<u64>,
    latency: Histogram<f64>,
    payload_bytes: Histogram<f64>,
}

impl RpcMetrics {
    fn new(meter: Meter) -> Self {
        let requests = meter
            .u64_counter("rpc_requests_total")
            .with_description("Total number of JSON-RPC requests serviced")
            .init();
        let latency = meter
            .f64_histogram("rpc_latency_seconds")
            .with_description("End-to-end RPC latency (seconds)")
            .init();
        let payload_bytes = meter
            .f64_histogram("rpc_payload_bytes")
            .with_description("Size of JSON payloads processed")
            .init();
        Self {
            requests,
            latency,
            payload_bytes,
        }
    }

    /// Record a request of the given method and latency.
    pub fn record_request(&self, method: &str, latency_secs: f64, bytes: usize) {
        let attrs = [KeyValue::new("method", method.to_string())];
        self.requests.add(1, &attrs);
        self.latency.record(latency_secs, &attrs);
        self.payload_bytes
            .record(bytes as f64, &[KeyValue::new("method", method.to_string())]);
    }
}
