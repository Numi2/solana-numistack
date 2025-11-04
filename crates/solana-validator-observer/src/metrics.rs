// Numan Thabit 2025
use anyhow::Result;
use once_cell::sync::Lazy;
use prometheus::{
    opts, Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Registry, TextEncoder,
};

static METRICS_ENCODER: Lazy<TextEncoder> = Lazy::new(TextEncoder::new);

#[derive(Clone)]
pub struct ObserverMetrics {
    registry: Registry,
    slot_propagation: HistogramVec,
    gossip_latency: HistogramVec,
    quic_latency: HistogramVec,
    rpc_latency: HistogramVec,
    packet_loss: GaugeVec,
    slot_lag: GaugeVec,
    scrape_errors: IntCounterVec,
}

impl ObserverMetrics {
    pub fn new() -> Self {
        let registry = Registry::new_custom(Some("solana_validator_observer".into()), None)
            .expect("failed to create registry");

        let slot_propagation = HistogramVec::new(
            HistogramOpts::new(
                "slot_propagation_delay_seconds",
                "Distribution of slot propagation delay per validator",
            )
            .buckets(vec![
                0.01, 0.02, 0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 2.0, 4.0, 8.0,
            ]),
            &["validator"],
        )
        .expect("failed to build slot histogram");

        let gossip_latency = HistogramVec::new(
            HistogramOpts::new(
                "gossip_rtt_seconds",
                "Round trip latency to gossip UDP ports",
            )
            .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5]),
            &["validator"],
        )
        .expect("failed to build gossip histogram");

        let quic_latency = HistogramVec::new(
            HistogramOpts::new("quic_rtt_seconds", "Round trip latency to QUIC UDP ports").buckets(
                vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0],
            ),
            &["validator"],
        )
        .expect("failed to build quic histogram");

        let rpc_latency = HistogramVec::new(
            HistogramOpts::new(
                "rpc_request_latency_seconds",
                "JSON-RPC round-trip latency per validator",
            )
            .buckets(vec![
                0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0,
            ]),
            &["validator"],
        )
        .expect("failed to build rpc latency histogram");

        let packet_loss = GaugeVec::new(
            opts!(
                "packet_loss_ratio",
                "Estimated inbound packet loss ratio derived from eBPF telemetry"
            ),
            &["validator"],
        )
        .expect("failed to build packet loss gauge");

        let slot_lag = GaugeVec::new(
            opts!(
                "slot_lag",
                "Difference between highest observed slot and validator-reported slot"
            ),
            &["validator"],
        )
        .expect("failed to build slot lag gauge");

        let scrape_errors = IntCounterVec::new(
            opts!(
                "scrape_errors_total",
                "Count of scrape errors per validator and protocol"
            ),
            &["validator", "protocol"],
        )
        .expect("failed to build scrape error counter");

        registry
            .register(Box::new(slot_propagation.clone()))
            .expect("register slot_propagation");
        registry
            .register(Box::new(gossip_latency.clone()))
            .expect("register gossip_latency");
        registry
            .register(Box::new(quic_latency.clone()))
            .expect("register quic_latency");
        registry
            .register(Box::new(rpc_latency.clone()))
            .expect("register rpc_latency");
        registry
            .register(Box::new(packet_loss.clone()))
            .expect("register packet_loss");
        registry
            .register(Box::new(slot_lag.clone()))
            .expect("register slot_lag");
        registry
            .register(Box::new(scrape_errors.clone()))
            .expect("register scrape_errors");

        Self {
            registry,
            slot_propagation,
            gossip_latency,
            quic_latency,
            rpc_latency,
            packet_loss,
            slot_lag,
            scrape_errors,
        }
    }

    pub fn record_slot_propagation(&self, validator: &str, delay: f64) {
        self.slot_propagation
            .with_label_values(&[validator])
            .observe(delay);
    }

    pub fn record_gossip_latency(&self, validator: &str, latency: f64) {
        self.gossip_latency
            .with_label_values(&[validator])
            .observe(latency);
    }

    pub fn record_quic_latency(&self, validator: &str, latency: f64) {
        self.quic_latency
            .with_label_values(&[validator])
            .observe(latency);
    }

    pub fn record_rpc_latency(&self, validator: &str, latency: f64) {
        self.rpc_latency
            .with_label_values(&[validator])
            .observe(latency);
    }

    pub fn set_slot_lag(&self, validator: &str, lag: f64) {
        self.slot_lag.with_label_values(&[validator]).set(lag);
    }

    pub fn set_packet_loss(&self, validator: &str, loss_ratio: f64) {
        self.packet_loss
            .with_label_values(&[validator])
            .set(loss_ratio);
    }

    pub fn inc_scrape_error(&self, validator: &str, protocol: &str) {
        self.scrape_errors
            .with_label_values(&[validator, protocol])
            .inc();
    }

    pub fn gather(&self) -> Result<String> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::with_capacity(8192);
        METRICS_ENCODER
            .encode(&metric_families, &mut buffer)
            .map_err(|e| anyhow::anyhow!("failed to encode metrics: {e}"))?;
        Ok(String::from_utf8(buffer).expect("prometheus output is utf8"))
    }
}
