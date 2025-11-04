// Numan Thabit 2025
use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use reqwest::Url;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr, DurationSeconds};
use tokio::fs;

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct ObserverConfig {
    #[serde_as(as = "DisplayFromStr")]
    pub metrics_bind: SocketAddr,
    #[serde(default)]
    pub validators: Vec<ValidatorConfig>,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub scrape_interval: Option<Duration>,
    #[serde(default)]
    pub alerting: Option<AlertingConfig>,
    #[serde(default)]
    pub flamegraph: FlamegraphConfig,
}

impl ObserverConfig {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let raw = fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read config file at {}", path.display()))?;
        let config: Self = toml::from_str(&raw)
            .with_context(|| format!("failed to parse {} as TOML", path.display()))?;
        Ok(config)
    }

    pub fn scrape_interval(&self) -> Duration {
        self.scrape_interval
            .unwrap_or_else(|| Duration::from_secs(2))
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct ValidatorConfig {
    pub name: String,
    #[serde_as(as = "DisplayFromStr")]
    pub gossip_addr: SocketAddr,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub quic_addr: Option<SocketAddr>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub rpc_url: Option<Url>,
    #[serde(default = "default_slot_lookback")]
    pub expected_slot_lookback: u64,
    #[serde(default)]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub max_slot_interval: Option<Duration>,
}

fn default_slot_lookback() -> u64 {
    64
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TelemetryConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub interface: Option<String>,
    #[serde(default)]
    pub validator_ports: Vec<u16>,
    #[serde(default)]
    pub bpf_program: Option<PathBuf>,
    #[serde(default)]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub sampling_interval: Option<Duration>,
    #[serde(default)]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub ring_buffer_interval: Option<Duration>,
}

fn default_enabled() -> bool {
    true
}

impl TelemetryConfig {
    pub fn sampling_interval(&self) -> Duration {
        self.sampling_interval
            .unwrap_or_else(|| Duration::from_millis(200))
    }

    pub fn ring_buffer_interval(&self) -> Duration {
        self.ring_buffer_interval
            .unwrap_or_else(|| Duration::from_millis(100))
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct AlertingConfig {
    #[serde_as(as = "DisplayFromStr")]
    pub webhook_url: Url,
    pub slot_lag_threshold: u64,
    #[serde(default)]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub cooldown: Option<Duration>,
}

impl AlertingConfig {
    pub fn cooldown(&self) -> Duration {
        self.cooldown.unwrap_or_else(|| Duration::from_secs(30))
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct FlamegraphConfig {
    #[serde(default = "default_flamegraph_enabled")]
    pub enabled: bool,
    #[serde(default)]
    #[serde_as(as = "Option<DurationSeconds<u64>>")]
    pub refresh_interval: Option<Duration>,
}

impl Default for FlamegraphConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval: Some(Duration::from_secs(30)),
        }
    }
}

impl FlamegraphConfig {
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
            .unwrap_or_else(|| Duration::from_secs(30))
    }
}

fn default_flamegraph_enabled() -> bool {
    true
}
