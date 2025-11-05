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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn write_temp_config(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("create temp file");
        std::io::Write::write_all(&mut file, contents.as_bytes()).expect("write config");
        file
    }

    #[tokio::test]
    async fn load_parses_valid_config() {
        let config_toml = r#"
            metrics_bind = "127.0.0.1:9090"

            [[validators]]
            name = "alpha"
            gossip_addr = "10.0.0.1:8001"
            expected_slot_lookback = 32

            [telemetry]
            enabled = true
            sampling_interval = 5
            ring_buffer_interval = 10
        "#;
        let file = write_temp_config(config_toml);
        let cfg = ObserverConfig::load(file.path())
            .await
            .expect("load config");
        assert_eq!(cfg.metrics_bind.to_string(), "127.0.0.1:9090");
        assert_eq!(cfg.validators.len(), 1);
        let validator = &cfg.validators[0];
        assert_eq!(validator.name, "alpha");
        assert_eq!(validator.expected_slot_lookback, 32);
        assert!(cfg.telemetry.enabled);
        assert_eq!(cfg.telemetry.sampling_interval().as_secs(), 5);
        assert_eq!(cfg.telemetry.ring_buffer_interval().as_secs(), 10);
    }

    #[tokio::test]
    async fn load_rejects_invalid_toml() {
        let config_toml = "not = [valid";
        let file = write_temp_config(config_toml);
        let err = ObserverConfig::load(file.path())
            .await
            .expect_err("invalid config");
        assert!(err.to_string().contains("failed to parse"));
    }

    #[test]
    fn telemetry_defaults_are_sensible() {
        let telemetry = TelemetryConfig::default();
        assert!(!telemetry.enabled);
        assert_eq!(telemetry.sampling_interval().as_millis(), 200);
        assert_eq!(telemetry.ring_buffer_interval().as_millis(), 100);
    }

    #[test]
    fn flamegraph_defaults_override_when_disabled() {
        let cfg = FlamegraphConfig {
            enabled: false,
            refresh_interval: None,
        };
        assert!(!cfg.enabled);
        assert_eq!(cfg.refresh_interval().as_secs(), 30);
    }

    #[test]
    fn alerting_default_cooldown() {
        let url = Url::parse("https://example.com/webhook").unwrap();
        let cfg = AlertingConfig {
            webhook_url: url,
            slot_lag_threshold: 50,
            cooldown: None,
        };
        assert_eq!(cfg.cooldown().as_secs(), 30);
    }
}
