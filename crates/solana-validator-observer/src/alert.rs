// Numan Thabit 2025
use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use reqwest::Client;
use serde::Serialize;
use tokio::time::Instant;

use crate::{config::AlertingConfig, state::ValidatorSnapshot};

#[derive(Clone)]
pub struct AlertingService {
    client: Client,
    config: AlertingConfig,
    last_sent: Arc<DashMap<String, Instant>>,
}

impl AlertingService {
    pub fn new(config: AlertingConfig) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .context("failed to build webhook client")?,
            config,
            last_sent: Arc::new(DashMap::new()),
        })
    }

    pub async fn maybe_trigger(&self, snapshot: &ValidatorSnapshot) -> Result<()> {
        let Some(slot_lag) = snapshot.slot_lag else {
            return Ok(());
        };
        if slot_lag < self.config.slot_lag_threshold as f64 {
            return Ok(());
        }

        if let Some(last) = self.last_sent.get(&snapshot.name) {
            if last.elapsed() < self.config.cooldown() {
                return Ok(());
            }
        }

        let payload = AlertPayload {
            validator: snapshot.name.clone(),
            slot_lag,
            threshold: self.config.slot_lag_threshold,
            timestamp: snapshot.last_updated.unwrap_or_else(Utc::now),
        };

        self.client
            .post(self.config.webhook_url.clone())
            .json(&payload)
            .send()
            .await
            .context("failed to send alert webhook")?;

        self.last_sent.insert(snapshot.name.clone(), Instant::now());
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct AlertPayload {
    validator: String,
    slot_lag: f64,
    threshold: u64,
    timestamp: DateTime<Utc>,
}
