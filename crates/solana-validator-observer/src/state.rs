// Numan Thabit 2025
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ValidatorSnapshot {
    pub name: String,
    pub last_slot: Option<u64>,
    pub highest_observed_slot: Option<u64>,
    pub slot_lag: Option<f64>,
    pub slot_propagation_delay_ms: Option<f64>,
    pub gossip_latency_ms: Option<f64>,
    pub quic_latency_ms: Option<f64>,
    pub rpc_latency_ms: Option<f64>,
    pub packet_loss_ratio: Option<f64>,
    pub last_updated: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct MutableValidatorSnapshot {
    name: String,
    last_slot: Option<u64>,
    slot_propagation_delay_ms: Option<f64>,
    slot_lag: Option<f64>,
    gossip_latency_ms: Option<f64>,
    quic_latency_ms: Option<f64>,
    rpc_latency_ms: Option<f64>,
    packet_loss_ratio: Option<f64>,
    last_updated: Option<DateTime<Utc>>,
}

impl MutableValidatorSnapshot {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            last_slot: None,
            slot_propagation_delay_ms: None,
            slot_lag: None,
            gossip_latency_ms: None,
            quic_latency_ms: None,
            rpc_latency_ms: None,
            packet_loss_ratio: None,
            last_updated: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObserverState {
    inner: Arc<DashMap<String, MutableValidatorSnapshot>>,
    global_highest_slot: Arc<AtomicU64>,
}

impl ObserverState {
    pub fn new<I, S>(validators: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let inner = DashMap::new();
        for validator in validators {
            let name = validator.as_ref().to_string();
            inner.insert(name.clone(), MutableValidatorSnapshot::new(&name));
        }
        Self {
            inner: Arc::new(inner),
            global_highest_slot: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn update_slot(&self, validator: &str, slot: u64, propagation: Duration) {
        let now = Utc::now();
        let highest = self.bump_global_slot(slot);
        self.with_validator_mut(validator, |entry| {
            entry.slot_propagation_delay_ms = Some(propagation.as_secs_f64() * 1_000.0);
            entry.last_slot = Some(slot);
            entry.slot_lag = Some(highest.saturating_sub(slot) as f64);
            entry.last_updated = Some(now);
        });
    }

    pub fn update_cluster_highest_slot(&self, slot: u64) {
        self.set_global_highest(slot);
    }

    pub fn update_gossip_latency(&self, validator: &str, latency: Duration) {
        let now = Utc::now();
        self.with_validator_mut(validator, |entry| {
            entry.gossip_latency_ms = Some(latency.as_secs_f64() * 1_000.0);
            entry.last_updated = Some(now);
        });
    }

    pub fn update_quic_latency(&self, validator: &str, latency: Duration) {
        let now = Utc::now();
        self.with_validator_mut(validator, |entry| {
            entry.quic_latency_ms = Some(latency.as_secs_f64() * 1_000.0);
            entry.last_updated = Some(now);
        });
    }

    pub fn update_packet_loss(&self, validator: &str, loss_ratio: f64) {
        let now = Utc::now();
        self.with_validator_mut(validator, |entry| {
            entry.packet_loss_ratio = Some(loss_ratio);
            entry.last_updated = Some(now);
        });
    }

    pub fn snapshots(&self) -> Vec<ValidatorSnapshot> {
        let cluster_highest = self.cluster_highest_slot();
        self.inner
            .iter()
            .map(|entry| ValidatorSnapshot {
                name: entry.name.clone(),
                last_slot: entry.last_slot,
                highest_observed_slot: cluster_highest,
                slot_lag: entry.slot_lag,
                slot_propagation_delay_ms: entry.slot_propagation_delay_ms,
                gossip_latency_ms: entry.gossip_latency_ms,
                quic_latency_ms: entry.quic_latency_ms,
                rpc_latency_ms: entry.rpc_latency_ms,
                packet_loss_ratio: entry.packet_loss_ratio,
                last_updated: entry.last_updated,
            })
            .collect()
    }

    pub fn get(&self, validator: &str) -> Option<ValidatorSnapshot> {
        let cluster_highest = self.cluster_highest_slot();
        self.inner.get(validator).map(|entry| ValidatorSnapshot {
            name: entry.name.clone(),
            last_slot: entry.last_slot,
            highest_observed_slot: cluster_highest,
            slot_lag: entry.slot_lag,
            slot_propagation_delay_ms: entry.slot_propagation_delay_ms,
            gossip_latency_ms: entry.gossip_latency_ms,
            quic_latency_ms: entry.quic_latency_ms,
            rpc_latency_ms: entry.rpc_latency_ms,
            packet_loss_ratio: entry.packet_loss_ratio,
            last_updated: entry.last_updated,
        })
    }

    pub fn update_rpc_latency(&self, validator: &str, latency: Duration) {
        let now = Utc::now();
        self.with_validator_mut(validator, |entry| {
            entry.rpc_latency_ms = Some(latency.as_secs_f64() * 1_000.0);
            entry.last_updated = Some(now);
        });
    }

    pub fn highest_slot(&self) -> Option<u64> {
        self.cluster_highest_slot()
    }

    pub fn cluster_highest_slot(&self) -> Option<u64> {
        let value = self.global_highest_slot.load(Ordering::Relaxed);
        if value == 0 {
            None
        } else {
            Some(value)
        }
    }

    fn bump_global_slot(&self, slot: u64) -> u64 {
        let mut current = self.global_highest_slot.load(Ordering::Relaxed);
        loop {
            if slot <= current {
                return current;
            }
            match self.global_highest_slot.compare_exchange(
                current,
                slot,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return slot,
                Err(actual) => current = actual,
            }
        }
    }

    fn set_global_highest(&self, slot: u64) {
        let mut current = self.global_highest_slot.load(Ordering::Relaxed);
        loop {
            if slot <= current {
                break;
            }
            match self.global_highest_slot.compare_exchange(
                current,
                slot,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn with_validator_mut<F>(&self, validator: &str, mut f: F)
    where
        F: FnMut(&mut MutableValidatorSnapshot),
    {
        if let Some(mut entry) = self.inner.get_mut(validator) {
            f(&mut entry);
            return;
        }

        let mut snapshot = MutableValidatorSnapshot::new(validator);
        f(&mut snapshot);
        self.inner.insert(validator.to_string(), snapshot);
    }
}
