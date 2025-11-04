// Numan Thabit 2025
use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use pprof::ProfilerGuard;
use tokio::{task::JoinHandle, time::interval};

use crate::config::FlamegraphConfig;

#[derive(Clone)]
pub struct FlamegraphService {
    guard: Arc<Mutex<ProfilerGuard<'static>>>,
    refresh_interval: Duration,
}

impl FlamegraphService {
    pub fn new(config: &FlamegraphConfig) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let guard = ProfilerGuard::new(100).context("failed to initialize profiler guard")?;
        Ok(Some(Self {
            guard: Arc::new(Mutex::new(guard)),
            refresh_interval: config.refresh_interval(),
        }))
    }

    pub fn spawn_refresh_task(&self) -> JoinHandle<()> {
        let guard = self.guard.clone();
        let refresh_interval = self.refresh_interval;
        tokio::spawn(async move {
            let mut ticker = interval(refresh_interval);
            loop {
                ticker.tick().await;
                match ProfilerGuard::new(100) {
                    Ok(new_guard) => {
                        *guard.lock() = new_guard;
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to refresh profiler guard");
                    }
                }
            }
        })
    }

    pub fn snapshot_svg(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        {
            let guard = self.guard.lock();
            let report = guard
                .report()
                .build()
                .context("failed to build flamegraph profile")?;
            report
                .flamegraph(&mut out)
                .context("failed to write flamegraph")?;
        }
        Ok(out)
    }
}
