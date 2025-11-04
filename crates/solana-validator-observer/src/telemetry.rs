// Numan Thabit 2025
use anyhow::Result;
#[cfg(all(feature = "ebpf", target_os = "linux"))]
use anyhow::{bail, Context};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::{
    task::JoinHandle,
    time::{interval, sleep},
};
use tracing::info;

use crate::{config::TelemetryConfig, metrics::ObserverMetrics, state::ObserverState};

pub fn spawn_telemetry(
    config: &TelemetryConfig,
    state: ObserverState,
    metrics: ObserverMetrics,
) -> Option<JoinHandle<Result<()>>> {
    if !config.enabled {
        return None;
    }
    info!(
        interface = %config.interface.as_deref().unwrap_or("auto"),
        ports = ?config.validator_ports,
        sampling_ms = config.sampling_interval().as_millis(),
        ring_buffer_ms = config.ring_buffer_interval().as_millis(),
        bpf_program = %config
            .bpf_program
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "default".to_string()),
        "starting telemetry task"
    );
    let config = config.clone();
    Some(tokio::spawn(
        async move { run(config, state, metrics).await },
    ))
}

async fn run(
    config: TelemetryConfig,
    state: ObserverState,
    metrics: ObserverMetrics,
) -> Result<()> {
    #[cfg(all(feature = "ebpf", target_os = "linux"))]
    {
        if let Err(err) = aya_runner(&config, &state, &metrics).await {
            tracing::warn!(error = ?err, "eBPF telemetry fallback to synthetic mode");
            synthetic_loop(&config, state, metrics).await
        } else {
            Ok(())
        }
    }

    #[cfg(not(all(feature = "ebpf", target_os = "linux")))]
    {
        synthetic_loop(&config, state, metrics).await
    }
}

#[cfg(all(feature = "ebpf", target_os = "linux"))]
async fn aya_runner(
    config: &TelemetryConfig,
    state: &ObserverState,
    metrics: &ObserverMetrics,
) -> Result<()> {
    use std::convert::TryFrom;
    use std::path::PathBuf;
    use std::time::Duration;

    use aya::{
        maps::{HashMap as AyaHashMap, MapRefMut},
        Bpf,
    };

    let program_path = config
        .bpf_program
        .clone()
        .unwrap_or_else(|| PathBuf::from("./ebpf/validator_net.bpf.o"));

    let mut bpf = Bpf::load_file(&program_path).with_context(|| {
        format!(
            "failed to load eBPF program from {}",
            program_path.display()
        )
    })?;

    // Optional map expected to be exported by the eBPF program. We gracefully
    // handle its absence by signalling the caller to fall back to synthetic mode.
    let Ok(mut stats) =
        AyaHashMap::<MapRefMut, u32, u64>::try_from(bpf.take_map("VALIDATOR_HIGHEST_SLOT")?)
    else {
        bail!("missing VALIDATOR_HIGHEST_SLOT map in eBPF program");
    };

    loop {
        for result in stats.iter()? {
            let (_validator_id, slot) = result?;
            state.update_cluster_highest_slot(*slot);
        }
        sleep(config.sampling_interval()).await;
        stats.clear()?;
    }
}

async fn synthetic_loop(
    config: &TelemetryConfig,
    state: ObserverState,
    metrics: ObserverMetrics,
) -> Result<()> {
    let mut hasher = DefaultHasher::new();
    config.interface.hash(&mut hasher);
    config.validator_ports.hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);
    let sampling_interval = config.sampling_interval();
    let ring_interval = config.ring_buffer_interval();
    let mut ring_ticker = interval(ring_interval);
    ring_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        for (idx, snapshot) in state.snapshots().into_iter().enumerate() {
            ring_ticker.tick().await;
            let port_entropy = if config.validator_ports.is_empty() {
                0.0
            } else {
                config.validator_ports[idx % config.validator_ports.len()] as f64
            };
            let delay_ms = rng.gen_range(10.0..120.0) + (port_entropy % 17.0);
            let slot_increment = rng.gen_range(1..4);
            let latest_slot = snapshot
                .last_slot
                .unwrap_or(rng.gen_range(1_000_000..1_050_000))
                + slot_increment;
            let packet_loss = rng.gen_range(0.0..0.02);

            metrics.record_slot_propagation(&snapshot.name, delay_ms / 1_000.0);
            metrics.set_packet_loss(&snapshot.name, packet_loss);
            let quic_latency = std::time::Duration::from_micros(
                ((delay_ms * 1_000.0) + port_entropy).max(1.0) as u64,
            );
            metrics.record_quic_latency(&snapshot.name, quic_latency.as_secs_f64());
            metrics.record_rpc_latency(&snapshot.name, quic_latency.as_secs_f64());
            state.update_slot(
                &snapshot.name,
                latest_slot,
                std::time::Duration::from_millis(delay_ms as u64),
            );
            state.update_packet_loss(&snapshot.name, packet_loss);
            state.update_quic_latency(&snapshot.name, quic_latency);
            state.update_rpc_latency(&snapshot.name, quic_latency);
            state.update_cluster_highest_slot(latest_slot);
        }

        sleep(sampling_interval).await;
    }
}
