// Numan Thabit 2025
mod alert;
mod config;
mod dashboard;
mod flamegraph;
mod http;
mod metrics;
mod scraper;
mod state;
mod telemetry;

use std::path::PathBuf;

use alert::AlertingService;
use anyhow::Result;
use clap::Parser;
use config::ObserverConfig;
use flamegraph::FlamegraphService;
use metrics::ObserverMetrics;
use state::ObserverState;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Parser)]
#[command(author, version, about = "Solana validator observability daemon")]
struct Cli {
    /// Path to the observer configuration file
    #[arg(long, default_value = "ops/solana-validator-observer.example.toml")]
    config: PathBuf,

    /// Optional path to export the Grafana dashboard JSON
    #[arg(long)]
    grafana_export: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let config = ObserverConfig::load(cli.config).await?;

    let metrics = ObserverMetrics::new();
    let validator_names: Vec<String> = config.validators.iter().map(|v| v.name.clone()).collect();
    let observer_state = ObserverState::new(&validator_names);

    let alerting = match config.alerting.clone() {
        Some(cfg) => Some(AlertingService::new(cfg)?),
        None => None,
    };

    let flamegraph = FlamegraphService::new(&config.flamegraph)?;
    if let Some(ref service) = flamegraph {
        service.spawn_refresh_task();
    }

    if let Some(path) = cli.grafana_export {
        dashboard::write_to(&path).await?;
        tracing::info!(path = %path.display(), "exported grafana dashboard");
    }

    let telemetry_handle =
        telemetry::spawn_telemetry(&config.telemetry, observer_state.clone(), metrics.clone());

    let scraper_handles = scraper::spawn_scrapers(
        config.validators.clone(),
        observer_state.clone(),
        metrics.clone(),
        config.scrape_interval(),
        alerting.clone(),
    );

    http::serve(
        config.metrics_bind,
        metrics,
        observer_state.clone(),
        flamegraph.clone(),
    )
    .await?;

    if let Some(handle) = telemetry_handle {
        handle.abort();
    }
    for handle in scraper_handles {
        handle.abort();
    }

    Ok(())
}
