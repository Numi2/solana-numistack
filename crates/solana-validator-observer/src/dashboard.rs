// Numan Thabit 2025
use std::path::Path;

use anyhow::{Context, Result};
use tokio::fs;

pub const DASHBOARD_JSON: &str =
    include_str!("../../../ops/solana-validator-observer-dashboard.json");

pub async fn write_to(path: &Path) -> Result<()> {
    fs::write(path, DASHBOARD_JSON)
        .await
        .with_context(|| format!("failed to write grafana dashboard to {}", path.display()))
}
