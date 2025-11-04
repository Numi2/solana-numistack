// Numan Thabit 2025
// crates/jito-client/src/bin/jito_bundle.rs
use anyhow::{anyhow, Result};
use base64::Engine;
use clap::Parser;
use jito_client::JitoClient;
use jito_client::JitoClientBuilder;
use std::fs;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "jito-bundle",
    version,
    about = "Submit a Jito bundle at max performance"
)]
struct Args {
    /// gRPC endpoint, e.g. https://ny.mainnet.block-engine.jito.wtf:443
    #[arg(long)]
    endpoint: Option<String>,
    /// Optional bearer token (if your provider requires)
    #[arg(long)]
    bearer: Option<String>,
    /// Enable gzip compression
    #[arg(long, default_value_t = false)]
    gzip: bool,
    /// Connect timeout in milliseconds
    #[arg(long)]
    connect_timeout_ms: Option<u64>,
    /// Per-RPC timeout in milliseconds
    #[arg(long)]
    rpc_timeout_ms: Option<u64>,
    /// Max number of retries for transient failures
    #[arg(long)]
    retries: Option<u32>,
    /// Initial retry backoff in milliseconds
    #[arg(long)]
    retry_initial_ms: Option<u64>,
    /// Max retry backoff in milliseconds
    #[arg(long)]
    retry_max_ms: Option<u64>,
    /// File containing base64-encoded signed transactions, one per line
    #[arg(long)]
    txs_b64_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let args = Args::parse();
    let endpoint = args
        .endpoint
        .or_else(|| std::env::var("JITO_ENDPOINT").ok())
        .ok_or_else(|| anyhow!("endpoint required (use --endpoint or JITO_ENDPOINT)"))?;

    let mut builder = JitoClientBuilder::new(endpoint);
    if let Some(b) = args.bearer.or_else(|| std::env::var("JITO_BEARER").ok()) {
        builder = builder.bearer(b);
    }
    if let Some(ms) = args.connect_timeout_ms {
        builder = builder.connect_timeout(std::time::Duration::from_millis(ms));
    }
    if let Some(ms) = args.rpc_timeout_ms {
        builder = builder.rpc_timeout(std::time::Duration::from_millis(ms));
    }
    if let Some(n) = args.retries {
        builder = builder.retries(n);
    }
    if let Some(ms) = args.retry_initial_ms {
        if let Some(max) = args.retry_max_ms {
            builder = builder.retry_backoff(ms, max);
        }
    }
    let gzip_enabled = args.gzip
        || std::env::var("JITO_GZIP")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);
    if gzip_enabled {
        builder = builder.compression(true);
    }
    let mut client = builder.connect().await?;

    let content = fs::read_to_string(args.txs_b64_file)?;
    let mut raw_txs = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let raw = base64::engine::general_purpose::STANDARD
            .decode(line)
            .map_err(|e| anyhow!("decode base64: {}", e))?;
        raw_txs.push(raw);
    }
    if raw_txs.is_empty() {
        return Err(anyhow!("no transactions provided"));
    }

    let bundle = JitoClient::build_bundle_from_signed_txs(raw_txs);
    let uuid = client.send_bundle(bundle).await?;
    println!("{uuid}");
    Ok(())
}
