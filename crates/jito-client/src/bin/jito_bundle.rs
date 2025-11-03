// crates/jito-client/src/bin/jito_bundle.rs
use anyhow::{anyhow, Result};
use base64::Engine;
use clap::Parser;
use jito_client::JitoClient;
use std::fs;

#[derive(Parser, Debug)]
#[command(name = "jito-bundle", version, about = "Submit a Jito bundle at max performance")]
struct Args {
    /// gRPC endpoint, e.g. https://ny.mainnet.block-engine.jito.wtf:443
    #[arg(long)]
    endpoint: Option<String>,
    /// Optional bearer token (if your provider requires)
    #[arg(long)]
    bearer: Option<String>,
    /// File containing base64-encoded signed transactions, one per line
    #[arg(long)]
    txs_b64_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let endpoint = args.endpoint.as_deref().ok_or_else(|| anyhow!("endpoint required, use --endpoint or JITO_ENDPOINT env var"))?;
    let mut client = if let Some(b) = args.bearer.as_deref() {
        JitoClient::connect_with_bearer(endpoint, b).await?
    } else {
        JitoClient::connect(endpoint).await?
    };

    let content = fs::read_to_string(args.txs_b64_file)?;
    let mut raw_txs = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let raw = base64::engine::general_purpose::STANDARD.decode(line).map_err(|e| anyhow!("decode base64: {}", e))?;
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