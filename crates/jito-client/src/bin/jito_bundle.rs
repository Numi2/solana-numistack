// crates/jito-client/src/bin/jito_bundle.rs
use anyhow::{anyhow, Result};
use clap::Parser;
use jito_client::JitoClient;
use std::fs;

#[derive(Parser, Debug)]
#[command(name = "jito-bundle", version, about = "Submit a Jito bundle at max performance")]
struct Args {
    /// gRPC endpoint, e.g. https://ny.mainnet.block-engine.jito.wtf:443
    #[arg(long, env = "JITO_ENDPOINT")]
    endpoint: String,
    /// Optional bearer token (if your provider requires)
    #[arg(long, env = "JITO_BEARER")]
    bearer: Option<String>,
    /// File containing base64-encoded signed transactions, one per line
    #[arg(long)]
    txs_b64_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut client = if let Some(b) = args.bearer.as_deref() {
        JitoClient::connect_with_bearer(&args.endpoint, b).await?
    } else {
        JitoClient::connect(&args.endpoint).await?
    };

    let content = fs::read_to_string(args.txs_b64_file)?;
    let mut raw_txs = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let raw = base64::decode(line).map_err(|e| anyhow!("decode base64: {}", e))?;
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