// Numan Thabit 2025
// crates/ultra-rpc-bridge/src/main.rs
#![forbid(unsafe_code)]
use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use clap::Parser;
use faststreams::{decode_record_from_slice, Record};
use futures_util::SinkExt;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::time;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Bridge faststreams → solana-ultra-rpc snapshot/delta sockets", rename_all = "kebab-case")]
struct Args {
    /// Aggregator UDS input (faststreams frames)
    #[arg(long, default_value = "/tmp/ultra-geyser.sock")]
    input_uds: String,

    /// Output snapshot UDS that solana-ultra-rpc connects to
    #[arg(long, default_value = "/tmp/ultra-aggregator.snapshot.sock")]
    snapshot_uds: String,

    /// Output delta UDS that solana-ultra-rpc connects to
    #[arg(long, default_value = "/tmp/ultra-aggregator.sock")]
    delta_uds: String,

    /// Max accounts per snapshot segment
    #[arg(long, default_value_t = 10_000)]
    snapshot_segment_accounts: usize,

    /// Flush interval for delta batches in milliseconds
    #[arg(long, default_value_t = 5u64)]
    delta_flush_ms: u64,

    /// Max updates per delta batch
    #[arg(long, default_value_t = 2048)]
    delta_batch_max: usize,

    /// Optional Prometheus metrics listen address
    #[arg(long)]
    metrics_addr: Option<String>,
}

#[derive(Clone, Serialize)]
struct AccountWire {
    pubkey: [u8; 32],
    lamports: u64,
    owner: [u8; 32],
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
}

#[derive(Clone, Serialize)]
struct SnapshotWireSegment {
    base_slot: u64,
    accounts: Vec<AccountWire>,
}

#[derive(Clone, Serialize)]
struct DeltaWire {
    pubkey: [u8; 32],
    slot: u64,
    account: Option<AccountWire>,
}

#[derive(Clone, Serialize)]
struct DeltaWireBatch {
    updates: Vec<DeltaWire>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    if let Some(addr) = &args.metrics_addr {
        let _ = PrometheusBuilder::new()
            .with_http_listener(addr.parse::<std::net::SocketAddr>().unwrap())
            .install();
    }

    // Prepare output listeners (bridge acts as server for RPC to connect)
    let (snapshot_tx, snapshot_rx) = mpsc::channel::<Vec<u8>>(16);
    let (delta_tx, delta_rx) = mpsc::channel::<Vec<u8>>(1024);

    // Start writers
    tokio::spawn(run_snapshot_writer(args.snapshot_uds.clone(), snapshot_rx));
    tokio::spawn(run_delta_writer(args.delta_uds.clone(), delta_rx));

    // Start reader and converter
    run_bridge(args, snapshot_tx, delta_tx).await
}

async fn run_snapshot_writer(path: String, mut rx: mpsc::Receiver<Vec<u8>>) {
    if std::path::Path::new(&path).exists() {
        let _ = std::fs::remove_file(&path);
    }
    let listener = match UnixListener::bind(&path) {
        Ok(l) => l,
        Err(e) => {
            error!(%e, uds = %path, "snapshot bind failed");
            return;
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660));
    }
    info!(uds = %path, "snapshot writer listening");

    // Accept a single client and stream segments, then close.
    match listener.accept().await {
        Ok((sock, _addr)) => {
            let mut framed = FramedWrite::new(sock, LengthDelimitedCodec::new());
            while let Some(seg) = rx.recv().await {
                if let Err(e) = framed.send(bytes::Bytes::from(seg)).await {
                    error!(%e, "snapshot write error");
                    break;
                }
            }
            // Drop framed to close the stream; solana-ultra-rpc will complete snapshot.
            info!("snapshot stream closed");
        }
        Err(e) => error!(%e, "snapshot accept failed"),
    }
}

async fn run_delta_writer(path: String, mut rx: mpsc::Receiver<Vec<u8>>) {
    if std::path::Path::new(&path).exists() {
        let _ = std::fs::remove_file(&path);
    }
    let listener = match UnixListener::bind(&path) {
        Ok(l) => l,
        Err(e) => {
            error!(%e, uds = %path, "delta bind failed");
            return;
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660));
    }
    info!(uds = %path, "delta writer listening");

    // Accept one client and keep streaming forever. If client disconnects, re-accept.
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                let mut framed = FramedWrite::new(sock, LengthDelimitedCodec::new());
                info!("delta client connected");
                while let Some(batch) = rx.recv().await {
                    if let Err(e) = framed.send(bytes::Bytes::from(batch)).await {
                        warn!(%e, "delta write error; waiting for new client");
                        break;
                    }
                }
                // channel ended; exit
                if rx.is_closed() {
                    break;
                }
            }
            Err(e) => {
                warn!(%e, "delta accept failed; retrying");
                time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn run_bridge(
    args: Args,
    snapshot_tx: mpsc::Sender<Vec<u8>>,
    delta_tx: mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    // Bind input UDS and accept producers (e.g., ys-consumer or load generator)
    if std::path::Path::new(&args.input_uds).exists() {
        let _ = std::fs::remove_file(&args.input_uds);
    }
    let listener = UnixListener::bind(&args.input_uds)
        .with_context(|| format!("bind {} failed", &args.input_uds))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&args.input_uds, std::fs::Permissions::from_mode(0o660));
    }
    info!(uds = %args.input_uds, "bridge input listening");

    // Snapshot and batching state lives across client connections
    let mut snapshot_accounts: HashMap<[u8; 32], AccountWire> = HashMap::new();
    let mut snapshot_active = true;
    let mut snapshot_last_slot: u64 = 0;
    let mut snapshot_sender: Option<mpsc::Sender<Vec<u8>>> = Some(snapshot_tx);
    let mut delta_batch: Vec<DeltaWire> = Vec::with_capacity(args.delta_batch_max);
    let mut last_flush = Instant::now();
    let flush_interval = Duration::from_millis(args.delta_flush_ms);
    let mut scratch: Vec<u8> = Vec::with_capacity(8 * 1024);

    loop {
        let (mut sock, _) = listener.accept().await?;
        #[cfg(unix)]
        {
            let _ = socket2::SockRef::from(&sock).set_recv_buffer_size(32 * 1024 * 1024);
        }
        info!("bridge accepted producer connection");
        let mut buf = BytesMut::with_capacity(1 << 20);
        loop {
            let n = sock.read_buf(&mut buf).await?;
            if n == 0 {
                info!("producer disconnected");
                break;
            }
            // decode frames
            loop {
                match decode_record_from_slice(&buf[..], &mut scratch) {
                    Ok((rec, consumed)) => {
                        buf.advance(consumed);
                        match rec {
                            Record::Account(a) => {
                                let wire = AccountWire {
                                    pubkey: a.pubkey,
                                    lamports: a.lamports,
                                    owner: a.owner,
                                    executable: a.executable,
                                    rent_epoch: a.rent_epoch,
                                    data: a.data,
                                };
                                if snapshot_active && a.is_startup {
                                    snapshot_last_slot = snapshot_last_slot.max(a.slot);
                                    snapshot_accounts.insert(a.pubkey, wire);
                                    gauge!("rpc_bridge_snapshot_accounts").set(snapshot_accounts.len() as f64);
                                } else {
                                    if snapshot_active {
                                        snapshot_active = false;
                                        if let Some(tx) = snapshot_sender.take() {
                                            emit_snapshot_segments(
                                                snapshot_last_slot,
                                                args.snapshot_segment_accounts,
                                                &snapshot_accounts,
                                                &tx,
                                            )
                                            .await;
                                            // drop tx to close snapshot stream
                                        }
                                        info!(accounts = snapshot_accounts.len(), slot = snapshot_last_slot, "snapshot emitted");
                                    }
                                    delta_batch.push(DeltaWire {
                                        pubkey: a.pubkey,
                                        slot: a.slot,
                                        account: Some(wire),
                                    });
                                }
                            }
                            Record::Slot { .. } => {}
                            _ => {}
                        }
                    }
                    Err(faststreams::StreamError::De(_)) => break,
                    Err(faststreams::StreamError::BadHeader) => {
                        counter!("rpc_bridge_bad_header_total").increment(1);
                        buf.advance(1);
                        break;
                    }
                    Err(_) => {
                        buf.advance(1);
                        break;
                    }
                }
            }

            // Flush deltas periodically
            if !delta_batch.is_empty()
                && (delta_batch.len() >= args.delta_batch_max
                    || last_flush.elapsed() >= flush_interval)
            {
                let batch = DeltaWireBatch { updates: std::mem::take(&mut delta_batch) };
                match bincode::serialize(&batch) {
                    Ok(bytes) => {
                        let _ = delta_tx.send(bytes).await;
                        counter!("rpc_bridge_delta_batches").increment(1);
                    }
                    Err(e) => warn!(%e, "delta serialize error"),
                }
                last_flush = Instant::now();
            }
        }
    }
}

async fn emit_snapshot_segments(
    base_slot: u64,
    chunk_size: usize,
    accounts: &HashMap<[u8; 32], AccountWire>,
    tx: &mpsc::Sender<Vec<u8>>,
) {
    if accounts.is_empty() {
        return;
    }
    let mut current: Vec<AccountWire> = Vec::with_capacity(chunk_size);
    for (_k, v) in accounts.iter() {
        current.push(v.clone());
        if current.len() >= chunk_size {
            let seg = SnapshotWireSegment { base_slot, accounts: std::mem::take(&mut current) };
            if let Ok(bytes) = bincode::serialize(&seg) {
                let _ = tx.send(bytes).await;
            }
        }
    }
    if !current.is_empty() {
        let seg = SnapshotWireSegment { base_slot, accounts: current };
        if let Ok(bytes) = bincode::serialize(&seg) {
            let _ = tx.send(bytes).await;
        }
    }
}


