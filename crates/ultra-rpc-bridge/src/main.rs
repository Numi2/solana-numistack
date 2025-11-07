// Numan Thabit 2025
// crates/ultra-rpc-bridge/src/main.rs
#![forbid(unsafe_code)]
use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use clap::Parser;
use faststreams::{decode_record_from_slice, Record};
use futures_util::SinkExt;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::time;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "Bridge faststreams → solana-ultra-rpc snapshot/delta sockets",
    rename_all = "kebab-case"
)]
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

#[derive(Clone, Serialize)]
enum DeltaStreamMessage {
    SnapshotComplete { slot: u64 },
    Updates(DeltaWireBatch),
}

async fn send_snapshot_complete(delta_tx: &mpsc::Sender<Vec<u8>>, slot: u64) -> Result<()> {
    let message = DeltaStreamMessage::SnapshotComplete { slot };
    let bytes = bincode::serialize(&message)
        .with_context(|| format!("failed to serialize snapshot-complete marker for slot {slot}"))?;
    delta_tx
        .send(bytes)
        .await
        .map_err(|e| anyhow!("delta channel send failed: {e}"))
}

async fn send_delta_updates(delta_tx: &mpsc::Sender<Vec<u8>>, batch: DeltaWireBatch) -> Result<()> {
    let message = DeltaStreamMessage::Updates(batch);
    let bytes = bincode::serialize(&message).context("failed to serialize delta batch message")?;
    delta_tx
        .send(bytes)
        .await
        .map_err(|e| anyhow!("delta channel send failed: {e}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    if let Some(addr) = &args.metrics_addr {
        let socket_addr: SocketAddr = addr
            .parse()
            .with_context(|| format!("failed to parse metrics listen address: {addr}"))?;
        PrometheusBuilder::new()
            .with_http_listener(socket_addr)
            .install()
            .context("failed to install Prometheus metrics exporter")?;
    }

    // Prepare output listeners (bridge acts as server for RPC to connect)
    let (snapshot_tx, snapshot_rx) = mpsc::channel::<Vec<u8>>(16);
    let (delta_tx, delta_rx) = mpsc::channel::<Vec<u8>>(8192);

    // Start writers
    tokio::spawn(run_snapshot_writer(args.snapshot_uds.clone(), snapshot_rx));
    tokio::spawn(run_delta_writer(args.delta_uds.clone(), delta_rx));

    // Start reader and converter
    run_bridge(args, snapshot_tx, delta_tx).await
}

async fn run_snapshot_writer(path: String, mut rx: mpsc::Receiver<Vec<u8>>) {
    if let Err(e) = std::fs::remove_file(&path) {
        if e.kind() != ErrorKind::NotFound {
            warn!(%e, uds = %path, "failed to remove existing snapshot socket");
        }
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
        if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660)) {
            warn!(%e, uds = %path, "failed to set snapshot socket permissions");
        }
    }
    info!(uds = %path, "snapshot writer listening");

    // Accept a single client and stream segments, then close.
    match listener.accept().await {
        Ok((sock, _addr)) => {
            let mut framed = FramedWrite::new(sock, LengthDelimitedCodec::new());
            while let Some(seg) = rx.recv().await {
                let bytes = Bytes::from(seg);
                if let Err(e) = framed.send(bytes).await {
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
    if let Err(e) = std::fs::remove_file(&path) {
        if e.kind() != ErrorKind::NotFound {
            warn!(%e, uds = %path, "failed to remove existing delta socket");
        }
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
        if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660)) {
            warn!(%e, uds = %path, "failed to set delta socket permissions");
        }
    }
    info!(uds = %path, "delta writer listening");

    // Accept one client and keep streaming forever. If client disconnects, re-accept.
    let mut pending_batches: VecDeque<Bytes> = VecDeque::new();
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                #[cfg(unix)]
                {
                    use socket2::SockRef;
                    let _ = SockRef::from(&sock).set_send_buffer_size(16 * 1024 * 1024);
                }
                let mut framed = FramedWrite::new(sock, LengthDelimitedCodec::new());
                info!("delta client connected");
                loop {
                    if pending_batches.is_empty() {
                        if rx.is_closed() {
                            info!("delta channel closed; shutting down writer");
                            return;
                        }
                        match rx.recv().await {
                            Some(batch) => pending_batches.push_back(Bytes::from(batch)),
                            None => {
                                info!("delta channel closed; shutting down writer");
                                return;
                            }
                        }
                    }

                    while let Ok(batch) = rx.try_recv() {
                        pending_batches.push_back(Bytes::from(batch));
                    }

                    let Some(bytes) = pending_batches.pop_front() else {
                        continue;
                    };

                    let to_send = bytes.clone();
                    if let Err(e) = framed.send(to_send).await {
                        warn!(%e, "delta write error; waiting for new client");
                        pending_batches.push_front(bytes);
                        break;
                    }
                }

                if rx.is_closed() && pending_batches.is_empty() {
                    info!("delta channel closed; shutting down writer");
                    return;
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
    let mut snapshot_complete_sent = false;
    let mut delta_batch: Vec<DeltaWire> = Vec::with_capacity(args.delta_batch_max);
    let mut last_flush = Instant::now();
    let base_flush = Duration::from_millis(args.delta_flush_ms);
    let mut cur_flush = base_flush;
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
                                    gauge!("rpc_bridge_snapshot_accounts")
                                        .set(snapshot_accounts.len() as f64);
                                } else {
                                    if snapshot_active {
                                        snapshot_active = false;
                                        if let Some(tx) = snapshot_sender.take() {
                                            if let Err(e) = emit_snapshot_segments(
                                                snapshot_last_slot,
                                                args.snapshot_segment_accounts,
                                                &snapshot_accounts,
                                                &tx,
                                            )
                                            .await
                                            {
                                                error!(%e, slot = snapshot_last_slot, "snapshot emission failed");
                                                return Err(e);
                                            }
                                            // drop tx to close snapshot stream
                                        }
                                        if !snapshot_complete_sent {
                                            if let Err(e) = send_snapshot_complete(
                                                &delta_tx,
                                                snapshot_last_slot,
                                            )
                                            .await
                                            {
                                                error!(%e, slot = snapshot_last_slot, "failed to notify snapshot completion");
                                                return Err(e);
                                            }
                                            snapshot_complete_sent = true;
                                        }
                                        info!(
                                            accounts = snapshot_accounts.len(),
                                            slot = snapshot_last_slot,
                                            "snapshot emitted"
                                        );
                                    } else if !snapshot_complete_sent {
                                        if let Err(e) =
                                            send_snapshot_complete(&delta_tx, snapshot_last_slot)
                                                .await
                                        {
                                            error!(%e, slot = snapshot_last_slot, "failed to notify snapshot completion");
                                            return Err(e);
                                        }
                                        snapshot_complete_sent = true;
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

            // Adaptive flush: shrink delay under pressure, restore slowly when low
            if delta_batch.len() >= args.delta_batch_max * 3 / 4 || buf.len() >= (1 << 18) {
                cur_flush = base_flush / 2;
                if cur_flush < Duration::from_millis(1) {
                    cur_flush = Duration::from_millis(1);
                }
            } else if cur_flush < base_flush {
                cur_flush = (cur_flush + Duration::from_millis(1)).min(base_flush);
            }

            // Flush deltas periodically
            if !delta_batch.is_empty()
                && (delta_batch.len() >= args.delta_batch_max
                    || last_flush.elapsed() >= cur_flush)
            {
                if !snapshot_complete_sent {
                    if let Err(e) = send_snapshot_complete(&delta_tx, snapshot_last_slot).await {
                        error!(%e, slot = snapshot_last_slot, "failed to notify snapshot completion");
                        return Err(e);
                    }
                    snapshot_complete_sent = true;
                }
                let batch = DeltaWireBatch {
                    updates: std::mem::take(&mut delta_batch),
                };
                if let Err(e) = send_delta_updates(&delta_tx, batch).await {
                    error!(%e, "delta channel send failed");
                    return Err(e);
                }
                counter!("rpc_bridge_delta_batches").increment(1);
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
) -> Result<()> {
    if accounts.is_empty() {
        return Ok(());
    }
    let mut current: Vec<AccountWire> = Vec::with_capacity(chunk_size);
    for (_k, v) in accounts.iter() {
        current.push(v.clone());
        if current.len() >= chunk_size {
            let accounts = std::mem::take(&mut current);
            let seg = SnapshotWireSegment {
                base_slot,
                accounts,
            };
            let bytes = bincode::serialize(&seg).with_context(|| {
                format!("failed to serialize snapshot segment for slot {base_slot}")
            })?;
            tx.send(bytes)
                .await
                .map_err(|e| anyhow!("snapshot channel send failed: {e}"))?;
        }
    }
    if !current.is_empty() {
        let seg = SnapshotWireSegment {
            base_slot,
            accounts: current,
        };
        let bytes = bincode::serialize(&seg).with_context(|| {
            format!("failed to serialize tail snapshot segment for slot {base_slot}")
        })?;
        tx.send(bytes)
            .await
            .map_err(|e| anyhow!("snapshot channel send failed: {e}"))?;
    }
    Ok(())
}
