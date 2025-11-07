// Numan Thabit 2025
//! Connector utilities for consuming the geyser ultra aggregator streams.

use std::collections::VecDeque;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::{Context as AnyhowContext, Result};
use futures::TryStreamExt;
use metrics::{gauge, histogram};
use serde::Deserialize;
use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::Stream;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::cache::{AccountUpdate, SnapshotSegment};

#[derive(Debug)]
struct Stamped<T> {
    at: Instant,
    value: T,
}

struct IngestRx<T> {
    rx: mpsc::Receiver<Stamped<T>>,
    queue_name: &'static str,
}

impl<T> IngestRx<T> {
    fn new(rx: mpsc::Receiver<Stamped<T>>, queue_name: &'static str) -> Self {
        Self { rx, queue_name }
    }
}

impl<T> Stream for IngestRx<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.rx).poll_recv(cx) {
            Poll::Ready(opt) => {
                if let Some(st) = opt {
                    let depth = this.rx.len() as f64;
                    let wait_ms = st.at.elapsed().as_secs_f64() * 1_000.0;
                    match this.queue_name {
                        "delta" => {
                            gauge!("ingest_delta_queue_depth", depth);
                            gauge!("ingest_oldest_msg_age_ms", wait_ms, "queue" => "delta");
                            histogram!("ingest_batch_wait_ms", wait_ms, "queue" => "delta");
                        }
                        "snapshot" => {
                            gauge!("ingest_snapshot_queue_depth", depth);
                            gauge!("ingest_oldest_msg_age_ms", wait_ms, "queue" => "snapshot");
                            histogram!("ingest_batch_wait_ms", wait_ms, "queue" => "snapshot");
                        }
                        _ => {}
                    }
                    Poll::Ready(Some(st.value))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn flush_backlog(
    backlog: &mut VecDeque<Stamped<Result<DeltaStreamItem>>>,
    tx: &mpsc::Sender<Stamped<Result<DeltaStreamItem>>>,
    soft_cap: usize,
    stale_dur: std::time::Duration,
) -> bool {
    // returns false if channel is closed
    while let Some(item) = backlog.pop_front() {
        match tx.try_send(item) {
            Ok(_) => {
                gauge!("ingest_delta_backlog_depth", backlog.len() as f64);
            }
            Err(TrySendError::Full(item)) => {
                // Decide whether to drop stale head under pressure
                if backlog.len() + 1 >= soft_cap && item.at.elapsed() >= stale_dur {
                    metrics::counter!("ingest_drop_total", 1u64, "queue" => "delta", "reason" => "stale");
                    gauge!("ingest_delta_backlog_depth", backlog.len() as f64);
                    // continue without requeuing this item (drop)
                    continue;
                }
                // push it back and stop flushing
                backlog.push_front(item);
                gauge!("ingest_delta_backlog_depth", backlog.len() as f64);
                break;
            }
            Err(TrySendError::Closed(_)) => return false,
        }
    }
    true
}

/// Establish a connection to the snapshot stream and expose it as an async stream of segments.
pub async fn connect_snapshot_stream(
    socket_path: &Path,
) -> Result<impl Stream<Item = Result<SnapshotSegment>>> {
    let stream = UnixStream::connect(socket_path).await.with_context(|| {
        format!(
            "failed to connect snapshot socket: {}",
            socket_path.display()
        )
    })?;
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec();
    let mut framed = FramedRead::new(stream, codec);

    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        gauge!("ultra_ingest_snapshot_stream_open", 1.0);
        while let Some(frame_res) = framed.try_next().await.transpose() {
            match frame_res {
                Ok(bytes) => {
                    let res = {
                        let t0 = Instant::now();
                        let res = decode_snapshot_segment(bytes.as_ref());
                        let dt = t0.elapsed().as_micros() as f64;
                        histogram!("ultra_ingest_snapshot_decode_us", dt);
                        res
                    };
                    match res {
                        Ok(segment) => {
                            let sstart = Instant::now();
                            let stamped = Stamped { at: Instant::now(), value: Ok(segment) };
                            if tx.send(stamped).await.is_err() {
                                break;
                            }
                            let send_wait = sstart.elapsed().as_micros() as f64;
                            histogram!("ultra_ingest_snapshot_send_wait_us", send_wait);
                        }
                        Err(err) => {
                            let _ = tx.send(Stamped { at: Instant::now(), value: Err(err) }).await;
                            break;
                        }
                    }
                },
                Err(err) => {
                    let _ = tx.send(Stamped { at: Instant::now(), value: Err(err.into()) }).await;
                    break;
                }
            }
        }
        gauge!("ultra_ingest_snapshot_stream_open", 0.0);
    });

    Ok(IngestRx::new(rx, "snapshot"))
}

/// Establish a stream of live account deltas.
pub async fn connect_delta_stream(
    socket_path: &Path,
) -> Result<impl Stream<Item = Result<DeltaStreamItem>>> {
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("failed to connect delta socket: {}", socket_path.display()))?;
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(4 * 1024 * 1024)
        .new_codec();
    let mut framed = FramedRead::new(stream, codec);

    let (tx, rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        gauge!("ultra_ingest_delta_stream_open", 1.0);
        let soft_cap: usize = std::env::var("ULTRA_INGEST_DELTA_SOFTCAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096);
        let stale_ms: u64 = std::env::var("ULTRA_INGEST_STALE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let stale_dur = std::time::Duration::from_millis(stale_ms);
        let mut backlog: VecDeque<Stamped<Result<DeltaStreamItem>>> = VecDeque::new();

        

        while let Some(frame_res) = framed.try_next().await.transpose() {
            match frame_res {
                Ok(bytes) => {
                    let res = {
                        let t0 = Instant::now();
                        let res = decode_delta_message(bytes.as_ref());
                        let dt = t0.elapsed().as_micros() as f64;
                        histogram!("ultra_ingest_delta_decode_us", dt);
                        res
                    };
                    match res {
                        Ok(item) => {
                            if let super::geyser::DeltaStreamItem::Updates(ref updates) = item {
                                histogram!("ultra_ingest_delta_updates", updates.len() as f64);
                            }
                            let stamped = Stamped { at: Instant::now(), value: Ok(item) };
                            // First try to flush backlog
                            if !flush_backlog(&mut backlog, &tx, soft_cap, stale_dur) { break; }
                            match tx.try_send(stamped) {
                                Ok(_) => {}
                                Err(TrySendError::Full(item)) => {
                                    backlog.push_back(item);
                                    gauge!("ingest_delta_backlog_depth", backlog.len() as f64);
                                    // Under sustained pressure, drop newest if not stale head
                                    if backlog.len() > soft_cap {
                                        // Prefer dropping stale head if any, else drop newest
                                        if let Some(front) = backlog.front() {
                                            if front.at.elapsed() >= stale_dur {
                                                backlog.pop_front();
                                                metrics::counter!("ingest_drop_total", 1u64, "queue" => "delta", "reason" => "stale");
                                            } else {
                                                backlog.pop_back();
                                                metrics::counter!("ingest_drop_total", 1u64, "queue" => "delta", "reason" => "full");
                                            }
                                        } else {
                                            backlog.pop_back();
                                            metrics::counter!("ingest_drop_total", 1u64, "queue" => "delta", "reason" => "full");
                                        }
                                        gauge!("ingest_delta_backlog_depth", backlog.len() as f64);
                                    }
                                }
                                Err(TrySendError::Closed(_)) => break,
                            }
                        }
                        Err(err) => {
                            let stamped = Stamped { at: Instant::now(), value: Err(err) };
                            if !flush_backlog(&mut backlog, &tx, soft_cap, stale_dur) { break; }
                            if let Err(e) = tx.try_send(stamped) {
                                match e {
                                    TrySendError::Full(st) => {
                                        backlog.push_back(st);
                                        gauge!("ingest_delta_backlog_depth", backlog.len() as f64);
                                    }
                                    TrySendError::Closed(_) => break,
                                }
                            }
                            break;
                        }
                    }
                },
                Err(err) => {
                    let stamped = Stamped { at: Instant::now(), value: Err(err.into()) };
                    if !flush_backlog(&mut backlog, &tx, soft_cap, stale_dur) { break; }
                    if let Err(e) = tx.try_send(stamped) {
                        match e {
                            TrySendError::Full(st) => {
                                backlog.push_back(st);
                                gauge!("ingest_delta_backlog_depth", backlog.len() as f64);
                            }
                            TrySendError::Closed(_) => break,
                        }
                    }
                    break;
                }
            }
        }
        gauge!("ultra_ingest_delta_stream_open", 0.0);
    });

    Ok(IngestRx::new(rx, "delta"))
}

/// Stream item emitted by the geyser delta transport.
#[derive(Debug)]
pub enum DeltaStreamItem {
    /// Marker indicating the snapshot baseline has been emitted.
    SnapshotComplete {
        /// Highest slot covered by the baseline snapshot.
        slot: u64,
    },
    /// Batch of incremental account updates originating after the baseline.
    Updates(Vec<AccountUpdate>),
}

fn decode_snapshot_segment(bytes: &[u8]) -> Result<SnapshotSegment> {
    let payload: SnapshotWireSegment = bincode::deserialize(bytes)?;
    let mut accounts = Vec::with_capacity(payload.accounts.len());
    for account in payload.accounts {
        accounts.push(account.try_into()?);
    }
    Ok(SnapshotSegment {
        base_slot: payload.base_slot,
        accounts,
    })
}

fn decode_delta_message(bytes: &[u8]) -> Result<DeltaStreamItem> {
    let payload: DeltaStreamMessage = bincode::deserialize(bytes)?;
    match payload {
        DeltaStreamMessage::SnapshotComplete { slot } => {
            Ok(DeltaStreamItem::SnapshotComplete { slot })
        }
        DeltaStreamMessage::Updates(batch) => {
            let updates: Vec<AccountUpdate> = batch
                .updates
                .into_iter()
                .map(AccountUpdate::try_from)
                .collect::<std::result::Result<_, _>>()?;
            Ok(DeltaStreamItem::Updates(updates))
        }
    }
}

#[derive(Deserialize)]
struct SnapshotWireSegment {
    base_slot: u64,
    accounts: Vec<AccountWire>,
}

#[derive(Deserialize)]
struct DeltaWireBatch {
    updates: Vec<DeltaWire>,
}

#[derive(Deserialize)]
enum DeltaStreamMessage {
    SnapshotComplete { slot: u64 },
    Updates(DeltaWireBatch),
}

#[derive(Clone, Deserialize)]
struct AccountWire {
    pubkey: [u8; 32],
    lamports: u64,
    owner: [u8; 32],
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
}

#[derive(Clone, Deserialize)]
struct DeltaWire {
    pubkey: [u8; 32],
    slot: u64,
    #[serde(default)]
    account: Option<AccountWire>,
}

impl TryFrom<AccountWire> for (Pubkey, AccountSharedData) {
    type Error = anyhow::Error;

    fn try_from(value: AccountWire) -> Result<Self, Self::Error> {
        let owner = Pubkey::try_from(value.owner.as_slice())?;
        let pubkey = Pubkey::try_from(value.pubkey.as_slice())?;
        let account = solana_sdk::account::Account {
            lamports: value.lamports,
            data: value.data,
            owner,
            executable: value.executable,
            rent_epoch: value.rent_epoch,
        };
        Ok((pubkey, AccountSharedData::from(account)))
    }
}

impl TryFrom<DeltaWire> for AccountUpdate {
    type Error = anyhow::Error;

    fn try_from(value: DeltaWire) -> Result<Self, Self::Error> {
        let pubkey = Pubkey::try_from(value.pubkey.as_slice())?;
        let data = match value.account {
            Some(account) => Some(AccountSharedData::from(solana_sdk::account::Account {
                lamports: account.lamports,
                data: account.data,
                owner: Pubkey::try_from(account.owner.as_slice())?,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
            })),
            None => None,
        };
        Ok(AccountUpdate {
            pubkey,
            data,
            slot: value.slot,
        })
    }
}
