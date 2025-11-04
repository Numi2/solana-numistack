// Numan Thabit 2025
//! Connector utilities for consuming the geyser ultra aggregator streams.

use std::path::Path;

use anyhow::{Context, Result};
use futures::TryStreamExt;
use serde::Deserialize;
use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::cache::{AccountUpdate, SnapshotSegment};

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

    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        while let Some(frame_res) = framed.try_next().await.transpose() {
            match frame_res {
                Ok(bytes) => match decode_snapshot_segment(bytes.as_ref()) {
                    Ok(segment) => {
                        if tx.send(Ok(segment)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        break;
                    }
                },
                Err(err) => {
                    let _ = tx.send(Err(err.into())).await;
                    break;
                }
            }
        }
    });

    Ok(ReceiverStream::new(rx))
}

/// Establish a stream of live account deltas.
pub async fn connect_delta_stream(
    socket_path: &Path,
) -> Result<impl Stream<Item = Result<Vec<AccountUpdate>>>> {
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("failed to connect delta socket: {}", socket_path.display()))?;
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(4 * 1024 * 1024)
        .new_codec();
    let mut framed = FramedRead::new(stream, codec);

    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        while let Some(frame_res) = framed.try_next().await.transpose() {
            match frame_res {
                Ok(bytes) => match decode_delta_batch(bytes.as_ref()) {
                    Ok(batch) => {
                        if tx.send(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        break;
                    }
                },
                Err(err) => {
                    let _ = tx.send(Err(err.into())).await;
                    break;
                }
            }
        }
    });

    Ok(ReceiverStream::new(rx))
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

fn decode_delta_batch(bytes: &[u8]) -> Result<Vec<AccountUpdate>> {
    let payload: DeltaWireBatch = bincode::deserialize(bytes)?;
    payload
        .updates
        .into_iter()
        .map(|wire| wire.try_into())
        .collect()
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
