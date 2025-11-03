// crates/ys-consumer/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use crossbeam_channel::{bounded, Receiver};
use faststreams::{encode_record_with, EncodeOptions, write_all_vectored, Record, TxUpdate, AccountUpdate, BlockMeta};
use futures::{SinkExt, StreamExt};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use yellowstone_grpc_client::GeyserGrpcClient;
use std::collections::HashMap;
use yellowstone_grpc_proto::prelude::{
    subscribe_update, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

fn uds_connect(path: &str) -> std::io::Result<UnixStream> {
    let s = UnixStream::connect(path)?;
    s.set_nonblocking(false)?;
    s.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
    Ok(s)
}

fn writer_loop(uds_path: String, rx: Receiver<Vec<u8>>, shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>, batch_max: usize, batch_bytes_max: usize) {
    let mut backoff = Duration::from_millis(50);
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
        match uds_connect(&uds_path) {
            Ok(mut stream) => {
                // Best-effort: enlarge send buffer
                let _ = socket2::SockRef::from(&stream).set_send_buffer_size(batch_bytes_max);
                let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_max);
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
                    match rx.recv_timeout(Duration::from_millis(1)) {
                        Ok(first) => {
                            let mut size = first.len();
                            batch.push(first);
                            let start = Instant::now();
                            while batch.len() < batch_max && size < batch_bytes_max {
                                if start.elapsed() >= Duration::from_millis(2) { break; }
                                match rx.try_recv() {
                                    Ok(m) => { size += m.len(); if size > batch_bytes_max { break; } batch.push(m); }
                                    Err(_) => break,
                                }
                            }
                            if let Err(e) = write_all_vectored(&mut stream, &batch) {
                                eprintln!("ys-consumer: write error: {e}");
                                break;
                            }
                            batch.clear();
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                    }
                }
                thread::sleep(Duration::from_millis(100));
                backoff = Duration::from_millis(50);
            }
            Err(err) => {
                eprintln!("ys-consumer: connect {} failed: {err}", uds_path);
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(2));
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let endpoint = std::env::var("YS_ENDPOINT").expect("YS_ENDPOINT");
    let x_token = std::env::var("YS_X_TOKEN").ok();
    let uds_path = std::env::var("ULTRA_UDS").unwrap_or_else(|_| "/var/run/ultra-geyser.sock".to_string());

    let mut builder = GeyserGrpcClient::build_from_static(Box::leak(endpoint.into_boxed_str()));
    if let Some(tok) = x_token {
        builder = builder.x_token(Some(tok))?;
    }
    let mut client = builder.connect().await?;

    let (mut tx, mut rx) = client.subscribe().await?;
    let mut filters = HashMap::new();
    filters.insert("".to_string(), SubscribeRequestFilterSlots::default());
    
    let mut accounts = HashMap::new();
    accounts.insert("".to_string(), SubscribeRequestFilterAccounts::default());
    
    let mut transactions = HashMap::new();
    transactions.insert("".to_string(), SubscribeRequestFilterTransactions::default());
    
    let mut blocks = HashMap::new();
    blocks.insert("".to_string(), SubscribeRequestFilterBlocks::default());
    
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert("".to_string(), SubscribeRequestFilterBlocksMeta::default());
    
    let req = SubscribeRequest {
        // Subscribe broadly; filter in downstream sink if needed
        slots: filters,
        accounts,
        transactions,
        blocks,
        blocks_meta,
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: Some(SubscribeRequestPing { id: 0 }),
        ..Default::default()
    };
    tx.send(req).await?;

    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (txq, rxq) = bounded::<Vec<u8>>(262_144);
    {
        let uds_path_clone = uds_path.clone();
        let sd = shutdown.clone();
        thread::Builder::new().name("ys-writer".into()).spawn(move || {
            writer_loop(uds_path_clone, rxq, &sd, 4096, 4 * 1024 * 1024);
        })?;
    }
    info!("connected to Yellowstone; forwarding to {}", uds_path);

    while let Some(upd) = rx.next().await {
        let upd = match upd { Ok(u) => u, Err(e) => { error!("stream error: {e}"); break; } };
        match upd.update_oneof {
            Some(subscribe_update::UpdateOneof::Transaction(t)) => {
                let mut sig = [0u8; 64];
                // Extract signature from transaction if available
                if let Some(tx_data) = &t.transaction {
                    if tx_data.signature.len() == 64 {
                        sig.copy_from_slice(&tx_data.signature);
                    }
                }
                let rec = Record::Tx(TxUpdate {
                    slot: t.slot,
                    signature: sig,
                    err: t.transaction.as_ref().and_then(|tx| tx.meta.as_ref()).and_then(|m| m.err.as_ref().cloned()).map(|e| format!("{:?}", e)),
                    vote: false, // is_vote not available in new structure
                });
                if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) { let _ = txq.try_send(buf); }
            }
            Some(subscribe_update::UpdateOneof::Account(a)) => {
                if let Some(acc) = &a.account {
                    let rec = Record::Account(AccountUpdate {
                        slot: a.slot,
                        is_startup: a.is_startup,
                        pubkey: bs58::decode(&acc.pubkey).into_vec().unwrap_or_default().try_into().unwrap_or([0;32]),
                        lamports: acc.lamports,
                        owner: bs58::decode(&acc.owner).into_vec().unwrap_or_default().try_into().unwrap_or([0;32]),
                        executable: acc.executable,
                        rent_epoch: acc.rent_epoch,
                        data: acc.data.clone(),
                    });
                    if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) { let _ = txq.try_send(buf); }
                }
            }
            Some(subscribe_update::UpdateOneof::Block(b)) => {
                let bh = if !b.blockhash.is_empty() {
                    bs58::decode(&b.blockhash).into_vec().ok().and_then(|v| v.try_into().ok())
                } else {
                    None
                };
                // leader field not available in new proto version, set to None
                let ld = None;
                let block_time = b.block_time.as_ref().and_then(|ts| if ts.timestamp != 0 { Some(ts.timestamp) } else { None });
                let rec = Record::Block(BlockMeta {
                    slot: b.slot,
                    blockhash: bh,
                    parent_slot: if b.parent_slot > 0 { b.parent_slot - 1 } else { 0 },
                    rewards_len: b.rewards.as_ref().map(|r| r.rewards.len()).unwrap_or(0) as u32,
                    block_time_unix: block_time,
                    leader: ld,
                });
                if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) { let _ = txq.try_send(buf); }
            }
            Some(subscribe_update::UpdateOneof::Slot(s)) => {
                let rec = Record::Slot { slot: s.slot, parent: s.parent, status: s.status as u8 };
                if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) { let _ = txq.try_send(buf); }
            }
            _ => {}
        }
    }
    Ok(())
}