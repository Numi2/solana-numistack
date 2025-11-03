// crates/ys-consumer/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use crossbeam_channel::{bounded, Receiver};
use crossbeam_queue::ArrayQueue;
use faststreams::{encode_record_with, EncodeOptions, write_all_vectored, Record, TxUpdate, AccountUpdate, BlockMeta};
use futures::{SinkExt, StreamExt};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::{Duration, Instant};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
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

fn writer_loop(uds_path: String, rx: Receiver<Vec<u8>>, shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>, batch_max: usize, batch_bytes_max: usize, flush_interval: Duration) {
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
                    match rx.recv_timeout(flush_interval) {
                        Ok(first) => {
                            let mut size = first.len();
                            batch.push(first);
                            let start = Instant::now();
                            while batch.len() < batch_max && size < batch_bytes_max {
                                if start.elapsed() >= flush_interval { break; }
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

fn writer_loop_spsc(uds_path: String, q: std::sync::Arc<ArrayQueue<Vec<u8>>>, shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>, batch_max: usize, batch_bytes_max: usize, flush_interval: Duration) {
    let mut backoff = Duration::from_millis(50);
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
        match uds_connect(&uds_path) {
            Ok(mut stream) => {
                let _ = socket2::SockRef::from(&stream).set_send_buffer_size(batch_bytes_max);
                let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_max);
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
                    match q.pop() {
                        Some(first) => {
                            let mut size = first.len();
                            batch.push(first);
                            let start = Instant::now();
                            while batch.len() < batch_max && size < batch_bytes_max {
                                if start.elapsed() >= flush_interval { break; }
                                if let Some(m) = q.pop() {
                                    size += m.len();
                                    if size > batch_bytes_max { break; }
                                    batch.push(m);
                                } else {
                                    break;
                                }
                            }
                            if let Err(e) = write_all_vectored(&mut stream, &batch) {
                                eprintln!("ys-consumer: write error: {e}");
                                break;
                            }
                            batch.clear();
                        }
                        None => {
                            thread::sleep(flush_interval);
                            continue;
                        }
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
    let metrics_addr = std::env::var("YS_METRICS_ADDR").ok();

    if let Some(addr) = metrics_addr.as_deref() {
        let _ = PrometheusBuilder::new().with_http_listener(addr.parse::<std::net::SocketAddr>().unwrap()).install();
    }

    let mut builder = GeyserGrpcClient::build_from_static(Box::leak(endpoint.into_boxed_str()));
    if let Some(tok) = x_token {
        builder = builder.x_token(Some(tok))?;
    }
    let mut client = builder.connect().await?;

    let (mut tx, mut rx) = client.subscribe().await?;
    fn env_bool(name: &str, default: bool) -> bool {
        match std::env::var(name) {
            Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "y"),
            Err(_) => default,
        }
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name).ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(default)
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(default)
    }

    let sub_slots = env_bool("YS_SUB_SLOTS", true);
    let sub_accounts = env_bool("YS_SUB_ACCOUNTS", true);
    let sub_transactions = env_bool("YS_SUB_TRANSACTIONS", true);
    let sub_blocks = env_bool("YS_SUB_BLOCKS", true);
    let sub_blocks_meta = env_bool("YS_SUB_BLOCKS_META", true);

    let mut slots = HashMap::new();
    if sub_slots { slots.insert("".to_string(), SubscribeRequestFilterSlots::default()); }
    let mut accounts = HashMap::new();
    if sub_accounts { accounts.insert("".to_string(), SubscribeRequestFilterAccounts::default()); }
    let mut transactions = HashMap::new();
    if sub_transactions { transactions.insert("".to_string(), SubscribeRequestFilterTransactions::default()); }
    let mut blocks = HashMap::new();
    if sub_blocks { blocks.insert("".to_string(), SubscribeRequestFilterBlocks::default()); }
    let mut blocks_meta = HashMap::new();
    if sub_blocks_meta { blocks_meta.insert("".to_string(), SubscribeRequestFilterBlocksMeta::default()); }
    
    let req = SubscribeRequest {
        slots,
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
    let queue_cap = env_usize("YS_QUEUE_CAP", 65_536);
    let batch_max = env_usize("YS_BATCH_MAX", 1024);
    let batch_bytes_max = env_usize("YS_BATCH_BYTES_MAX", 2 * 1024 * 1024);
    let flush_interval = Duration::from_millis(env_u64("YS_FLUSH_INTERVAL_MS", 1));
    let use_spsc = env_bool("YS_SPSC", false);

    // queue and writer
    let (txq_opt, spsc_q_opt) = if use_spsc {
        (None, Some(std::sync::Arc::new(ArrayQueue::<Vec<u8>>::new(queue_cap))))
    } else {
        let (txq, rxq) = bounded::<Vec<u8>>(queue_cap);
        let uds_path_clone = uds_path.clone();
        let sd = shutdown.clone();
        thread::Builder::new().name("ys-writer".into()).spawn(move || {
            writer_loop(uds_path_clone, rxq, &sd, batch_max, batch_bytes_max, flush_interval);
        })?;
        (Some(txq), None)
    };

    if let Some(q) = &spsc_q_opt {
        let uds_path_clone = uds_path.clone();
        let sd = shutdown.clone();
        let q_clone = q.clone();
        thread::Builder::new().name("ys-writer".into()).spawn(move || {
            writer_loop_spsc(uds_path_clone, q_clone, &sd, batch_max, batch_bytes_max, flush_interval);
        })?;
    }
    info!("connected to Yellowstone; forwarding to {}", uds_path);

    // metrics: queue depth sampler
    if metrics_addr.is_some() {
        if let Some(txq) = &txq_opt {
            let txq = txq.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(250));
                loop {
                    tick.tick().await;
                    gauge!("ys_consumer_queue_depth").set(txq.len() as f64);
                }
            });
        }
        if let Some(q) = &spsc_q_opt {
            let q = q.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(250));
                loop {
                    tick.tick().await;
                    gauge!("ys_consumer_queue_depth").set(q.len() as f64);
                }
            });
        }
    }

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
                if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) {
                    if let Some(txq) = &txq_opt {
                        if txq.try_send(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                    } else if let Some(q) = &spsc_q_opt {
                        if q.push(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                    }
                }
            }
            Some(subscribe_update::UpdateOneof::Account(a)) => {
                if let Some(acc) = &a.account {
                    let pubkey = bs58::decode(&acc.pubkey)
                        .into_vec()
                        .ok()
                        .and_then(|v| v.try_into().ok())
                        .unwrap_or([0; 32]);
                    let owner = bs58::decode(&acc.owner)
                        .into_vec()
                        .ok()
                        .and_then(|v| v.try_into().ok())
                        .unwrap_or([0; 32]);
                    let rec = Record::Account(AccountUpdate {
                        slot: a.slot,
                        is_startup: a.is_startup,
                        pubkey,
                        lamports: acc.lamports,
                        owner,
                        executable: acc.executable,
                        rent_epoch: acc.rent_epoch,
                        data: acc.data.clone(),
                    });
                    if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) {
                        if let Some(txq) = &txq_opt {
                            if txq.try_send(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                        } else if let Some(q) = &spsc_q_opt {
                            if q.push(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                        }
                    }
                }
            }
            Some(subscribe_update::UpdateOneof::Block(b)) => {
                let bh = if !b.blockhash.is_empty() {
                    bs58::decode(&b.blockhash).into_vec().ok().and_then(|v| v.try_into().ok())
                } else { None };
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
                if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) {
                    if let Some(txq) = &txq_opt {
                        if txq.try_send(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                    } else if let Some(q) = &spsc_q_opt {
                        if q.push(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                    }
                }
            }
            Some(subscribe_update::UpdateOneof::Slot(s)) => {
                let rec = Record::Slot { slot: s.slot, parent: s.parent, status: s.status as u8 };
                if let Ok(buf) = encode_record_with(&rec, EncodeOptions::latency_uds()) {
                    if let Some(txq) = &txq_opt {
                        if txq.try_send(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                    } else if let Some(q) = &spsc_q_opt {
                        if q.push(buf).is_err() { counter!("ys_consumer_dropped_total").increment(1); }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}