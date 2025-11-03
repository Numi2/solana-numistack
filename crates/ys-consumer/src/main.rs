// crates/ys-consumer/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use crossbeam_channel::{bounded, Receiver};
use faststreams::{encode_record, write_all_vectored, Record, TxUpdate, AccountUpdate, BlockMeta};
use futures::{SinkExt, StreamExt};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeUpdate,
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
                let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_max);
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
                    match rx.recv_timeout(Duration::from_millis(5)) {
                        Ok(first) => {
                            let mut size = first.len();
                            batch.push(first);
                            while batch.len() < batch_max && size < batch_bytes_max {
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
    // Use compression if supported
    builder = builder.accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip);
    let mut client = builder.connect().await?;

    let (mut tx, mut rx) = client.subscribe().await?;
    let req = SubscribeRequest {
        // Subscribe broadly; filter in downstream sink if needed
        slots: Some(SubscribeRequestFilterSlots::default()),
        accounts: Some(SubscribeRequestFilterAccounts::default()),
        transactions: Some(SubscribeRequestFilterTransactions::default()),
        blocks: Some(SubscribeRequestFilterBlocks::default()),
        blocks_meta: Some(SubscribeRequestFilterBlocksMeta::default()),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: Some(true),
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
                if let Ok(v) = bs58::decode(&t.signature).into_vec() { if v.len()==64 { sig.copy_from_slice(&v[..]); } }
                let rec = Record::Tx(TxUpdate {
                    slot: t.slot,
                    signature: sig,
                    err: if t.is_failed { Some("failed".into()) } else { None },
                    vote: t.is_vote,
                });
                if let Ok(buf) = encode_record(&rec) { let _ = txq.try_send(buf); }
            }
            Some(subscribe_update::UpdateOneof::Account(a)) => {
                let rec = Record::Account(AccountUpdate {
                    slot: a.slot,
                    is_startup: false,
                    pubkey: bs58::decode(a.pubkey).into_vec().unwrap_or_default().try_into().unwrap_or([0;32]),
                    lamports: a.lamports as u64,
                    owner: bs58::decode(a.owner).into_vec().unwrap_or_default().try_into().unwrap_or([0;32]),
                    executable: a.executable,
                    rent_epoch: a.rent_epoch as u64,
                    data: a.data,
                });
                if let Ok(buf) = encode_record(&rec) { let _ = txq.try_send(buf); }
            }
            Some(subscribe_update::UpdateOneof::Block(b)) => {
                let bh = b.blockhash.and_then(|s| bs58::decode(s).into_vec().ok()).and_then(|v| v.try_into().ok());
                let ld = b.leader.and_then(|s| bs58::decode(s).into_vec().ok()).and_then(|v| v.try_into().ok());
                let rec = Record::Block(BlockMeta {
                    slot: b.slot,
                    blockhash: bh,
                    parent_slot: b.parent_slot.unwrap_or(0),
                    rewards_len: b.rewards.map(|r| r.rewards.len()).unwrap_or(0) as u32,
                    block_time_unix: b.block_time,
                    leader: ld,
                });
                if let Ok(buf) = encode_record(&rec) { let _ = txq.try_send(buf); }
            }
            Some(subscribe_update::UpdateOneof::Slot(s)) => {
                let rec = Record::Slot { slot: s.slot, parent: s.parent, status: s.status as u8 };
                if let Ok(buf) = encode_record(&rec) { let _ = txq.try_send(buf); }
            }
            _ => {}
        }
    }
    Ok(())
}