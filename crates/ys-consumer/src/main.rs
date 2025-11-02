// crates/ys-consumer/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use faststreams::{encode_record, Record, TxUpdate, AccountUpdate, BlockMeta};
use futures::{SinkExt, StreamExt};
use std::io::Write;
use std::os::unix::net::UnixStream;
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

    let mut uds = uds_connect(&uds_path)?;
    info!("connected to Yellowstone; forwarding to {uds_path}");

    while let Some(upd) = rx.next().await {
        let upd = match upd { Ok(u) => u, Err(e) => { error!("stream error: {e}"); break; } };
        match upd.update_oneof {
            Some(subscribe_update::UpdateOneof::Transaction(t)) => {
                let rec = Record::Tx(TxUpdate {
                    slot: t.slot,
                    signature_b58: t.signature,
                    err: if t.is_failed { Some("failed".into()) } else { None },
                    vote: t.is_vote,
                });
                if let Ok(buf) = encode_record(&rec) { let _ = uds.write_all(&buf); }
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
                if let Ok(buf) = encode_record(&rec) { let _ = uds.write_all(&buf); }
            }
            Some(subscribe_update::UpdateOneof::Block(b)) => {
                let rec = Record::Block(BlockMeta {
                    slot: b.slot,
                    blockhash_b58: b.blockhash,
                    parent_slot: b.parent_slot.unwrap_or(0),
                    rewards_len: b.rewards.map(|r| r.rewards.len()).unwrap_or(0) as u32,
                    block_time_unix: b.block_time,
                    leader: b.leader,
                });
                if let Ok(buf) = encode_record(&rec) { let _ = uds.write_all(&buf); }
            }
            Some(subscribe_update::UpdateOneof::Slot(s)) => {
                let rec = Record::Slot { slot: s.slot, parent: s.parent, status: s.status as u8 };
                if let Ok(buf) = encode_record(&rec) { let _ = uds.write_all(&buf); }
            }
            _ => {}
        }
    }
    Ok(())
}