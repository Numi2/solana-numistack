// Numan Thabit 2025
//! Ingest pipeline wiring validator Geyser streams into the cache.

use std::sync::Arc;

use tokio_stream::{Stream, StreamExt};
use metrics::{counter, histogram};
use std::time::Instant;
use once_cell::sync::Lazy;

use crate::cache::{AccountCache, AccountCacheBuilder, AccountUpdate, SnapshotSegment};
use crate::ingest::geyser::DeltaStreamItem;
use crate::rpc::SlotTracker;

pub mod geyser;

/// Bootstrap the cache by replaying a snapshot stream to completion.
pub async fn prewarm_from_snapshot<S>(
    cache: &AccountCache,
    slot_tracker: &SlotTracker,
    mut stream: S,
) -> anyhow::Result<()>
where
    S: Stream<Item = anyhow::Result<SnapshotSegment>> + Unpin,
{
    let mut builder = AccountCacheBuilder::empty(cache.shard_count());
    let mut last_slot = 0u64;
    while let Some(segment) = stream.try_next().await? {
        last_slot = segment.base_slot;
        segment.hydrate(&mut builder);
    }
    cache.publish(builder);
    slot_tracker.update(last_slot);
    Ok(())
}

/// Apply a stream of update batches, publishing snapshots atomically.
pub async fn apply_deltas<S>(
    cache: Arc<AccountCache>,
    slot_tracker: Arc<SlotTracker>,
    mut stream: S,
) -> anyhow::Result<()>
where
    S: Stream<Item = anyhow::Result<DeltaStreamItem>> + Unpin,
{
    let mut snapshot_ready = false;
    let mut pending: Vec<Vec<AccountUpdate>> = Vec::new();

    while let Some(item) = stream.try_next().await? {
        match item {
            DeltaStreamItem::SnapshotComplete { slot } => {
                snapshot_ready = true;
                slot_tracker.update(slot);
                for batch in pending.drain(..) {
                    publish_updates(&cache, &slot_tracker, batch);
                }
            }
            DeltaStreamItem::Updates(batch) => {
                if batch.is_empty() {
                    continue;
                }
                if !snapshot_ready {
                    pending.push(batch);
                    continue;
                }
                publish_updates(&cache, &slot_tracker, batch);
            }
        }
    }
    Ok(())
}

static MAX_MICROBATCH_UPDATES: Lazy<usize> = Lazy::new(|| {
    std::env::var("ULTRA_INGEST_MAX_MICROBATCH_UPDATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024)
});
static MAX_MICROBATCH_LATENCY_MS: Lazy<u64> = Lazy::new(|| {
    std::env::var("ULTRA_INGEST_MAX_MICROBATCH_WAIT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
});

fn publish_updates(
    cache: &Arc<AccountCache>,
    slot_tracker: &Arc<SlotTracker>,
    batch: Vec<AccountUpdate>,
) {
    if batch.is_empty() {
        return;
    }
    histogram!("ingest_batch_len", batch.len() as f64);
    if batch.len() <= *MAX_MICROBATCH_UPDATES {
        let t0 = Instant::now();
        let snapshot = cache.snapshot();
        let mut builder = AccountCacheBuilder::from_snapshot(&snapshot, cache.shard_mask());
        let mut max_slot = 0u64;
        let batch_len = batch.len();
        for update in batch {
            max_slot = max_slot.max(update.slot);
            update.apply(&mut builder);
        }
        cache.publish(builder);
        slot_tracker.update(max_slot);
        histogram!("ultra_ingest_publish_ms", t0.elapsed().as_secs_f64() * 1_000.0);
        histogram!("ultra_ingest_publish_updates", (*MAX_MICROBATCH_UPDATES).min(batch_len) as f64);
        histogram!("microbatch_size", batch_len as f64);
        histogram!("microbatch_service_ms", t0.elapsed().as_secs_f64() * 1_000.0);
        return;
    }

    // Micro-batch large update sets to bound publish latency.
    let total = batch.len();
    let mut processed = 0usize;
    let mut max_slot_overall = 0u64;
    let mut it = batch.into_iter();
    loop {
        let mut count = 0usize;
        let t0 = Instant::now();
        let deadline = std::time::Duration::from_millis(*MAX_MICROBATCH_LATENCY_MS);
        let snapshot = cache.snapshot();
        let mut builder = AccountCacheBuilder::from_snapshot(&snapshot, cache.shard_mask());
        let mut max_slot = 0u64;
        let mut reason = "items";
        while count < *MAX_MICROBATCH_UPDATES {
            if let Some(update) = it.next() {
                max_slot = max_slot.max(update.slot);
                update.apply(&mut builder);
                count += 1;
                if t0.elapsed() >= deadline {
                    reason = "timer";
                    break;
                }
            } else {
                break;
            }
        }
        if count == 0 {
            break;
        }
        cache.publish(builder);
        slot_tracker.update(max_slot);
        processed += count;
        max_slot_overall = max_slot_overall.max(max_slot);
        let svc_ms = t0.elapsed().as_secs_f64() * 1_000.0;
        histogram!("ultra_ingest_publish_ms", svc_ms);
        histogram!("ultra_ingest_publish_updates", count as f64);
        histogram!("microbatch_size", count as f64);
        histogram!("microbatch_service_ms", svc_ms);
        counter!("microbatch_flush_reason", 1u64, "reason" => reason);
        if processed >= total {
            break;
        }
    }
    let chunks = total.div_ceil(*MAX_MICROBATCH_UPDATES);
    counter!("ultra_ingest_publish_chunks", chunks as u64);
}
