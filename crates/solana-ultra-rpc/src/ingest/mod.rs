// Numan Thabit 2025
//! Ingest pipeline wiring validator Geyser streams into the cache.

use std::sync::Arc;

use tokio_stream::{Stream, StreamExt};

use crate::cache::{AccountCache, AccountCacheBuilder, AccountUpdate, SnapshotSegment};
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
    S: Stream<Item = anyhow::Result<Vec<AccountUpdate>>> + Unpin,
{
    while let Some(batch) = stream.try_next().await? {
        if batch.is_empty() {
            continue;
        }
        let snapshot = cache.snapshot();
        let mut builder = AccountCacheBuilder::from_snapshot(&snapshot, cache.shard_mask());
        let mut max_slot = 0u64;
        for update in batch {
            max_slot = max_slot.max(update.slot);
            update.apply(&mut builder);
        }
        cache.publish(builder);
        slot_tracker.update(max_slot);
    }
    Ok(())
}
