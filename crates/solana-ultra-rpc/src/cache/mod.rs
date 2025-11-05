// Numan Thabit 13.37 - 2025
//! Lock-free account cache built around ArcSwap snapshots.

use std::sync::Arc;

use arc_swap::ArcSwap;
use base64::Engine;
use hashbrown::HashMap;
use once_cell::sync::Lazy;
use solana_sdk::account::{AccountSharedData, ReadableAccount};
use solana_sdk::pubkey::Pubkey;

static BASE64_ENGINE: Lazy<base64::engine::general_purpose::GeneralPurpose> =
    Lazy::new(|| base64::engine::general_purpose::STANDARD);

/// Immutable shard content wrapped in an `Arc` to enable copy-on-write semantics.
type ShardContent = HashMap<Pubkey, Arc<AccountRecord>>;

/// Type alias for a shard map reference counted across snapshots.
type ShardMap = Arc<ShardContent>;

/// Shared vector of shard maps.
pub type ShardSnapshot = Arc<Vec<ShardMap>>;

/// Copy-on-write account cache leveraging `ArcSwap` for readers.
#[derive(Debug)]
pub struct AccountCache {
    shards: ArcSwap<Vec<ShardMap>>,
    shard_mask: usize,
}

impl AccountCache {
    /// Build an empty cache with the provided number of shards (must be a power of two).
    pub fn new(shard_count: usize) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "shard count must be power of two"
        );
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Arc::new(HashMap::new()));
        }
        Self {
            shards: ArcSwap::new(Arc::new(shards)),
            shard_mask: shard_count - 1,
        }
    }

    /// Return the shard mask derived from the configured shard count.
    #[inline]
    pub fn shard_mask(&self) -> usize {
        self.shard_mask
    }

    /// Total number of shards in the cache.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shard_mask + 1
    }

    /// Obtain the current shard snapshot.
    #[inline]
    pub fn snapshot(&self) -> ShardSnapshot {
        self.shards.load_full()
    }

    /// Look up an account entry by pubkey without acquiring any locks.
    #[inline]
    pub fn get(&self, pubkey: &Pubkey) -> Option<Arc<AccountRecord>> {
        let shards = self.shards.load();
        let shard = &shards[self.shard_index(pubkey)];
        shard.get(pubkey).cloned()
    }

    /// Publish a newly constructed shard set, making it visible to all readers atomically.
    pub fn publish(&self, builder: AccountCacheBuilder) {
        self.shards.store(builder.into_arc());
    }

    fn shard_index(&self, pubkey: &Pubkey) -> usize {
        let bytes = pubkey.to_bytes();
        (bytes[0] as usize) & self.shard_mask
    }
}

/// Immutable account record held inside a shard.
#[derive(Debug)]
pub struct AccountRecord {
    slot: u64,
    lamports: u64,
    owner: Pubkey,
    owner_b58: Arc<str>,
    executable: bool,
    rent_epoch: u64,
    data: Arc<AccountSharedData>,
    data_base64: Arc<str>,
    data_len: usize,
}

impl AccountRecord {
    /// Construct a record from shared data and a slot.
    pub fn new(slot: u64, account: AccountSharedData) -> Self {
        let owner = *account.owner();
        let owner_b58 = Arc::<str>::from(owner.to_string());
        let data_slice = account.data();
        let data_len = data_slice.len();
        let data_base64 = if data_len == 0 {
            Arc::<str>::from("")
        } else {
            Arc::<str>::from(BASE64_ENGINE.encode(data_slice))
        };
        Self {
            slot,
            lamports: account.lamports(),
            owner_b58,
            executable: account.executable(),
            rent_epoch: account.rent_epoch(),
            data: Arc::new(account),
            owner,
            data_base64,
            data_len,
        }
    }

    /// Slot at which the account was observed.
    pub fn slot(&self) -> u64 {
        self.slot
    }

    /// Lamports held by the account.
    pub fn lamports(&self) -> u64 {
        self.lamports
    }

    /// Account owner program id.
    pub fn owner(&self) -> Pubkey {
        self.owner
    }

    /// Owner program id encoded as base58.
    #[inline]
    pub fn owner_str(&self) -> &str {
        &self.owner_b58
    }

    /// Reference-counted owner program id string.
    #[inline]
    pub fn owner_arc(&self) -> Arc<str> {
        self.owner_b58.clone()
    }

    /// Whether the account is executable.
    pub fn executable(&self) -> bool {
        self.executable
    }

    /// Rent epoch from the ledger.
    pub fn rent_epoch(&self) -> u64 {
        self.rent_epoch
    }

    /// Underlying shared account data.
    pub fn data(&self) -> Arc<AccountSharedData> {
        self.data.clone()
    }

    /// Borrow the raw account data without cloning the backing storage.
    #[inline]
    pub fn data_slice(&self) -> &[u8] {
        self.data.data()
    }

    /// Base64 encoded representation of the account data.
    #[inline]
    pub fn data_base64(&self) -> Arc<str> {
        self.data_base64.clone()
    }

    /// Length of the original account data (pre-encoding).
    #[inline]
    pub fn data_len(&self) -> usize {
        self.data_len
    }
}

/// Builder for producing new shard snapshots using copy-on-write semantics.
pub struct AccountCacheBuilder {
    shard_mask: usize,
    shards: Vec<ShardMap>,
}

impl AccountCacheBuilder {
    /// Start from an existing snapshot, cloning only the touched shards.
    pub fn from_snapshot(snapshot: &ShardSnapshot, shard_mask: usize) -> Self {
        let shards = snapshot.as_ref().clone();
        Self { shard_mask, shards }
    }

    /// Build an empty builder for bootstrapping from scratch.
    pub fn empty(shard_count: usize) -> Self {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Arc::new(HashMap::new()));
        }
        Self {
            shard_mask: shard_count - 1,
            shards,
        }
    }

    /// Insert or update an account entry in-place.
    pub fn upsert(&mut self, pubkey: Pubkey, entry: Arc<AccountRecord>) {
        let shard_idx = (pubkey.to_bytes()[0] as usize) & self.shard_mask;
        let shard = Arc::make_mut(&mut self.shards[shard_idx]);
        shard.insert(pubkey, entry);
    }

    /// Remove an account from the snapshot.
    pub fn delete(&mut self, pubkey: &Pubkey) {
        let shard_idx = (pubkey.to_bytes()[0] as usize) & self.shard_mask;
        let shard = Arc::make_mut(&mut self.shards[shard_idx]);
        shard.remove(pubkey);
    }

    fn into_arc(self) -> Arc<Vec<ShardMap>> {
        Arc::new(self.shards)
    }
}

/// Delta emitted from the ingest layer.
#[derive(Debug)]
pub struct AccountUpdate {
    /// Account public key.
    pub pubkey: Pubkey,
    /// Optional update; `None` means delete.
    pub data: Option<AccountSharedData>,
    /// Slot of the update.
    pub slot: u64,
}

impl AccountUpdate {
    /// Convert the update into cache actions.
    pub fn apply(self, builder: &mut AccountCacheBuilder) {
        match self.data {
            Some(account) => {
                let record = Arc::new(AccountRecord::new(self.slot, account));
                builder.upsert(self.pubkey, record);
            }
            None => builder.delete(&self.pubkey),
        }
    }
}

/// Snapshot bootstrap payload.
#[derive(Debug)]
pub struct SnapshotSegment {
    /// Start slot covered by this segment.
    pub base_slot: u64,
    /// Accounts captured in this segment.
    pub accounts: Vec<(Pubkey, AccountSharedData)>,
}

impl SnapshotSegment {
    /// Apply the snapshot segment to a builder.
    pub fn hydrate(self, builder: &mut AccountCacheBuilder) {
        for (pubkey, account) in self.accounts {
            let record = Arc::new(AccountRecord::new(self.base_slot, account));
            builder.upsert(pubkey, record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::account::{Account, AccountSharedData};
    use solana_sdk::pubkey::Pubkey;

    fn sample_account(data: &[u8]) -> AccountSharedData {
        let owner = Pubkey::new_unique();
        AccountSharedData::from(Account {
            lamports: 1_000,
            data: data.to_vec(),
            owner,
            executable: false,
            rent_epoch: 0,
        })
    }

    #[test]
    fn account_record_encodes_base64() {
        let owner = Pubkey::new_unique();
        let data = vec![1u8, 2, 3, 4];
        let account = AccountSharedData::from(Account {
            lamports: 99,
            data: data.clone(),
            owner,
            executable: true,
            rent_epoch: 7,
        });
        let record = AccountRecord::new(123, account.clone());
        assert_eq!(record.slot(), 123);
        assert_eq!(record.lamports(), 99);
        assert_eq!(record.owner(), owner);
        assert!(record.executable());
        assert_eq!(record.rent_epoch(), 7);
        assert_eq!(record.data_len(), data.len());
        assert_eq!(record.data_slice(), data.as_slice());
        let expected = base64::engine::general_purpose::STANDARD.encode(&data);
        assert_eq!(record.data_base64().as_ref(), expected.as_str());
    }

    #[test]
    fn account_cache_publish_and_get_roundtrip() {
        let cache = AccountCache::new(8);
        let pubkey = Pubkey::new_unique();
        let account = sample_account(&[7u8; 5]);
        let mut builder = AccountCacheBuilder::empty(cache.shard_count());
        AccountUpdate {
            pubkey,
            data: Some(account.clone()),
            slot: 42,
        }
        .apply(&mut builder);
        cache.publish(builder);

        let fetched = cache.get(&pubkey).expect("account present");
        assert_eq!(fetched.slot(), 42);
        assert_eq!(fetched.data_len(), 5);
        assert_eq!(fetched.data_slice(), &[7u8; 5]);
    }

    #[test]
    fn account_update_delete_removes_entry() {
        let cache = AccountCache::new(4);
        let pubkey = Pubkey::new_unique();
        let account = sample_account(&[9u8; 3]);
        let mut builder = AccountCacheBuilder::empty(cache.shard_count());
        AccountUpdate {
            pubkey,
            data: Some(account),
            slot: 1,
        }
        .apply(&mut builder);
        cache.publish(builder);

        let snapshot = cache.snapshot();
        let mut builder = AccountCacheBuilder::from_snapshot(&snapshot, cache.shard_mask());
        AccountUpdate {
            pubkey,
            data: None,
            slot: 2,
        }
        .apply(&mut builder);
        cache.publish(builder);

        assert!(cache.get(&pubkey).is_none());
    }

    #[test]
    fn snapshot_segment_hydrates_multiple_accounts() {
        let cache = AccountCache::new(2);
        let pubkey_a = Pubkey::new_unique();
        let pubkey_b = Pubkey::new_unique();
        let segment = SnapshotSegment {
            base_slot: 77,
            accounts: vec![
                (pubkey_a, sample_account(&[1, 2, 3])),
                (pubkey_b, sample_account(&[4, 5])),
            ],
        };
        let mut builder = AccountCacheBuilder::empty(cache.shard_count());
        segment.hydrate(&mut builder);
        cache.publish(builder);

        let rec_a = cache.get(&pubkey_a).expect("record a");
        assert_eq!(rec_a.slot(), 77);
        assert_eq!(rec_a.data_slice(), &[1, 2, 3]);
        let rec_b = cache.get(&pubkey_b).expect("record b");
        assert_eq!(rec_b.slot(), 77);
        assert_eq!(rec_b.data_slice(), &[4, 5]);
    }
}
