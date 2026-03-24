//! Sharded LRU cache for SST data blocks.
//!
//! The cache is split into [`NUM_SHARDS`] independent LRU caches, each
//! protected by a [`parking_lot::Mutex`], so that concurrent readers
//! rarely contend on the same lock.
//!
//! Within each shard the implementation uses a [`HashMap`] for O(1)
//! lookup and a [`Vec`] access-order list with lazy cleanup of stale
//! entries.  This keeps the code simple while still providing correct
//! LRU eviction behaviour.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

/// Number of independent shards.
const NUM_SHARDS: usize = 16;

/// Threshold (fraction of stale entries) at which the order vector is
/// compacted.  When more than 50 % of the entries in the order vector
/// are stale we rebuild it.
const COMPACTION_THRESHOLD_NUMER: usize = 1;
const COMPACTION_THRESHOLD_DENOM: usize = 2;

// ---------------------------------------------------------------------------
// CacheKey
// ---------------------------------------------------------------------------

/// Cache key: `(file_number, block_offset)` uniquely identifies a block.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub file_number: u64,
    pub offset: u64,
}

// ---------------------------------------------------------------------------
// Internal node stored in each shard's hash map
// ---------------------------------------------------------------------------

/// Value and charge associated with a cache entry.
struct LruNode {
    value: Arc<Vec<u8>>,
    charge: usize,
    /// Generation counter — incremented each time the entry is touched.
    /// Used to distinguish live entries in the order vector from stale
    /// (already-promoted) ghosts.
    generation: u64,
}

/// An element in the order vector.  May be *stale* if the entry has been
/// re-promoted since this record was pushed.
struct OrderEntry {
    key: CacheKey,
    generation: u64,
}

// ---------------------------------------------------------------------------
// LruShard
// ---------------------------------------------------------------------------

/// A single shard of the LRU cache.
struct LruShard {
    map: HashMap<CacheKey, LruNode>,
    /// Access order: front = oldest, back = newest.
    order: Vec<OrderEntry>,
    /// Total charge (bytes) stored in this shard.
    usage: usize,
    /// Maximum charge allowed before eviction kicks in.
    capacity: usize,
    /// Monotonically increasing generation counter for this shard.
    next_generation: u64,
    /// Number of stale (outdated-generation) entries in `order`.
    stale_count: usize,
}

impl LruShard {
    fn new(capacity: usize) -> Self {
        LruShard {
            map: HashMap::new(),
            order: Vec::new(),
            usage: 0,
            capacity,
            next_generation: 0,
            stale_count: 0,
        }
    }

    /// Allocate the next generation number.
    fn alloc_generation(&mut self) -> u64 {
        let g = self.next_generation;
        self.next_generation += 1;
        g
    }

    /// Insert or replace an entry.  Evicts LRU entries if over capacity.
    fn insert(&mut self, key: CacheKey, value: Arc<Vec<u8>>, charge: usize) {
        // If the key already exists, remove its charge first.
        if let Some(old) = self.map.remove(&key) {
            self.usage -= old.charge;
            // The old order entry becomes stale.
            self.stale_count += 1;
        }

        let gen = self.alloc_generation();
        self.map.insert(
            key,
            LruNode {
                value,
                charge,
                generation: gen,
            },
        );
        self.order.push(OrderEntry {
            key,
            generation: gen,
        });
        self.usage += charge;

        // Evict until we are within capacity.
        self.evict();

        // Compact the order vector if too many stale entries have built up.
        self.maybe_compact();
    }

    /// Look up an entry, promoting it to most-recently-used.
    fn get(&mut self, key: &CacheKey) -> Option<Arc<Vec<u8>>> {
        if !self.map.contains_key(key) {
            return None;
        }

        // Allocate generation before borrowing the map entry.
        let gen = self.alloc_generation();

        let node = self.map.get_mut(key).unwrap();
        node.generation = gen;
        let value = Arc::clone(&node.value);

        self.order.push(OrderEntry {
            key: *key,
            generation: gen,
        });
        self.stale_count += 1; // the old order entry is now stale

        Some(value)
    }

    /// Remove an entry from the shard.
    fn erase(&mut self, key: &CacheKey) {
        if let Some(old) = self.map.remove(key) {
            self.usage -= old.charge;
            self.stale_count += 1; // its order entry becomes stale
        }
    }

    /// Evict least-recently-used entries until `usage <= capacity`.
    fn evict(&mut self) {
        while self.usage > self.capacity && !self.map.is_empty() {
            // Scan from the front (oldest) and skip stale entries.
            loop {
                if self.order.is_empty() {
                    return;
                }
                let front = &self.order[0];
                let front_key = front.key;
                let front_gen = front.generation;

                // Check if this order entry is still live.
                let is_live = self
                    .map
                    .get(&front_key)
                    .is_some_and(|n| n.generation == front_gen);

                if is_live {
                    // Evict this entry.
                    let node = self.map.remove(&front_key).unwrap();
                    self.usage -= node.charge;
                    self.order.remove(0);
                    break;
                } else {
                    // Stale — discard and continue.
                    self.order.remove(0);
                    self.stale_count = self.stale_count.saturating_sub(1);
                }
            }
        }
    }

    /// Compact the order vector by removing stale entries when they exceed
    /// the threshold fraction.
    fn maybe_compact(&mut self) {
        if self.order.len() > 32
            && self.stale_count * COMPACTION_THRESHOLD_DENOM
                > self.order.len() * COMPACTION_THRESHOLD_NUMER
        {
            self.order.retain(|entry| {
                self.map
                    .get(&entry.key)
                    .is_some_and(|n| n.generation == entry.generation)
            });
            self.stale_count = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// LruCache (public, sharded)
// ---------------------------------------------------------------------------

/// A sharded LRU cache for SST data blocks.
///
/// The cache is split into [`NUM_SHARDS`] independent shards so that
/// concurrent readers rarely contend on the same lock.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use xdb::cache::lru::{LruCache, CacheKey};
///
/// let cache = LruCache::new(1024);
/// let key = CacheKey { file_number: 1, offset: 0 };
/// let data = Arc::new(vec![1, 2, 3]);
/// cache.insert(key, data.clone(), 3);
/// assert_eq!(cache.get(&key).unwrap(), data);
/// ```
pub struct LruCache {
    shards: Vec<Mutex<LruShard>>,
    #[allow(dead_code)]
    capacity: usize,
    #[allow(dead_code)]
    per_shard_capacity: usize,
}

impl LruCache {
    /// Create a new LRU cache with the given total capacity in bytes.
    pub fn new(capacity: usize) -> Self {
        let per_shard = capacity / NUM_SHARDS;
        let shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(LruShard::new(per_shard)))
            .collect();
        LruCache {
            shards,
            capacity,
            per_shard_capacity: per_shard,
        }
    }

    /// Insert a block into the cache.
    ///
    /// If the key is already present, its value and charge are replaced.
    /// Oldest entries are evicted if the shard exceeds its capacity.
    pub fn insert(&self, key: CacheKey, value: Arc<Vec<u8>>, charge: usize) {
        let shard = self.shard(&key);
        shard.lock().insert(key, value, charge);
    }

    /// Look up a cached block.  Returns `None` on a cache miss.
    ///
    /// A successful lookup promotes the entry to most-recently-used.
    pub fn get(&self, key: &CacheKey) -> Option<Arc<Vec<u8>>> {
        let shard = self.shard(key);
        shard.lock().get(key)
    }

    /// Remove an entry from the cache.
    pub fn erase(&self, key: &CacheKey) {
        let shard = self.shard(key);
        shard.lock().erase(key);
    }

    /// Total bytes currently stored across all shards.
    pub fn total_charge(&self) -> usize {
        self.shards.iter().map(|s| s.lock().usage).sum()
    }

    /// Select the shard for a given key.
    fn shard(&self, key: &CacheKey) -> &Mutex<LruShard> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % NUM_SHARDS;
        &self.shards[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(file: u64, offset: u64) -> CacheKey {
        CacheKey {
            file_number: file,
            offset,
        }
    }

    fn make_value(data: &[u8]) -> Arc<Vec<u8>> {
        Arc::new(data.to_vec())
    }

    #[test]
    fn insert_and_get() {
        let cache = LruCache::new(1024);
        let k = make_key(1, 0);
        let v = make_value(b"hello");
        cache.insert(k, v.clone(), 5);
        assert_eq!(cache.get(&k).unwrap(), v);
    }

    #[test]
    fn miss_returns_none() {
        let cache = LruCache::new(1024);
        assert!(cache.get(&make_key(99, 99)).is_none());
    }

    #[test]
    fn erase_removes_entry() {
        let cache = LruCache::new(1024);
        let k = make_key(1, 0);
        cache.insert(k, make_value(b"data"), 4);
        cache.erase(&k);
        assert!(cache.get(&k).is_none());
    }

    #[test]
    fn eviction_when_over_capacity() {
        // Create a cache with 16 shards.  Total capacity = 16 bytes,
        // so each shard has capacity = 1 byte.  Inserting two entries
        // into the same shard should evict the first.
        let cache = LruCache::new(NUM_SHARDS); // 1 byte per shard

        // Force both keys into the same shard by brute-forcing keys
        // and checking which shard they land in.
        let k1 = make_key(0, 0);
        let shard_idx = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            k1.hash(&mut h);
            (h.finish() as usize) % NUM_SHARDS
        };

        // Find a second key in the same shard.
        let mut k2 = make_key(0, 1);
        for i in 0u64..1000 {
            let candidate = make_key(0, i + 1);
            let idx = {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                candidate.hash(&mut h);
                (h.finish() as usize) % NUM_SHARDS
            };
            if idx == shard_idx {
                k2 = candidate;
                break;
            }
        }

        cache.insert(k1, make_value(b"a"), 1);
        assert!(cache.get(&k1).is_some());

        // Insert k2 — should evict k1 (shard capacity is 1).
        cache.insert(k2, make_value(b"b"), 1);
        assert!(cache.get(&k1).is_none(), "k1 should have been evicted");
        assert!(cache.get(&k2).is_some());
    }

    #[test]
    fn total_charge_tracking() {
        let cache = LruCache::new(1024);
        assert_eq!(cache.total_charge(), 0);

        cache.insert(make_key(1, 0), make_value(b"aaaa"), 4);
        assert_eq!(cache.total_charge(), 4);

        cache.insert(make_key(2, 0), make_value(b"bb"), 2);
        assert_eq!(cache.total_charge(), 6);

        cache.erase(&make_key(1, 0));
        assert_eq!(cache.total_charge(), 2);
    }

    #[test]
    fn replace_existing_key() {
        let cache = LruCache::new(1024);
        let k = make_key(5, 100);

        cache.insert(k, make_value(b"old"), 3);
        assert_eq!(cache.total_charge(), 3);

        cache.insert(k, make_value(b"new_value"), 9);
        assert_eq!(cache.total_charge(), 9);
        assert_eq!(&**cache.get(&k).unwrap(), b"new_value");
    }

    #[test]
    fn multiple_shards_work_independently() {
        // 100 entries * 8 bytes = 800 bytes total.  With 16 shards we need
        // enough per-shard capacity so that even an unlucky hash distribution
        // doesn't evict entries.  16 * 800 = 12800 is more than enough.
        let cache = LruCache::new(12800);

        // Insert many keys -- they will be spread across shards.
        for i in 0u64..100 {
            let k = make_key(i, 0);
            let v = make_value(&i.to_le_bytes());
            cache.insert(k, v, 8);
        }

        // All should be retrievable (capacity is large enough).
        for i in 0u64..100 {
            let k = make_key(i, 0);
            let v = cache.get(&k).expect("entry should be present");
            assert_eq!(&**v, &i.to_le_bytes());
        }
    }

    #[test]
    fn lru_order_is_respected() {
        // Shard-level test: directly test a single shard to validate
        // that recently-accessed entries survive eviction.
        let mut shard = LruShard::new(3); // capacity 3 bytes

        let k1 = make_key(0, 0);
        let k2 = make_key(0, 1);
        let k3 = make_key(0, 2);

        shard.insert(k1, make_value(b"1"), 1);
        shard.insert(k2, make_value(b"2"), 1);
        shard.insert(k3, make_value(b"3"), 1);

        // Access k1 so it becomes most recently used.
        assert!(shard.get(&k1).is_some());

        // Insert k4 — should evict k2 (the least recently used).
        let k4 = make_key(0, 3);
        shard.insert(k4, make_value(b"4"), 1);

        assert!(shard.get(&k2).is_none(), "k2 should have been evicted");
        assert!(shard.get(&k1).is_some(), "k1 was accessed recently");
        assert!(shard.get(&k3).is_some());
        assert!(shard.get(&k4).is_some());
    }
}
