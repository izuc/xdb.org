//! A concurrent SkipList-based MemTable.
//!
//! The skiplist supports a single writer with concurrent readers. Writers must
//! be synchronized externally (e.g., via a mutex). Readers may proceed
//! concurrently with a writer thanks to atomic next-pointer stores with
//! Release/Acquire ordering.

use crate::error;
use crate::types::{
    compare_internal_key, extract_tag, extract_user_key, tag_sequence, tag_value_type, InternalKey,
    LookupKey, SequenceNumber, ValueType,
};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

/// Maximum height a skiplist node may reach.
const MAX_HEIGHT: usize = 12;

/// Branching factor probability: 1 in 4 chance of promoting to the next level.
const BRANCHING_FACTOR: u32 = 4;

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// A single node in the skiplist.
///
/// Uses a fixed-size array for forward pointers instead of `Vec` to
/// eliminate one heap allocation per insert. The 96 bytes of unused
/// pointer slots (for nodes shorter than MAX_HEIGHT) is negligible
/// compared to the allocation overhead saved on the hot write path.
struct Node {
    key: Vec<u8>,
    value: Vec<u8>,
    #[allow(dead_code)] // Tracked per-node for structural clarity; actual height read from SkipList::max_height
    height: usize,
    next: [AtomicPtr<Node>; MAX_HEIGHT],
}

impl Node {
    fn new(key: Vec<u8>, value: Vec<u8>, height: usize) -> *mut Node {
        let node = Box::new(Node {
            key,
            value,
            height,
            next: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        });
        Box::into_raw(node)
    }

    /// Read the next pointer at the given level with Acquire ordering.
    #[inline]
    unsafe fn get_next(node: *const Node, level: usize) -> *mut Node {
        (*node).next[level].load(Ordering::Acquire)
    }

    /// Set the next pointer at the given level with Release ordering.
    #[inline]
    unsafe fn set_next(node: *mut Node, level: usize, next: *mut Node) {
        (*node).next[level].store(next, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// SkipList
// ---------------------------------------------------------------------------

/// A lock-free (single-writer, multi-reader) skiplist ordered by internal key.
struct SkipList {
    /// Sentinel head node. Its key/value are unused.
    head: *mut Node,
    /// Current maximum height among all inserted nodes.
    max_height: AtomicUsize,
}

// SkipList is safe to share across threads: reads use Acquire loads, the
// single writer uses Release stores, and the head pointer is immutable after
// construction.
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

impl SkipList {
    fn new() -> Self {
        let head = Node::new(Vec::new(), Vec::new(), MAX_HEIGHT);
        SkipList {
            head,
            max_height: AtomicUsize::new(1),
        }
    }

    /// Current effective height of the skiplist.
    #[inline]
    fn current_height(&self) -> usize {
        self.max_height.load(Ordering::Acquire)
    }

    /// Generate a random height in [1, MAX_HEIGHT] with P = 1/BRANCHING_FACTOR.
    ///
    /// Uses a fast thread-local xorshift32 PRNG instead of the
    /// cryptographic-strength `rand::random()` — skiplist level
    /// selection doesn't need crypto quality and this is on the
    /// hot write path.
    fn random_height() -> usize {
        use std::cell::Cell;
        use std::hash::{Hash, Hasher};
        thread_local! {
            static RNG: Cell<u32> = {
                // Seed from thread ID so each thread produces a unique sequence.
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::thread::current().id().hash(&mut hasher);
                let seed = hasher.finish() as u32;
                Cell::new(if seed == 0 { 0xdeadbeef } else { seed })
            };
        }
        let mut height = 1;
        RNG.with(|rng| {
            while height < MAX_HEIGHT {
                let mut x = rng.get();
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                rng.set(x);
                if x % BRANCHING_FACTOR != 0 {
                    break;
                }
                height += 1;
            }
        });
        height
    }

    /// Find the first node whose key is >= `target`.
    ///
    /// If `prev` is `Some`, fills in the predecessor node at each level (used
    /// by insert to wire up the new node).
    fn find_greater_or_equal(
        &self,
        target: &[u8],
        mut prev: Option<&mut [*mut Node; MAX_HEIGHT]>,
    ) -> *mut Node {
        let mut current = self.head;
        let mut level = self.current_height() - 1;

        loop {
            let next = unsafe { Node::get_next(current, level) };
            if !next.is_null()
                && compare_internal_key(unsafe { &(*next).key }, target) == std::cmp::Ordering::Less
            {
                current = next;
            } else {
                if let Some(ref mut p) = prev {
                    p[level] = current;
                }
                if level == 0 {
                    return next;
                }
                level -= 1;
            }
        }
    }

    /// Insert a key-value pair into the skiplist.
    ///
    /// # Safety
    ///
    /// The caller must ensure single-writer access.
    fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut prev = [ptr::null_mut::<Node>(); MAX_HEIGHT];
        // Initialise all prev slots to head.
        for slot in prev.iter_mut() {
            *slot = self.head;
        }

        let _found = self.find_greater_or_equal(&key, Some(&mut prev));

        let height = Self::random_height();
        let current_max = self.current_height();
        if height > current_max {
            // For levels above the old max, the predecessor is head.
            for slot in prev.iter_mut().take(height).skip(current_max) {
                *slot = self.head;
            }
            // Relaxed is fine here — readers will see the updated height after
            // the Release stores on the next pointers.
            self.max_height.store(height, Ordering::Release);
        }

        let new_node = Node::new(key, value, height);

        // Wire up from bottom to top so that concurrent readers always see a
        // consistent next chain at every level they can reach.
        #[allow(clippy::needless_range_loop)]
        for i in 0..height {
            unsafe {
                // new_node.next[i] = prev[i].next[i]
                let next = Node::get_next(prev[i], i);
                Node::set_next(new_node, i, next);
                // prev[i].next[i] = new_node
                Node::set_next(prev[i], i, new_node);
            }
        }
    }

    /// Return the first real node (after head), or null if the list is empty.
    fn first_node(&self) -> *mut Node {
        unsafe { Node::get_next(self.head, 0) }
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        let mut current = unsafe { Node::get_next(self.head, 0) };
        while !current.is_null() {
            let next = unsafe { Node::get_next(current, 0) };
            unsafe {
                drop(Box::from_raw(current));
            }
            current = next;
        }
        // Drop the head sentinel.
        unsafe {
            drop(Box::from_raw(self.head));
        }
    }
}

// ---------------------------------------------------------------------------
// MemTable
// ---------------------------------------------------------------------------

/// An in-memory table backed by a skiplist, ordered by internal key.
///
/// Writers must be externally synchronized; readers may proceed concurrently.
pub struct MemTable {
    list: SkipList,
    memory_usage: AtomicUsize,
    /// Fast flag: set to true when any RangeDeletion entry is added.
    /// Allows `collect_range_tombstones_from_mem` to skip a full scan
    /// when no range tombstones exist (the common case).
    has_range_tombstones: AtomicBool,
}

impl MemTable {
    /// Create an empty memtable.
    pub fn new() -> Self {
        MemTable {
            list: SkipList::new(),
            memory_usage: AtomicUsize::new(0),
            has_range_tombstones: AtomicBool::new(false),
        }
    }

    /// Whether this memtable contains any range tombstones.
    pub fn has_range_tombstones(&self) -> bool {
        self.has_range_tombstones.load(Ordering::Relaxed)
    }

    /// Insert an entry into the memtable.
    ///
    /// The key is encoded as an internal key (`user_key ++ tag`). The caller
    /// must ensure single-writer access.
    pub fn add(
        &self,
        seq: SequenceNumber,
        value_type: ValueType,
        user_key: &[u8],
        value: &[u8],
    ) {
        let internal_key = InternalKey::new(user_key, seq, value_type);
        let key_bytes = internal_key.as_bytes().to_vec();
        let value_bytes = value.to_vec();

        // Approximate overhead: key + value + node pointers + bookkeeping.
        let usage = key_bytes.len()
            + value_bytes.len()
            + std::mem::size_of::<Node>()
            + MAX_HEIGHT * std::mem::size_of::<AtomicPtr<Node>>();
        self.memory_usage.fetch_add(usage, Ordering::Relaxed);

        if value_type == ValueType::RangeDeletion {
            self.has_range_tombstones.store(true, Ordering::Relaxed);
        }

        self.list.insert(key_bytes, value_bytes);
    }

    /// Look up a user key at a given sequence number.
    ///
    /// Returns:
    /// - `Some(Ok(value))` if the key was found with `ValueType::Value`.
    /// - `Some(Err(NotFound))` if the key was found with `ValueType::Deletion`.
    /// - `None` if the key is not present in this memtable.
    pub fn get(&self, lookup_key: &LookupKey) -> Option<error::Result<(Vec<u8>, SequenceNumber)>> {
        // Fast path: skip the full 12-level skiplist descent for empty tables.
        if self.list.first_node().is_null() {
            return None;
        }

        let target = lookup_key.internal_key();
        let node = self.list.find_greater_or_equal(target, None);

        if node.is_null() {
            return None;
        }

        let node_key = unsafe { &(*node).key };
        let node_user_key = extract_user_key(node_key);
        let lookup_user_key = lookup_key.user_key();

        if node_user_key != lookup_user_key {
            return None;
        }

        // The user keys match. Verify the sequence number is <= the requested
        // snapshot sequence (internal key ordering places newer sequences first,
        // but find_greater_or_equal may land on a version newer than our snapshot).
        let tag = extract_tag(node_key);
        let node_seq = tag_sequence(tag);
        let lookup_seq = tag_sequence(extract_tag(lookup_key.internal_key()));
        if node_seq > lookup_seq {
            // The entry is newer than our snapshot. Walk forward to find an
            // older version, or determine the key doesn't exist at this snapshot.
            let mut cur = unsafe { (*node).next[0].load(Ordering::Acquire) };
            while !cur.is_null() {
                let cur_key = unsafe { &(*cur).key };
                let cur_user = extract_user_key(cur_key);
                if cur_user != lookup_user_key {
                    return None; // no version at this snapshot
                }
                let cur_tag = extract_tag(cur_key);
                let cur_seq = tag_sequence(cur_tag);
                if cur_seq <= lookup_seq {
                    return match tag_value_type(cur_tag) {
                        Some(ValueType::Value) => {
                            let value = unsafe { (*cur).value.clone() };
                            Some(Ok((value, cur_seq)))
                        }
                        Some(ValueType::Deletion) | Some(ValueType::RangeDeletion) => {
                            Some(Err(error::Error::not_found(
                                String::from_utf8_lossy(lookup_user_key).to_string(),
                            )))
                        }
                        None => None,
                    };
                }
                cur = unsafe { (*cur).next[0].load(Ordering::Acquire) };
            }
            return None;
        }

        match tag_value_type(tag) {
            Some(ValueType::Value) => {
                let value = unsafe { (*node).value.clone() };
                Some(Ok((value, node_seq)))
            }
            Some(ValueType::Deletion) | Some(ValueType::RangeDeletion) => {
                Some(Err(error::Error::not_found(
                    String::from_utf8_lossy(lookup_user_key).to_string(),
                )))
            }
            None => None,
        }
    }

    /// Approximate bytes of memory used by this memtable.
    pub fn approximate_memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Return an iterator over all entries in internal key order.
    pub fn iter(&self) -> MemTableIterator<'_> {
        MemTableIterator {
            list: &self.list,
            current: ptr::null_mut(),
        }
    }

    /// Returns `true` if the memtable contains no entries.
    pub fn is_empty(&self) -> bool {
        self.list.first_node().is_null()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MemTableIterator
// ---------------------------------------------------------------------------

/// Iterator over entries in a [`MemTable`], in internal key order.
///
/// The iterator borrows the memtable and traverses the level-0 linked list.
pub struct MemTableIterator<'a> {
    list: &'a SkipList,
    current: *mut Node,
}

impl<'a> MemTableIterator<'a> {
    /// Returns `true` if the iterator is positioned on a valid entry.
    pub fn valid(&self) -> bool {
        !self.current.is_null()
    }

    /// Position the iterator at the first entry (smallest key).
    pub fn seek_to_first(&mut self) {
        self.current = self.list.first_node();
    }

    /// Position the iterator at the first entry whose key is >= `target`.
    ///
    /// `target` is an encoded internal key.
    pub fn seek(&mut self, target: &[u8]) {
        self.current = self.list.find_greater_or_equal(target, None);
    }

    /// Advance the iterator to the next entry.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not valid.
    pub fn next(&mut self) {
        assert!(self.valid(), "MemTableIterator::next() called on invalid iterator");
        self.current = unsafe { Node::get_next(self.current, 0) };
    }

    /// Return the encoded internal key of the current entry.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not valid.
    pub fn key(&self) -> &[u8] {
        assert!(self.valid(), "MemTableIterator::key() called on invalid iterator");
        unsafe { &(*self.current).key }
    }

    /// Return the value of the current entry.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not valid.
    pub fn value(&self) -> &[u8] {
        assert!(self.valid(), "MemTableIterator::value() called on invalid iterator");
        unsafe { &(*self.current).value }
    }
}

// SAFETY: the iterator only holds a shared reference to the skiplist and a raw
// pointer that is only dereferenced while the borrow is live.
unsafe impl Send for MemTableIterator<'_> {}

// ---------------------------------------------------------------------------
// OwnedMemTableIterator — holds Arc<MemTable> for use in Box<dyn XdbIterator>
// ---------------------------------------------------------------------------

/// An owning iterator over a [`MemTable`] that keeps it alive via `Arc`.
///
/// Unlike [`MemTableIterator`] which borrows the memtable (and thus can't be
/// stored in a `Box<dyn XdbIterator + 'static>`), this variant holds an
/// `Arc<MemTable>`. This eliminates the need to copy the entire memtable
/// into a `Vec` when constructing database-level iterators.
pub struct OwnedMemTableIterator {
    _mem: Arc<MemTable>,
    head: *mut Node,
    current: *mut Node,
}

// SAFETY: The Arc<MemTable> ensures the SkipList (and all its nodes) stays
// alive for the lifetime of this iterator. The raw pointer `current` always
// points to a valid node within the SkipList or is null.
unsafe impl Send for OwnedMemTableIterator {}

impl OwnedMemTableIterator {
    /// Create a new owning iterator. The Arc keeps the memtable alive.
    pub fn new(mem: Arc<MemTable>) -> Self {
        let head = mem.list.head;
        OwnedMemTableIterator {
            _mem: mem,
            head,
            current: ptr::null_mut(),
        }
    }
}

impl crate::iterator::XdbIterator for OwnedMemTableIterator {
    fn valid(&self) -> bool {
        !self.current.is_null()
    }

    fn seek_to_first(&mut self) {
        self.current = unsafe { Node::get_next(self.head, 0) };
    }

    fn seek(&mut self, target: &[u8]) {
        // Walk the skiplist from head using the actual current height
        // (not MAX_HEIGHT) to avoid traversing empty upper levels.
        let mut current = self.head;
        let mut level = self._mem.list.current_height() - 1;
        loop {
            let next = unsafe { Node::get_next(current, level) };
            if !next.is_null()
                && compare_internal_key(unsafe { &(*next).key }, target)
                    == std::cmp::Ordering::Less
            {
                current = next;
            } else {
                if level == 0 {
                    self.current = next;
                    return;
                }
                level -= 1;
            }
        }
    }

    fn next(&mut self) {
        if self.valid() {
            self.current = unsafe { Node::get_next(self.current, 0) };
        }
    }

    fn key(&self) -> &[u8] {
        unsafe { &(*self.current).key }
    }

    fn value(&self) -> &[u8] {
        unsafe { &(*self.current).value }
    }

    fn seek_to_last(&mut self) {
        // Walk level 0 to find the last node.
        let mut node = unsafe { Node::get_next(self.head, 0) };
        if node.is_null() {
            self.current = ptr::null_mut();
            return;
        }
        loop {
            let next = unsafe { Node::get_next(node, 0) };
            if next.is_null() {
                self.current = node;
                return;
            }
            node = next;
        }
    }

    fn prev(&mut self) {
        if !self.valid() {
            return;
        }
        // Walk level 0 from head to find the node before current.
        let target = self.current;
        let mut prev_node = ptr::null_mut();
        let mut node = unsafe { Node::get_next(self.head, 0) };
        while !node.is_null() && node != target {
            prev_node = node;
            node = unsafe { Node::get_next(node, 0) };
        }
        self.current = prev_node; // null if current was the first node
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::tag_sequence;

    #[test]
    fn basic_insert_and_get() {
        let mt = MemTable::new();
        mt.add(1, ValueType::Value, b"key1", b"value1");
        mt.add(2, ValueType::Value, b"key2", b"value2");

        let lk1 = LookupKey::new(b"key1", 1);
        let (result, _seq) = mt.get(&lk1).unwrap().unwrap();
        assert_eq!(result, b"value1");

        let lk2 = LookupKey::new(b"key2", 2);
        let (result, _seq) = mt.get(&lk2).unwrap().unwrap();
        assert_eq!(result, b"value2");
    }

    #[test]
    fn get_returns_none_for_absent_key() {
        let mt = MemTable::new();
        mt.add(1, ValueType::Value, b"key1", b"value1");

        let lk = LookupKey::new(b"key_missing", 1);
        assert!(mt.get(&lk).is_none());
    }

    #[test]
    fn newer_sequence_wins() {
        let mt = MemTable::new();
        mt.add(1, ValueType::Value, b"key", b"old");
        mt.add(2, ValueType::Value, b"key", b"new");

        let lk = LookupKey::new(b"key", 2);
        let (result, _) = mt.get(&lk).unwrap().unwrap();
        assert_eq!(result, b"new");

        let lk1 = LookupKey::new(b"key", 1);
        let (result, _) = mt.get(&lk1).unwrap().unwrap();
        assert_eq!(result, b"old");
    }

    #[test]
    fn deletion_marker() {
        let mt = MemTable::new();
        mt.add(1, ValueType::Value, b"key", b"value");
        mt.add(2, ValueType::Deletion, b"key", b"");

        // Reading at seq=2 should see the deletion.
        let lk = LookupKey::new(b"key", 2);
        let result = mt.get(&lk);
        assert!(result.is_some());
        assert!(result.unwrap().is_err());

        let lk1 = LookupKey::new(b"key", 1);
        let (result, _) = mt.get(&lk1).unwrap().unwrap();
        assert_eq!(result, b"value");
    }

    #[test]
    fn iterator_ordering() {
        let mt = MemTable::new();
        mt.add(1, ValueType::Value, b"cherry", b"c");
        mt.add(2, ValueType::Value, b"apple", b"a");
        mt.add(3, ValueType::Value, b"banana", b"b");

        let mut iter = mt.iter();
        iter.seek_to_first();

        // Internal key order: user_key ascending, then sequence descending.
        // Since all user keys are distinct, order is: apple, banana, cherry.
        assert!(iter.valid());
        assert_eq!(extract_user_key(iter.key()), b"apple");
        assert_eq!(iter.value(), b"a");

        iter.next();
        assert!(iter.valid());
        assert_eq!(extract_user_key(iter.key()), b"banana");
        assert_eq!(iter.value(), b"b");

        iter.next();
        assert!(iter.valid());
        assert_eq!(extract_user_key(iter.key()), b"cherry");
        assert_eq!(iter.value(), b"c");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn iterator_seek() {
        let mt = MemTable::new();
        mt.add(1, ValueType::Value, b"a", b"1");
        mt.add(2, ValueType::Value, b"c", b"3");
        mt.add(3, ValueType::Value, b"e", b"5");

        let mut iter = mt.iter();
        // Seek to "c" at max sequence so it finds the entry for "c".
        let target = InternalKey::new(b"c", u64::MAX >> 8, ValueType::Value);
        iter.seek(target.as_bytes());

        assert!(iter.valid());
        assert_eq!(extract_user_key(iter.key()), b"c");
        assert_eq!(iter.value(), b"3");
    }

    #[test]
    fn iterator_multiple_versions() {
        let mt = MemTable::new();
        mt.add(1, ValueType::Value, b"key", b"v1");
        mt.add(2, ValueType::Value, b"key", b"v2");

        let mut iter = mt.iter();
        iter.seek_to_first();

        // Newer version (seq=2) first.
        assert!(iter.valid());
        let tag = extract_tag(iter.key());
        assert_eq!(tag_sequence(tag), 2);
        assert_eq!(iter.value(), b"v2");

        iter.next();
        assert!(iter.valid());
        let tag = extract_tag(iter.key());
        assert_eq!(tag_sequence(tag), 1);
        assert_eq!(iter.value(), b"v1");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn memory_usage_tracking() {
        let mt = MemTable::new();
        assert_eq!(mt.approximate_memory_usage(), 0);

        mt.add(1, ValueType::Value, b"k", b"v");
        let usage = mt.approximate_memory_usage();
        assert!(usage > 0, "memory usage should increase after add");

        mt.add(2, ValueType::Value, b"key2", b"value2");
        assert!(
            mt.approximate_memory_usage() > usage,
            "memory usage should increase further"
        );
    }

    #[test]
    fn is_empty() {
        let mt = MemTable::new();
        assert!(mt.is_empty());

        mt.add(1, ValueType::Value, b"key", b"val");
        assert!(!mt.is_empty());
    }
}
