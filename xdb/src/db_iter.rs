//! User-facing database iterator.
//!
//! `DbIterator` wraps a merging iterator over internal keys and presents
//! a clean view of (user_key, value) pairs. It:
//!
//! - Filters entries by snapshot sequence number
//! - Deduplicates: shows only the newest version of each user key
//! - Hides deletion tombstones
//! - Supports forward iteration: `seek_to_first`, `seek`, `next`

use crate::iterator::XdbIterator;
use crate::types::*;

/// Direction of iteration.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Direction {
    Forward,
}

/// A user-facing iterator that filters internal keys to present a clean
/// view of (user_key, value) pairs.
///
/// Created via `Db::iter()`.
pub struct DbIterator {
    /// The underlying merging iterator over internal keys.
    inner: Box<dyn XdbIterator>,
    /// Only return entries with sequence <= this.
    sequence: SequenceNumber,
    /// Current user key (decoded from internal key).
    saved_key: Vec<u8>,
    /// Current value.
    saved_value: Vec<u8>,
    /// Whether the iterator is positioned at a valid entry.
    valid: bool,
    /// Current direction.
    direction: Direction,
}

impl DbIterator {
    /// Create a new `DbIterator` wrapping an internal merging iterator.
    ///
    /// Only entries with sequence number <= `sequence` will be visible.
    pub fn new(inner: Box<dyn XdbIterator>, sequence: SequenceNumber) -> Self {
        DbIterator {
            inner,
            sequence,
            saved_key: Vec::new(),
            saved_value: Vec::new(),
            valid: false,
            direction: Direction::Forward,
        }
    }

    /// Is the iterator positioned at a valid entry?
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Position at the first entry.
    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.direction = Direction::Forward;
        self.find_next_user_entry();
    }

    /// Position at the first entry with user_key >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        // Build an internal key with the target user_key and max sequence
        // so we find the newest version of this key or the first key after it.
        let ikey = InternalKey::new(target, MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.inner.seek(ikey.as_bytes());
        self.direction = Direction::Forward;
        self.find_next_user_entry();
    }

    /// Advance to the next entry.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not currently valid.
    pub fn next(&mut self) {
        assert!(self.valid, "DbIterator::next() called on invalid iterator");
        // Skip all internal keys with the same user key as the current entry
        // (there may be older versions or deletion markers).
        self.skip_current_user_key();
        self.find_next_user_entry();
    }

    /// The current user key.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not currently valid.
    pub fn key(&self) -> &[u8] {
        assert!(self.valid, "DbIterator::key() called on invalid iterator");
        &self.saved_key
    }

    /// The current value.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is not currently valid.
    pub fn value(&self) -> &[u8] {
        assert!(self.valid, "DbIterator::value() called on invalid iterator");
        &self.saved_value
    }

    /// Advance the inner iterator past all entries with the same user key
    /// as `saved_key`.
    fn skip_current_user_key(&mut self) {
        let current_user_key = self.saved_key.clone();
        while self.inner.valid() {
            let ikey = self.inner.key();
            if let Some(parsed) = ParsedInternalKey::from_bytes(ikey) {
                if parsed.user_key != current_user_key.as_slice() {
                    break;
                }
            } else {
                break;
            }
            self.inner.next();
        }
    }

    /// Scan forward through the inner iterator to find the next visible
    /// user entry: the newest version of a user key with seq <= self.sequence
    /// that is a `Value` (not a `Deletion`).
    fn find_next_user_entry(&mut self) {
        self.valid = false;

        while self.inner.valid() {
            let ikey = self.inner.key();
            let parsed = match ParsedInternalKey::from_bytes(ikey) {
                Some(p) => p,
                None => {
                    // Malformed key, skip it.
                    self.inner.next();
                    continue;
                }
            };

            // Skip entries newer than our snapshot.
            if parsed.sequence > self.sequence {
                self.inner.next();
                continue;
            }

            // This is the newest visible version of this user key.
            match parsed.value_type {
                ValueType::Value => {
                    self.saved_key = parsed.user_key.to_vec();
                    self.saved_value = self.inner.value().to_vec();
                    self.valid = true;
                    // Don't advance inner -- next() will call skip_current_user_key.
                    return;
                }
                ValueType::Deletion => {
                    // Key was deleted; skip all remaining versions of this user key.
                    let deleted_user_key = parsed.user_key.to_vec();
                    self.inner.next();
                    while self.inner.valid() {
                        if let Some(p2) = ParsedInternalKey::from_bytes(self.inner.key()) {
                            if p2.user_key != deleted_user_key.as_slice() {
                                break;
                            }
                        } else {
                            break;
                        }
                        self.inner.next();
                    }
                    // Continue the outer loop to process the next user key.
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::XdbIterator;

    /// A trivial iterator over a pre-sorted `Vec` of key-value pairs.
    /// Keys must be encoded internal keys (user_key + 8-byte tag).
    struct VecIterator {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        pos: usize,
    }

    impl VecIterator {
        fn boxed(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Box<dyn XdbIterator> {
            Box::new(VecIterator {
                entries,
                pos: usize::MAX,
            })
        }
    }

    impl XdbIterator for VecIterator {
        fn valid(&self) -> bool {
            self.pos < self.entries.len()
        }

        fn seek_to_first(&mut self) {
            self.pos = 0;
        }

        fn seek(&mut self, target: &[u8]) {
            // Use internal key comparison for correct ordering.
            self.pos = self
                .entries
                .iter()
                .position(|(k, _)| compare_internal_key(k, target) != std::cmp::Ordering::Less)
                .unwrap_or(self.entries.len());
        }

        fn next(&mut self) {
            self.pos += 1;
        }

        fn key(&self) -> &[u8] {
            &self.entries[self.pos].0
        }

        fn value(&self) -> &[u8] {
            &self.entries[self.pos].1
        }
    }

    /// Helper: build an internal key and value pair for test entries.
    fn make_entry(
        user_key: &[u8],
        seq: SequenceNumber,
        vt: ValueType,
        value: &[u8],
    ) -> (Vec<u8>, Vec<u8>) {
        let ikey = InternalKey::new(user_key, seq, vt);
        (ikey.as_bytes().to_vec(), value.to_vec())
    }

    /// Helper: collect all (user_key, value) pairs from a DbIterator.
    fn collect_all(iter: &mut DbIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        iter.seek_to_first();
        while iter.valid() {
            result.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next();
        }
        result
    }

    #[test]
    fn iterate_simple_puts() {
        // Three distinct keys, all puts, single version each.
        let entries = vec![
            make_entry(b"apple", 3, ValueType::Value, b"red"),
            make_entry(b"banana", 2, ValueType::Value, b"yellow"),
            make_entry(b"cherry", 1, ValueType::Value, b"dark"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 10);

        let pairs = collect_all(&mut iter);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (b"apple".to_vec(), b"red".to_vec()));
        assert_eq!(pairs[1], (b"banana".to_vec(), b"yellow".to_vec()));
        assert_eq!(pairs[2], (b"cherry".to_vec(), b"dark".to_vec()));
    }

    #[test]
    fn deleted_keys_are_hidden() {
        // "banana" is deleted at seq=4. The iterator should skip it.
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"red"),
            make_entry(b"banana", 4, ValueType::Deletion, b""),
            make_entry(b"banana", 2, ValueType::Value, b"yellow"),
            make_entry(b"cherry", 3, ValueType::Value, b"dark"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 10);

        let pairs = collect_all(&mut iter);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], (b"apple".to_vec(), b"red".to_vec()));
        assert_eq!(pairs[1], (b"cherry".to_vec(), b"dark".to_vec()));
    }

    #[test]
    fn only_newest_version_shown() {
        // "key" has two versions: seq=5 ("new") and seq=3 ("old").
        // Only the newest visible version should appear.
        let entries = vec![
            make_entry(b"key", 5, ValueType::Value, b"new"),
            make_entry(b"key", 3, ValueType::Value, b"old"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 10);

        let pairs = collect_all(&mut iter);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], (b"key".to_vec(), b"new".to_vec()));
    }

    #[test]
    fn sequence_filtering_older_snapshot() {
        // "key" has versions at seq=5 and seq=3.
        // A snapshot at seq=4 should only see the seq=3 version.
        let entries = vec![
            make_entry(b"key", 5, ValueType::Value, b"new"),
            make_entry(b"key", 3, ValueType::Value, b"old"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 4); // snapshot at seq=4

        let pairs = collect_all(&mut iter);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], (b"key".to_vec(), b"old".to_vec()));
    }

    #[test]
    fn sequence_filtering_hides_all_versions() {
        // "key" only has versions at seq=5 and seq=3.
        // A snapshot at seq=2 should see nothing for this key.
        let entries = vec![
            make_entry(b"key", 5, ValueType::Value, b"new"),
            make_entry(b"key", 3, ValueType::Value, b"old"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 2); // snapshot at seq=2

        let pairs = collect_all(&mut iter);
        assert_eq!(pairs.len(), 0);
    }

    #[test]
    fn deletion_visible_only_at_snapshot() {
        // "key" deleted at seq=5, put at seq=3.
        // Snapshot at seq=4: deletion not visible, put at seq=3 is visible.
        let entries = vec![
            make_entry(b"key", 5, ValueType::Deletion, b""),
            make_entry(b"key", 3, ValueType::Value, b"alive"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 4);

        let pairs = collect_all(&mut iter);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], (b"key".to_vec(), b"alive".to_vec()));
    }

    #[test]
    fn seek_positions_correctly() {
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"a"),
            make_entry(b"banana", 2, ValueType::Value, b"b"),
            make_entry(b"cherry", 3, ValueType::Value, b"c"),
            make_entry(b"date", 4, ValueType::Value, b"d"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 10);

        // Seek to "banana".
        iter.seek(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"banana");
        assert_eq!(iter.value(), b"b");

        // next should go to "cherry".
        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");
        assert_eq!(iter.value(), b"c");
    }

    #[test]
    fn seek_between_keys() {
        // Seek to a key that doesn't exist; should land on the next one.
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"a"),
            make_entry(b"cherry", 2, ValueType::Value, b"c"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 10);

        iter.seek(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");
    }

    #[test]
    fn seek_past_end() {
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"a"),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 10);

        iter.seek(b"zzz");
        assert!(!iter.valid());
    }

    #[test]
    fn empty_iterator() {
        let inner = VecIterator::boxed(vec![]);
        let mut iter = DbIterator::new(inner, 10);

        iter.seek_to_first();
        assert!(!iter.valid());
    }

    #[test]
    fn mixed_puts_and_deletes_across_keys() {
        // Multiple keys with varying states.
        let entries = vec![
            make_entry(b"a", 10, ValueType::Value, b"val_a"),
            make_entry(b"b", 9, ValueType::Deletion, b""),
            make_entry(b"b", 5, ValueType::Value, b"val_b_old"),
            make_entry(b"c", 8, ValueType::Value, b"val_c"),
            make_entry(b"c", 3, ValueType::Value, b"val_c_old"),
            make_entry(b"d", 7, ValueType::Deletion, b""),
        ];
        let inner = VecIterator::boxed(entries);
        let mut iter = DbIterator::new(inner, 100);

        let pairs = collect_all(&mut iter);
        // "a" is live, "b" is deleted, "c" is live (newest version), "d" is deleted.
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], (b"a".to_vec(), b"val_a".to_vec()));
        assert_eq!(pairs[1], (b"c".to_vec(), b"val_c".to_vec()));
    }
}
