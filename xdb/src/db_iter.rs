//! User-facing database iterator.
//!
//! `DbIterator` wraps a merging iterator over internal keys and presents
//! a clean view of (user_key, value) pairs. It:
//!
//! - Filters entries by snapshot sequence number
//! - Deduplicates: shows only the newest version of each user key
//! - Hides deletion tombstones (including range tombstones)
//! - Supports forward iteration: `seek_to_first`, `seek`, `next`
//! - Supports backward iteration: `seek_to_last`, `prev`

use crate::iterator::XdbIterator;
use crate::types::*;

/// A user-facing iterator that filters internal keys to present a clean
/// view of (user_key, value) pairs.
///
/// Created via `Db::iter()`.
pub struct DbIterator {
    inner: Box<dyn XdbIterator>,
    sequence: SequenceNumber,
    saved_key: Vec<u8>,
    saved_value: Vec<u8>,
    valid: bool,
    /// Range tombstones: (start_key_inclusive, end_key_exclusive, sequence).
    range_tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>,
    /// Optional upper bound (exclusive). The iterator automatically becomes
    /// invalid when advancing past this key.
    pub(crate) upper_bound: Option<Vec<u8>>,
    /// Number of bytes to strip from the front of returned keys.
    /// Used by column family iterators to hide the CF prefix byte.
    key_prefix_len: usize,
}

impl DbIterator {
    /// Create a new `DbIterator` wrapping an internal merging iterator.
    pub fn new(inner: Box<dyn XdbIterator>, sequence: SequenceNumber) -> Self {
        DbIterator {
            inner,
            sequence,
            saved_key: Vec::new(),
            saved_value: Vec::new(),
            valid: false,
            range_tombstones: Vec::new(),
            upper_bound: None,
            key_prefix_len: 0,
        }
    }

    /// Create a `DbIterator` with pre-collected range tombstones.
    pub fn with_range_tombstones(
        inner: Box<dyn XdbIterator>,
        sequence: SequenceNumber,
        range_tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>,
    ) -> Self {
        DbIterator {
            inner,
            sequence,
            saved_key: Vec::new(),
            saved_value: Vec::new(),
            valid: false,
            range_tombstones,
            upper_bound: None,
            key_prefix_len: 0,
        }
    }

    /// Set an upper bound (exclusive) for the iterator. The iterator will
    /// automatically become invalid when it advances past this key.
    /// Must be called before seeking.
    pub fn set_upper_bound(&mut self, bound: Vec<u8>) {
        self.upper_bound = Some(bound);
    }

    /// Set a lower bound (inclusive) by seeking to it. Convenience method
    /// equivalent to calling `seek(bound)`.
    pub fn set_lower_bound(&mut self, bound: &[u8]) {
        self.seek(bound);
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Position at the first entry.
    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.find_next_user_entry();
    }

    /// Position at the last entry.
    pub fn seek_to_last(&mut self) {
        self.inner.seek_to_last();
        self.find_prev_user_entry();
    }

    /// Position at the first entry with user_key >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        let ikey = InternalKey::new(target, MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.inner.seek(ikey.as_bytes());
        self.find_next_user_entry();
    }

    /// Position at the last entry with user_key <= `target`.
    ///
    /// This is the reverse counterpart to `seek()`: while `seek(target)`
    /// positions at the first key >= target, `seek_for_prev(target)`
    /// positions at the last key <= target. Useful for reverse iteration
    /// from a specific point.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        // If target is at or past the upper bound, position at the last
        // key below the bound instead.
        if let Some(ref bound) = self.upper_bound {
            if target >= bound.as_slice() {
                let ikey = InternalKey::new(bound, MAX_SEQUENCE_NUMBER, ValueType::Value);
                self.inner.seek(ikey.as_bytes());
                if self.inner.valid() {
                    self.inner.prev();
                } else {
                    self.inner.seek_to_last();
                }
                self.find_prev_user_entry();
                return;
            }
        }

        // Normal case: target is within the valid range.
        self.seek(target);
        if self.valid {
            if self.saved_key.as_slice() == target {
                return;
            }
            // We overshot (key > target), go back one entry.
            self.prev();
        } else {
            // seek() returned invalid — either target is past all keys,
            // or we overshot into/past the upper bound.
            if let Some(bound) = self.upper_bound.clone() {
                // Position at the last key below the bound.
                let ikey = InternalKey::new(&bound, MAX_SEQUENCE_NUMBER, ValueType::Value);
                self.inner.seek(ikey.as_bytes());
                if self.inner.valid() {
                    self.inner.prev();
                } else {
                    self.inner.seek_to_last();
                }
                self.find_prev_user_entry();
            } else {
                self.seek_to_last();
            }
        }
    }

    /// Advance to the next entry.
    pub fn next(&mut self) {
        assert!(self.valid, "DbIterator::next() called on invalid iterator");
        self.skip_forward_past_current();
        self.find_next_user_entry();
    }

    /// Move to the previous entry.
    pub fn prev(&mut self) {
        assert!(self.valid, "DbIterator::prev() called on invalid iterator");
        // The inner iterator may be at the current saved_key or past it.
        // Skip backward past all entries for the current user key.
        let current = self.saved_key.clone();
        // First, ensure inner is on or before the current user key.
        while self.inner.valid() {
            if let Some(p) = ParsedInternalKey::from_bytes(self.inner.key()) {
                if p.user_key < current.as_slice() {
                    break;
                }
            }
            self.inner.prev();
        }
        self.find_prev_user_entry();
    }

    /// Set the number of bytes to strip from the front of returned keys.
    /// Used internally by column family iterators.
    pub(crate) fn set_key_prefix_len(&mut self, len: usize) {
        self.key_prefix_len = len;
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.valid, "DbIterator::key() called on invalid iterator");
        if self.key_prefix_len > 0 && self.saved_key.len() > self.key_prefix_len {
            &self.saved_key[self.key_prefix_len..]
        } else {
            &self.saved_key
        }
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.valid, "DbIterator::value() called on invalid iterator");
        &self.saved_value
    }

    // -----------------------------------------------------------------------
    // Forward scan helpers
    // -----------------------------------------------------------------------

    /// Skip the inner iterator past all entries for the current saved_key.
    fn skip_forward_past_current(&mut self) {
        let current = self.saved_key.clone();
        while self.inner.valid() {
            if let Some(p) = ParsedInternalKey::from_bytes(self.inner.key()) {
                if p.user_key != current.as_slice() {
                    break;
                }
            } else {
                break;
            }
            self.inner.next();
        }
    }

    /// Scan forward to find the next visible user entry.
    fn find_next_user_entry(&mut self) {
        self.valid = false;

        while self.inner.valid() {
            // Copy key bytes to avoid borrow conflicts.
            let ikey = self.inner.key().to_vec();
            let parsed = match ParsedInternalKey::from_bytes(&ikey) {
                Some(p) => p,
                None => {
                    self.inner.next();
                    continue;
                }
            };

            if parsed.sequence > self.sequence {
                self.inner.next();
                continue;
            }

            let user_key = parsed.user_key.to_vec();
            let seq = parsed.sequence;

            match parsed.value_type {
                ValueType::Value => {
                    if self.is_range_deleted(&user_key, seq) {
                        self.skip_forward_past_user_key(&user_key);
                        continue;
                    }
                    // Check upper bound before returning.
                    if let Some(ref ub) = self.upper_bound {
                        if user_key.as_slice() >= ub.as_slice() {
                            self.valid = false;
                            return;
                        }
                    }
                    self.saved_key = user_key;
                    self.saved_value = self.inner.value().to_vec();
                    self.valid = true;
                    return;
                }
                ValueType::Deletion | ValueType::RangeDeletion => {
                    self.inner.next();
                    while self.inner.valid() {
                        if let Some(p2) = ParsedInternalKey::from_bytes(self.inner.key()) {
                            if p2.user_key != user_key.as_slice() {
                                break;
                            }
                        } else {
                            break;
                        }
                        self.inner.next();
                    }
                }
            }
        }
    }

    fn skip_forward_past_user_key(&mut self, user_key: &[u8]) {
        let owned = user_key.to_vec();
        self.inner.next();
        while self.inner.valid() {
            if let Some(p) = ParsedInternalKey::from_bytes(self.inner.key()) {
                if p.user_key != owned.as_slice() {
                    break;
                }
            } else {
                break;
            }
            self.inner.next();
        }
    }

    // -----------------------------------------------------------------------
    // Backward scan helpers
    // -----------------------------------------------------------------------

    /// Going backward, find the previous visible user entry.
    ///
    /// When scanning backward, for the same user key we encounter the
    /// **oldest** version first (lowest sequence). We must collect all
    /// versions and pick the newest visible one.
    fn find_prev_user_entry(&mut self) {
        self.valid = false;

        while self.inner.valid() {
            let ikey = self.inner.key().to_vec();
            let parsed = match ParsedInternalKey::from_bytes(&ikey) {
                Some(p) => p,
                None => {
                    self.inner.prev();
                    continue;
                }
            };

            let user_key = parsed.user_key.to_vec();

            // Scan backward through all versions of this user key,
            // tracking the newest visible version.
            let mut best_vt: Option<ValueType> = None;
            let mut best_value = Vec::new();
            let mut best_seq: SequenceNumber = 0;

            // Process the current entry.
            if parsed.sequence <= self.sequence {
                best_vt = Some(parsed.value_type);
                best_value = self.inner.value().to_vec();
                best_seq = parsed.sequence;
            }

            // Continue backward through remaining versions of this user key.
            self.inner.prev();
            while self.inner.valid() {
                if let Some(p) = ParsedInternalKey::from_bytes(self.inner.key()) {
                    if p.user_key != user_key.as_slice() {
                        break;
                    }
                    if p.sequence <= self.sequence && p.sequence >= best_seq {
                        best_vt = Some(p.value_type);
                        best_value = self.inner.value().to_vec();
                        best_seq = p.sequence;
                    }
                } else {
                    break;
                }
                self.inner.prev();
            }

            // inner is now before all versions of user_key, or invalid.
            match best_vt {
                Some(ValueType::Value) => {
                    if !self.is_range_deleted(&user_key, best_seq) {
                        self.saved_key = user_key;
                        self.saved_value = best_value;
                        self.valid = true;
                        return;
                    }
                }
                _ => {
                    // Deleted, range-deleted, or no visible version.
                }
            }
            // Continue to next (earlier) user key.
        }
    }

    // -----------------------------------------------------------------------
    // Range tombstone checking
    // -----------------------------------------------------------------------

    /// Check if a user key at the given sequence is covered by a range
    /// tombstone with a higher or equal sequence number.
    fn is_range_deleted(&self, user_key: &[u8], seq: SequenceNumber) -> bool {
        for (start, end, tomb_seq) in &self.range_tombstones {
            if user_key >= start.as_slice()
                && user_key < end.as_slice()
                && *tomb_seq > seq
                && *tomb_seq <= self.sequence
            {
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::XdbIterator;

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
        fn seek_to_last(&mut self) {
            if self.entries.is_empty() {
                self.pos = 0;
            } else {
                self.pos = self.entries.len() - 1;
            }
        }
        fn prev(&mut self) {
            if self.pos == 0 || self.pos == usize::MAX {
                self.pos = usize::MAX;
            } else {
                self.pos -= 1;
            }
        }
    }

    fn make_entry(
        user_key: &[u8],
        seq: SequenceNumber,
        vt: ValueType,
        value: &[u8],
    ) -> (Vec<u8>, Vec<u8>) {
        let ikey = InternalKey::new(user_key, seq, vt);
        (ikey.as_bytes().to_vec(), value.to_vec())
    }

    fn collect_forward(iter: &mut DbIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        iter.seek_to_first();
        while iter.valid() {
            result.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next();
        }
        result
    }

    fn collect_backward(iter: &mut DbIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        iter.seek_to_last();
        while iter.valid() {
            result.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.prev();
        }
        result
    }

    #[test]
    fn iterate_simple_puts() {
        let entries = vec![
            make_entry(b"apple", 3, ValueType::Value, b"red"),
            make_entry(b"banana", 2, ValueType::Value, b"yellow"),
            make_entry(b"cherry", 1, ValueType::Value, b"dark"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"apple");
        assert_eq!(pairs[2].0, b"cherry");
    }

    #[test]
    fn deleted_keys_are_hidden() {
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"red"),
            make_entry(b"banana", 4, ValueType::Deletion, b""),
            make_entry(b"banana", 2, ValueType::Value, b"yellow"),
            make_entry(b"cherry", 3, ValueType::Value, b"dark"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, b"apple");
        assert_eq!(pairs[1].0, b"cherry");
    }

    #[test]
    fn only_newest_version_shown() {
        let entries = vec![
            make_entry(b"key", 5, ValueType::Value, b"new"),
            make_entry(b"key", 3, ValueType::Value, b"old"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, b"new");
    }

    #[test]
    fn sequence_filtering_older_snapshot() {
        let entries = vec![
            make_entry(b"key", 5, ValueType::Value, b"new"),
            make_entry(b"key", 3, ValueType::Value, b"old"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 4);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, b"old");
    }

    #[test]
    fn sequence_filtering_hides_all_versions() {
        let entries = vec![
            make_entry(b"key", 5, ValueType::Value, b"new"),
            make_entry(b"key", 3, ValueType::Value, b"old"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 2);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 0);
    }

    #[test]
    fn deletion_visible_only_at_snapshot() {
        let entries = vec![
            make_entry(b"key", 5, ValueType::Deletion, b""),
            make_entry(b"key", 3, ValueType::Value, b"alive"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 4);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, b"alive");
    }

    #[test]
    fn seek_positions_correctly() {
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"a"),
            make_entry(b"banana", 2, ValueType::Value, b"b"),
            make_entry(b"cherry", 3, ValueType::Value, b"c"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        iter.seek(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"banana");
    }

    #[test]
    fn seek_between_keys() {
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"a"),
            make_entry(b"cherry", 2, ValueType::Value, b"c"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        iter.seek(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");
    }

    #[test]
    fn seek_past_end() {
        let entries = vec![make_entry(b"apple", 1, ValueType::Value, b"a")];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        iter.seek(b"zzz");
        assert!(!iter.valid());
    }

    #[test]
    fn empty_iterator() {
        let mut iter = DbIterator::new(VecIterator::boxed(vec![]), 10);
        iter.seek_to_first();
        assert!(!iter.valid());
    }

    #[test]
    fn mixed_puts_and_deletes_across_keys() {
        let entries = vec![
            make_entry(b"a", 10, ValueType::Value, b"val_a"),
            make_entry(b"b", 9, ValueType::Deletion, b""),
            make_entry(b"b", 5, ValueType::Value, b"val_b_old"),
            make_entry(b"c", 8, ValueType::Value, b"val_c"),
            make_entry(b"c", 3, ValueType::Value, b"val_c_old"),
            make_entry(b"d", 7, ValueType::Deletion, b""),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 100);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[1].0, b"c");
    }

    // -- Reverse iteration tests --

    #[test]
    fn seek_to_last_simple() {
        let entries = vec![
            make_entry(b"apple", 1, ValueType::Value, b"a"),
            make_entry(b"banana", 2, ValueType::Value, b"b"),
            make_entry(b"cherry", 3, ValueType::Value, b"c"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        iter.seek_to_last();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");
        assert_eq!(iter.value(), b"c");
    }

    #[test]
    fn prev_walks_backward() {
        let entries = vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        let pairs = collect_backward(&mut iter);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"c");
        assert_eq!(pairs[1].0, b"b");
        assert_eq!(pairs[2].0, b"a");
    }

    #[test]
    fn prev_skips_deletions() {
        let entries = vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 4, ValueType::Deletion, b""),
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        let pairs = collect_backward(&mut iter);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, b"c");
        assert_eq!(pairs[1].0, b"a");
    }

    #[test]
    fn prev_newest_version_backward() {
        let entries = vec![
            make_entry(b"key", 5, ValueType::Value, b"new"),
            make_entry(b"key", 3, ValueType::Value, b"old"),
        ];
        let mut iter = DbIterator::new(VecIterator::boxed(entries), 10);
        iter.seek_to_last();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key");
        assert_eq!(iter.value(), b"new");
    }

    #[test]
    fn forward_backward_consistency() {
        let entries = vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
        ];
        let entries2 = entries.clone();
        let fwd = collect_forward(&mut DbIterator::new(VecIterator::boxed(entries), 10));
        let mut bwd = collect_backward(&mut DbIterator::new(VecIterator::boxed(entries2), 10));
        bwd.reverse();
        assert_eq!(fwd, bwd);
    }

    // -- Range tombstone tests --

    #[test]
    fn range_tombstone_hides_covered_keys() {
        let entries = vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
            make_entry(b"d", 4, ValueType::Value, b"4"),
        ];
        // Range tombstone [b, d) at seq=5 covers "b" and "c"
        let tombstones = vec![(b"b".to_vec(), b"d".to_vec(), 5u64)];
        let mut iter =
            DbIterator::with_range_tombstones(VecIterator::boxed(entries), 10, tombstones);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[1].0, b"d");
    }

    #[test]
    fn range_tombstone_respects_sequence() {
        let entries = vec![
            make_entry(b"a", 10, ValueType::Value, b"1"),
            make_entry(b"b", 8, ValueType::Value, b"2"),
        ];
        // Range tombstone at seq=5 should NOT hide b at seq=8 (put is newer)
        let tombstones = vec![(b"a".to_vec(), b"c".to_vec(), 5u64)];
        let mut iter =
            DbIterator::with_range_tombstones(VecIterator::boxed(entries), 10, tombstones);
        let pairs = collect_forward(&mut iter);
        assert_eq!(pairs.len(), 2);
    }
}
