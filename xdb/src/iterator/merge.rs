//! Merging iterator that combines multiple sorted child iterators into
//! a single sorted sequence.
//!
//! When multiple children have the same key, the child with the lower
//! index wins -- this ensures newer data (e.g. memtable) shadows older
//! data (e.g. SST files) when children are ordered from newest to oldest.

use super::XdbIterator;
use crate::types::compare_internal_key;
use std::cmp::Ordering;

/// A merging iterator over multiple sorted child iterators.
///
/// The iterator always yields the entry with the smallest key across all
/// children.  Key comparison uses [`compare_internal_key`] by default.
///
/// The number of children is expected to be small (10-20), so finding
/// the minimum is done with a linear scan rather than a heap.
pub struct MergingIterator {
    children: Vec<Box<dyn XdbIterator>>,
    /// Index of the child currently positioned at the smallest key, or
    /// `None` if no child is valid.
    current: Option<usize>,
    /// Key comparison function.
    comparator: fn(&[u8], &[u8]) -> Ordering,
}

impl MergingIterator {
    /// Create a merging iterator over the given children.
    ///
    /// Keys are compared with [`compare_internal_key`].
    pub fn new(children: Vec<Box<dyn XdbIterator>>) -> Self {
        MergingIterator {
            children,
            current: None,
            comparator: compare_internal_key,
        }
    }

    /// Create a merging iterator with a custom key comparator.
    #[allow(dead_code)]
    pub fn with_comparator(
        children: Vec<Box<dyn XdbIterator>>,
        comparator: fn(&[u8], &[u8]) -> Ordering,
    ) -> Self {
        MergingIterator {
            children,
            current: None,
            comparator,
        }
    }

    /// Linear scan to find the child with the smallest current key.
    ///
    /// On ties (equal keys), the child with the lower index wins.
    fn find_smallest(&mut self) {
        self.current = None;
        let mut smallest_idx: Option<usize> = None;

        for (i, child) in self.children.iter().enumerate() {
            if !child.valid() {
                continue;
            }
            match smallest_idx {
                None => {
                    smallest_idx = Some(i);
                }
                Some(si) => {
                    let ord = (self.comparator)(child.key(), self.children[si].key());
                    if ord == Ordering::Less {
                        smallest_idx = Some(i);
                    }
                    // On Equal, keep the lower index (si), since si < i.
                }
            }
        }
        self.current = smallest_idx;
    }
}

impl XdbIterator for MergingIterator {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn seek_to_first(&mut self) {
        for child in &mut self.children {
            child.seek_to_first();
        }
        self.find_smallest();
    }

    fn seek(&mut self, target: &[u8]) {
        for child in &mut self.children {
            child.seek(target);
        }
        self.find_smallest();
    }

    fn next(&mut self) {
        let idx = self.current.expect("next() called on invalid iterator");
        self.children[idx].next();
        self.find_smallest();
    }

    fn key(&self) -> &[u8] {
        let idx = self.current.expect("key() called on invalid iterator");
        self.children[idx].key()
    }

    fn value(&self) -> &[u8] {
        let idx = self.current.expect("value() called on invalid iterator");
        self.children[idx].value()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial iterator over a pre-sorted `Vec` of key-value pairs.
    struct VecIterator {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        pos: usize,
    }

    impl VecIterator {
        fn boxed(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Box<dyn XdbIterator> {
            // Start in an invalid state (pos past end) until seek is called.
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
            // Linear scan for simplicity in tests.
            self.pos = self
                .entries
                .iter()
                .position(|(k, _)| k.as_slice() >= target)
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

    /// Helper: collect all key-value pairs from a merging iterator
    /// (after seek_to_first).
    fn collect_all(iter: &mut MergingIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        iter.seek_to_first();
        while iter.valid() {
            result.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next();
        }
        result
    }

    #[test]
    fn merge_two_sorted_sequences() {
        // Use raw byte keys (not internal keys) with a simple lexicographic
        // comparator so the test is easy to reason about.
        let a = VecIterator::boxed(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"e".to_vec(), b"5".to_vec()),
        ]);
        let b = VecIterator::boxed(vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
            (b"f".to_vec(), b"6".to_vec()),
        ]);

        let mut iter = MergingIterator::with_comparator(vec![a, b], |x, y| x.cmp(y));
        let pairs = collect_all(&mut iter);

        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(
            keys,
            vec![
                &b"a"[..],
                &b"b"[..],
                &b"c"[..],
                &b"d"[..],
                &b"e"[..],
                &b"f"[..],
            ]
        );
    }

    #[test]
    fn merge_with_empty_iterators() {
        let a = VecIterator::boxed(vec![]);
        let b = VecIterator::boxed(vec![
            (b"x".to_vec(), b"1".to_vec()),
        ]);
        let c = VecIterator::boxed(vec![]);

        let mut iter = MergingIterator::with_comparator(vec![a, b, c], |x, y| x.cmp(y));
        let pairs = collect_all(&mut iter);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, b"x");
    }

    #[test]
    fn merge_all_empty() {
        let mut iter = MergingIterator::with_comparator(vec![], |x, y| x.cmp(y));
        iter.seek_to_first();
        assert!(!iter.valid());
    }

    #[test]
    fn overlapping_keys_earlier_iterator_wins() {
        // Both iterators have key "k" but the first (lower index) should
        // appear first in the merged output.
        let a = VecIterator::boxed(vec![
            (b"k".to_vec(), b"from_a".to_vec()),
        ]);
        let b = VecIterator::boxed(vec![
            (b"k".to_vec(), b"from_b".to_vec()),
        ]);

        let mut iter = MergingIterator::with_comparator(vec![a, b], |x, y| x.cmp(y));
        iter.seek_to_first();

        assert!(iter.valid());
        assert_eq!(iter.key(), b"k");
        assert_eq!(iter.value(), b"from_a", "lower-index child should come first");

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"k");
        assert_eq!(iter.value(), b"from_b");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn seek_to_middle() {
        let a = VecIterator::boxed(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"e".to_vec(), b"5".to_vec()),
        ]);
        let b = VecIterator::boxed(vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
            (b"f".to_vec(), b"6".to_vec()),
        ]);

        let mut iter = MergingIterator::with_comparator(vec![a, b], |x, y| x.cmp(y));
        iter.seek(b"c");

        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"3");

        iter.next();
        assert_eq!(iter.key(), b"d");

        iter.next();
        assert_eq!(iter.key(), b"e");

        iter.next();
        assert_eq!(iter.key(), b"f");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn seek_past_all_entries() {
        let a = VecIterator::boxed(vec![
            (b"a".to_vec(), b"1".to_vec()),
        ]);

        let mut iter = MergingIterator::with_comparator(vec![a], |x, y| x.cmp(y));
        iter.seek(b"z");
        assert!(!iter.valid());
    }

    #[test]
    fn merge_with_internal_key_comparator() {
        use crate::types::{InternalKey, ValueType};

        // Build internal keys: same user key "foo" with different sequences.
        // Higher sequence number should sort first.
        let k1 = InternalKey::new(b"foo", 200, ValueType::Value);
        let k2 = InternalKey::new(b"foo", 100, ValueType::Value);
        let k3 = InternalKey::new(b"bar", 300, ValueType::Value);

        let a = VecIterator::boxed(vec![
            (k3.as_bytes().to_vec(), b"bar_val".to_vec()),
        ]);
        let b = VecIterator::boxed(vec![
            (k1.as_bytes().to_vec(), b"foo_200".to_vec()),
            (k2.as_bytes().to_vec(), b"foo_100".to_vec()),
        ]);

        let mut iter = MergingIterator::new(vec![a, b]);
        iter.seek_to_first();

        // "bar" < "foo" lexicographically.
        assert!(iter.valid());
        assert_eq!(iter.value(), b"bar_val");

        iter.next();
        assert_eq!(iter.value(), b"foo_200"); // seq 200 before seq 100

        iter.next();
        assert_eq!(iter.value(), b"foo_100");

        iter.next();
        assert!(!iter.valid());
    }
}
