//! Snapshot management for MVCC-style consistent reads.
//!
//! A `Snapshot` captures the database state at a specific sequence number.
//! While a snapshot is held, compaction will not delete data that the snapshot
//! might need to read. Snapshots are lightweight -- they just record a sequence
//! number.

use parking_lot::Mutex;
use std::sync::Arc;

use crate::types::SequenceNumber;

/// An immutable point-in-time view of the database.
///
/// Created via `Db::snapshot()`. The snapshot remains valid until dropped.
/// While held, the data visible at the snapshot's sequence number is preserved.
#[derive(Debug)]
pub struct Snapshot {
    sequence: SequenceNumber,
    id: u64,
}

impl Snapshot {
    /// The sequence number this snapshot was taken at.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }
}

/// Tracks all active snapshots.
///
/// Used by the database to determine the oldest sequence number that must
/// be preserved during compaction.
pub struct SnapshotList {
    inner: Mutex<SnapshotInner>,
}

struct SnapshotInner {
    snapshots: Vec<SnapshotEntry>,
    next_id: u64,
}

struct SnapshotEntry {
    id: u64,
    sequence: SequenceNumber,
}

impl SnapshotList {
    /// Create an empty snapshot list.
    pub fn new() -> Self {
        SnapshotList {
            inner: Mutex::new(SnapshotInner {
                snapshots: Vec::new(),
                next_id: 1,
            }),
        }
    }

    /// Create a new snapshot at the given sequence number.
    pub fn create(&self, sequence: SequenceNumber) -> Arc<Snapshot> {
        let mut inner = self.inner.lock();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.snapshots.push(SnapshotEntry { id, sequence });
        Arc::new(Snapshot { sequence, id })
    }

    /// Release a snapshot. After this, the snapshot's data may be reclaimed
    /// by compaction.
    pub fn release(&self, snapshot: &Snapshot) {
        let mut inner = self.inner.lock();
        inner.snapshots.retain(|s| s.id != snapshot.id);
    }

    /// Return the sequence number of the oldest active snapshot,
    /// or `None` if there are no active snapshots.
    pub fn oldest_sequence(&self) -> Option<SequenceNumber> {
        let inner = self.inner.lock();
        inner.snapshots.iter().map(|s| s.sequence).min()
    }

    /// Number of active snapshots.
    pub fn count(&self) -> usize {
        self.inner.lock().snapshots.len()
    }

    /// Whether there are any active snapshots.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().snapshots.is_empty()
    }
}

impl Default for SnapshotList {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_release() {
        let list = SnapshotList::new();
        assert!(list.is_empty());
        assert_eq!(list.count(), 0);

        let snap = list.create(10);
        assert_eq!(snap.sequence(), 10);
        assert_eq!(list.count(), 1);
        assert!(!list.is_empty());

        list.release(&snap);
        assert!(list.is_empty());
        assert_eq!(list.count(), 0);
    }

    #[test]
    fn oldest_sequence_tracks_minimum() {
        let list = SnapshotList::new();

        // No snapshots: oldest is None.
        assert_eq!(list.oldest_sequence(), None);

        let snap1 = list.create(100);
        assert_eq!(list.oldest_sequence(), Some(100));

        let snap2 = list.create(50);
        assert_eq!(list.oldest_sequence(), Some(50));

        let snap3 = list.create(200);
        assert_eq!(list.oldest_sequence(), Some(50));

        // Release the oldest (seq=50). Now oldest should be 100.
        list.release(&snap2);
        assert_eq!(list.oldest_sequence(), Some(100));

        // Release seq=100. Now oldest should be 200.
        list.release(&snap1);
        assert_eq!(list.oldest_sequence(), Some(200));

        // Release the last one.
        list.release(&snap3);
        assert_eq!(list.oldest_sequence(), None);
    }

    #[test]
    fn releasing_oldest_updates_minimum() {
        let list = SnapshotList::new();

        let snap_old = list.create(5);
        let _snap_mid = list.create(15);
        let _snap_new = list.create(25);

        assert_eq!(list.oldest_sequence(), Some(5));

        list.release(&snap_old);
        assert_eq!(list.oldest_sequence(), Some(15));
    }

    #[test]
    fn multiple_snapshots_at_same_sequence() {
        let list = SnapshotList::new();

        let snap_a = list.create(42);
        let snap_b = list.create(42);
        let snap_c = list.create(42);

        assert_eq!(list.count(), 3);
        assert_eq!(list.oldest_sequence(), Some(42));

        // Release one -- the other two still hold the sequence.
        list.release(&snap_a);
        assert_eq!(list.count(), 2);
        assert_eq!(list.oldest_sequence(), Some(42));

        list.release(&snap_b);
        assert_eq!(list.count(), 1);
        assert_eq!(list.oldest_sequence(), Some(42));

        list.release(&snap_c);
        assert_eq!(list.count(), 0);
        assert_eq!(list.oldest_sequence(), None);
    }

    #[test]
    fn snapshot_ids_are_unique() {
        let list = SnapshotList::new();

        let snap1 = list.create(10);
        let snap2 = list.create(10);

        // Even though the sequence is the same, they have different ids
        // and releasing one doesn't affect the other.
        list.release(&snap1);
        assert_eq!(list.count(), 1);
        assert_eq!(list.oldest_sequence(), Some(10));

        list.release(&snap2);
        assert_eq!(list.count(), 0);
    }

    #[test]
    fn default_trait() {
        let list = SnapshotList::default();
        assert!(list.is_empty());
    }

    #[test]
    fn release_nonexistent_is_harmless() {
        let list = SnapshotList::new();
        let snap = list.create(10);
        list.release(&snap);

        // Releasing again should be a no-op (entry already removed).
        list.release(&snap);
        assert!(list.is_empty());
    }
}
