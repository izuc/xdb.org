//! End-to-end integration tests for xdb.
//!
//! These tests exercise the database through its public API only.
//! Data verification after flush uses the iterator (which merges memtable
//! and SST sources) rather than point-get, because the iterator provides
//! the most comprehensive cross-layer validation.

use std::sync::Arc;
use xdb::{Db, Options, WriteBatch, WriteOptions};
use tempfile::TempDir;

/// Open a fresh database with default (large) memtable so writes stay
/// in-memory unless explicitly flushed.
fn open_db(dir: &TempDir) -> Arc<Db> {
    let opts = Options::default()
        .create_if_missing(true)
        .bloom_bits_per_key(10);
    Db::open(opts, dir.path()).unwrap()
}

/// Collect all (key, value) pairs from a DbIterator into a sorted vec.
fn collect_iter(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result = Vec::new();
    let mut iter = db.iter();
    iter.seek_to_first();
    while iter.valid() {
        result.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next();
    }
    result
}

// -----------------------------------------------------------------------
// Basic CRUD operations (in-memory, no flush)
// -----------------------------------------------------------------------

#[test]
fn basic_put_get_delete() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // Put and get
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));

    // Overwrite
    db.put(b"key1", b"value2").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value2".to_vec()));

    // Delete
    db.delete(b"key1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), None);

    // Non-existent key
    assert_eq!(db.get(b"no_such_key").unwrap(), None);
}

#[test]
fn write_batch_atomicity() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    let mut batch = WriteBatch::new();
    batch.put(b"a", b"1");
    batch.put(b"b", b"2");
    batch.put(b"c", b"3");
    batch.delete(b"b");
    db.write(WriteOptions::default(), batch).unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn empty_key_and_value() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"", b"empty_key").unwrap();
    assert_eq!(db.get(b"").unwrap(), Some(b"empty_key".to_vec()));

    db.put(b"empty_val", b"").unwrap();
    assert_eq!(db.get(b"empty_val").unwrap(), Some(b"".to_vec()));
}

#[test]
fn many_overwrites_same_key() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    for i in 0..1000u32 {
        db.put(b"counter", format!("{}", i).as_bytes()).unwrap();
    }

    assert_eq!(db.get(b"counter").unwrap(), Some(b"999".to_vec()));
}

// -----------------------------------------------------------------------
// Flush and iterator-based verification
// -----------------------------------------------------------------------

#[test]
fn flush_preserves_all_data_via_iterator() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    for i in 0..100u32 {
        db.put(
            format!("key{:04}", i).as_bytes(),
            format!("val{}", i).as_bytes(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    // Verify all data through the iterator
    let entries = collect_iter(&db);
    assert_eq!(entries.len(), 100);
    for (idx, (key, val)) in entries.iter().enumerate() {
        assert_eq!(key, format!("key{:04}", idx).as_bytes());
        assert_eq!(val, format!("val{}", idx).as_bytes());
    }
}

#[test]
fn flush_then_write_more_data() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // First batch: goes to SST after flush
    let keys_first: Vec<String> = (0..50).map(|i| format!("k{:03}", i)).collect();
    for (i, key) in keys_first.iter().enumerate() {
        db.put(key.as_bytes(), format!("v{}", i).as_bytes())
            .unwrap();
    }
    db.flush().unwrap();

    // Second batch: stays in memtable
    for i in 50..100 {
        db.put(
            format!("k{:03}", i).as_bytes(),
            format!("v{}", i).as_bytes(),
        )
        .unwrap();
    }

    // Full forward scan via iterator merges SST + memtable
    let mut iter = db.iter();
    iter.seek_to_first();
    let mut count = 0;
    let mut prev_key = Vec::new();
    while iter.valid() {
        assert!(
            iter.key() > prev_key.as_slice(),
            "keys must be in sorted order"
        );
        prev_key = iter.key().to_vec();
        count += 1;
        iter.next();
    }
    assert_eq!(count, 100);
}

#[test]
fn large_values() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    let large_value = vec![0x42u8; 100_000]; // 100 KB value
    db.put(b"big", &large_value).unwrap();

    // Verify in-memory
    let result = db.get(b"big").unwrap();
    assert_eq!(result, Some(large_value.clone()));

    // Flush and verify via iterator
    db.flush().unwrap();
    let entries = collect_iter(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, b"big");
    assert_eq!(entries[0].1, large_value);
}

#[test]
fn delete_before_flush_is_preserved() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"alive", b"yes").unwrap();
    db.put(b"dead", b"yes").unwrap();
    db.delete(b"dead").unwrap();
    db.flush().unwrap();

    let entries = collect_iter(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, b"alive");
    assert_eq!(entries[0].1, b"yes");
}

// -----------------------------------------------------------------------
// Iterator: seek, ordering, scan
// -----------------------------------------------------------------------

#[test]
fn iterator_full_scan_ordering() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    for i in 0..100u32 {
        db.put(
            format!("k{:03}", i).as_bytes(),
            format!("v{}", i).as_bytes(),
        )
        .unwrap();
    }

    let mut iter = db.iter();
    iter.seek_to_first();
    let mut count = 0;
    let mut prev_key = Vec::new();
    while iter.valid() {
        let key = iter.key().to_vec();
        assert!(key > prev_key, "keys must be in ascending order");
        prev_key = key;
        count += 1;
        iter.next();
    }
    assert_eq!(count, 100);
}

#[test]
fn iterator_seek() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    for i in 0..10u32 {
        db.put(
            format!("key{}", i).as_bytes(),
            format!("val{}", i).as_bytes(),
        )
        .unwrap();
    }

    let mut iter = db.iter();

    // Seek to existing key
    iter.seek(b"key5");
    assert!(iter.valid());
    assert_eq!(iter.key(), b"key5");

    // Seek to non-existing key lands on next
    iter.seek(b"key55");
    assert!(iter.valid());
    assert_eq!(iter.key(), b"key6");

    // Seek past end
    iter.seek(b"zzz");
    assert!(!iter.valid());
}

#[test]
fn iterator_seek_to_first_on_empty_db() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    let mut iter = db.iter();
    iter.seek_to_first();
    assert!(!iter.valid());
}

// -----------------------------------------------------------------------
// Snapshot isolation
// -----------------------------------------------------------------------

#[test]
fn snapshot_isolation() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"key1", b"v1").unwrap();
    db.put(b"key2", b"v2").unwrap();

    // Take snapshot
    let snap = db.snapshot();

    // Modify after snapshot
    db.put(b"key1", b"v1_updated").unwrap();
    db.put(b"key3", b"v3").unwrap();
    db.delete(b"key2").unwrap();

    // Current state via get (in-memory)
    assert_eq!(db.get(b"key1").unwrap(), Some(b"v1_updated".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), None);
    assert_eq!(db.get(b"key3").unwrap(), Some(b"v3".to_vec()));

    // Snapshot-based iterator should see old state
    let mut iter = db.iter_with_snapshot(&snap);
    iter.seek_to_first();

    assert!(iter.valid());
    assert_eq!(iter.key(), b"key1");
    assert_eq!(iter.value(), b"v1");
    iter.next();
    assert!(iter.valid());
    assert_eq!(iter.key(), b"key2");
    assert_eq!(iter.value(), b"v2");
    iter.next();
    assert!(!iter.valid()); // key3 didn't exist at snapshot time

    db.release_snapshot(&snap);
}

#[test]
fn snapshot_does_not_see_later_deletes() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    let snap = db.snapshot();

    db.delete(b"a").unwrap();
    db.delete(b"b").unwrap();

    // Current state: both deleted
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), None);

    // Snapshot: both alive
    let mut iter = db.iter_with_snapshot(&snap);
    iter.seek_to_first();
    assert!(iter.valid());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"1");
    iter.next();
    assert!(iter.valid());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"2");
    iter.next();
    assert!(!iter.valid());

    db.release_snapshot(&snap);
}

// -----------------------------------------------------------------------
// Concurrency
// -----------------------------------------------------------------------

#[test]
fn concurrent_reads_from_memtable() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    for i in 0..100u32 {
        db.put(
            format!("k{:03}", i).as_bytes(),
            format!("v{}", i).as_bytes(),
        )
        .unwrap();
    }

    let mut handles = Vec::new();
    for t in 0..4 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for i in 0..100u32 {
                let key = format!("k{:03}", i);
                let expected = format!("v{}", i);
                let val = db.get(key.as_bytes()).unwrap();
                assert_eq!(
                    val,
                    Some(expected.into_bytes()),
                    "thread {} failed on key {}",
                    t,
                    key
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_iterator_scans() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    for i in 0..50u32 {
        db.put(
            format!("k{:03}", i).as_bytes(),
            format!("v{}", i).as_bytes(),
        )
        .unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut iter = db.iter();
            iter.seek_to_first();
            let mut count = 0;
            while iter.valid() {
                count += 1;
                iter.next();
            }
            assert_eq!(count, 50);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

// -----------------------------------------------------------------------
// Statistics
// -----------------------------------------------------------------------

#[test]
fn statistics_tracking() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.get(b"a").unwrap();
    db.get(b"b").unwrap();
    db.get(b"nonexistent").unwrap();

    let stats = db.stats();
    use std::sync::atomic::Ordering;
    assert!(stats.writes.load(Ordering::Relaxed) >= 2);
    assert!(stats.reads.load(Ordering::Relaxed) >= 3);
}

#[test]
fn flush_increments_flush_counter() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"x", b"y").unwrap();
    db.flush().unwrap();

    let stats = db.stats();
    use std::sync::atomic::Ordering;
    assert!(
        stats.flushes.load(Ordering::Relaxed) >= 1,
        "flush counter should be at least 1"
    );
}

// -----------------------------------------------------------------------
// Write batch
// -----------------------------------------------------------------------

#[test]
fn write_batch_multiple_operations() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // Pre-populate
    db.put(b"existing", b"old_value").unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"new1", b"v1");
    batch.put(b"new2", b"v2");
    batch.put(b"existing", b"updated");
    batch.delete(b"new1");
    db.write(WriteOptions::default(), batch).unwrap();

    assert_eq!(db.get(b"existing").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(db.get(b"new1").unwrap(), None);
    assert_eq!(db.get(b"new2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn empty_write_batch() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"key", b"value").unwrap();

    let batch = WriteBatch::new();
    db.write(WriteOptions::default(), batch).unwrap();

    // Data should still be intact
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
}

// -----------------------------------------------------------------------
// Edge cases
// -----------------------------------------------------------------------

#[test]
fn put_delete_put_same_key() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    db.put(b"key", b"first").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"first".to_vec()));

    db.delete(b"key").unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);

    db.put(b"key", b"second").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"second".to_vec()));
}

#[test]
fn delete_nonexistent_key() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // Deleting a key that was never inserted should not error
    db.delete(b"ghost").unwrap();
    assert_eq!(db.get(b"ghost").unwrap(), None);
}

#[test]
fn many_distinct_keys() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // Insert 500 distinct keys (all in memtable)
    for i in 0..500u32 {
        let key = format!("key{:06}", i);
        let val = format!("value{:06}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Verify via get (all in memtable)
    for i in 0..500u32 {
        let key = format!("key{:06}", i);
        let val = format!("value{:06}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "missing key: {}",
            key
        );
    }

    // Verify via iterator for sorted order
    let entries = collect_iter(&db);
    assert_eq!(entries.len(), 500);
    for i in 0..500 {
        let expected_key = format!("key{:06}", i);
        assert_eq!(entries[i].0, expected_key.as_bytes());
    }
}

#[test]
fn synced_write() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    let mut batch = WriteBatch::new();
    batch.put(b"synced", b"data");
    let mut opts = WriteOptions::default();
    opts.sync = true;
    db.write(opts, batch).unwrap();

    assert_eq!(db.get(b"synced").unwrap(), Some(b"data".to_vec()));
}
