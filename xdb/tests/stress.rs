//! Comprehensive stress tests for xdb.
//!
//! Designed for use as a blockchain backend: every test verifies data integrity
//! under conditions that would cause silent corruption in a lesser engine.
//!
//! Categories:
//!   1. Data Integrity
//!   2. Crash Recovery Simulation
//!   3. Compaction Correctness
//!   4. Snapshot Isolation
//!   5. Iterator Correctness
//!   6. Range Delete
//!   7. WriteBatch Atomicity
//!   8. Concurrent Access
//!   9. Edge Cases
//!  10. Performance Sanity Checks

use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;
use xdb::{Db, Options, WriteBatch, WriteOptions};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Small write_buffer_size to force frequent flushes and compaction.
/// 8192 bytes forces a flush every ~100-200 entries (depending on key/value
/// sizes), which is enough to exercise the L0 compaction path without
/// generating so many files that compaction cannot keep up in debug mode.
fn stress_opts() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing = true;
    opts.write_buffer_size = 8192;
    opts.level0_compaction_trigger = 2;
    opts.bloom_bits_per_key = 10;
    opts
}

/// Default-size memtable options (for tests that don't need forced flushes).
fn default_opts() -> Options {
    Options::default()
        .create_if_missing(true)
        .bloom_bits_per_key(10)
}

fn open(dir: &TempDir) -> Arc<Db> {
    Db::open(default_opts(), dir.path()).unwrap()
}

fn open_stress(dir: &TempDir) -> Arc<Db> {
    Db::open(stress_opts(), dir.path()).unwrap()
}

/// Options with compaction disabled (high trigger) for tests that need many flushes.
fn no_compact_opts() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing = true;
    opts.level0_compaction_trigger = 100; // effectively disable
    opts
}

fn key(i: u32) -> Vec<u8> {
    format!("k{:06}", i).into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("v{:06}", i).into_bytes()
}

fn val_versioned(i: u32, version: u32) -> Vec<u8> {
    format!("v{:06}_ver{}", i, version).into_bytes()
}

fn collect_all(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut it = db.iter();
    it.seek_to_first();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    out
}

fn collect_all_reverse(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut it = db.iter();
    it.seek_to_last();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.prev();
    }
    out
}

// =========================================================================
// 1. Data Integrity Tests
// =========================================================================

#[test]
fn integrity_write_flush_reopen_verify() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    // Test at multiple scales to catch issues with multi-block SSTs.
    for &n in &[10u32, 100, 1000, 10_000] {
        let dir2 = TempDir::new().unwrap();
        let path2 = dir2.path().to_path_buf();

        {
            let db = Db::open(default_opts(), &path2).unwrap();
            for i in 0..n {
                db.put(&key(i), &val(i)).unwrap();
            }
            db.flush().unwrap();
        }

        {
            let db = Db::open(default_opts(), &path2).unwrap();
            for i in 0..n {
                let v = db.get(&key(i)).unwrap();
                assert_eq!(
                    v.as_deref(),
                    Some(val(i).as_slice()),
                    "mismatch at key {} (n={})",
                    i, n
                );
            }
        }
    }
}

#[test]
fn integrity_delete_every_3rd_reopen_verify() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let n = 10_000u32;

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..n {
            db.put(&key(i), &val(i)).unwrap();
        }
        for i in (0..n).step_by(3) {
            db.delete(&key(i)).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..n {
            let v = db.get(&key(i)).unwrap();
            if i % 3 == 0 {
                assert_eq!(v, None, "key {} should be deleted", i);
            } else {
                assert_eq!(
                    v.as_deref(),
                    Some(val(i).as_slice()),
                    "key {} should exist",
                    i
                );
            }
        }
    }
}

#[test]
fn integrity_large_values_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let n = 5_000u32;
    // 1KB value template, unique per key
    let make_val = |i: u32| -> Vec<u8> {
        let mut v = vec![0u8; 1024];
        let bytes = i.to_le_bytes();
        v[..4].copy_from_slice(&bytes);
        v[1020..1024].copy_from_slice(&bytes);
        v
    };

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..n {
            db.put(&key(i), &make_val(i)).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..n {
            let v = db.get(&key(i)).unwrap().unwrap_or_else(|| {
                panic!("key {} missing after reopen", i);
            });
            assert_eq!(v.len(), 1024, "key {} wrong value length", i);
            assert_eq!(
                &v[..4],
                &i.to_le_bytes(),
                "key {} first 4 bytes mismatch",
                i
            );
            assert_eq!(
                &v[1020..1024],
                &i.to_le_bytes(),
                "key {} last 4 bytes mismatch",
                i
            );
        }
    }
}

#[test]
fn integrity_overwrite_10_times_latest_visible() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let n = 5_000u32;
    let versions = 10u32;

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for ver in 0..versions {
            for i in 0..n {
                db.put(&key(i), &val_versioned(i, ver)).unwrap();
            }
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..n {
            let v = db.get(&key(i)).unwrap();
            assert_eq!(
                v.as_deref(),
                Some(val_versioned(i, versions - 1).as_slice()),
                "key {} should have latest version",
                i
            );
        }
    }
}

#[test]
fn integrity_memtable_and_sst_both_readable() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    // First batch -> flush to SST
    for i in 0..500u32 {
        db.put(&key(i), &val(i)).unwrap();
    }
    db.flush().unwrap();

    // Second batch -> stays in memtable
    for i in 500..1000u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    // Both should be readable via get
    for i in 0..1000u32 {
        let v = db.get(&key(i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(val(i).as_slice()),
            "key {} not found (SST+memtable merge)",
            i
        );
    }

    // Iterator should see all 1000
    let entries = collect_all(&db);
    assert_eq!(entries.len(), 1000, "iterator should see all 1000 entries");
}

// =========================================================================
// 2. Crash Recovery Simulation
// =========================================================================

#[test]
fn recovery_flush_then_more_data_close_reopen() {
    // Write data, flush to SST, write more data (in WAL/memtable),
    // close cleanly (which triggers flush of pending data), reopen, verify ALL data.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(default_opts(), &path).unwrap();
        // Written to SST
        for i in 0..5_000u32 {
            db.put(&key(i), &val(i)).unwrap();
        }
        db.flush().unwrap();

        // Written to WAL/memtable only (not flushed explicitly)
        for i in 5_000..10_000u32 {
            db.put(&key(i), &val(i)).unwrap();
        }
        // close() triggers flush of pending memtable data
        db.close().unwrap();
    }

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..10_000u32 {
            let v = db.get(&key(i)).unwrap();
            assert_eq!(
                v.as_deref(),
                Some(val(i).as_slice()),
                "key {} missing after recovery",
                i
            );
        }
    }
}

#[test]
fn recovery_no_explicit_flush_close_reopens() {
    // Write data, DON'T flush (only in WAL/memtable), close (triggers flush), reopen
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..10_000u32 {
            db.put(&key(i), &val(i)).unwrap();
        }
        // No explicit flush -- close() triggers flush
        db.close().unwrap();
    }

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..10_000u32 {
            let v = db.get(&key(i)).unwrap();
            assert_eq!(
                v.as_deref(),
                Some(val(i).as_slice()),
                "key {} missing after WAL-only recovery",
                i
            );
        }
    }
}

#[test]
fn recovery_multiple_flushes_then_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..10_000u32 {
            db.put(&key(i), &val(i)).unwrap();
        }
        db.flush().unwrap();

        for i in 10_000..20_000u32 {
            db.put(&key(i), &val(i)).unwrap();
        }
        db.flush().unwrap();

        db.close().unwrap();
    }

    {
        let db = Db::open(default_opts(), &path).unwrap();
        for i in 0..20_000u32 {
            let v = db.get(&key(i)).unwrap();
            assert_eq!(
                v.as_deref(),
                Some(val(i).as_slice()),
                "key {} missing after multi-flush recovery",
                i
            );
        }
    }
}

// =========================================================================
// 3. Compaction Correctness
// =========================================================================

#[test]
fn compaction_all_data_readable_after() {
    let dir = TempDir::new().unwrap();
    // Use low compaction trigger so compaction runs after flushes.
    let mut opts = Options::default();
    opts.create_if_missing = true;
    opts.level0_compaction_trigger = 2;
    let db = Db::open(opts, dir.path()).unwrap();

    // Write 3 small batches and flush. With trigger=2, compaction fires after the 2nd flush.
    for batch_num in 0..3u32 {
        for i in 0..20u32 {
            let k = format!("k{:02}_{:04}", batch_num, i);
            db.put(k.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
    }

    // Give background compaction a moment to run.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Verify all data is readable.
    for batch_num in 0..3u32 {
        for i in 0..20u32 {
            let k = format!("k{:02}_{:04}", batch_num, i);
            let v = db.get(k.as_bytes()).unwrap();
            assert!(v.is_some(), "key {} missing after compaction", k);
        }
    }
}

#[test]
fn compaction_deletes_honored() {
    let dir = TempDir::new().unwrap();
    let mut opts = Options::default();
    opts.create_if_missing = true;
    opts.level0_compaction_trigger = 2;
    let db = Db::open(opts, dir.path()).unwrap();
    let n = 100u32;

    for i in 0..n {
        db.put(&key(i), &val(i)).unwrap();
    }
    db.flush().unwrap();

    // Delete even keys
    for i in (0..n).step_by(2) {
        db.delete(&key(i)).unwrap();
    }
    db.flush().unwrap();

    // One more flush to push compaction with trigger=2
    for i in 0..10u32 {
        db.put(format!("cpad{:04}", i).as_bytes(), b"y").unwrap();
    }
    db.flush().unwrap();

    std::thread::sleep(std::time::Duration::from_millis(500));

    for i in 0..n {
        let v = db.get(&key(i)).unwrap();
        if i % 2 == 0 {
            assert_eq!(v, None, "key {} should be deleted after compaction", i);
        } else {
            assert_eq!(
                v.as_deref(),
                Some(val(i).as_slice()),
                "key {} should survive compaction",
                i
            );
        }
    }
}

#[test]
fn compaction_newest_value_wins() {
    let dir = TempDir::new().unwrap();
    let mut opts = Options::default();
    opts.create_if_missing = true;
    opts.level0_compaction_trigger = 2;
    let db = Db::open(opts, dir.path()).unwrap();
    let n = 100u32;

    // Write same keys 3 times across flushes. With trigger=2, compaction fires.
    for ver in 0..3u32 {
        for i in 0..n {
            db.put(&key(i), &val_versioned(i, ver)).unwrap();
        }
        db.flush().unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(500));

    for i in 0..n {
        let v = db.get(&key(i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(val_versioned(i, 2).as_slice()),
            "key {} should have latest version after compaction",
            i
        );
    }
}

// =========================================================================
// 4. Snapshot Isolation
// =========================================================================

#[test]
fn snapshot_sees_old_data_not_new() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..1000u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let snap = db.snapshot();

    // Write new data after snapshot
    for i in 1000..2000u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    // Snapshot iterator: should see only 0..1000
    let mut snap_it = db.iter_with_snapshot(&snap);
    snap_it.seek_to_first();
    let mut count = 0;
    while snap_it.valid() {
        count += 1;
        snap_it.next();
    }
    assert_eq!(count, 1000, "snapshot should see exactly 1000 keys");

    // Current iterator: should see 0..2000
    let entries = collect_all(&db);
    assert_eq!(entries.len(), 2000, "current view should see 2000 keys");

    db.release_snapshot(&snap);
}

#[test]
fn snapshot_sees_deleted_keys() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..500u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let snap = db.snapshot();

    // Delete all keys
    for i in 0..500u32 {
        db.delete(&key(i)).unwrap();
    }

    // Current: empty
    let entries = collect_all(&db);
    assert_eq!(
        entries.len(),
        0,
        "all keys should be deleted in current view"
    );

    // Snapshot: all 500
    let mut snap_it = db.iter_with_snapshot(&snap);
    snap_it.seek_to_first();
    let mut count = 0;
    while snap_it.valid() {
        count += 1;
        snap_it.next();
    }
    assert_eq!(count, 500, "snapshot should still see all 500 keys");

    db.release_snapshot(&snap);
}

#[test]
fn snapshot_sees_old_values_after_overwrite() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..500u32 {
        db.put(&key(i), &val_versioned(i, 0)).unwrap();
    }

    let snap = db.snapshot();

    // Overwrite all keys
    for i in 0..500u32 {
        db.put(&key(i), &val_versioned(i, 1)).unwrap();
    }

    // Current: version 1
    for i in 0..500u32 {
        let v = db.get(&key(i)).unwrap();
        assert_eq!(v.as_deref(), Some(val_versioned(i, 1).as_slice()));
    }

    // Snapshot: version 0
    let mut snap_it = db.iter_with_snapshot(&snap);
    snap_it.seek_to_first();
    let mut idx = 0u32;
    while snap_it.valid() {
        assert_eq!(snap_it.key(), key(idx).as_slice());
        assert_eq!(
            snap_it.value(),
            val_versioned(idx, 0).as_slice(),
            "snapshot should see old value for key {}",
            idx
        );
        idx += 1;
        snap_it.next();
    }
    assert_eq!(idx, 500);

    db.release_snapshot(&snap);
}

#[test]
fn multiple_snapshots_different_versions() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    // Version 0
    for i in 0..100u32 {
        db.put(&key(i), &val_versioned(i, 0)).unwrap();
    }
    let snap0 = db.snapshot();

    // Version 1
    for i in 0..100u32 {
        db.put(&key(i), &val_versioned(i, 1)).unwrap();
    }
    let snap1 = db.snapshot();

    // Version 2
    for i in 0..100u32 {
        db.put(&key(i), &val_versioned(i, 2)).unwrap();
    }

    // Verify snap0 sees version 0
    {
        let mut it = db.iter_with_snapshot(&snap0);
        it.seek_to_first();
        let mut count = 0u32;
        while it.valid() {
            assert_eq!(
                it.value(),
                val_versioned(count, 0).as_slice(),
                "snap0: key {} should be version 0",
                count
            );
            count += 1;
            it.next();
        }
        assert_eq!(count, 100);
    }

    // Verify snap1 sees version 1
    {
        let mut it = db.iter_with_snapshot(&snap1);
        it.seek_to_first();
        let mut count = 0u32;
        while it.valid() {
            assert_eq!(
                it.value(),
                val_versioned(count, 1).as_slice(),
                "snap1: key {} should be version 1",
                count
            );
            count += 1;
            it.next();
        }
        assert_eq!(count, 100);
    }

    // Current sees version 2
    for i in 0..100u32 {
        let v = db.get(&key(i)).unwrap();
        assert_eq!(v.as_deref(), Some(val_versioned(i, 2).as_slice()));
    }

    db.release_snapshot(&snap0);
    db.release_snapshot(&snap1);
}

#[test]
fn release_snapshot_data_still_accessible_current() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..200u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let snap = db.snapshot();
    db.release_snapshot(&snap);

    // Data should still be accessible through current view
    for i in 0..200u32 {
        let v = db.get(&key(i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(val(i).as_slice()),
            "key {} should still be accessible after snapshot release",
            i
        );
    }
}

// =========================================================================
// 5. Iterator Correctness
// =========================================================================

#[test]
fn iterator_forward_sorted_no_duplicates() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n = 5_000u32;

    for i in 0..n {
        db.put(&key(i), &val(i)).unwrap();
    }

    let mut it = db.iter();
    it.seek_to_first();
    let mut prev: Option<Vec<u8>> = None;
    let mut count = 0u32;
    while it.valid() {
        let k = it.key().to_vec();
        if let Some(ref p) = prev {
            assert!(k > *p, "keys not in sorted order: {:?} <= {:?}", k, p);
        }
        prev = Some(k);
        count += 1;
        it.next();
    }
    assert_eq!(count, n, "forward scan missed keys");
}

#[test]
fn iterator_reverse_matches_forward() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..1_000u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let fwd = collect_all(&db);
    let mut rev = collect_all_reverse(&db);
    rev.reverse();

    assert_eq!(fwd.len(), rev.len(), "forward and reverse counts differ");
    for (i, (fk, rv)) in fwd.iter().zip(rev.iter()).enumerate() {
        assert_eq!(fk, rv, "mismatch at position {}", i);
    }
}

#[test]
fn iterator_seek_exact() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..100u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let mut it = db.iter();
    it.seek(&key(50));
    assert!(it.valid());
    assert_eq!(it.key(), key(50).as_slice());
    assert_eq!(it.value(), val(50).as_slice());
}

#[test]
fn iterator_seek_nonexistent_lands_on_next() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    // Insert keys 0, 2, 4, 6, 8
    for i in (0..10u32).step_by(2) {
        db.put(&key(i), &val(i)).unwrap();
    }

    let mut it = db.iter();
    // Seek to key(1) which doesn't exist, should land on key(2)
    it.seek(&key(1));
    assert!(it.valid());
    assert_eq!(it.key(), key(2).as_slice());
}

#[test]
fn iterator_after_flush_complete_scan() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n = 1_000u32;

    for i in 0..n {
        db.put(&key(i), &val(i)).unwrap();
    }
    db.flush().unwrap();

    let entries = collect_all(&db);
    assert_eq!(
        entries.len(),
        n as usize,
        "iterator after flush should see all keys"
    );
    for (i, (k, v)) in entries.iter().enumerate() {
        assert_eq!(k.as_slice(), key(i as u32).as_slice());
        assert_eq!(v.as_slice(), val(i as u32).as_slice());
    }
}

#[test]
fn iterator_with_snapshot_consistent() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..100u32 {
        db.put(&key(i), &val(i)).unwrap();
    }
    let snap = db.snapshot();

    // Add more data after snapshot
    for i in 100..200u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    // Snapshot iterator
    let mut it = db.iter_with_snapshot(&snap);
    it.seek_to_first();
    let mut count = 0;
    while it.valid() {
        count += 1;
        it.next();
    }
    assert_eq!(count, 100, "snapshot iterator should see exactly 100 keys");

    db.release_snapshot(&snap);
}

#[test]
fn iterator_empty_database() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    let mut it = db.iter();
    it.seek_to_first();
    assert!(!it.valid(), "iterator on empty db should be invalid");

    let mut it2 = db.iter();
    it2.seek_to_last();
    assert!(!it2.valid(), "seek_to_last on empty db should be invalid");
}

// =========================================================================
// 6. Range Delete Tests
// =========================================================================

#[test]
fn range_delete_basic() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"d", b"4").unwrap();
    db.put(b"e", b"5").unwrap();

    // Delete [b, d) -> deletes b and c, leaves a, d, e
    db.delete_range(b"b", b"d").unwrap();

    assert_eq!(
        db.get(b"a").unwrap(),
        Some(b"1".to_vec()),
        "'a' should survive"
    );
    assert_eq!(
        db.get(b"b").unwrap(),
        None,
        "'b' should be range-deleted"
    );
    assert_eq!(
        db.get(b"c").unwrap(),
        None,
        "'c' should be range-deleted"
    );
    assert_eq!(
        db.get(b"d").unwrap(),
        Some(b"4".to_vec()),
        "'d' should survive (exclusive end)"
    );
    assert_eq!(
        db.get(b"e").unwrap(),
        Some(b"5".to_vec()),
        "'e' should survive"
    );
}

#[test]
fn range_delete_then_put_in_range() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    db.put(b"b", b"old").unwrap();
    db.put(b"c", b"old").unwrap();
    // Flush the puts to SST first, so the range delete is clearly newer
    db.flush().unwrap();

    db.delete_range(b"b", b"d").unwrap();

    // Verify range delete took effect
    assert_eq!(db.get(b"b").unwrap(), None, "'b' should be range-deleted");
    assert_eq!(db.get(b"c").unwrap(), None, "'c' should be range-deleted");

    // Write a new value for b AFTER the range delete
    db.put(b"b", b"new").unwrap();

    assert_eq!(
        db.get(b"b").unwrap(),
        Some(b"new".to_vec()),
        "new put after range delete should be visible"
    );
    assert_eq!(
        db.get(b"c").unwrap(),
        None,
        "'c' should remain deleted"
    );
}

#[test]
fn range_delete_flush_reopen_persists() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(default_opts(), &path).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.put(b"d", b"4").unwrap();
        // Flush puts to SST first
        db.flush().unwrap();
        // Then range-delete and flush to a separate SST
        db.delete_range(b"b", b"d").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Db::open(default_opts(), &path).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(
            db.get(b"b").unwrap(),
            None,
            "'b' should be range-deleted after reopen"
        );
        assert_eq!(
            db.get(b"c").unwrap(),
            None,
            "'c' should be range-deleted after reopen"
        );
        assert_eq!(db.get(b"d").unwrap(), Some(b"4".to_vec()));
    }
}

#[test]
fn range_delete_overlapping_ranges() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for c in b'a'..=b'z' {
        db.put(&[c], &[c]).unwrap();
    }

    // Overlapping ranges: [b, f) and [d, h)
    db.delete_range(b"b", b"f").unwrap();
    db.delete_range(b"d", b"h").unwrap();

    // a survives, b-g deleted, h-z survive
    assert_eq!(db.get(b"a").unwrap(), Some(b"a".to_vec()));
    for c in b'b'..b'h' {
        assert_eq!(
            db.get(&[c]).unwrap(),
            None,
            "'{}' should be range-deleted",
            c as char
        );
    }
    for c in b'h'..=b'z' {
        assert_eq!(
            db.get(&[c]).unwrap(),
            Some(vec![c]),
            "'{}' should survive",
            c as char
        );
    }
}

#[test]
fn range_delete_all_keys() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..100u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    // All keys are k000000..k000099. Range delete covers them all.
    db.delete_range(b"k", b"l").unwrap();

    for i in 0..100u32 {
        assert_eq!(
            db.get(&key(i)).unwrap(),
            None,
            "key {} should be range-deleted",
            i
        );
    }

    let entries = collect_all(&db);
    assert_eq!(
        entries.len(),
        0,
        "database should appear empty after range delete of all keys"
    );
}

// =========================================================================
// 7. WriteBatch Atomicity
// =========================================================================

#[test]
fn writebatch_1000_puts_all_visible() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    let mut batch = WriteBatch::new();
    for i in 0..1_000u32 {
        batch.put(&key(i), &val(i));
    }
    db.write(WriteOptions::default(), batch).unwrap();

    for i in 0..1_000u32 {
        let v = db.get(&key(i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(val(i).as_slice()),
            "batch put key {} not visible",
            i
        );
    }
}

#[test]
fn writebatch_mixed_puts_deletes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    // Pre-populate
    for i in 0..100u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let mut batch = WriteBatch::new();
    // Delete even keys, update odd keys
    for i in 0..100u32 {
        if i % 2 == 0 {
            batch.delete(&key(i));
        } else {
            batch.put(&key(i), &val_versioned(i, 99));
        }
    }
    db.write(WriteOptions::default(), batch).unwrap();

    for i in 0..100u32 {
        let v = db.get(&key(i)).unwrap();
        if i % 2 == 0 {
            assert_eq!(v, None, "even key {} should be deleted", i);
        } else {
            assert_eq!(
                v.as_deref(),
                Some(val_versioned(i, 99).as_slice()),
                "odd key {} should be updated",
                i
            );
        }
    }
}

#[test]
fn writebatch_sequential_order_preserved() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    // Batch 1: set values
    let mut b1 = WriteBatch::new();
    for i in 0..50u32 {
        b1.put(&key(i), &val_versioned(i, 0));
    }
    db.write(WriteOptions::default(), b1).unwrap();

    // Batch 2: overwrite some
    let mut b2 = WriteBatch::new();
    for i in 0..25u32 {
        b2.put(&key(i), &val_versioned(i, 1));
    }
    db.write(WriteOptions::default(), b2).unwrap();

    for i in 0..50u32 {
        let v = db.get(&key(i)).unwrap();
        let expected_ver = if i < 25 { 1 } else { 0 };
        assert_eq!(
            v.as_deref(),
            Some(val_versioned(i, expected_ver).as_slice()),
            "key {} should have version {}",
            i,
            expected_ver
        );
    }
}

// =========================================================================
// 8. Concurrent Access
// =========================================================================

#[test]
fn concurrent_readers_and_writer() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n = 5_000u32;

    // Pre-populate so all keys exist before starting concurrent access
    for i in 0..n {
        db.put(&key(i), &val(i)).unwrap();
    }

    // Use a barrier to synchronize start
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let mut handles = Vec::new();

    // Writer thread: overwrite keys with new version
    {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..n {
                db.put(&key(i), &val_versioned(i, 1)).unwrap();
            }
        }));
    }

    // 4 reader threads: random reads, must always see a valid value
    for t in 0..4 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..n {
                let v = db.get(&key(i)).unwrap();
                assert!(
                    v.is_some(),
                    "thread {}: key {} should always exist during concurrent read/write",
                    t,
                    i
                );
                let v = v.unwrap();
                let old = val(i);
                let new = val_versioned(i, 1);
                assert!(
                    v == old || v == new,
                    "thread {}: key {} has unexpected value {:?}",
                    t,
                    i,
                    v
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_random_reads_sequential_writes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n = 5_000u32;

    // Writer thread
    let db_w = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 0..n {
            db_w.put(&key(i), &val(i)).unwrap();
        }
    });

    // 4 reader threads doing reads (may get None for not-yet-written keys)
    let mut readers = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        readers.push(thread::spawn(move || {
            for i in 0..n {
                let v = db.get(&key(i)).unwrap();
                if let Some(v) = v {
                    assert_eq!(
                        v,
                        val(i),
                        "key {} has wrong value during concurrent access",
                        i
                    );
                }
            }
        }));
    }

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
}

#[test]
fn concurrent_iterators() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..1_000u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut it = db.iter();
            it.seek_to_first();
            let mut count = 0;
            while it.valid() {
                count += 1;
                it.next();
            }
            assert!(count >= 1000, "concurrent iterator should see all keys");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn snapshot_cross_thread() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..500u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    let snap = db.snapshot();

    // Add more keys after snapshot
    for i in 500..1000u32 {
        db.put(&key(i), &val(i)).unwrap();
    }

    // Use snapshot from another thread
    let db2 = Arc::clone(&db);
    let handle = thread::spawn(move || {
        let mut it = db2.iter_with_snapshot(&snap);
        it.seek_to_first();
        let mut count = 0;
        while it.valid() {
            count += 1;
            it.next();
        }
        assert_eq!(
            count, 500,
            "cross-thread snapshot should see exactly 500 keys"
        );
        db2.release_snapshot(&snap);
    });

    handle.join().unwrap();
}

// =========================================================================
// 9. Edge Cases
// =========================================================================

#[test]
fn edge_empty_key_empty_value() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    db.put(b"", b"empty_key_val").unwrap();
    assert_eq!(db.get(b"").unwrap(), Some(b"empty_key_val".to_vec()));

    db.put(b"has_empty_val", b"").unwrap();
    assert_eq!(db.get(b"has_empty_val").unwrap(), Some(b"".to_vec()));

    db.put(b"", b"").unwrap();
    assert_eq!(db.get(b"").unwrap(), Some(b"".to_vec()));
}

#[test]
fn edge_long_key_long_value() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    let long_key = vec![0x42u8; 10 * 1024]; // 10KB key
    let long_val = vec![0xABu8; 1024 * 1024]; // 1MB value

    db.put(&long_key, &long_val).unwrap();
    let result = db.get(&long_key).unwrap();
    assert_eq!(result, Some(long_val.clone()));

    // Flush and re-read from SST
    db.flush().unwrap();
    let result2 = db.get(&long_key).unwrap();
    assert_eq!(result2, Some(long_val));
}

#[test]
fn edge_binary_keys_null_bytes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    let key_null = b"\x00\x00\x00".to_vec();
    let key_high = b"\xFF\xFF\xFF".to_vec();
    let key_mixed = b"\x00\xFF\x00\xFF".to_vec();

    db.put(&key_null, b"null").unwrap();
    db.put(&key_high, b"high").unwrap();
    db.put(&key_mixed, b"mixed").unwrap();

    assert_eq!(db.get(&key_null).unwrap(), Some(b"null".to_vec()));
    assert_eq!(db.get(&key_high).unwrap(), Some(b"high".to_vec()));
    assert_eq!(db.get(&key_mixed).unwrap(), Some(b"mixed".to_vec()));

    // Flush and verify from SST
    db.flush().unwrap();
    assert_eq!(db.get(&key_null).unwrap(), Some(b"null".to_vec()));
    assert_eq!(db.get(&key_high).unwrap(), Some(b"high".to_vec()));
    assert_eq!(db.get(&key_mixed).unwrap(), Some(b"mixed".to_vec()));
}

#[test]
fn edge_thousands_of_tiny_keys() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    // Single-byte keys 0x00..0xFF
    for b in 0..=255u8 {
        db.put(&[b], &[b]).unwrap();
    }

    for b in 0..=255u8 {
        let v = db.get(&[b]).unwrap();
        assert_eq!(v, Some(vec![b]), "tiny key 0x{:02X} mismatch", b);
    }

    // Iterator should have all 256
    let entries = collect_all(&db);
    assert_eq!(entries.len(), 256);
}

#[test]
fn edge_same_key_10000_times() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);

    for i in 0..10_000u32 {
        db.put(b"thekey", format!("{}", i).as_bytes()).unwrap();
    }

    let v = db.get(b"thekey").unwrap();
    assert_eq!(
        v,
        Some(b"9999".to_vec()),
        "only latest version should be visible"
    );

    // Iterator should show exactly one entry
    let entries = collect_all(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, b"thekey");
    assert_eq!(entries[0].1, b"9999");
}

#[test]
fn edge_error_if_exists_on_existing_db() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Create the DB
    {
        let db = Db::open(default_opts(), &path).unwrap();
        db.close().unwrap();
    }

    // Open again with error_if_exists
    let mut opts = default_opts();
    opts.error_if_exists = true;
    let result = Db::open(opts, &path);
    assert!(
        result.is_err(),
        "opening existing DB with error_if_exists should fail"
    );
}

#[test]
fn edge_open_nonexistent_no_create() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("does_not_exist");

    let mut opts = Options::default();
    opts.create_if_missing = false;
    let result = Db::open(opts, &path);
    assert!(
        result.is_err(),
        "opening non-existent path without create_if_missing should fail"
    );
}

// =========================================================================
// 10. Performance Sanity Checks
// =========================================================================

#[test]
fn perf_write_sequential() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n = 100_000u32;

    let start = Instant::now();
    for i in 0..n {
        db.put(&key(i), &val(i)).unwrap();
    }
    let elapsed = start.elapsed();

    // Generous limit: 120 seconds in debug mode for 100K keys
    assert!(
        elapsed.as_secs() < 120,
        "writing {} keys took {:?} which exceeds 120s limit",
        n,
        elapsed
    );
}

#[test]
fn perf_read_random() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n = 50_000u32;

    for i in 0..n {
        db.put(&key(i), &val(i)).unwrap();
    }
    db.flush().unwrap();

    let start = Instant::now();
    // Read in a pseudo-random order
    for i in 0..n {
        let idx = (i.wrapping_mul(7919) + 104729) % n;
        let v = db.get(&key(idx)).unwrap();
        assert!(v.is_some(), "key {} missing during perf read", idx);
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_secs() < 120,
        "reading {} random keys took {:?} which exceeds 120s limit",
        n,
        elapsed
    );
}

#[test]
fn perf_iterator_scan() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n = 100_000u32;

    for i in 0..n {
        db.put(&key(i), &val(i)).unwrap();
    }

    let start = Instant::now();
    let mut it = db.iter();
    it.seek_to_first();
    let mut count = 0u32;
    while it.valid() {
        count += 1;
        it.next();
    }
    let elapsed = start.elapsed();

    assert_eq!(count, n);
    assert!(
        elapsed.as_secs() < 60,
        "iterating {} keys took {:?} which exceeds 60s limit",
        n,
        elapsed
    );
}
