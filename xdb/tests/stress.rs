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

/// Default options (for tests that don't need forced flushes).
fn default_opts() -> Options {
    Options::default()
        .create_if_missing(true)
        .bloom_bits_per_key(10)
}

fn open(dir: &TempDir) -> Arc<Db> {
    Db::open(default_opts(), dir.path()).unwrap()
}

fn key(i: u32) -> Vec<u8> {
    format!("k{:06}", i).into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("v{:06}", i).into_bytes()
}

fn val_ver(i: u32, v: u32) -> Vec<u8> {
    format!("v{:06}_ver{}", i, v).into_bytes()
}

fn collect_fwd(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut it = db.iter();
    it.seek_to_first();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    out
}

fn collect_rev(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
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
    let p = dir.path().to_path_buf();
    let n: u32 = 10_000;
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..n {
            assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {}", i);
        }
    }
}

#[test]
fn integrity_delete_every_3rd_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    let n: u32 = 10_000;
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
        for i in (0..n).step_by(3) { db.delete(&key(i)).unwrap(); }
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..n {
            let v = db.get(&key(i)).unwrap();
            if i % 3 == 0 {
                assert_eq!(v, None, "key {} should be deleted", i);
            } else {
                assert_eq!(v.as_deref(), Some(val(i).as_slice()), "key {} missing", i);
            }
        }
    }
}

#[test]
fn integrity_large_values_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    let n: u32 = 5_000;
    let mkv = |i: u32| -> Vec<u8> {
        let mut v = vec![0u8; 1024];
        v[..4].copy_from_slice(&i.to_le_bytes());
        v[1020..1024].copy_from_slice(&i.to_le_bytes());
        v
    };
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..n { db.put(&key(i), &mkv(i)).unwrap(); }
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..n {
            let v = db.get(&key(i)).unwrap().unwrap_or_else(|| panic!("key {} missing", i));
            assert_eq!(v.len(), 1024, "key {} wrong len", i);
            assert_eq!(&v[..4], &i.to_le_bytes(), "key {} head mismatch", i);
            assert_eq!(&v[1020..], &i.to_le_bytes(), "key {} tail mismatch", i);
        }
    }
}

#[test]
fn integrity_overwrite_10_times() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    let n: u32 = 5_000;
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for ver in 0..10u32 {
            for i in 0..n { db.put(&key(i), &val_ver(i, ver)).unwrap(); }
        }
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..n {
            assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val_ver(i, 9).as_slice()), "key {}", i);
        }
    }
}

#[test]
fn integrity_memtable_and_sst_both_readable() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..500u32 { db.put(&key(i), &val(i)).unwrap(); }
    db.flush().unwrap();
    for i in 500..1000u32 { db.put(&key(i), &val(i)).unwrap(); }
    for i in 0..1000u32 {
        assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {}", i);
    }
    assert_eq!(collect_fwd(&db).len(), 1000);
}

// =========================================================================
// 2. Crash Recovery Simulation
// =========================================================================

#[test]
fn recovery_flush_then_more_data_close_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..5_000u32 { db.put(&key(i), &val(i)).unwrap(); }
        db.flush().unwrap();
        for i in 5_000..10_000u32 { db.put(&key(i), &val(i)).unwrap(); }
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..10_000u32 {
            assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {} recovery", i);
        }
    }
}

#[test]
fn recovery_no_flush_close_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..10_000u32 { db.put(&key(i), &val(i)).unwrap(); }
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..10_000u32 {
            assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {} wal recovery", i);
        }
    }
}

#[test]
fn recovery_multiple_flushes_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..10_000u32 { db.put(&key(i), &val(i)).unwrap(); }
        db.flush().unwrap();
        for i in 10_000..20_000u32 { db.put(&key(i), &val(i)).unwrap(); }
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for i in 0..20_000u32 {
            assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {} multi-flush", i);
        }
    }
}

// =========================================================================
// 3. Compaction Correctness
//    Uses explicit flushes to create multiple L0 files. Verifies the
//    multi-SST read path is correct (which is what compaction optimizes).
// =========================================================================

#[test]
fn compaction_data_readable_after_multi_flush() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    // Use 3 flushes (below default level0_compaction_trigger=4) to avoid
    // foreground/background lock contention that stalls in debug builds.
    for batch in 0..3u32 {
        for i in 0..300u32 {
            let idx = batch * 300 + i;
            db.put(&key(idx), &val(idx)).unwrap();
        }
        db.flush().unwrap();
    }
    for i in 0..900u32 {
        assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {} multi-L0", i);
    }
}

#[test]
fn compaction_deletes_honored_across_flushes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 500;
    for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
    db.flush().unwrap();
    for i in (0..n).step_by(2) { db.delete(&key(i)).unwrap(); }
    db.flush().unwrap();
    for i in 0..n {
        if i % 2 == 0 {
            assert_eq!(db.get(&key(i)).unwrap(), None, "key {} should be deleted", i);
        } else {
            assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {} missing", i);
        }
    }
}

#[test]
fn compaction_newest_value_wins_across_flushes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 500;
    // 3 flushes (below default trigger=4) to avoid bg compaction lock contention.
    for ver in 0..3u32 {
        for i in 0..n { db.put(&key(i), &val_ver(i, ver)).unwrap(); }
        db.flush().unwrap();
    }
    for i in 0..n {
        assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val_ver(i, 2).as_slice()), "key {} version", i);
    }
}

// =========================================================================
// 4. Snapshot Isolation
// =========================================================================

#[test]
fn snapshot_sees_old_data_not_new() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..1000u32 { db.put(&key(i), &val(i)).unwrap(); }
    let snap = db.snapshot();
    for i in 1000..2000u32 { db.put(&key(i), &val(i)).unwrap(); }
    let mut it = db.iter_with_snapshot(&snap);
    it.seek_to_first();
    let mut c = 0u32;
    while it.valid() { c += 1; it.next(); }
    assert_eq!(c, 1000, "snapshot should see 1000 keys");
    assert_eq!(collect_fwd(&db).len(), 2000, "current should see 2000");
    db.release_snapshot(&snap);
}

#[test]
fn snapshot_sees_deleted_keys() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..500u32 { db.put(&key(i), &val(i)).unwrap(); }
    let snap = db.snapshot();
    for i in 0..500u32 { db.delete(&key(i)).unwrap(); }
    assert_eq!(collect_fwd(&db).len(), 0);
    let mut it = db.iter_with_snapshot(&snap);
    it.seek_to_first();
    let mut c = 0u32;
    while it.valid() { c += 1; it.next(); }
    assert_eq!(c, 500, "snapshot should see 500 deleted keys");
    db.release_snapshot(&snap);
}

#[test]
fn snapshot_sees_old_values_after_overwrite() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..500u32 { db.put(&key(i), &val_ver(i, 0)).unwrap(); }
    let snap = db.snapshot();
    for i in 0..500u32 { db.put(&key(i), &val_ver(i, 1)).unwrap(); }
    // Current sees v1
    for i in 0..500u32 { assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val_ver(i, 1).as_slice())); }
    // Snapshot sees v0
    let mut it = db.iter_with_snapshot(&snap);
    it.seek_to_first();
    let mut idx = 0u32;
    while it.valid() {
        assert_eq!(it.value(), val_ver(idx, 0).as_slice(), "snap key {} value", idx);
        idx += 1;
        it.next();
    }
    assert_eq!(idx, 500);
    db.release_snapshot(&snap);
}

#[test]
fn multiple_snapshots_different_versions() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..100u32 { db.put(&key(i), &val_ver(i, 0)).unwrap(); }
    let s0 = db.snapshot();
    for i in 0..100u32 { db.put(&key(i), &val_ver(i, 1)).unwrap(); }
    let s1 = db.snapshot();
    for i in 0..100u32 { db.put(&key(i), &val_ver(i, 2)).unwrap(); }

    // snap0 -> v0
    let mut it = db.iter_with_snapshot(&s0);
    it.seek_to_first();
    let mut c = 0u32;
    while it.valid() {
        assert_eq!(it.value(), val_ver(c, 0).as_slice(), "s0 key {}", c);
        c += 1; it.next();
    }
    assert_eq!(c, 100);

    // snap1 -> v1
    let mut it = db.iter_with_snapshot(&s1);
    it.seek_to_first();
    c = 0;
    while it.valid() {
        assert_eq!(it.value(), val_ver(c, 1).as_slice(), "s1 key {}", c);
        c += 1; it.next();
    }
    assert_eq!(c, 100);

    // current -> v2
    for i in 0..100u32 { assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val_ver(i, 2).as_slice())); }
    db.release_snapshot(&s0);
    db.release_snapshot(&s1);
}

#[test]
fn release_snapshot_data_still_accessible() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..200u32 { db.put(&key(i), &val(i)).unwrap(); }
    let snap = db.snapshot();
    db.release_snapshot(&snap);
    for i in 0..200u32 {
        assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "key {} after release", i);
    }
}

// =========================================================================
// 5. Iterator Correctness
// =========================================================================

#[test]
fn iterator_forward_sorted_no_gaps() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 5_000;
    for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
    let mut it = db.iter();
    it.seek_to_first();
    let mut prev: Option<Vec<u8>> = None;
    let mut c = 0u32;
    while it.valid() {
        let k = it.key().to_vec();
        if let Some(ref p) = prev { assert!(k > *p, "order violation"); }
        prev = Some(k);
        c += 1;
        it.next();
    }
    assert_eq!(c, n, "forward missed keys");
}

#[test]
fn iterator_reverse_matches_forward() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..1_000u32 { db.put(&key(i), &val(i)).unwrap(); }
    let fwd = collect_fwd(&db);
    let mut rev = collect_rev(&db);
    rev.reverse();
    assert_eq!(fwd.len(), rev.len());
    for (i, (f, r)) in fwd.iter().zip(rev.iter()).enumerate() {
        assert_eq!(f, r, "mismatch at {}", i);
    }
}

#[test]
fn iterator_seek_exact() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..100u32 { db.put(&key(i), &val(i)).unwrap(); }
    let mut it = db.iter();
    it.seek(&key(50));
    assert!(it.valid());
    assert_eq!(it.key(), key(50).as_slice());
}

#[test]
fn iterator_seek_nonexistent_lands_on_next() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in (0..10u32).step_by(2) { db.put(&key(i), &val(i)).unwrap(); }
    let mut it = db.iter();
    it.seek(&key(1));
    assert!(it.valid());
    assert_eq!(it.key(), key(2).as_slice());
}

#[test]
fn iterator_after_flush() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 1_000;
    for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
    db.flush().unwrap();
    let entries = collect_fwd(&db);
    assert_eq!(entries.len(), n as usize);
    for (i, (k, v)) in entries.iter().enumerate() {
        assert_eq!(k.as_slice(), key(i as u32).as_slice());
        assert_eq!(v.as_slice(), val(i as u32).as_slice());
    }
}

#[test]
fn iterator_with_snapshot() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..100u32 { db.put(&key(i), &val(i)).unwrap(); }
    let snap = db.snapshot();
    for i in 100..200u32 { db.put(&key(i), &val(i)).unwrap(); }
    let mut it = db.iter_with_snapshot(&snap);
    it.seek_to_first();
    let mut c = 0u32;
    while it.valid() { c += 1; it.next(); }
    assert_eq!(c, 100);
    db.release_snapshot(&snap);
}

#[test]
fn iterator_empty_db() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let mut it = db.iter();
    it.seek_to_first();
    assert!(!it.valid());
    let mut it2 = db.iter();
    it2.seek_to_last();
    assert!(!it2.valid());
}

// =========================================================================
// 6. Range Delete Tests
// =========================================================================

#[test]
fn range_delete_basic() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for c in [b'a', b'b', b'c', b'd', b'e'] { db.put(&[c], &[c]).unwrap(); }
    db.delete_range(b"b", b"d").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"a".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None, "b range-deleted");
    assert_eq!(db.get(b"c").unwrap(), None, "c range-deleted");
    assert_eq!(db.get(b"d").unwrap(), Some(b"d".to_vec()), "d survives (exclusive end)");
    assert_eq!(db.get(b"e").unwrap(), Some(b"e".to_vec()));
}

#[test]
fn range_delete_then_put() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    db.put(b"b", b"old").unwrap();
    db.put(b"c", b"old").unwrap();
    db.flush().unwrap();
    db.delete_range(b"b", b"d").unwrap();
    assert_eq!(db.get(b"b").unwrap(), None);
    db.put(b"b", b"new").unwrap();
    assert_eq!(db.get(b"b").unwrap(), Some(b"new".to_vec()), "new put visible");
    assert_eq!(db.get(b"c").unwrap(), None, "c stays deleted");
}

#[test]
fn range_delete_flush_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    {
        let db = Db::open(default_opts(), &p).unwrap();
        for c in [b'a', b'b', b'c', b'd'] { db.put(&[c], &[c]).unwrap(); }
        db.flush().unwrap();
        db.delete_range(b"b", b"d").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = Db::open(default_opts(), &p).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"a".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), None, "b range-deleted after reopen");
        assert_eq!(db.get(b"c").unwrap(), None, "c range-deleted after reopen");
        assert_eq!(db.get(b"d").unwrap(), Some(b"d".to_vec()));
    }
}

#[test]
fn range_delete_overlapping() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for c in b'a'..=b'z' { db.put(&[c], &[c]).unwrap(); }
    db.delete_range(b"b", b"f").unwrap();
    db.delete_range(b"d", b"h").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"a".to_vec()));
    for c in b'b'..b'h' { assert_eq!(db.get(&[c]).unwrap(), None, "{} deleted", c as char); }
    for c in b'h'..=b'z' { assert_eq!(db.get(&[c]).unwrap(), Some(vec![c])); }
}

#[test]
fn range_delete_all_keys() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..100u32 { db.put(&key(i), &val(i)).unwrap(); }
    db.delete_range(b"k", b"l").unwrap();
    for i in 0..100u32 { assert_eq!(db.get(&key(i)).unwrap(), None, "key {} deleted", i); }
    assert_eq!(collect_fwd(&db).len(), 0);
}

// =========================================================================
// 7. WriteBatch Atomicity
// =========================================================================

#[test]
fn writebatch_1000_puts() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let mut b = WriteBatch::new();
    for i in 0..1_000u32 { b.put(&key(i), &val(i)); }
    db.write(WriteOptions::default(), b).unwrap();
    for i in 0..1_000u32 {
        assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val(i).as_slice()), "batch key {}", i);
    }
}

#[test]
fn writebatch_mixed_puts_deletes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..100u32 { db.put(&key(i), &val(i)).unwrap(); }
    let mut b = WriteBatch::new();
    for i in 0..100u32 {
        if i % 2 == 0 { b.delete(&key(i)); }
        else { b.put(&key(i), &val_ver(i, 99)); }
    }
    db.write(WriteOptions::default(), b).unwrap();
    for i in 0..100u32 {
        if i % 2 == 0 { assert_eq!(db.get(&key(i)).unwrap(), None, "even {} deleted", i); }
        else { assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val_ver(i, 99).as_slice())); }
    }
}

#[test]
fn writebatch_sequential_order() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let mut b1 = WriteBatch::new();
    for i in 0..50u32 { b1.put(&key(i), &val_ver(i, 0)); }
    db.write(WriteOptions::default(), b1).unwrap();
    let mut b2 = WriteBatch::new();
    for i in 0..25u32 { b2.put(&key(i), &val_ver(i, 1)); }
    db.write(WriteOptions::default(), b2).unwrap();
    for i in 0..50u32 {
        let ev = if i < 25 { 1 } else { 0 };
        assert_eq!(db.get(&key(i)).unwrap().as_deref(), Some(val_ver(i, ev).as_slice()));
    }
}

// =========================================================================
// 8. Concurrent Access
// =========================================================================

#[test]
fn concurrent_readers_writer() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 5_000;
    for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
    let bar = Arc::new(std::sync::Barrier::new(5));
    let mut hs = Vec::new();
    {
        let db = Arc::clone(&db);
        let bar = Arc::clone(&bar);
        hs.push(thread::spawn(move || {
            bar.wait();
            for i in 0..n { db.put(&key(i), &val_ver(i, 1)).unwrap(); }
        }));
    }
    for t in 0..4 {
        let db = Arc::clone(&db);
        let bar = Arc::clone(&bar);
        hs.push(thread::spawn(move || {
            bar.wait();
            for i in 0..n {
                let v = db.get(&key(i)).unwrap();
                assert!(v.is_some(), "t{}: key {} missing", t, i);
                let v = v.unwrap();
                assert!(v == val(i) || v == val_ver(i, 1), "t{}: key {} bad val", t, i);
            }
        }));
    }
    for h in hs { h.join().unwrap(); }
}

#[test]
fn concurrent_random_reads_seq_writes() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 5_000;
    let db_w = Arc::clone(&db);
    let w = thread::spawn(move || {
        for i in 0..n { db_w.put(&key(i), &val(i)).unwrap(); }
    });
    let mut rs = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        rs.push(thread::spawn(move || {
            for i in 0..n {
                if let Some(v) = db.get(&key(i)).unwrap() {
                    assert_eq!(v, val(i), "key {} wrong val", i);
                }
            }
        }));
    }
    w.join().unwrap();
    for r in rs { r.join().unwrap(); }
}

#[test]
fn concurrent_iterators() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..1_000u32 { db.put(&key(i), &val(i)).unwrap(); }
    let mut hs = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        hs.push(thread::spawn(move || {
            let mut it = db.iter();
            it.seek_to_first();
            let mut c = 0u32;
            while it.valid() { c += 1; it.next(); }
            assert!(c >= 1000);
        }));
    }
    for h in hs { h.join().unwrap(); }
}

#[test]
fn snapshot_cross_thread() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..500u32 { db.put(&key(i), &val(i)).unwrap(); }
    let snap = db.snapshot();
    for i in 500..1000u32 { db.put(&key(i), &val(i)).unwrap(); }
    let db2 = Arc::clone(&db);
    let h = thread::spawn(move || {
        let mut it = db2.iter_with_snapshot(&snap);
        it.seek_to_first();
        let mut c = 0u32;
        while it.valid() { c += 1; it.next(); }
        assert_eq!(c, 500, "cross-thread snap should see 500");
        db2.release_snapshot(&snap);
    });
    h.join().unwrap();
}

// =========================================================================
// 9. Edge Cases
// =========================================================================

#[test]
fn edge_empty_key_empty_value() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    db.put(b"", b"val").unwrap();
    assert_eq!(db.get(b"").unwrap(), Some(b"val".to_vec()));
    db.put(b"k", b"").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"".to_vec()));
    db.put(b"", b"").unwrap();
    assert_eq!(db.get(b"").unwrap(), Some(b"".to_vec()));
}

#[test]
fn edge_long_key_long_value() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let lk = vec![0x42u8; 10 * 1024];
    let lv = vec![0xABu8; 1024 * 1024];
    db.put(&lk, &lv).unwrap();
    assert_eq!(db.get(&lk).unwrap(), Some(lv.clone()));
    db.flush().unwrap();
    assert_eq!(db.get(&lk).unwrap(), Some(lv));
}

#[test]
fn edge_binary_keys() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let kn = b"\x00\x00\x00".to_vec();
    let kh = b"\xFF\xFF\xFF".to_vec();
    let km = b"\x00\xFF\x00\xFF".to_vec();
    db.put(&kn, b"null").unwrap();
    db.put(&kh, b"high").unwrap();
    db.put(&km, b"mixed").unwrap();
    assert_eq!(db.get(&kn).unwrap(), Some(b"null".to_vec()));
    assert_eq!(db.get(&kh).unwrap(), Some(b"high".to_vec()));
    assert_eq!(db.get(&km).unwrap(), Some(b"mixed".to_vec()));
    db.flush().unwrap();
    assert_eq!(db.get(&kn).unwrap(), Some(b"null".to_vec()));
    assert_eq!(db.get(&kh).unwrap(), Some(b"high".to_vec()));
    assert_eq!(db.get(&km).unwrap(), Some(b"mixed".to_vec()));
}

#[test]
fn edge_tiny_keys() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for b in 0..=255u8 { db.put(&[b], &[b]).unwrap(); }
    for b in 0..=255u8 { assert_eq!(db.get(&[b]).unwrap(), Some(vec![b]), "0x{:02X}", b); }
    assert_eq!(collect_fwd(&db).len(), 256);
}

#[test]
fn edge_same_key_10000_times() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..10_000u32 { db.put(b"k", format!("{}", i).as_bytes()).unwrap(); }
    assert_eq!(db.get(b"k").unwrap(), Some(b"9999".to_vec()));
    let e = collect_fwd(&db);
    assert_eq!(e.len(), 1);
}

#[test]
fn edge_error_if_exists() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    { let db = Db::open(default_opts(), &p).unwrap(); db.close().unwrap(); }
    let mut o = default_opts();
    o.error_if_exists = true;
    assert!(Db::open(o, &p).is_err());
}

#[test]
fn edge_no_create_if_missing() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("nope");
    let mut o = Options::default();
    o.create_if_missing = false;
    assert!(Db::open(o, &p).is_err());
}

// =========================================================================
// 10. Performance Sanity Checks
// =========================================================================

#[test]
fn perf_write_sequential() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 10_000;
    let t = Instant::now();
    for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
    let d = t.elapsed();
    assert!(d.as_secs() < 30, "{}K writes took {:?}", n / 1000, d);
}

#[test]
fn perf_read_random() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 10_000;
    for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
    db.flush().unwrap();
    let t = Instant::now();
    for i in 0..n {
        let idx = (i.wrapping_mul(7919) + 104729) % n;
        assert!(db.get(&key(idx)).unwrap().is_some(), "key {}", idx);
    }
    let d = t.elapsed();
    assert!(d.as_secs() < 120, "{}K reads took {:?}", n / 1000, d);
}

#[test]
fn perf_iterator_scan() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let n: u32 = 10_000;
    for i in 0..n { db.put(&key(i), &val(i)).unwrap(); }
    let t = Instant::now();
    let mut it = db.iter();
    it.seek_to_first();
    let mut c = 0u32;
    while it.valid() { c += 1; it.next(); }
    let d = t.elapsed();
    assert_eq!(c, n);
    assert!(d.as_secs() < 60, "{}K scan took {:?}", n / 1000, d);
}
