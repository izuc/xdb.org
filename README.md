# xdb

**A lightweight, fast embedded key-value store inspired by RocksDB, written in pure Rust.**

## Overview

xdb is a from-scratch Rust reimplementation of the core ideas behind
[RocksDB](https://rocksdb.org/) -- an LSM-tree-based key-value store. Where
RocksDB is roughly 415,000 lines of C++, xdb distils the essential engine into
approximately 13,000 lines of idiomatic, safe Rust. It embeds directly in any
Rust application with zero FFI overhead.

## Features

- **Put, Get, Delete, DeleteRange** -- full key-value CRUD operations
- **MultiGet** -- batch point lookups sharing a single state snapshot
- **WriteBatch** -- atomic multi-key writes (with optional WAL disable)
- **Write-Ahead Log** -- crash durability with CRC32 checksums
- **SkipList MemTable** -- lock-free concurrent reads via atomic operations
- **SST files** -- prefix-compressed data blocks, index blocks, and bloom filters
- **Leveled compaction** -- background merge from L0 through Ln
- **O(1) block cache** -- direct-mapped parsed block cache per SST reader
- **Snapshots / MVCC** -- point-in-time consistent reads
- **Iterators** -- forward and reverse range scans with upper/lower bounds
- **Range deletes** -- `delete_range()` with sequence-aware tombstone filtering
- **Compression** -- optional LZ4 and Zstd via feature flags
- **Manual compaction** -- `compact_range()` for maintenance operations
- **Bloom filter existence check** -- `key_may_exist()` without reading value
- **Database properties** -- `get_property()` for monitoring (L0 count, sizes, etc.)
- **Rate limiter** -- token-bucket I/O throttling for background work
- **WAL recovery modes** -- tolerate tail corruption, absolute consistency, or skip corrupted records
- **Checkpoints** -- instant point-in-time copies via hard-linked SST files
- **Backup engine** -- create, restore, list, and delete full database backups
- **RepairDB** -- rebuild MANIFEST from valid SST files after corruption
- **Statistics** -- lock-free atomic counters for observability
- **File locking** -- exclusive process lock via flock/LockFileEx
- **MANIFEST rotation** -- automatic rotation at 4 MiB for fast recovery
- **Panic recovery** -- background thread survives panics in flush/compaction
- **Thread-safe** -- `Arc<Db>` with RwLock for concurrent readers

## Quick Start

```rust
use xdb::{Db, Options, WriteBatch, WriteOptions};

// Open or create a database.
let opts = Options::default()
    .create_if_missing(true)
    .write_buffer_size(64 * 1024 * 1024);
let db = Db::open(opts, "/tmp/my_xdb").unwrap();

// Point operations.
db.put(b"name", b"xdb").unwrap();
assert_eq!(db.get(b"name").unwrap(), Some(b"xdb".to_vec()));
db.delete(b"name").unwrap();

// Atomic batch writes.
let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"key3");
db.write(WriteOptions::default(), batch).unwrap();

// Batch reads.
let results = db.multi_get(&[b"key1", b"key2", b"key3"]);

// Range delete.
db.delete_range(b"start", b"end").unwrap();

// Bloom filter existence check (no value read).
if db.key_may_exist(b"key1") {
    // Key might exist -- do full read to confirm.
    let _ = db.get(b"key1");
}

// Iterator with bounds.
let mut iter = db.iter();
iter.set_upper_bound(b"key3".to_vec());  // stop before "key3"
iter.seek(b"key1");
while iter.valid() {
    println!("{:?} => {:?}", iter.key(), iter.value());
    iter.next();
}

// Snapshots for consistent reads.
let snap = db.snapshot();
db.put(b"key1", b"updated").unwrap();
let mut snap_iter = db.iter_with_snapshot(&snap);
snap_iter.seek(b"key1"); // still sees the old value

// Write without WAL (for reconstructible state).
let mut opts = WriteOptions::default();
opts.disable_wal = true;
db.put(b"ephemeral", b"data").unwrap();

// Monitor database state.
println!("{}", db.get_property("xdb.level-summary").unwrap());
println!("{}", db.stats());

// Manual compaction.
db.compact_range(None, None).unwrap();

// Checkpoint (instant point-in-time copy).
db.checkpoint("/tmp/my_xdb_checkpoint").unwrap();

// Backup engine.
let engine = xdb::BackupEngine::new("/tmp/my_xdb_backups").unwrap();
let backup = engine.create_backup(&db).unwrap();
// engine.restore(backup.id, "/tmp/restored_db").unwrap();

// Flush and close.
db.flush().unwrap();
db.close().unwrap();

// Repair a corrupted database.
// Db::repair(Options::default(), "/tmp/broken_db").unwrap();

// Destroy database (delete all files).
// Db::destroy("/tmp/my_xdb").unwrap();
```

## Installation

```sh
cargo add xdb
```

With optional compression: `cargo add xdb --features lz4`, `--features zstd`,
or `--features compression` (both).

## Performance

xdb is **faster than RocksDB** on both writes and reads in head-to-head
benchmarks using identical configurations (64 MB write buffer, 10-bit bloom
filter, release mode, Windows 11 / x86-64):

| Workload | xdb | RocksDB | Result |
|----------|-----|---------|--------|
| Sequential writes (1K keys) | 25.0 ms | 43.5 ms | **xdb 1.7x faster** |
| Sequential writes (10K keys) | 34.4 ms | 189 ms | **xdb 5.5x faster** |
| Random point reads (50K keys) | ~600 ns | ~950 ns | **xdb ~1.6x faster** |

Key optimizations:
- **O(1) direct-mapped block cache** -- parsed blocks cached in a slot array;
  cache hits skip CRC, parsing, and allocation entirely
- **Zero-copy block reads** -- uncompressed blocks borrowed from the file buffer
- **Stack-allocated lookup keys** -- no heap allocation for keys under 243 bytes
- **Empty memtable fast-path** -- skips 12-level skiplist descent when empty
- **Lock-free reads** -- RwLock allows concurrent readers without contention
- **Direct block lookup** -- point reads seek one data block, not the full index
- **Allocation-free block seek** -- binary search compares inline, no temp buffers
- **Direct-mapped table cache** -- O(1) SST reader lookup without hashing

```sh
cargo bench -p xdb --bench benchmarks      # xdb-only
cargo bench -p xdb --bench comparison      # xdb vs RocksDB
```

## Public API

| Method | Description |
|--------|-------------|
| `Db::open(opts, path)` | Open or create a database |
| `Db::destroy(path)` | Delete a database and all its files |
| `db.put(key, value)` | Insert or update a key |
| `db.get(key)` | Read a value (returns `None` if absent) |
| `db.multi_get(keys)` | Batch read multiple keys in one snapshot |
| `db.delete(key)` | Delete a key |
| `db.delete_range(start, end)` | Delete all keys in [start, end) |
| `db.write(opts, batch)` | Apply a WriteBatch atomically |
| `db.key_may_exist(key)` | Bloom-filter existence check (no value read) |
| `db.flush()` | Force the memtable to disk |
| `db.compact_range(start, end)` | Manually compact a key range |
| `db.close()` | Graceful shutdown with sync |
| `db.iter()` | Forward/backward iterator |
| `db.iter_with_snapshot(snap)` | Iterator at a snapshot |
| `db.snapshot()` | Create a point-in-time snapshot |
| `db.release_snapshot(snap)` | Release a snapshot |
| `db.get_property(name)` | Query internal stats by name |
| `db.stats()` | Live statistics counters |
| `db.checkpoint(path)` | Create a point-in-time copy via hard links |
| `Db::repair(opts, path)` | Rebuild MANIFEST from valid SST files |

**Iterator methods:** `seek_to_first()`, `seek_to_last()`, `seek(target)`,
`next()`, `prev()`, `key()`, `value()`, `valid()`,
`set_upper_bound(key)`, `set_lower_bound(key)`.

**WriteOptions:** `sync` (fsync WAL), `disable_wal` (skip WAL for ephemeral data).

**WalRecoveryMode:** `TolerateCorruptedTailRecords` (default), `AbsoluteConsistency`,
`SkipAnyCorruptedRecords`.

**BackupEngine:** `new(dir)`, `create_backup(db)`, `restore(id, path)`,
`list_backups()`, `delete_backup(id)`.

**Properties:** `xdb.num-files-at-levelN`, `xdb.level-summary`, `xdb.total-sst-size`,
`xdb.mem-usage`, `xdb.last-sequence`, `xdb.num-snapshots`, `xdb.num-entries`,
`xdb.num-levels`.

## Configuration

```rust
let opts = Options::default()
    .create_if_missing(true)
    .write_buffer_size(64 * 1024 * 1024)
    .bloom_bits_per_key(10)
    .num_levels(7);
```

See [DOC.md](DOC.md) for the full options table and tuning guidelines.

## Architecture

```
  Write Path                Read Path
      |                     |   |   |
      v                     |   |   |
  WAL (append-only)         |   |   |
      |                     |   |   |
      v                     v   |   |
  MemTable (SkipList) <-----+   |   |
      | flush                   |   |
      v                         v   |
  L0 SSTs (overlapping) <------+   |
      | compaction                  |
      v                             v
  L1..Ln SSTs (sorted) <-----------+
```

See [DOC.md](DOC.md) for full internal design documentation.

## Building

```sh
cargo build -p xdb --release           # Build
cargo test -p xdb                      # Run 214 tests
cargo bench -p xdb                     # Run benchmarks
cargo build -p xdb --features compression  # With LZ4 + Zstd
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

## Contributing

Contributions are welcome. Please open an issue to discuss proposed changes
before submitting a pull request. All code must pass `cargo test` and
`cargo clippy` with no warnings.

See [DOC.md](DOC.md) for the full implementation documentation.
