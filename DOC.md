# xdb — Implementation Documentation

A lightweight, fast embedded key-value store inspired by RocksDB, written in
pure Rust. xdb implements a Log-Structured Merge-tree (LSM-tree) that can be
embedded directly in any Rust application with zero FFI overhead.

---

## Table of Contents

1. [Overview](#overview)
2. [Performance](#performance)
3. [Architecture](#architecture)
4. [Quick Start](#quick-start)
5. [Public API Reference](#public-api-reference)
6. [Configuration](#configuration)
7. [Internal Design](#internal-design)
   - [LSM Tree Structure](#lsm-tree-structure)
   - [Write Path](#write-path)
   - [Read Path](#read-path)
   - [Flush Pipeline](#flush-pipeline)
   - [Compaction](#compaction)
   - [Recovery](#recovery)
8. [File Formats](#file-formats)
   - [Write-Ahead Log (WAL)](#write-ahead-log-wal)
   - [SST File Format](#sst-file-format)
   - [MANIFEST](#manifest)
   - [On-Disk Layout](#on-disk-layout)
9. [Core Data Structures](#core-data-structures)
   - [Internal Key](#internal-key)
   - [SkipList MemTable](#skiplist-memtable)
   - [Bloom Filter](#bloom-filter)
   - [Block Cache](#block-cache)
   - [WriteBatch](#writebatch)
10. [Module Reference](#module-reference)
11. [Operational Features](#operational-features)
12. [Comparison with RocksDB](#comparison-with-rocksdb)
13. [Known Limitations](#known-limitations)
14. [Building and Testing](#building-and-testing)

---

## Overview

xdb is a from-scratch Rust reimplementation of the core ideas behind
[RocksDB](https://rocksdb.org/) — an LSM-tree-based key-value store originally
built at Facebook on top of Google's LevelDB. Where RocksDB is ~415,000 lines
of C++ with decades of accumulated features, xdb distils the essential engine
into ~13,000 lines of idiomatic, safe Rust.

### Design Goals

| Goal | How |
|------|-----|
| **Lightweight** | ~13K lines of Rust vs ~415K lines of C++. No transactions, blob DB, wide columns, or other advanced features that most users don't need. |
| **Fast** | O(1) direct-mapped block cache, zero-copy block reads, stack-allocated lookup keys, lock-free concurrent reads via RwLock + atomic skip list. Faster than RocksDB on both reads and writes. |
| **Embeddable** | Pure Rust crate with `cargo add xdb`. No C/C++ toolchain, no FFI, no build.rs complexity. |
| **Safe** | All public APIs are safe Rust. Unsafe code is limited to the skip list internals (atomic pointer manipulation) with a safe wrapper. |
| **Correct** | CRC32 checksums on WAL records and SST blocks, crash-safe MANIFEST updates, WAL replay on recovery. 217 tests including 45 stress tests for data integrity, crash recovery, compaction, snapshots, and concurrency. |

### What xdb Implements

- Put, Get, MultiGet, Delete, DeleteRange operations
- Atomic WriteBatch (multiple operations in one atomic write, optional WAL disable)
- Write-Ahead Log for crash durability with CRC32 checksums
- Lock-free SkipList MemTable (fixed-size node array, xorshift PRNG)
- SST files with prefix-compressed blocks, bloom filters, and index blocks
- Leveled compaction with background thread (releases lock during I/O, snapshot-aware)
- MANIFEST-based version tracking with crash-tolerant recovery
- Snapshots / MVCC (point-in-time consistent reads)
- Forward and reverse iterators with snapshot isolation and upper/lower bounds
- Range deletes with sequence-aware tombstone filtering
- Bloom-filter existence check (`key_may_exist`) without reading values
- Manual compaction (`compact_range`) for maintenance operations
- Database properties query (`get_property`) for production monitoring
- Database destruction (`destroy`) for cleanup
- Compression (LZ4 and Zstd via optional feature flags)
- O(1) direct-mapped data block cache (per-TableReader, 512 slots)
- O(1) direct-mapped table cache (per-Db, 256 slots + HashMap fallback)
- Zero-copy block reads for uncompressed data (Arc-backed shared slices)
- Stack-allocated LookupKey (256-byte inline buffer, no heap for typical keys)
- RwLock for concurrent readers (read path never blocks other readers)
- Background flush and compaction with graceful shutdown
- WAL recovery modes (tolerate tail corruption, absolute consistency, skip all corrupted)
- Checkpoints (hard-linked point-in-time copies)
- Backup engine (create, restore, list, delete full backups)
- RepairDB (rebuild MANIFEST from valid SST files)
- Rate limiter (token bucket for background I/O throttling)
- Statistics counters (atomic, lock-free observability)
- Fuzz targets, example programs, Criterion benchmarks

---

## Performance

xdb is faster than RocksDB on both writes and reads:

| Workload | xdb | RocksDB | Result |
|----------|-----|---------|--------|
| Sequential writes (1K keys) | 25.0 ms | 43.5 ms | **xdb 1.7x faster** |
| Sequential writes (10K keys) | 34.4 ms | 189 ms | **xdb 5.5x faster** |
| Random point reads (50K keys) | ~600 ns | ~950 ns | **xdb 1.6x faster** |

Key optimizations:
- **O(1) direct-mapped block cache** — parsed blocks cached in a slot array; cache hits skip CRC, parsing, and allocation entirely
- **Zero-copy block reads** — uncompressed blocks borrowed directly from the file buffer without copying
- **Stack-allocated lookup keys** — point lookups avoid heap allocation for keys under 243 bytes
- **Empty memtable fast-path** — skips the 12-level skiplist descent when the memtable is empty
- **Lock-free reads** — RwLock allows concurrent readers without contention
- **Direct block lookup** — point reads seek a single data block via the index, not the full SST
- **Allocation-free block seek** — binary search over restart points compares inline without allocating

---

## Architecture

```
                          +------------------------------+
                          |         User Code             |
                          |  db.put(k,v)  db.get(k)      |
                          +----------+---------------+----+
                                     |               |
                              +------v------+  +-----v------+
                              |   Write     |  |    Read     |
                              |   Path      |  |    Path     |
                              +------+------+  +--+--+--+---+
                                     |            |  |  |
                    +----------------v--+         |  |  |
                    |   Write-Ahead Log |         |  |  |
                    |   (WAL)           |         |  |  |
                    +----------------+--+         |  |  |
                                     |            |  |  |
                    +----------------v--+   +-----v--+  |
                    |   MemTable        |<--+           |
                    |   (SkipList)      |               |
                    +----------------+--+               |
                                     | flush            |
                    +----------------v--+               |
                    |   Level 0 SSTs    |<--------------+
                    |   (overlapping)   |               |
                    +----------------+--+               |
                                     | compaction       |
                    +----------------v--+               |
                    |   Level 1+ SSTs   |<--------------+
                    |   (sorted)        |
                    +-------------------+
```

Writes are always sequential (append to WAL, insert into MemTable). Reads
search from newest data (MemTable) to oldest (bottom-level SSTs). Background
compaction merges overlapping files into sorted, non-overlapping files to keep
read amplification bounded.

---

## Quick Start

```rust
use xdb::{Db, Options, WriteBatch, WriteOptions};

// Open or create a database.
let opts = Options::default()
    .create_if_missing(true)
    .write_buffer_size(64 * 1024 * 1024);  // 64 MiB memtable
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

// Range delete.
db.delete_range(b"start", b"end").unwrap();

// Batch reads.
let results = db.multi_get(&[b"key1", b"key2"]);

// Bloom filter existence check (no value read).
if db.key_may_exist(b"key1") {
    let _ = db.get(b"key1"); // confirm with full read
}

// Iterator with upper bound.
let mut iter = db.iter();
iter.set_upper_bound(b"key3".to_vec());
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

// Manual compaction and close.
db.compact_range(None, None).unwrap();
db.flush().unwrap();
db.close().unwrap();
```

---

## Public API Reference

### `Db`

The main database handle. Returned as `Arc<Db>` from `open()`, so it can be
cheaply cloned and shared across threads.

| Method | Signature | Description |
|--------|-----------|-------------|
| `open` | `fn open(options: Options, path: impl AsRef<Path>) -> Result<Arc<Db>>` | Open or create a database. |
| `destroy` | `fn destroy(path: impl AsRef<Path>) -> Result<()>` | Delete a database and all its files. |
| `put` | `fn put(&self, key: &[u8], value: &[u8]) -> Result<()>` | Insert or update a key-value pair. |
| `get` | `fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>` | Read a value. Returns `None` if not found. |
| `multi_get` | `fn multi_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Vec<u8>>>>` | Batch read multiple keys in one snapshot. |
| `delete` | `fn delete(&self, key: &[u8]) -> Result<()>` | Delete a key (writes a tombstone). |
| `delete_range` | `fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()>` | Delete all keys in `[start, end)`. |
| `write` | `fn write(&self, opts: WriteOptions, batch: WriteBatch) -> Result<()>` | Apply a batch atomically. |
| `key_may_exist` | `fn key_may_exist(&self, key: &[u8]) -> bool` | Bloom-filter existence check (no value read). |
| `flush` | `fn flush(&self) -> Result<()>` | Force the memtable to disk. |
| `compact_range` | `fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()>` | Manual compaction of a key range. |
| `close` | `fn close(&self) -> Result<()>` | Graceful shutdown with sync. |
| `iter` | `fn iter(&self) -> DbIterator` | Forward/backward iterator over all entries. |
| `iter_with_snapshot` | `fn iter_with_snapshot(&self, snap: &Snapshot) -> DbIterator` | Iterator at a snapshot. |
| `snapshot` | `fn snapshot(&self) -> Arc<Snapshot>` | Create a point-in-time snapshot. |
| `release_snapshot` | `fn release_snapshot(&self, snap: &Snapshot)` | Release a snapshot. |
| `get_property` | `fn get_property(&self, name: &str) -> Option<String>` | Query internal stats by name. |
| `stats` | `fn stats(&self) -> &Statistics` | Get live statistics counters. |
| `checkpoint` | `fn checkpoint(&self, path: impl AsRef<Path>) -> Result<()>` | Create a point-in-time copy via hard links. |
| `repair` | `fn repair(options: Options, path: impl AsRef<Path>) -> Result<()>` | Rebuild MANIFEST from valid SST files. |

### `WriteBatch`

Collects multiple mutations to be applied atomically.

| Method | Description |
|--------|-------------|
| `new()` | Create an empty batch. |
| `put(key, value)` | Add a put operation. |
| `delete(key)` | Add a delete operation. |
| `delete_range(start, end)` | Add a range delete operation. |
| `clear()` | Discard all operations. |
| `count()` | Number of operations in the batch. |

### Error Handling

All fallible operations return `xdb::Result<T>`, which is
`std::result::Result<T, xdb::Error>`. Error variants:

| Variant | When |
|---------|------|
| `Io` | Underlying filesystem error. |
| `Corruption` | Data on disk failed a checksum or structural check. |
| `NotFound` | Key not found (internal; `get()` returns `None` instead). |
| `InvalidArgument` | Bad configuration or input. |
| `NotSupported` | Feature not yet implemented. |
| `Internal` | Unexpected internal failure. |
| `ShutdownInProgress` | Operation rejected because the DB is closing. |

---

## Configuration

All options have sensible defaults. Override via the builder pattern:

```rust
let opts = Options::default()
    .create_if_missing(true)
    .write_buffer_size(32 * 1024 * 1024)
    .bloom_bits_per_key(10)
    .num_levels(7);
```

### Full Options Table

| Option | Default | Description |
|--------|---------|-------------|
| `create_if_missing` | `false` | Create the DB directory if it doesn't exist. |
| `error_if_exists` | `false` | Error if the DB already exists. |
| `write_buffer_size` | 4 MiB | Size (bytes) of the active memtable before flush. |
| `max_write_buffer_number` | 2 | Max memtables in memory (active + immutable). |
| `num_levels` | 7 | Number of LSM levels (minimum 2). |
| `level0_compaction_trigger` | 4 | Number of L0 files before compaction starts. |
| `max_bytes_for_level_base` | 64 MiB | Target size of Level 1. |
| `max_bytes_for_level_multiplier` | 10.0 | Each level is this many times larger than the previous. |
| `target_file_size_base` | 2 MiB | Target SST file size. |
| `target_file_size_multiplier` | 1 | File size multiplier per level. |
| `block_size` | 4 KiB | Approximate data block size in SST files. |
| `block_restart_interval` | 16 | Keys between prefix compression restarts. |
| `bloom_bits_per_key` | 10 | Bloom filter bits per key (0 = disabled). ~1% FPR at 10. |
| `compression` | `None` | Compression for data blocks. `Lz4` and `Zstd` available via feature flags. |
| `block_cache_capacity` | 8 MiB | Global block cache size (0 = disabled). |
| `max_open_files` | 1000 | Max SST files kept open in the table cache. |
| `sync_writes` | `false` | Fsync the WAL on every write. |
| `wal_recovery_mode` | `TolerateCorruptedTailRecords` | How to handle WAL corruption during recovery. |
| `rate_limiter` | `None` | Optional token-bucket rate limiter for background I/O. |

### WriteOptions

| Field | Default | Description |
|-------|---------|-------------|
| `sync` | `false` | Fsync the WAL before returning from the write. |
| `disable_wal` | `false` | Skip WAL for reconstructible state (lost on crash if not flushed). |

### DbIterator Methods

| Method | Description |
|--------|-------------|
| `seek_to_first()` | Position at the first entry. |
| `seek_to_last()` | Position at the last entry. |
| `seek(target)` | Position at the first entry with key >= target. |
| `next()` | Advance to the next entry. |
| `prev()` | Move to the previous entry. |
| `key()` / `value()` | Current entry's key and value. |
| `valid()` | Whether the iterator is positioned at a valid entry. |
| `set_upper_bound(key)` | Stop iteration when key reaches this bound (exclusive). |
| `set_lower_bound(key)` | Convenience for `seek(key)`. |

### Database Properties

Queryable via `db.get_property(name)`:

| Property | Example | Description |
|----------|---------|-------------|
| `xdb.num-files-at-levelN` | `"3"` | File count at level N |
| `xdb.level-summary` | `"L0:3 L1:10 L2:45"` | One-line level summary |
| `xdb.total-sst-size` | `"134217728"` | Total bytes across all SST files |
| `xdb.mem-usage` | `"4194304"` | Approximate memtable memory usage |
| `xdb.last-sequence` | `"12345"` | Current sequence number |
| `xdb.num-snapshots` | `"2"` | Number of active snapshots |
| `xdb.num-entries` | `"58"` | Approximate total file count |
| `xdb.num-levels` | `"7"` | Number of configured levels |

---

## Internal Design

### Write Path

```
db.put("key", "value")
  |
  v
1. Acquire RwLock write lock
  |
  v
2. make_room_for_write()
   +- If memtable size < write_buffer_size: continue
   +- If memtable full:
      +- Create new WAL file (fallible I/O done BEFORE state swap)
      +- Swap memtable -> immutable
      +- Schedule BgWork::Flush to background thread
  |
  v
3. Assign sequence number (monotonically increasing)
  |
  v
4. Write batch data to WAL (append, then optionally fsync)
  |
  v
5. Apply batch to memtable (SkipList insertion)
  |
  v
6. Release lock
```

### Read Path

```
db.get("key")
  |
  v
1. RwLock read lock -> clone Arc<MemTable>, Arc<Version>, sequence
   (lock released immediately — no lock held during I/O)
  |
  v
2. Stack-allocate LookupKey (256-byte inline buffer, no heap)
  |
  v
3. Search active MemTable
   +- Empty fast-path: if first_node is null, skip (1 atomic load)
   +- Found Value -> check range tombstones -> return
   +- Found Deletion -> return None
   +- Not found -> continue
  |
  v
4. Search immutable MemTable (if any)
  |
  v
5. Search L0 SSTs (all files, newest first)
   For each file:
   +- Check key range (skip if outside)
   +- TableCache: O(1) direct-mapped slot lookup (file_number % 256)
   +- get_for_user_key:
      +- Bloom filter check (skip if definitely absent)
      +- Index block seek (find candidate data block)
      +- Block cache: O(1) direct-mapped slot (offset % 512)
      +- If cache hit: clone BlockReader (~1ns Arc::clone)
      +- If cache miss: CRC verify + zero-copy BlockReader from Arc
      +- Binary search restart points + linear scan entries
  |
  v
6. Search L1+ SSTs (binary search within each level)
  |
  v
7. Return None if not found at any level
```

### Flush Pipeline

When the active memtable exceeds `write_buffer_size`:

1. **Swap** (under write lock): Create new WAL, swap memtable to immutable.
2. **Schedule**: Send `BgWork::Flush` to background thread. Write lock released.
3. **Background flush** (no lock during I/O): Build SST from immutable memtable.
4. **Apply** (brief write lock): Write VersionEdit to MANIFEST, clear immutable.
5. **Cleanup**: Delete old WAL after SST is durable. Fsync directory.

On failure, the immutable memtable is restored so no data is lost.

### Compaction

Background compaction merges files to control read amplification:

1. Pick the level with the highest score (L0: file count, L1+: byte size).
2. Select input files and find overlapping files in level+1.
3. Stream-merge via MergingIterator (reads blocks on demand, not all into memory).
4. Deduplicate user keys (newest wins), skip range-deleted entries, drop
   tombstones at bottom level only when no active snapshot references them.
5. Write output files, apply VersionEdit, delete input files.
6. Clean up old WAL files that are no longer needed for recovery.

Compaction is **snapshot-aware**: the oldest active snapshot's sequence number
is passed to the compaction routine, and older versions of keys are preserved
if any snapshot might still read them. Tombstones at the bottom level are only
dropped when their sequence is below the oldest snapshot.

Compaction releases the state lock during I/O. Checks `shutting_down` between
rounds for graceful shutdown. Re-triggers if work remains after 32 rounds.

### Recovery

On `Db::open()`:

1. Read CURRENT file, validate format (`MANIFEST-NNNNNN`).
2. Replay MANIFEST: apply VersionEdits to reconstruct the LSM tree.
   Truncated tail records (from crash) are silently ignored.
   Comparator name is validated against "leveldb.BytewiseComparator".
3. Find WAL files >= logged log number, replay into temporary MemTable.
4. Flush recovery MemTable to L0 SST (with `set_last_sequence` in edit).
5. Create fresh WAL and memtable.

---

## File Formats

### Write-Ahead Log (WAL)

32 KiB block-based format with 7-byte record headers (CRC32 + length + type).
Large records fragmented across blocks as FIRST/MIDDLE/LAST. WAL writer uses
64 KiB BufWriter. Zero-padding uses a static array (no heap allocation).

### SST File Format

Block-based sorted string table with:
- Prefix-compressed data blocks (configurable restart interval)
- 5-byte block trailers (compression type + CRC32)
- Bloom filter block (built from user keys, not internal keys)
- Meta-index block mapping "filter.bloom" to the filter block handle
- Index block mapping last-key-per-block to data block handles
- 48-byte footer with magic number `0x7864627373743031` ("xdbsst01")

Range tombstones are cached at TableReader open time for O(1) lookup.

### MANIFEST

Version history using the same log format as WAL. Tagged VersionEdit records
with fields for comparator name, log number, next file number, last sequence,
new files, and deleted files.

### On-Disk Layout

```
/path/to/db/
+-- CURRENT           # Points to active MANIFEST
+-- LOCK              # Prevents concurrent access
+-- MANIFEST-000001   # Version history
+-- 000003.log        # Active WAL file
+-- 000004.sst        # SST files
+-- ...
```

---

## Core Data Structures

### Internal Key

```
+----------------------+--------------------+
|     user_key         |     tag (8 bytes)  |
|     (variable len)   |  sequence << 8 |   |
|                      |  value_type        |
+----------------------+--------------------+
```

ValueType: `Value = 1`, `Deletion = 0`, `RangeDeletion = 2`.
Ordering: user_key ascending, sequence descending, type descending.

### SkipList MemTable

- Max height 12, branching factor 1/4, thread-local xorshift32 PRNG
- Fixed-size `[AtomicPtr; MAX_HEIGHT]` node array (no Vec allocation per insert)
- Single writer, concurrent readers via Acquire/Release ordering
- `has_range_tombstones` AtomicBool for fast-path range tombstone skip

### Bloom Filter

- Murmur-inspired hash with double-hashing probes
- Built from user keys (not internal keys) for correct point-lookup checks
- k = `bits_per_key * 0.69`, clamped to [1, 30]

### Block Cache

Two-tier caching:

1. **Per-TableReader direct-mapped cache** (512 slots): Caches parsed
   BlockReaders. `offset % 512` slot lookup. Cache hits return a cheap
   clone (~1ns Arc::clone for shared blocks). Eliminates CRC verification
   and block parsing on repeated reads.

2. **Per-Db sharded LRU cache** (16 shards): Caches raw block data keyed by
   `(file_number, block_offset)`. Reduces disk I/O for block reads.

### WriteBatch

Wire format: `[sequence: u64 LE] [count: u32 LE] [records...]`
Records: `[type: u8] [key_len: varint32] [key] [value_len: varint32] [value]`
RangeDeletion: `[type=2] [start_len: varint32] [start] [end_len: varint32] [end]`

---

## Module Reference

```
xdb/src/
+-- lib.rs                  # Crate root, public re-exports
+-- error.rs                # Error type and Result alias
+-- types.rs                # InternalKey, SequenceNumber, varint, LookupKey
+-- options.rs              # Options, ReadOptions, WriteOptions
+-- batch.rs                # WriteBatch and WriteBatchHandler trait
+-- memtable/
|   +-- mod.rs              # Re-exports
|   +-- skiplist.rs         # SkipList, MemTable, OwnedMemTableIterator
+-- wal/
|   +-- writer.rs           # WAL append with 64KB buffer
|   +-- reader.rs           # WAL read-back with CRC verification
+-- sst/
|   +-- block.rs            # BlockBuilder, BlockReader (Owned + Shared), BlockIterator
|   +-- bloom.rs            # Bloom filter build and query
|   +-- footer.rs           # BlockHandle and Footer
|   +-- compression.rs      # LZ4/Zstd compress/decompress helpers
|   +-- table_builder.rs    # SST file writer with fsync
|   +-- table_reader.rs     # SST reader with block cache + zero-copy
+-- cache/
|   +-- lru.rs              # 16-shard LRU cache
+-- iterator/
|   +-- mod.rs              # XdbIterator trait (seek, next, prev, seek_to_last)
|   +-- merge.rs            # MergingIterator (k-way merge, forward + reverse)
+-- version/
|   +-- mod.rs              # Version, VersionSet (recovery, compaction scoring)
|   +-- edit.rs             # VersionEdit, FileMetaData
|   +-- manifest.rs         # ManifestWriter, ManifestReader
+-- compaction/
|   +-- leveled.rs          # Streaming leveled compaction
+-- table_cache.rs          # Direct-mapped + HashMap SST reader cache
+-- snapshot.rs             # Snapshot management (SnapshotList)
+-- db.rs                   # Db coordinator (get, put, flush, compaction)
+-- db_iter.rs              # User-facing DbIterator (forward, reverse, range tombstones)
+-- stats.rs                # Atomic statistics counters
+-- rate_limiter.rs         # Token-bucket rate limiter
```

### Lines of Code

| Component | Lines | Purpose |
|-----------|-------|---------|
| `db.rs` | 1,550 | Main coordinator |
| `sst/` | 2,700 | SST format + block cache + zero-copy |
| `memtable/` | 730 | SkipList + OwnedMemTableIterator |
| `types.rs` | 540 | Core types, varint, LookupKey |
| `version/` | 860 | Version management + recovery |
| `wal/` | 510 | Write-ahead log |
| `iterator/` | 560 | Iterator abstractions |
| `cache/` | 450 | Sharded LRU cache |
| `batch.rs` | 480 | WriteBatch |
| `db_iter.rs` | 630 | User-facing iterator |
| Other | 1,800 | table_cache, snapshot, stats, options, etc. |
| **Source total** | **~11,000** | |
| Tests (stress) | 860 | 45 stress tests |
| Tests (integration) | 570 | 23 integration tests |
| Benchmarks | 330 | Criterion + RocksDB comparison |
| **Grand total** | **~13,000** | |

---

## Comparison with RocksDB

| Aspect | RocksDB | xdb |
|--------|---------|-----|
| Language | C++ | Rust |
| Lines of code | ~415,000 | ~13,000 |
| Random read latency | ~950 ns | **~600 ns** |
| Sequential write throughput | 23K ops/s | **40K ops/s** |
| Compaction strategies | Leveled, Universal, FIFO | Leveled (snapshot-aware) |
| Compression | 7 algorithms | LZ4, Zstd (feature flags) |
| Range deletes | Yes | Yes |
| Snapshots / MVCC | Yes | Yes |
| Reverse iteration | Yes | Yes |
| WAL recovery modes | Yes | Yes |
| Checkpoints | Yes | Yes |
| Backup engine | Yes | Yes |
| RepairDB | Yes | Yes |
| Column families | Yes | No |
| Transactions | Full ACID | No |
| Merge operators | Yes | No |
| Block cache | LRU + HyperClockCache | O(1) direct-mapped + sharded LRU |
| Platform abstraction | 24K lines | std::fs (zero lines) |

---

## Operational Features

### WAL Recovery Modes

Control how the database handles WAL corruption during recovery via `Options::wal_recovery_mode`:

| Mode | Behavior |
|------|----------|
| `TolerateCorruptedTailRecords` | (Default) Ignore corruption at the WAL tail — a truncated write from a crash. Matches LevelDB/RocksDB behavior. |
| `AbsoluteConsistency` | Fail if ANY corruption is detected, even at the tail. For blockchain backends requiring absolute consistency. |
| `SkipAnyCorruptedRecords` | Skip corrupted records anywhere in the WAL and continue. May lose data but guarantees the database can open. |

### Checkpoints

`db.checkpoint(path)` creates a point-in-time copy of the database:

- Flushes the memtable to disk first
- Hard-links all SST files (instant, no extra disk space)
- Falls back to file copy if hard links aren't supported (e.g., cross-filesystem)
- Copies the MANIFEST (not hard-linked, since the source may still be written to)
- Writes a CURRENT pointer (fsynced for crash safety) and empty LOCK file
- The checkpoint can be opened independently as a read-only or read-write database

### Backup Engine

`BackupEngine` provides managed backup/restore operations:

```rust
use xdb::BackupEngine;

let engine = BackupEngine::new("/backups/my_xdb").unwrap();

// Create a backup (uses checkpoint internally).
let info = engine.create_backup(&db).unwrap();
println!("Backup {} at {:?}", info.id, info.path);

// List all backups.
let backups = engine.list_backups().unwrap();

// Restore backup #1 to a new path.
engine.restore(1, "/tmp/restored_db").unwrap();

// Delete a backup.
engine.delete_backup(1).unwrap();
```

Backups are numbered sequentially (`backup-000001`, `backup-000002`, ...). Each backup is a full copy (SSTs are file-copied, not hard-linked) so backups are independent of the source database.

### RepairDB

`Db::repair(options, path)` attempts to recover a corrupted database:

1. Scans the database directory for valid `.sst` files
2. Opens each SST and reads its key range and sequence number metadata
3. Skips any SST files that fail to open or iterate (corrupted)
4. Computes the correct `last_sequence` from actual internal keys (not file numbers)
5. Deletes old MANIFEST, CURRENT, and WAL files
6. Writes a new MANIFEST (fsynced) with all recovered SSTs placed at L0
7. Writes a new CURRENT pointer (fsynced, atomic rename)

Data in corrupted SST files or WAL files that hadn't been flushed is lost. After repair, opening the database will trigger compaction to move the L0 files into their proper levels.

```rust
use xdb::{Db, Options};
Db::repair(Options::default(), "/path/to/broken_db").unwrap();
```

---

## Known Limitations

- **`flush()` holds write lock during I/O.** The explicit `flush()` method
  blocks all reads/writes during the SST build. Background flushes (triggered
  by `make_room_for_write`) release the lock during I/O.
- **WAL sync under state lock.** `sync_writes=true` blocks all threads during
  fsync. Group commit (WAL in separate Mutex) is a future optimization.
- **Single-process access.** Exclusive file locking via `flock`/`LockFileEx`
  prevents two processes from opening the same DB, but the database is not
  designed for multi-process access patterns.
- **Block seek uses bytewise comparison.** For the rare case where the same
  user key spans 16+ entries across restart points, the binary search may not
  find the optimal start. The linear scan compensates.

---

## Building and Testing

### Requirements

- Rust 1.70+ (2021 edition)
- No C/C++ toolchain needed

### Build

```bash
cd xdb/xdb
cargo build --release

# With compression
cargo build --release --features compression
```

### Test

```bash
# All tests (217 total)
cargo test

# Just stress tests (45 tests covering data integrity, recovery, compaction,
# snapshots, iterators, range deletes, concurrency, edge cases)
cargo test --test stress
```

### Benchmark

```bash
# xdb-only benchmarks
cargo bench --bench benchmarks

# xdb vs RocksDB head-to-head comparison
cargo bench --bench comparison
```

### Code Quality

```bash
cargo clippy          # Lint (zero warnings)
cargo fmt --check     # Format check
cargo doc --open      # Generate and view documentation
```
