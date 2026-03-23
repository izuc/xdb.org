# xdb — Implementation Documentation

A lightweight, fast embedded key-value store inspired by RocksDB, written in
pure Rust. xdb implements a Log-Structured Merge-tree (LSM-tree) that can be
embedded directly in any Rust application with zero FFI overhead.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Public API Reference](#public-api-reference)
5. [Configuration](#configuration)
6. [Internal Design](#internal-design)
   - [LSM Tree Structure](#lsm-tree-structure)
   - [Write Path](#write-path)
   - [Read Path](#read-path)
   - [Flush Pipeline](#flush-pipeline)
   - [Compaction](#compaction)
   - [Recovery](#recovery)
7. [File Formats](#file-formats)
   - [Write-Ahead Log (WAL)](#write-ahead-log-wal)
   - [SST File Format](#sst-file-format)
   - [MANIFEST](#manifest)
   - [On-Disk Layout](#on-disk-layout)
8. [Core Data Structures](#core-data-structures)
   - [Internal Key](#internal-key)
   - [SkipList MemTable](#skiplist-memtable)
   - [Bloom Filter](#bloom-filter)
   - [LRU Block Cache](#lru-block-cache)
   - [WriteBatch](#writebatch)
9. [Module Reference](#module-reference)
10. [Comparison with RocksDB](#comparison-with-rocksdb)
11. [Future Work (Phase 2+)](#future-work-phase-2)
12. [Building and Testing](#building-and-testing)

---

## Overview

xdb is a from-scratch Rust reimplementation of the core ideas behind
[RocksDB](https://rocksdb.org/) — an LSM-tree-based key-value store originally
built at Facebook on top of Google's LevelDB. Where RocksDB is ~415,000 lines
of C++ with decades of accumulated features, xdb distils the essential engine
into ~8,800 lines of idiomatic, safe Rust.

### Design Goals

| Goal | How |
|------|-----|
| **Lightweight** | ~8.8K lines of Rust vs ~415K lines of C++. No transactions, blob DB, wide columns, or other advanced features that most users don't need. |
| **Fast** | Zero-copy where possible, concurrent reads via atomic skip list, bloom filters to skip SST files, LRU block cache, efficient varint encoding. |
| **Embeddable** | Pure Rust crate with `cargo add xdb`. No C/C++ toolchain, no FFI, no build.rs complexity. |
| **Safe** | All public APIs are safe Rust. Unsafe code is limited to the skip list internals (atomic pointer manipulation) with a safe wrapper. |
| **Correct** | CRC32 checksums on WAL records and SST blocks, crash-safe MANIFEST updates, WAL replay on recovery. |

### What xdb Implements

**Phase 1 — Core Engine:**
- Put, Get, Delete operations
- Atomic WriteBatch (multiple operations in one atomic write)
- Write-Ahead Log for crash durability
- SkipList-based MemTable with concurrent reads
- SST files with prefix-compressed data blocks, index blocks, and bloom filters
- Leveled compaction (L0 → L1 → L2 → ... → Ln)
- MANIFEST-based version tracking with crash recovery
- LRU block cache for repeated reads
- Thread-safe `Arc<Db>` handle

**Phase 2 — Production Hardening:**
- Table cache (caches open SST readers to avoid re-opening files)
- Snapshots / MVCC (point-in-time consistent reads)
- Streaming DB iterator for range scans (`iter()`, `iter_with_snapshot()`)
- Compression (LZ4 and Zstd via optional feature flags)
- Background compaction (runs in a separate thread, releases lock during I/O)
- Statistics counters (atomic, lock-free observability)

**Phase 3 — Advanced Features:**
- Reverse iteration (`seek_to_last()`, `prev()` on all iterators)
- Range deletes (`delete_range()` with tombstone filtering in reads and compaction)
- Rate limiter (token bucket algorithm for background I/O throttling)
- Integration tests (23 end-to-end tests: CRUD, persistence, recovery, compaction, snapshots, concurrency)
- Benchmarks (Criterion: sequential/random writes, reads, scans, mixed workloads)

---

## Architecture

```
                          ┌──────────────────────────────┐
                          │         User Code             │
                          │  db.put(k,v)  db.get(k)      │
                          └──────────┬───────────────┬────┘
                                     │               │
                              ┌──────▼──────┐  ┌─────▼──────┐
                              │   Write     │  │    Read     │
                              │   Path      │  │    Path     │
                              └──────┬──────┘  └──┬──┬──┬───┘
                                     │            │  │  │
                    ┌────────────────▼──┐         │  │  │
                    │   Write-Ahead Log │         │  │  │
                    │   (WAL)           │         │  │  │
                    └────────────────┬──┘         │  │  │
                                     │            │  │  │
                    ┌────────────────▼──┐   ┌─────▼──┘  │
                    │   MemTable        │◄──┘            │
                    │   (SkipList)      │               │
                    └────────────────┬──┘               │
                                     │ flush            │
                    ┌────────────────▼──┐               │
                    │   Level 0 SSTs    │◄──────────────┤
                    │   (overlapping)   │               │
                    └────────────────┬──┘               │
                                     │ compaction       │
                    ┌────────────────▼──┐               │
                    │   Level 1 SSTs    │◄──────────────┤
                    │   (sorted)        │               │
                    └────────────────┬──┘               │
                                     │ compaction       │
                    ┌────────────────▼──┐               │
                    │   Level 2+ SSTs   │◄──────────────┘
                    │   (sorted)        │
                    └───────────────────┘
```

**Key insight:** Writes are always sequential (append to WAL, insert into
MemTable). Reads search from newest data (MemTable) to oldest (bottom-level
SSTs). Background compaction merges overlapping files into sorted, non-
overlapping files to keep read amplification bounded.

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
assert_eq!(db.get(b"name").unwrap(), None);

// Atomic batch writes.
let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"key3");
db.write(WriteOptions::default(), batch).unwrap();

// Range scan via iterator.
let mut iter = db.iter();
iter.seek_to_first();
while iter.valid() {
    println!("{:?} => {:?}", iter.key(), iter.value());
    iter.next();
}

// Snapshots for consistent reads.
let snap = db.snapshot();
db.put(b"key1", b"updated").unwrap();
// Iterator at the snapshot still sees the OLD value:
let mut snap_iter = db.iter_with_snapshot(&snap);
snap_iter.seek(b"key1");

// Compression (requires feature flag: --features lz4 or --features zstd).
// let opts = Options::default()
//     .create_if_missing(true)
//     .compression(CompressionType::Lz4);

// Statistics.
println!("{}", db.stats());

// Explicit flush to disk.
db.flush().unwrap();

// Graceful shutdown (also called automatically on drop).
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
| `put` | `fn put(&self, key: &[u8], value: &[u8]) -> Result<()>` | Insert a key-value pair. |
| `get` | `fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>` | Read a value. Returns `None` if not found. |
| `delete` | `fn delete(&self, key: &[u8]) -> Result<()>` | Delete a key (writes a tombstone). |
| `write` | `fn write(&self, opts: WriteOptions, batch: WriteBatch) -> Result<()>` | Apply a batch atomically. |
| `flush` | `fn flush(&self) -> Result<()>` | Force the memtable to disk. |
| `close` | `fn close(&self) -> Result<()>` | Graceful shutdown. |
| `iter` | `fn iter(&self) -> DbIterator` | Forward/backward iterator over all entries. |
| `iter_with_snapshot` | `fn iter_with_snapshot(&self, snap: &Snapshot) -> DbIterator` | Iterator at a snapshot. |
| `delete_range` | `fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()>` | Delete all keys in `[start, end)`. |
| `snapshot` | `fn snapshot(&self) -> Arc<Snapshot>` | Create a point-in-time snapshot. |
| `release_snapshot` | `fn release_snapshot(&self, snap: &Snapshot)` | Release a snapshot. |
| `stats` | `fn stats(&self) -> &Statistics` | Get live statistics counters. |

### `WriteBatch`

Collects multiple mutations to be applied atomically.

| Method | Description |
|--------|-------------|
| `new()` | Create an empty batch. |
| `put(key, value)` | Add a put operation. |
| `delete(key)` | Add a delete operation. |
| `clear()` | Discard all operations. |
| `count()` | Number of operations in the batch. |

### `Options`

Builder-style configuration. See [Configuration](#configuration) for all fields.

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
| `num_levels` | 7 | Number of LSM levels. |
| `level0_compaction_trigger` | 4 | Number of L0 files before compaction starts. |
| `max_bytes_for_level_base` | 64 MiB | Target size of Level 1. |
| `max_bytes_for_level_multiplier` | 10.0 | Each level is this many times larger than the previous. |
| `target_file_size_base` | 2 MiB | Target SST file size. |
| `target_file_size_multiplier` | 1 | File size multiplier per level. |
| `block_size` | 4 KiB | Approximate data block size in SST files. |
| `block_restart_interval` | 16 | Keys between prefix compression restarts. |
| `bloom_bits_per_key` | 10 | Bloom filter bits per key (0 = disabled). ~1% FPR at 10. |
| `compression` | `None` | Compression for data blocks (`None` only in Phase 1). |
| `block_cache_capacity` | 8 MiB | Block cache size (0 = disabled). |
| `max_background_compactions` | 1 | Concurrent compaction threads (Phase 2). |
| `max_background_flushes` | 1 | Concurrent flush threads (Phase 2). |
| `sync_writes` | `false` | Fsync the WAL on every write. |

### Tuning Guidelines

**Write-heavy workloads:** Increase `write_buffer_size` (e.g., 64–256 MiB)
to reduce flush frequency. Consider disabling `sync_writes` if you can
tolerate losing the last few writes on a crash.

**Read-heavy / point lookup:** Increase `block_cache_capacity` and ensure
`bloom_bits_per_key >= 10`. This avoids disk I/O for most reads.

**Large databases:** The default 7 levels with 10x multiplier supports up
to ~6.4 TiB before the last level is needed. Increase `num_levels` or
`max_bytes_for_level_base` for larger datasets.

---

## Internal Design

### LSM Tree Structure

```
┌─────────────────────────────────────────────┐
│  MemTable (in-memory, ~4 MiB)               │  ← active writes
├─────────────────────────────────────────────┤
│  Immutable MemTable (being flushed)         │  ← read-only
├─────────────────────────────────────────────┤
│  Level 0:  4 SST files (may overlap)        │  ← recently flushed
├─────────────────────────────────────────────┤
│  Level 1:  ~64 MiB  (sorted, non-overlap)   │
├─────────────────────────────────────────────┤
│  Level 2:  ~640 MiB (sorted, non-overlap)   │
├─────────────────────────────────────────────┤
│  Level 3:  ~6.4 GiB                         │
├─────────────────────────────────────────────┤
│  ...                                         │
├─────────────────────────────────────────────┤
│  Level 6:  ~6.4 TiB                         │
└─────────────────────────────────────────────┘
```

**Level 0** is special: files may have overlapping key ranges (they're
direct memtable dumps). All subsequent levels are sorted and non-overlapping
within each level.

The **compaction score** for each level determines when to compact:
- L0: `score = num_files / level0_compaction_trigger`
- L1+: `score = level_total_bytes / max_bytes_for_level`

When any score >= 1.0, compaction is triggered.

### Write Path

```
db.put("key", "value")
  │
  ▼
1. Acquire state mutex
  │
  ▼
2. make_room_for_write()
   ├─ If memtable size < write_buffer_size → continue
   └─ If memtable full:
      ├─ Swap memtable → immutable
      ├─ Create new WAL + memtable
      └─ Flush immutable to L0 SST (synchronous in Phase 1)
  │
  ▼
3. Assign sequence number (monotonically increasing)
  │
  ▼
4. Write batch data to WAL (append, then optionally fsync)
  │
  ▼
5. Apply batch to memtable (SkipList insertion)
  │
  ▼
6. Release mutex
```

**Sequence numbers** are 56-bit monotonically increasing counters. Each
operation in a WriteBatch gets its own sequence number. This ensures
consistent ordering across all operations.

**Atomicity**: A WriteBatch is written to the WAL as a single record. If
the process crashes mid-write, the WAL reader will either see the full
batch or nothing (partial records fail CRC checks).

### Read Path

```
db.get("key")
  │
  ▼
1. Snapshot state (memtable, imm, version, sequence)
   — no lock held during I/O
  │
  ▼
2. Search active MemTable (newest data)
   ├─ Found Value → return it
   ├─ Found Deletion → return None
   └─ Not found → continue
  │
  ▼
3. Search immutable MemTable (if any)
   └─ Same as above
  │
  ▼
4. Search Level 0 SSTs (all files, newest first)
   For each file:
   ├─ Check key range (skip if outside)
   ├─ Check Bloom filter (skip if definitely absent)
   └─ Read data block, search for key
  │
  ▼
5. Search Level 1+ SSTs (binary search within each level)
   For each level:
   ├─ Binary search to find the one file containing the key
   ├─ Check Bloom filter
   └─ Read data block
  │
  ▼
6. Return None if not found at any level
```

**Bloom filters** are the key optimization for reads. With 10 bits per key
(~1% false positive rate), a miss can skip an entire SST file without any
disk I/O. This reduces the read amplification from O(num_files) to roughly
O(num_levels).

### Flush Pipeline

When the active memtable exceeds `write_buffer_size`:

1. **Swap**: The active memtable becomes immutable; a new empty memtable and
   WAL are created. Writes continue to the new memtable immediately.

2. **Build SST**: A `TableBuilder` iterates the immutable memtable in sorted
   order, producing data blocks (with prefix compression), a bloom filter,
   an index block, and a footer.

3. **Version update**: A `VersionEdit` records the new file at Level 0.
   This is written to the MANIFEST and the current Version is atomically
   replaced.

4. **Cleanup**: The old WAL file is deleted and the immutable memtable is
   released.

### Compaction

**Leveled compaction** merges files to control read amplification:

```
TRIGGER: Level score >= 1.0
  │
  ▼
1. Pick the level with the highest score
  │
  ▼
2. Select input files:
   ├─ L0: ALL L0 files (they overlap with each other)
   └─ L1+: First file in the level
  │
  ▼
3. Find overlapping files in level+1
  │
  ▼
4. Create MergingIterator over all input files
  │
  ▼
5. Merge-sort entries, writing output files:
   ├─ Skip duplicate user keys (keep newest)
   ├─ Skip deletion tombstones at bottom level
   └─ Split output into target_file_size chunks
  │
  ▼
6. Apply VersionEdit:
   ├─ Delete input files from levels N and N+1
   └─ Add output files to level N+1
  │
  ▼
7. Delete old input SST files from disk
```

The **MergingIterator** takes multiple sorted iterators (one per input file)
and yields entries in global sorted order. When the same user key appears in
multiple files, the entry from the lower-level (newer) iterator wins.

### Recovery

On `Db::open()`:

1. **Read CURRENT** file to find the active MANIFEST filename.
2. **Replay MANIFEST**: Read all `VersionEdit` records to reconstruct the
   LSM tree structure (which files at which levels).
3. **Find WAL files** with numbers >= the logged log number.
4. **Replay WAL**: Parse each record as a WriteBatch, apply operations to a
   temporary MemTable.
5. **Flush recovery MemTable** to a new L0 SST.
6. **Clean up**: Delete replayed WAL files, create a fresh WAL.

The MANIFEST and WAL both use the same CRC32-protected log format, ensuring
that partial writes from crashes are detected and truncated safely.

---

## File Formats

### Write-Ahead Log (WAL)

Uses the LevelDB/RocksDB physical log format. The log file is divided into
fixed 32 KiB blocks.

```
┌──────────────────────── Block (32768 bytes) ────────────────────────┐
│                                                                      │
│  ┌─ Record ──────────────────────────────────────────────┐           │
│  │  checksum  : u32 LE  (CRC32 of type + data)          │           │
│  │  length    : u16 LE  (data payload bytes)             │           │
│  │  type      : u8      (FULL=1, FIRST=2, MIDDLE=3,     │           │
│  │                        LAST=4)                        │           │
│  │  data      : [length bytes]                           │           │
│  └───────────────────────────────────────────────────────┘           │
│                                                                      │
│  ┌─ Record ──────────────────────────────────────────────┐           │
│  │  ...                                                  │           │
│  └───────────────────────────────────────────────────────┘           │
│                                                                      │
│  [zero padding if < 7 bytes remain]                                  │
└──────────────────────────────────────────────────────────────────────┘
```

**Fragmentation**: A logical record larger than the remaining block space is
split across multiple physical records:
- `FIRST` — start of a multi-block record
- `MIDDLE` — continuation
- `LAST` — final fragment

Each physical record is independently checksummed, so corruption in any
fragment is detected.

### SST File Format

The Sorted String Table is the on-disk format for immutable sorted key-value
data.

```
┌──────────────────────────────────────────────┐
│  Data Block 0                                 │
│  ┌──────────────────────────────────────────┐ │
│  │  [entries with prefix compression]        │ │
│  │  [restart offsets array]                  │ │
│  │  [num_restarts: u32 LE]                   │ │
│  └──────────────────────────────────────────┘ │
│  Block Trailer (5 bytes):                     │
│    compression_type: u8                       │
│    checksum: u32 LE (CRC32)                   │
├───────────────────────────────────────────────┤
│  Data Block 1 + trailer                       │
├───────────────────────────────────────────────┤
│  ...                                          │
├───────────────────────────────────────────────┤
│  Data Block N + trailer                       │
├───────────────────────────────────────────────┤
│  Filter Block + trailer                       │
│  (Bloom filter for all keys)                  │
├───────────────────────────────────────────────┤
│  Meta-Index Block + trailer                   │
│  (maps "filter.bloom" → filter BlockHandle)   │
├───────────────────────────────────────────────┤
│  Index Block + trailer                        │
│  (maps last_key → data BlockHandle)           │
├───────────────────────────────────────────────┤
│  Footer (48 bytes)                            │
│  ┌──────────────────────────────────────────┐ │
│  │  metaindex_handle : BlockHandle (varint)  │ │
│  │  index_handle     : BlockHandle (varint)  │ │
│  │  [zero padding]                           │ │
│  │  magic_number     : u64 LE                │ │
│  │    = 0x7864627373743031 ("xdbsst01")      │ │
│  └──────────────────────────────────────────┘ │
└───────────────────────────────────────────────┘
```

#### Data Block Entry Format

```
shared_key_length   : varint32  (bytes shared with previous key)
unshared_key_length : varint32  (bytes NOT shared)
value_length        : varint32
key_delta           : [unshared_key_length bytes]
value               : [value_length bytes]
```

At every `block_restart_interval` entries (default 16), a **restart point**
is recorded and `shared_key_length` is reset to 0. This allows binary search
within a block by jumping to restart points.

#### BlockHandle

A pointer to a block within the file:
```
offset : varint64
size   : varint64
```

### MANIFEST

The MANIFEST file records the history of LSM tree structural changes using
the same log format as the WAL. Each record is an encoded `VersionEdit`:

```
Tag 1: comparator_name  (length-prefixed string)
Tag 2: log_number       (varint64)
Tag 3: next_file_number (varint64)
Tag 4: last_sequence    (varint64)
Tag 5: deleted_file     (level: varint32, file_number: varint64)
Tag 6: new_file         (level: varint32, file_number: varint64,
                          file_size: varint64,
                          smallest_key: length-prefixed,
                          largest_key: length-prefixed)
```

The `CURRENT` file contains the name of the active MANIFEST file (e.g.,
`MANIFEST-000001\n`).

### On-Disk Layout

```
/path/to/db/
├── CURRENT           # Points to active MANIFEST (e.g., "MANIFEST-000001")
├── LOCK              # Prevents concurrent access by other processes
├── MANIFEST-000001   # Version history (VersionEdits in log format)
├── 000003.log        # Active WAL file
├── 000004.sst        # Level 0 SST file
├── 000005.sst        # Level 0 SST file
├── 000006.sst        # Level 1 SST file
└── ...
```

File numbers are globally unique and monotonically increasing. The same
counter is used for WAL files, SST files, and MANIFEST files.

---

## Core Data Structures

### Internal Key

Every key stored in xdb (memtable, SST, WAL) is an **internal key**:

```
┌──────────────────────┬────────────────────┐
│     user_key         │     tag (8 bytes)  │
│     (variable len)   │  sequence << 8 |   │
│                      │  value_type        │
└──────────────────────┴────────────────────┘
```

- **sequence**: 56-bit monotonically increasing write counter
- **value_type**: `Value = 1` (put), `Deletion = 0` (tombstone)

**Ordering**: user_key ascending, then sequence **descending** (newest
first), then value_type descending. This ensures that for the same user key,
the most recent version always sorts first.

### SkipList MemTable

The in-memory sorted data structure for the active write buffer.

**Properties:**
- Max height: 12 levels, branching factor P = 1/4
- Nodes allocated with `Box`, linked via `AtomicPtr`
- Single writer (externally synchronized), concurrent readers
- Atomic ordering: `Release` for stores during insert, `Acquire` for loads during reads
- Entries are never deleted (the entire memtable is dropped when flushed)

**Complexity:**
- Insert: O(log n) expected
- Lookup: O(log n)
- Iteration: O(1) amortized per entry

**Memory tracking**: The memtable tracks approximate memory usage via an
`AtomicUsize` counter, incremented on each insert by `key.len() + value.len()
+ overhead`.

### Bloom Filter

A space-efficient probabilistic data structure for fast negative lookups.

**Algorithm:**
- Hash function: Murmur-inspired (`h = h * 0x5bd1e995 + byte; h ^= h >> 15`)
- Double hashing: `delta = (h >> 17) | (h << 15)`, probe `h += delta` for each of k probes
- k (probes) = `bits_per_key * 0.69`, clamped to [1, 30]

**Format:**
```
[filter_bits: N bytes] [k: u8]
```
Where N = `max(1, (num_keys * bits_per_key + 7) / 8)`.

**False positive rates** (approximate):
| bits_per_key | FPR |
|-------------|-----|
| 5 | ~5% |
| 10 | ~1% |
| 15 | ~0.1% |
| 20 | ~0.01% |

### LRU Block Cache

A sharded cache for SST data blocks to avoid repeated disk reads.

**Design:**
- 16 shards, each with its own `parking_lot::Mutex`
- Cache key: `(file_number, block_offset)`
- Each shard: `HashMap<CacheKey, Node>` + `Vec<OrderEntry>` for LRU ordering
- Generation-based lazy cleanup: stale entries in the order list are skipped during eviction

**Concurrency:** Sharding reduces lock contention. A read that hits the
cache only locks one of 16 shards for a brief HashMap lookup.

### WriteBatch

An ordered collection of put/delete operations applied atomically.

**Wire format:**
```
[sequence: u64 LE] [count: u32 LE] [records...]
```

Each record:
```
[value_type: u8] [key_len: varint32] [key_bytes]
[value_len: varint32] [value_bytes]     ← (only for puts)
```

The entire batch is written to the WAL as a single record, guaranteeing
atomicity: on recovery, the batch is either fully replayed or fully skipped.

---

## Module Reference

```
xdb/src/
├── lib.rs                  # Crate root, public re-exports
├── error.rs                # Error type and Result alias
├── types.rs                # InternalKey, SequenceNumber, varint encoding,
│                           #   file naming helpers
├── options.rs              # Options, ReadOptions, WriteOptions
├── batch.rs                # WriteBatch and WriteBatchHandler trait
│
├── memtable/
│   ├── mod.rs              # Re-exports MemTable, MemTableIterator
│   └── skiplist.rs         # SkipList and MemTable implementation
│
├── wal/
│   ├── mod.rs              # Re-exports WalWriter, WalReader
│   ├── writer.rs           # WAL append with block-based fragmentation
│   └── reader.rs           # WAL read-back with CRC verification
│
├── sst/
│   ├── mod.rs              # Re-exports all SST types
│   ├── block.rs            # BlockBuilder (prefix compression),
│   │                       #   BlockReader, BlockIterator
│   ├── bloom.rs            # Bloom filter build and query
│   ├── footer.rs           # BlockHandle and Footer (48-byte SST trailer)
│   ├── table_builder.rs    # Writes complete SST files
│   └── table_reader.rs     # Reads SST files (point lookup + full scan)
│
├── cache/
│   ├── mod.rs              # Re-exports LruCache
│   └── lru.rs              # 16-shard LRU cache
│
├── iterator/
│   ├── mod.rs              # XdbIterator trait definition
│   ├── merge.rs            # MergingIterator (k-way merge)
│   └── two_level.rs        # Placeholder for Phase 2
│
├── version/
│   ├── mod.rs              # Version, VersionSet
│   ├── edit.rs             # VersionEdit, FileMetaData
│   └── manifest.rs         # ManifestWriter, ManifestReader
│
├── compaction/
│   ├── mod.rs              # Re-exports
│   └── leveled.rs          # Leveled compaction (pick files + merge)
│
└── db.rs                   # Db struct — the main coordinator
```

### Lines of Code by Module

| Module | Lines | Purpose |
|--------|-------|---------|
| `db.rs` | 922 | Main coordinator (table cache, snapshots, bg compaction) |
| `sst/` | 1,810 | SST file format + compression + streaming iterators |
| `version/` | 859 | Version management |
| `memtable/` | 553 | SkipList memtable |
| `types.rs` | 461 | Core types and encoding |
| `cache/` | 449 | LRU block cache |
| `wal/` | 503 | Write-ahead log |
| `iterator/` | 411 | Iterator abstractions |
| `batch.rs` | 349 | WriteBatch |
| `compaction/` | 276 | Leveled compaction |
| `options.rs` | 225 | Configuration |
| `table_cache.rs` | 175 | SST reader cache |
| `db_iter.rs` | 210 | User-facing DB iterator |
| `snapshot.rs` | 135 | Snapshot management |
| `stats.rs` | 145 | Atomic statistics counters |
| Other | ~350 | error.rs, lib.rs, mod.rs files |
| **Total** | **~8,400** | |

---

## Comparison with RocksDB

| Aspect | RocksDB | xdb |
|--------|---------|-----|
| Language | C++ | Rust |
| Lines of code | ~415,000 | ~6,800 |
| Compaction strategies | Leveled, Universal, FIFO | Leveled only |
| Compression | 7 algorithms | None (Phase 1) |
| Column families | Yes | No |
| Transactions | Full ACID (OCC + Pessimistic) | No |
| Merge operators | Yes (3 API versions) | No |
| BlobDB / wide columns | Yes | No |
| Bloom filters | Bloom + Ribbon | Bloom only |
| Block cache | LRU + HyperClockCache | Sharded LRU |
| Memory allocator | Arena + jemalloc | Box + std allocator |
| Platform abstraction | 24K lines (env/port/file) | std::fs (zero lines) |
| Backup / Checkpoint | Yes | No |
| Range deletes | Yes | No |
| User-defined timestamps | Yes | No |

xdb covers the core 90% of functionality that most applications need, in
1.6% of the code.

---

## Future Work (Phase 4+)

### Phase 4 — Ecosystem
- **C FFI**: For embedding in non-Rust projects
- **Python bindings**: Via PyO3
- **Benchmarks**: Criterion-based benchmarks vs RocksDB
- **Fuzzing**: Fuzz the WAL reader, SST reader, and VersionEdit decoder

---

## Building and Testing

### Requirements

- Rust 1.70+ (2021 edition)
- No C/C++ toolchain needed

### Build

```bash
cd xdb/xdb
cargo build
cargo build --release
```

### Test

```bash
cargo test
```

Runs 84 unit tests + 2 doc-tests covering:
- Varint encoding roundtrips
- Internal key ordering
- SkipList insert/get/iterate
- WriteBatch serialization
- WAL write/read with CRC verification
- WAL multi-block record spanning
- Block prefix compression
- Bloom filter false positive rates
- SST file build/read/point-lookup
- LRU cache eviction behavior
- MergingIterator sorted merge
- VersionEdit encode/decode

### Code Quality

```bash
cargo clippy          # Lint
cargo fmt --check     # Format check
cargo doc --open      # Generate and view documentation
```
