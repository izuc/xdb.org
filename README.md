# xdb

**A lightweight, fast embedded key-value store inspired by RocksDB, written in pure Rust.**

<!-- Badges placeholder -->
<!-- [![Crates.io](https://img.shields.io/crates/v/xdb.svg)](https://crates.io/crates/xdb) -->
<!-- [![docs.rs](https://docs.rs/xdb/badge.svg)](https://docs.rs/xdb) -->
<!-- [![CI](https://github.com/your-org/xdb/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/xdb/actions) -->
<!-- [![License](https://img.shields.io/crates/l/xdb.svg)](LICENSE) -->

## Overview

xdb is a from-scratch Rust reimplementation of the core ideas behind
[RocksDB](https://rocksdb.org/) -- an LSM-tree-based key-value store. Where
RocksDB is roughly 415,000 lines of C++, xdb distils the essential engine into
approximately 8,800 lines of idiomatic, safe Rust. It embeds directly in any
Rust application with zero FFI overhead.

## Features

- **Put, Get, Delete** -- basic key-value operations
- **WriteBatch** -- atomic multi-key writes
- **Write-Ahead Log** -- crash durability with CRC32 checksums
- **SkipList MemTable** -- concurrent reads via atomic operations
- **SST files** -- prefix-compressed data blocks, index blocks, and bloom filters
- **Leveled compaction** -- background merge from L0 through Ln
- **LRU block cache** -- configurable cache for repeated reads
- **Snapshots / MVCC** -- point-in-time consistent reads
- **Streaming iterators** -- forward and reverse range scans
- **Range deletes** -- `delete_range()` with tombstone filtering
- **Compression** -- optional LZ4 and Zstd via feature flags
- **Rate limiter** -- token-bucket I/O throttling for background work
- **Statistics** -- lock-free atomic counters for observability
- **Thread-safe** -- `Arc<Db>` handle safe to share across threads

## Quick Start

```rust
use xdb::{Db, Options, WriteBatch, WriteOptions};

// Open or create a database.
let opts = Options::default()
    .create_if_missing(true)
    .write_buffer_size(64 * 1024 * 1024); // 64 MiB memtable
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
let mut snap_iter = db.iter_with_snapshot(&snap);
snap_iter.seek(b"key1"); // still sees the old value
```

## Installation

```sh
cargo add xdb
```

With optional compression: `cargo add xdb --features lz4`, `--features zstd`,
or `--features compression` (both).

## Performance

Criterion-based benchmarks cover sequential writes, random writes, point reads,
range scans, and mixed workloads.

```sh
cargo bench -p xdb
```

Benchmark reports are generated in `target/criterion/`.

## Configuration

xdb is configured through the `Options` struct using a builder pattern:

```rust
let opts = Options::default()
    .create_if_missing(true)
    .write_buffer_size(64 * 1024 * 1024)
    .max_levels(7)
    .level0_compaction_trigger(4)
    .bloom_bits_per_key(10);
```

See [DOC.md](DOC.md) for the full list of configuration options and their
descriptions.

## Architecture

xdb uses a Log-Structured Merge-tree (LSM-tree). Writes append to a
write-ahead log and insert into an in-memory SkipList. When the memtable is
full it flushes to an L0 SST file. Background compaction merges overlapping
SST files into sorted, non-overlapping files at deeper levels.

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

Writes are always sequential. Reads search from newest data (MemTable) to
oldest (bottom-level SSTs). See [DOC.md](DOC.md) for the full internal design
documentation.

## Building

```sh
# Build
cargo build -p xdb

# Run tests
cargo test -p xdb

# Run benchmarks
cargo bench -p xdb

# Build with compression
cargo build -p xdb --features compression
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
