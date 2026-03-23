//! # xdb
//!
//! A lightweight, fast embedded key-value store inspired by RocksDB, written
//! in pure Rust.
//!
//! xdb implements a Log-Structured Merge-tree (LSM-tree) with:
//! - Write-ahead log for durability
//! - SkipList memtable for fast in-memory writes
//! - Sorted String Table (SST) files with bloom filters
//! - Leveled compaction
//! - LRU block cache
//!
//! ## Quick start
//!
//! ```no_run
//! use xdb::{Db, Options};
//!
//! let opts = Options::default().create_if_missing(true);
//! let db = Db::open(opts, "/tmp/my_xdb").unwrap();
//!
//! db.put(b"hello", b"world").unwrap();
//! assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
//! db.delete(b"hello").unwrap();
//! ```

pub mod error;
pub mod types;
pub mod options;
pub mod batch;
pub mod memtable;
pub mod wal;
pub mod sst;
pub mod cache;
pub mod iterator;
pub mod version;
pub mod table_cache;
pub mod snapshot;
pub mod db_iter;
pub mod stats;
pub mod compaction;
pub mod db;

// Re-export the main public API at crate root.
pub use db::Db;
pub use error::{Error, Result};
pub use options::{Options, ReadOptions, WriteOptions};
pub use batch::WriteBatch;
pub use snapshot::Snapshot;
pub use db_iter::DbIterator;
pub use stats::Statistics;
