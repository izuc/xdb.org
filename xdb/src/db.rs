//! Main database coordinator.
//!
//! The `Db` struct is the primary entry point for interacting with xdb.
//! It coordinates the memtable, WAL, SST files, version management, and
//! compaction into a single thread-safe handle.
//!
//! ## Thread safety
//!
//! `Db` is wrapped in `Arc` and all public methods take `&self`. Internal
//! mutable state is protected by a `parking_lot::Mutex`. Multiple threads
//! can safely read and write concurrently.
//!
//! ## Phase 2 features
//!
//! - **Table cache**: SST readers are cached to avoid re-opening files.
//! - **Snapshots**: Point-in-time consistent reads via `snapshot()`.
//! - **DB iterator**: Range scans via `iter()` / `iter_from()`.
//! - **Statistics**: Atomic counters for observability via `stats()`.
//! - **Background compaction**: Compaction runs in a background thread,
//!   releasing the lock during I/O so writes can proceed concurrently.

use crate::batch::{WriteBatch, WriteBatchHandler};
use crate::db_iter::DbIterator;
use crate::error::{Error, Result};
use crate::iterator::{MergingIterator, XdbIterator};
use crate::memtable::MemTable;
use crate::options::{Options, WalRecoveryMode, WriteOptions};
use crate::snapshot::{Snapshot, SnapshotList};
use crate::sst::TableBuilder;
use crate::stats::Statistics;
use crate::table_cache::TableCache;
use crate::types::*;
use crate::version::edit::{FileMetaData, VersionEdit};
use crate::version::{Version, VersionSet};
use crate::wal::WalWriter;

use crossbeam_channel::Sender;
use parking_lot::{Mutex, RwLock};
use fs4::fs_std::FileExt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Weak};
use std::thread;

// ---------------------------------------------------------------------------
// Background work types
// ---------------------------------------------------------------------------

enum BgWork {
    Flush,
    MaybeCompact,
    Shutdown,
}

// ---------------------------------------------------------------------------
// Db
// ---------------------------------------------------------------------------

/// The main database handle.
///
/// Thread-safe: can be shared across threads via `Arc<Db>`. All public
/// methods take `&self`. A single LOCK file prevents multiple processes
/// from opening the same database concurrently.
pub struct Db {
    dbname: PathBuf,
    options: Arc<Options>,
    state: RwLock<DbState>,
    table_cache: Arc<TableCache>,
    snapshots: SnapshotList,
    stats: Arc<Statistics>,
    shutting_down: AtomicBool,
    bg_sender: Sender<BgWork>,
    _bg_handle: Mutex<Option<thread::JoinHandle<()>>>,
    _lock_file: fs::File,
}

struct DbState {
    mem: Arc<MemTable>,
    imm: Option<Arc<MemTable>>,
    wal: Option<WalWriter>,
    versions: VersionSet,
    bg_compaction_scheduled: bool,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

impl Db {
    /// Open a database at the given path.
    pub fn open(options: Options, path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let dbname = path.as_ref().to_path_buf();

        // Validate options.
        if options.num_levels < 2 {
            return Err(Error::invalid_argument(
                "num_levels must be at least 2 (L0 + one target level)",
            ));
        }

        // Check if the database already exists.
        let db_exists = dbname.join(CURRENT_FILE_NAME).exists();

        if options.error_if_exists && db_exists {
            return Err(Error::invalid_argument(format!(
                "database {:?} already exists and error_if_exists is true",
                dbname
            )));
        }
        if options.create_if_missing {
            fs::create_dir_all(&dbname)?;
        }
        if !dbname.exists() {
            return Err(Error::invalid_argument(format!(
                "database directory {:?} does not exist and create_if_missing is false",
                dbname
            )));
        }

        let lock_path = dbname.join(LOCK_FILE_NAME);
        let lock_file = fs::File::create(&lock_path)?;
        lock_file.try_lock_exclusive().map_err(|e| {
            Error::Io(std::io::Error::other(
                format!("database {:?} is locked by another process: {}", dbname, e),
            ))
        })?;

        let opts = Arc::new(options);

        // Table cache for SST readers.
        let table_cache = Arc::new(TableCache::new(
            &dbname,
            Arc::clone(&opts),
            opts.max_open_files,
        ));

        let stats = Arc::new(Statistics::new());

        // Version set + recovery.
        let mut versions = VersionSet::new(&dbname, (*opts).clone());
        let recovered = versions.recover()?;

        if !recovered {
            let mut edit = VersionEdit::new();
            edit.set_comparator_name("leveldb.BytewiseComparator".to_string());
            edit.set_log_number(0);
            edit.set_next_file_number(2);
            edit.set_last_sequence(0);
            versions.log_and_apply(edit)?;
        }

        // WAL recovery.
        let logged_log_number = versions.log_number();
        let wal_files = Self::find_wal_files(&dbname, logged_log_number)?;

        if !wal_files.is_empty() {
            let recovery_mem = MemTable::new();
            let mut max_sequence = versions.last_sequence();

            for (_num, wal_path) in &wal_files {
                Self::replay_wal(wal_path, &recovery_mem, &mut max_sequence, opts.wal_recovery_mode)?;
            }
            versions.set_last_sequence(max_sequence);

            if !recovery_mem.is_empty() {
                Self::flush_memtable_to_l0(
                    &dbname,
                    &recovery_mem,
                    &mut versions,
                    &opts,
                )?;
            }

            for (_num, wal_path) in &wal_files {
                if let Err(e) = fs::remove_file(wal_path) { log::warn!("failed to delete file: {}", e); }
            }
        }

        // Fresh WAL + memtable.
        let wal_number = versions.new_file_number();
        let wal_path = dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;
        versions.set_log_number(wal_number);

        let (bg_sender, bg_receiver) = crossbeam_channel::unbounded();

        let db = Arc::new(Db {
            dbname,
            options: opts,
            state: RwLock::new(DbState {
                mem: Arc::new(MemTable::new()),
                imm: None,
                wal: Some(WalWriter::new(wal_file)),
                versions,
                bg_compaction_scheduled: false,
            }),
            table_cache,
            snapshots: SnapshotList::new(),
            stats,
            shutting_down: AtomicBool::new(false),
            bg_sender,
            _bg_handle: Mutex::new(None),
            _lock_file: lock_file,
        });

        // Spawn background thread for compaction and flush.
        let weak = Arc::downgrade(&db);
        let thread_name = format!("xdb-bg-{}", db.dbname.display());
        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || Self::background_worker(weak, bg_receiver))
            .map_err(Error::Io)?;
        *db._bg_handle.lock() = Some(handle);

        Ok(db)
    }

    /// Destroy a database by removing all files in its directory.
    ///
    /// The database must NOT be open. This permanently deletes all data.
    pub fn destroy(path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    /// Insert a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(WriteOptions::default(), batch)
    }

    /// Check if a key might exist without reading its value.
    ///
    /// Uses bloom filters for a fast negative: returns `false` if the key
    /// is **definitely not** in any SST file. Returns `true` if the key
    /// **might** exist (could be a false positive from the bloom filter,
    /// or the key is in the memtable).
    ///
    /// This is cheaper than `get()` because it skips reading the data block.
    /// For definitive existence checks, use `get()`.
    pub fn key_may_exist(&self, key: &[u8]) -> bool {
        let (mem, imm, version, sequence) = {
            let state = self.state.read();
            (
                Arc::clone(&state.mem),
                state.imm.as_ref().map(Arc::clone),
                state.versions.current(),
                state.versions.last_sequence(),
            )
        };

        // Check memtable.
        let lookup = LookupKey::new(key, sequence);
        if mem.get(&lookup).is_some() {
            return true;
        }
        if let Some(ref imm) = imm {
            if imm.get(&lookup).is_some() {
                return true;
            }
        }

        // Check SST files via bloom filters only.
        for level in 0..version.num_levels {
            for file_meta in version.files_at_level(level) {
                if let Ok(reader) = self.table_cache.get_reader(
                    file_meta.number,
                    file_meta.file_size,
                ) {
                    if reader.bloom_may_contain(key) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(WriteOptions::default(), batch)
    }

    /// Check if a key exists in the database.
    ///
    /// Unlike `key_may_exist()` (which uses bloom filters and may return
    /// false positives), this performs a full lookup and returns an exact
    /// answer. Unlike `get()`, it avoids copying the value bytes when the
    /// caller only needs to know whether the key is present.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        self.get(key).map(|v| v.is_some())
    }

    /// Delete all keys in the range `[start_key, end_key)`.
    ///
    /// The start key is inclusive, the end key is exclusive. This writes a
    /// range tombstone that efficiently covers the entire range without
    /// needing to enumerate individual keys.
    pub fn delete_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete_range(start_key, end_key);
        self.write(WriteOptions::default(), batch)
    }

    /// Read a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.shutting_down.load(AtomicOrdering::Acquire) {
            return Err(Error::ShutdownInProgress);
        }
        Statistics::record(&self.stats.reads, 1);

        let (mem, imm, version, sequence) = {
            let state = self.state.read();
            (
                Arc::clone(&state.mem),
                state.imm.as_ref().map(Arc::clone),
                state.versions.current(),
                state.versions.last_sequence(),
            )
        };

        let mem_has_tombstones = mem.has_range_tombstones()
            || imm.as_ref().is_some_and(|i| i.has_range_tombstones());

        self.get_with_state(key, sequence, &mem, imm.as_deref(), &version, mem_has_tombstones)
    }

    /// Look up multiple keys in a single call.
    ///
    /// More efficient than calling `get()` for each key because the state
    /// snapshot and range tombstone collection are shared across all lookups.
    pub fn multi_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Vec<u8>>>> {
        if self.shutting_down.load(AtomicOrdering::Acquire) {
            return keys.iter().map(|_| Err(Error::ShutdownInProgress)).collect();
        }

        let (mem, imm, version, sequence) = {
            let state = self.state.read();
            (
                Arc::clone(&state.mem),
                state.imm.as_ref().map(Arc::clone),
                state.versions.current(),
                state.versions.last_sequence(),
            )
        };

        let mem_has_tombstones = mem.has_range_tombstones()
            || imm.as_ref().is_some_and(|i| i.has_range_tombstones());

        keys.iter()
            .map(|key| {
                self.get_with_state(key, sequence, &mem, imm.as_deref(), &version, mem_has_tombstones)
            })
            .collect()
    }

    /// Core lookup logic shared by `get()` and `multi_get()`.
    ///
    /// Searches the memtable, immutable memtable, and SST files for the given
    /// key at the specified sequence number.
    fn get_with_state(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
        mem: &MemTable,
        imm: Option<&MemTable>,
        version: &Version,
        mem_has_tombstones: bool,
    ) -> Result<Option<Vec<u8>>> {
        let lookup = LookupKey::new(key, sequence);

        // 1. Active memtable.
        if let Some(result) = mem.get(&lookup) {
            Statistics::record(&self.stats.memtable_hits, 1);
            return match result {
                Ok((value, value_seq)) => {
                    if mem_has_tombstones {
                        let range_tombstones = self.collect_range_tombstones_for_get(
                            key, mem, imm, version, sequence,
                        );
                        if Self::is_key_range_deleted(key, value_seq, &range_tombstones) {
                            return Ok(None);
                        }
                    }
                    Statistics::record(&self.stats.bytes_read, value.len() as u64);
                    Ok(Some(value))
                }
                Err(Error::NotFound(_)) => Ok(None),
                Err(e) => Err(e),
            };
        }

        // 2. Immutable memtable.
        if let Some(imm_table) = imm {
            if let Some(result) = imm_table.get(&lookup) {
                Statistics::record(&self.stats.memtable_hits, 1);
                return match result {
                    Ok((value, value_seq)) => {
                        if mem_has_tombstones {
                            let range_tombstones = self.collect_range_tombstones_for_get(
                                key, mem, imm, version, sequence,
                            );
                            if Self::is_key_range_deleted(key, value_seq, &range_tombstones) {
                                return Ok(None);
                            }
                        }
                        Statistics::record(&self.stats.bytes_read, value.len() as u64);
                        Ok(Some(value))
                    }
                    Err(Error::NotFound(_)) => Ok(None),
                    Err(e) => Err(e),
                };
            }
        }
        Statistics::record(&self.stats.memtable_misses, 1);

        // 3. SST files level by level.
        // L0: all files, newest first.
        for file_meta in version.files_at_level(0) {
            let file_smallest_user = file_meta.smallest_key.user_key();
            let file_largest_user = file_meta.largest_key.user_key();
            if key < file_smallest_user || key > file_largest_user {
                continue;
            }
            match self.search_sst_file(file_meta, key, sequence)? {
                SearchResult::Found(value, value_seq) => {
                    if self.check_range_tombstones_for_found_key(
                        key, value_seq, mem_has_tombstones, mem, imm,
                        version, sequence,
                    )? {
                        return Ok(None);
                    }
                    return Ok(Some(value));
                }
                SearchResult::Deleted => return Ok(None),
                SearchResult::NotFound => continue,
            }
        }

        // L1+: binary search within each level.
        for level in 1..version.num_levels {
            let files = version.files_at_level(level);
            if files.is_empty() {
                continue;
            }
            let idx = files.partition_point(|f| f.largest_key.user_key() < key);
            if idx >= files.len() {
                continue;
            }
            let file_meta = &files[idx];
            if key < file_meta.smallest_key.user_key() {
                continue;
            }
            match self.search_sst_file(file_meta, key, sequence)? {
                SearchResult::Found(value, value_seq) => {
                    if self.check_range_tombstones_for_found_key(
                        key, value_seq, mem_has_tombstones, mem, imm,
                        version, sequence,
                    )? {
                        return Ok(None);
                    }
                    return Ok(Some(value));
                }
                SearchResult::Deleted => return Ok(None),
                SearchResult::NotFound => continue,
            }
        }

        Ok(None)
    }

    /// Check range tombstones lazily, only when a value has been found.
    ///
    /// Must check tombstones from ALL sources (memtable, imm, and ALL SST
    /// files) because a newer L0 file may contain a range tombstone that
    /// covers a value found in an older file.
    #[allow(clippy::too_many_arguments)]
    fn check_range_tombstones_for_found_key(
        &self,
        user_key: &[u8],
        value_seq: SequenceNumber,
        mem_has_tombstones: bool,
        mem: &MemTable,
        imm: Option<&MemTable>,
        version: &Version,
        snapshot_seq: SequenceNumber,
    ) -> Result<bool> {
        // Fast path: if no memtable has tombstones, check whether ANY
        // SST file has cached tombstones before doing the full scan.
        if !mem_has_tombstones {
            let mut any_sst_tombstones = false;
            for level in 0..version.num_levels {
                for file_meta in version.files_at_level(level) {
                    if let Ok(reader) = self.table_cache.get_reader(
                        file_meta.number,
                        file_meta.file_size,
                    ) {
                        if !reader.range_tombstones().is_empty() {
                            any_sst_tombstones = true;
                            break;
                        }
                    }
                }
                if any_sst_tombstones {
                    break;
                }
            }
            if !any_sst_tombstones {
                return Ok(false);
            }
        }

        // Slow path: tombstones exist somewhere, do the full collection.
        // Use snapshot_seq to find all visible tombstones, then check if
        // any of those tombstones cover the value (using value_seq).
        let range_tombstones = self.collect_range_tombstones_for_get(
            user_key, mem, imm, version, snapshot_seq,
        );
        Ok(Self::is_key_range_deleted(user_key, value_seq, &range_tombstones))
    }

    /// Apply a `WriteBatch` atomically.
    pub fn write(&self, write_opts: WriteOptions, mut batch: WriteBatch) -> Result<()> {
        if self.shutting_down.load(AtomicOrdering::Acquire) {
            return Err(Error::ShutdownInProgress);
        }

        // Empty batches are a no-op — don't write to WAL or touch sequence numbers.
        if batch.count() == 0 {
            return Ok(());
        }

        let mut state = self.state.write();
        self.make_room_for_write(&mut state)?;

        let last_seq = state.versions.last_sequence();
        let first_seq = last_seq + 1;
        batch.set_sequence(first_seq);
        state
            .versions
            .set_last_sequence(last_seq + batch.count() as u64);

        if !write_opts.disable_wal {
            if let Some(ref mut wal) = state.wal {
                wal.add_record(batch.data())?;
                if write_opts.sync || self.options.sync_writes {
                    wal.sync()?;
                }
            }
        }

        let mem = Arc::clone(&state.mem);
        let mut handler = MemTableInserter {
            mem: &mem,
            sequence: first_seq,
        };
        batch.iterate(&mut handler)?;

        Statistics::record(&self.stats.writes, 1);
        Statistics::record(&self.stats.bytes_written, batch.approximate_size() as u64);

        Ok(())
    }

    /// Force flush the current memtable to disk.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.write();

        if state.mem.is_empty() {
            return Ok(());
        }

        // Create the new WAL file BEFORE swapping memtables, so that an
        // I/O failure (e.g., "too many open files") leaves state untouched.
        let wal_number = state.versions.new_file_number();
        let wal_path = self.dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;

        let old_mem = std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
        state.imm = Some(old_mem);
        let old_wal_number = state.versions.log_number();
        state.wal = Some(WalWriter::new(wal_file));
        state.versions.set_log_number(wal_number);

        self.do_flush(&mut state)?;

        let old_wal_path = self.dbname.join(wal_file_name(old_wal_number));
        if let Err(e) = fs::remove_file(&old_wal_path) { log::warn!("failed to delete file: {}", e); }

        Ok(())
    }

    /// Create a point-in-time snapshot of the database.
    ///
    /// The snapshot can be used with `ReadOptions` to perform consistent reads.
    /// Drop or release the snapshot when no longer needed.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        let seq = self.state.read().versions.last_sequence();
        self.snapshots.create(seq)
    }

    /// Release a snapshot. After release, compaction may reclaim data that
    /// was preserved for this snapshot.
    pub fn release_snapshot(&self, snapshot: &Snapshot) {
        self.snapshots.release(snapshot);
    }

    /// Create a forward iterator over the entire database.
    ///
    /// The iterator sees a consistent snapshot at the current sequence number.
    /// Entries are yielded in user-key order with deletions hidden and
    /// duplicates resolved to the newest version.
    pub fn iter(&self) -> DbIterator {
        let seq = self.state.read().versions.last_sequence();
        self.iter_at_sequence(seq)
    }

    /// Create a forward iterator using a specific snapshot.
    pub fn iter_with_snapshot(&self, snapshot: &Snapshot) -> DbIterator {
        self.iter_at_sequence(snapshot.sequence())
    }

    /// Create an iterator that scans all keys starting with `prefix`.
    ///
    /// The iterator is positioned at the first key >= `prefix` and will
    /// automatically stop when it reaches a key that no longer starts with
    /// `prefix`. This is equivalent to calling `iter()` with `seek(prefix)`
    /// and `set_upper_bound(prefix_successor)`.
    pub fn prefix_iterator(&self, prefix: &[u8]) -> DbIterator {
        let mut iter = self.iter();
        if let Some(upper) = prefix_successor(prefix) {
            iter.set_upper_bound(upper);
        }
        iter.seek(prefix);
        iter
    }

    /// Get a reference to the database statistics.
    pub fn stats(&self) -> &Statistics {
        &self.stats
    }

    /// Query internal database statistics by property name.
    ///
    /// Returns `None` for unrecognized property names. Supported properties:
    ///
    /// - `"xdb.num-files-at-levelN"` -- number of SST files at level N
    /// - `"xdb.num-levels"` -- total number of levels
    /// - `"xdb.last-sequence"` -- current sequence number
    /// - `"xdb.num-snapshots"` -- number of active snapshots
    /// - `"xdb.num-entries"` -- approximate total entries (sum of all file counts)
    /// - `"xdb.total-sst-size"` -- total bytes across all SST files
    /// - `"xdb.mem-usage"` -- approximate memtable memory usage
    /// - `"xdb.background-errors"` -- placeholder, always `"0"`
    /// - `"xdb.level-summary"` -- one-line summary like `"L0:3 L1:10 L2:45 ..."`
    pub fn get_property(&self, name: &str) -> Option<String> {
        let state = self.state.read();
        let version = state.versions.current();

        match name {
            "xdb.num-levels" => Some(version.num_levels.to_string()),
            "xdb.last-sequence" => Some(state.versions.last_sequence().to_string()),
            "xdb.num-snapshots" => Some(self.snapshots.count().to_string()),
            "xdb.num-entries" => Some(version.total_file_count().to_string()),
            "xdb.mem-usage" => Some(state.mem.approximate_memory_usage().to_string()),
            "xdb.background-errors" => Some("0".to_string()),
            "xdb.total-sst-size" => {
                let total: u64 = (0..version.num_levels)
                    .flat_map(|l| version.files_at_level(l))
                    .map(|f| f.file_size)
                    .sum();
                Some(total.to_string())
            }
            "xdb.level-summary" => {
                let parts: Vec<String> = (0..version.num_levels)
                    .map(|l| format!("L{}:{}", l, version.files_at_level(l).len()))
                    .collect();
                Some(parts.join(" "))
            }
            _ => {
                // Check for "xdb.num-files-at-levelN"
                if let Some(level_str) = name.strip_prefix("xdb.num-files-at-level") {
                    if let Ok(level) = level_str.parse::<usize>() {
                        if level < version.num_levels {
                            return Some(version.files_at_level(level).len().to_string());
                        }
                    }
                }
                None
            }
        }
    }

    /// Manually compact a key range across all levels.
    ///
    /// If `start` and `end` are both `None`, the entire database is compacted.
    /// This is a synchronous operation that blocks until complete.
    pub fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        use crate::compaction;

        for level in 0..self.options.num_levels - 1 {
            let mut state = self.state.write();
            let version = state.versions.current();
            let files = version.files_at_level(level);

            if files.is_empty() {
                continue;
            }

            // Check if any files at this level overlap the range.
            let has_overlap = match (start, end) {
                (None, None) => true,
                (Some(s), Some(e)) => files.iter().any(|f| {
                    f.smallest_key.user_key() <= e && f.largest_key.user_key() >= s
                }),
                (Some(s), None) => files.iter().any(|f| f.largest_key.user_key() >= s),
                (None, Some(e)) => files.iter().any(|f| f.smallest_key.user_key() <= e),
            };

            if !has_overlap {
                continue;
            }

            // Build a CompactionState for this level.
            let comp_state = compaction::pick_compaction_files(&version, level, &self.options);
            if comp_state.input_files[0].is_empty() {
                continue;
            }

            // Pre-allocate file numbers for compaction output.
            let input_count: usize = comp_state.input_files.iter().map(|f| f.len()).sum();
            let prealloc = (input_count + 10).max(20);
            let mut next_file = state.versions.new_file_number();
            for _ in 0..prealloc {
                let _ = state.versions.new_file_number();
            }

            let oldest_snap = self.snapshots.oldest_sequence();

            // Release lock during I/O.
            drop(state);

            let mut comp_state = comp_state;
            let edit = compaction::compact(
                &self.dbname,
                &mut comp_state,
                &self.options,
                &mut next_file,
                oldest_snap,
            )?;

            // Re-acquire lock to apply edit.
            let mut state = self.state.write();
            while state.versions.manifest_file_number() < next_file {
                state.versions.new_file_number();
            }
            state.versions.log_and_apply(edit)?;
            drop(state);

            // Cleanup compacted input files.
            for files in &comp_state.input_files {
                for f in files {
                    self.table_cache.evict(f.number);
                    if let Err(e) = fs::remove_file(self.dbname.join(sst_file_name(f.number))) {
                        log::warn!("failed to delete compacted file: {}", e);
                    }
                }
            }
            self.sync_dir();
        }

        Ok(())
    }

    /// Graceful shutdown: flush pending data and stop background threads.
    pub fn close(&self) -> Result<()> {
        if self
            .shutting_down
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        // Signal background thread to stop.
        let _ = self.bg_sender.send(BgWork::Shutdown);

        // Wait for the background thread to finish. This blocks until the
        // current flush/compaction round completes (the shutting_down flag
        // causes the compaction loop to exit early between rounds).
        if let Some(handle) = self._bg_handle.lock().take() {
            let _ = handle.join();
        }

        let mut state = self.state.write();
        // Flush any pending immutable memtable first so it is not lost
        // when we overwrite state.imm below.
        if state.imm.is_some() {
            self.do_flush(&mut state)?;
        }
        if !state.mem.is_empty() {
            let old_mem = std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
            state.imm = Some(old_mem);
            self.do_flush(&mut state)?;
        }
        if let Some(ref mut wal) = state.wal {
            wal.sync()?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Fsync the database directory to ensure newly created files (WAL, SST)
    /// are durable in the directory listing. Without this, a crash could
    /// cause a successfully written file to vanish from the directory.
    fn sync_dir(&self) {
        // Best-effort: on some platforms (e.g., older Windows) opening a
        // directory as a File may fail. Log a warning instead of failing
        // the entire operation.
        match fs::File::open(&self.dbname).and_then(|d| d.sync_all()) {
            Ok(()) => {}
            Err(e) => log::warn!("directory fsync failed (non-fatal): {}", e),
        }
    }

    /// Build a DbIterator at a given sequence number.
    fn iter_at_sequence(&self, sequence: SequenceNumber) -> DbIterator {
        let (mem, imm, version) = {
            let state = self.state.read();
            (
                Arc::clone(&state.mem),
                state.imm.as_ref().map(Arc::clone),
                state.versions.current(),
            )
        };

        let mut children: Vec<Box<dyn XdbIterator>> = Vec::new();

        // Memtable iterators — use Arc-based owning iterators to avoid
        // copying the entire memtable into a Vec. O(1) instead of O(N).
        use crate::memtable::OwnedMemTableIterator;
        if !mem.is_empty() {
            children.push(Box::new(OwnedMemTableIterator::new(Arc::clone(&mem))));
        }
        if let Some(ref imm) = imm {
            if !imm.is_empty() {
                children.push(Box::new(OwnedMemTableIterator::new(Arc::clone(imm))));
            }
        }

        // Collect range tombstones from memtables.
        let mut range_tombstones = Vec::new();
        Self::collect_range_tombstones_from_mem(&mem, sequence, &mut range_tombstones);
        if let Some(ref imm) = imm {
            Self::collect_range_tombstones_from_mem(imm, sequence, &mut range_tombstones);
        }

        // SST file iterators + range tombstones in a single pass.
        for level in 0..version.num_levels {
            for file_meta in version.files_at_level(level) {
                if let Ok(reader) = self.table_cache.get_reader(
                    file_meta.number,
                    file_meta.file_size,
                ) {
                    // Use cached range tombstones (parsed once at open time).
                    for (start, end, tomb_seq) in reader.range_tombstones() {
                        if *tomb_seq <= sequence {
                            range_tombstones.push((
                                start.clone(),
                                end.clone(),
                                *tomb_seq,
                            ));
                        }
                    }
                    children.push(Box::new(reader.iter()));
                }
            }
        }

        let merger = MergingIterator::new(children);
        DbIterator::with_range_tombstones(Box::new(merger), sequence, range_tombstones)
    }

    /// Collect range tombstones from a memtable.
    ///
    /// Uses a fast-path flag: if the memtable has never seen a RangeDeletion
    /// entry, the scan is skipped entirely. This keeps get() O(log N) for
    /// the common case where delete_range() is not used.
    fn collect_range_tombstones_from_mem(
        mem: &MemTable,
        sequence: SequenceNumber,
        out: &mut Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>,
    ) {
        if !mem.has_range_tombstones() {
            return;
        }
        let mut iter = mem.iter();
        iter.seek_to_first();
        while iter.valid() {
            if let Some(p) = ParsedInternalKey::from_bytes(iter.key()) {
                if p.value_type == ValueType::RangeDeletion && p.sequence <= sequence {
                    out.push((p.user_key.to_vec(), iter.value().to_vec(), p.sequence));
                }
            }
            iter.next();
        }
    }

    /// Collect range tombstones relevant to a point lookup for a specific key.
    ///
    /// Uses cached tombstones from each `TableReader` (parsed once at open
    /// time) instead of scanning entire SST files. This keeps point lookups
    /// O(log N) instead of O(N).
    fn collect_range_tombstones_for_get(
        &self,
        user_key: &[u8],
        mem: &MemTable,
        imm: Option<&MemTable>,
        version: &Version,
        sequence: SequenceNumber,
    ) -> Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> {
        let mut tombstones = Vec::new();

        // Memtable range tombstones.
        Self::collect_range_tombstones_from_mem(mem, sequence, &mut tombstones);
        if let Some(imm) = imm {
            Self::collect_range_tombstones_from_mem(imm, sequence, &mut tombstones);
        }

        // SST file range tombstones — use cached tombstones parsed at open time.
        for level in 0..version.num_levels {
            for file_meta in version.files_at_level(level) {
                if let Ok(reader) = self.table_cache.get_reader(
                    file_meta.number,
                    file_meta.file_size,
                ) {
                    for (start, end, tomb_seq) in reader.range_tombstones() {
                        if *tomb_seq <= sequence
                            && start.as_slice() <= user_key
                            && user_key < end.as_slice()
                        {
                            tombstones.push((start.clone(), end.clone(), *tomb_seq));
                        }
                    }
                }
            }
        }

        tombstones
    }

    /// Check whether a user key at a given value sequence is covered by a
    /// range tombstone. A tombstone only covers values whose sequence is
    /// OLDER (lower) than the tombstone's sequence.
    fn is_key_range_deleted(
        user_key: &[u8],
        value_seq: SequenceNumber,
        tombstones: &[(Vec<u8>, Vec<u8>, SequenceNumber)],
    ) -> bool {
        for (start, end, tomb_seq) in tombstones {
            if user_key >= start.as_slice()
                && user_key < end.as_slice()
                && *tomb_seq > value_seq
            {
                return true;
            }
        }
        false
    }



    fn make_room_for_write(&self, state: &mut DbState) -> Result<()> {
        if state.mem.approximate_memory_usage() < self.options.write_buffer_size {
            return Ok(());
        }

        // If there's still an immutable memtable being flushed, we must
        // wait for it to complete before swapping in another one.
        if state.imm.is_some() {
            self.do_flush(state)?;
        }

        // Create the new WAL BEFORE swapping memtables so an I/O failure
        // (e.g., fd exhaustion) doesn't leave state inconsistent.
        let wal_number = state.versions.new_file_number();
        let wal_path = self.dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;

        let old_mem = std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
        state.imm = Some(old_mem);
        let _old_wal_number = state.versions.log_number();
        state.wal = Some(WalWriter::new(wal_file));
        state.versions.set_log_number(wal_number);

        // Schedule the flush in the background thread so the write path
        // returns quickly. The old WAL is cleaned up AFTER the flush
        // completes (inside do_flush), not here — otherwise a crash
        // before the SST is written would lose the data.
        let _ = self.bg_sender.send(BgWork::Flush);

        // Proactively trigger compaction if L0 is getting full.
        let l0_count = state.versions.current().files_at_level(0).len();
        if l0_count >= self.options.level0_compaction_trigger && !state.bg_compaction_scheduled {
            state.bg_compaction_scheduled = true;
            let _ = self.bg_sender.send(BgWork::MaybeCompact);
        }

        Ok(())
    }

    /// Flush the immutable memtable to an L0 SST file.
    ///
    /// Releases the state lock during disk I/O so concurrent reads and
    /// writes can proceed. Re-acquires the lock only to apply the
    /// VersionEdit and clean up. On failure, restores `imm` so no data
    /// is lost.
    fn do_flush(&self, state: &mut DbState) -> Result<()> {
        let imm = match state.imm.take() {
            Some(m) => m,
            None => return Ok(()),
        };

        let file_number = state.versions.new_file_number();
        let log_number = state.versions.log_number();
        let sst_path = self.dbname.join(sst_file_name(file_number));

        // --- Disk I/O (no lock needed — imm is immutable) ---------------
        let flush_result = self.write_memtable_to_sst(&imm, &sst_path);

        match flush_result {
            Ok((file_size, smallest_key, largest_key)) => {
                // Throttle flush I/O via rate limiter if configured.
                if let Some(ref limiter) = self.options.rate_limiter {
                    limiter.request(file_size as usize);
                }

                if let (true, Some(sk)) = (file_size > 0, smallest_key) {
                    let meta = FileMetaData {
                        number: file_number,
                        file_size,
                        smallest_key: InternalKey::from_bytes(sk),
                        largest_key: InternalKey::from_bytes(largest_key),
                    };
                    let mut edit = VersionEdit::new();
                    edit.set_log_number(log_number);
                    edit.set_last_sequence(state.versions.last_sequence());
                    edit.add_file(0, meta);
                    if let Err(e) = state.versions.log_and_apply(edit) {
                        if let Err(e2) = fs::remove_file(&sst_path) {
                            log::warn!("failed to delete orphaned SST: {}", e2);
                        }
                        // Restore imm so data is not lost.
                        state.imm = Some(imm);
                        return Err(e);
                    }
                } else if let Err(e) = fs::remove_file(&sst_path) {
                    log::warn!("failed to delete empty SST: {}", e);
                }

                self.sync_dir();
                Statistics::record(&self.stats.flushes, 1);

                if state.versions.pick_compaction().is_some()
                    && !state.bg_compaction_scheduled
                {
                    state.bg_compaction_scheduled = true;
                    let _ = self.bg_sender.send(BgWork::MaybeCompact);
                }

                Ok(())
            }
            Err(e) => {
                if let Err(e2) = fs::remove_file(&sst_path) {
                    log::warn!("failed to delete partial SST: {}", e2);
                }
                // Restore imm so data is not lost.
                state.imm = Some(imm);
                Err(e)
            }
        }
    }

    /// Write a memtable to an SST file. Pure I/O, no lock needed.
    fn write_memtable_to_sst(
        &self,
        mem: &MemTable,
        sst_path: &std::path::Path,
    ) -> Result<(u64, Option<Vec<u8>>, Vec<u8>)> {
        let sst_file = fs::File::create(sst_path)?;
        let mut builder = TableBuilder::new(sst_file, (*self.options).clone());

        let mut iter = mem.iter();
        iter.seek_to_first();

        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Vec<u8> = Vec::new();

        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            if smallest_key.is_none() {
                smallest_key = Some(key.to_vec());
            }
            largest_key = key.to_vec();
            builder.add(key, value)?;
            iter.next();
        }

        let file_size = builder.finish()?;
        Ok((file_size, smallest_key, largest_key))
    }

    /// Search a single SST file for a user key via the table cache.
    ///
    /// Uses `TableReader::get_for_user_key` which reads only the single
    /// data block that may contain the key, instead of materializing the
    /// entire index.
    fn search_sst_file(
        &self,
        file_meta: &FileMetaData,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<SearchResult> {
        let reader = self.table_cache.get_reader(
            file_meta.number,
            file_meta.file_size,
        )?;

        match reader.get_for_user_key(user_key, sequence)? {
            Some(crate::sst::UserKeyResult::Found(value, value_seq)) => {
                Ok(SearchResult::Found(value, value_seq))
            }
            Some(crate::sst::UserKeyResult::Deleted) => Ok(SearchResult::Deleted),
            None => Ok(SearchResult::NotFound),
        }
    }

    // -----------------------------------------------------------------------
    // Background compaction
    // -----------------------------------------------------------------------

    /// Background worker loop: receives work items and processes them.
    /// Panics in flush/compaction are caught so the thread stays alive.
    fn background_worker(weak: Weak<Db>, receiver: crossbeam_channel::Receiver<BgWork>) {
        while let Ok(work) = receiver.recv() {
            match work {
                BgWork::Shutdown => break,
                BgWork::Flush => {
                    if let Some(db) = weak.upgrade() {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            db.do_background_flush()
                        })) {
                            Ok(Err(e)) => log::error!("background flush failed: {}", e),
                            Err(_) => log::error!("background flush panicked"),
                            _ => {}
                        }
                    }
                }
                BgWork::MaybeCompact => {
                    if let Some(db) = weak.upgrade() {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            db.do_background_compaction()
                        })) {
                            Ok(Err(e)) => log::error!("background compaction failed: {}", e),
                            Err(_) => log::error!("background compaction panicked"),
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    /// Flush the immutable memtable in the background thread.
    ///
    /// Releases the state lock during disk I/O so concurrent reads
    /// and writes can proceed while the SST is being written.
    fn do_background_flush(&self) -> Result<()> {
        // Step 1: Take the immutable memtable and file info under lock.
        let (imm, file_number, log_number, sst_path) = {
            let mut state = self.state.write();
            let imm = match state.imm.take() {
                Some(m) => m,
                None => return Ok(()),
            };
            let file_number = state.versions.new_file_number();
            let log_number = state.versions.log_number();
            let sst_path = self.dbname.join(sst_file_name(file_number));
            (imm, file_number, log_number, sst_path)
        };
        // Lock released — concurrent reads/writes can proceed.

        // Step 2: Write the SST file (no lock held).
        let flush_result = self.write_memtable_to_sst(&imm, &sst_path);

        // Step 3: Re-acquire lock to apply the edit or restore imm.
        let mut state = self.state.write();

        match flush_result {
            Ok((file_size, smallest_key, largest_key)) => {
                if let Some(ref limiter) = self.options.rate_limiter {
                    limiter.request(file_size as usize);
                }

                if let (true, Some(sk)) = (file_size > 0, smallest_key) {
                    let meta = FileMetaData {
                        number: file_number,
                        file_size,
                        smallest_key: InternalKey::from_bytes(sk),
                        largest_key: InternalKey::from_bytes(largest_key),
                    };
                    let mut edit = VersionEdit::new();
                    edit.set_log_number(log_number);
                    edit.set_last_sequence(state.versions.last_sequence());
                    edit.add_file(0, meta);
                    if let Err(e) = state.versions.log_and_apply(edit) {
                        if let Err(e2) = fs::remove_file(&sst_path) {
                            log::warn!("failed to delete orphaned SST: {}", e2);
                        }
                        state.imm = Some(imm);
                        return Err(e);
                    }
                } else if let Err(e) = fs::remove_file(&sst_path) {
                    log::warn!("failed to delete empty SST: {}", e);
                }

                self.sync_dir();
                Statistics::record(&self.stats.flushes, 1);

                // Clean up old WAL files that are no longer needed.
                // After the flush, the MANIFEST records log_number as the
                // current WAL, so any older WALs are safe to delete.
                self.cleanup_old_wals(log_number);

                if state.versions.pick_compaction().is_some()
                    && !state.bg_compaction_scheduled
                {
                    state.bg_compaction_scheduled = true;
                    let _ = self.bg_sender.send(BgWork::MaybeCompact);
                }

                Ok(())
            }
            Err(e) => {
                if let Err(e2) = fs::remove_file(&sst_path) {
                    log::warn!("failed to delete partial SST: {}", e2);
                }
                state.imm = Some(imm);
                Err(e)
            }
        }
    }

    /// Delete WAL files older than the given log number.
    fn cleanup_old_wals(&self, current_log_number: FileNumber) {
        if let Ok(entries) = fs::read_dir(&self.dbname) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(num_str) = name.strip_suffix(".log") {
                    if let Ok(num) = num_str.parse::<u64>() {
                        if num < current_log_number {
                            if let Err(e) = fs::remove_file(entry.path()) {
                                log::warn!("failed to delete old WAL {}: {}", name, e);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Run compaction in the background. Releases the lock during I/O.
    fn do_background_compaction(&self) -> Result<()> {
        use crate::compaction;

        // Limit iterations to prevent infinite loops on corrupt metadata.
        const MAX_COMPACTION_ROUNDS: usize = 32;

        for _round in 0..MAX_COMPACTION_ROUNDS {
            // Abort early if the database is shutting down so close()
            // doesn't block waiting for a long-running compaction.
            if self.shutting_down.load(AtomicOrdering::Acquire) {
                break;
            }

            // Step 1: Pick compaction under lock.
            let comp_info = {
                let mut state = self.state.write();
                state.bg_compaction_scheduled = false;

                match state.versions.pick_compaction() {
                    Some((level, _score)) => {
                        let version = state.versions.current();
                        let comp = compaction::pick_compaction_files(
                            &version, level, &self.options,
                        );
                        if comp.input_files[0].is_empty() {
                            break;
                        }
                        // Pre-allocate file numbers for compaction output.
                        // Estimate: one output file per input file, plus some headroom.
                        let input_count: usize = comp.input_files.iter()
                            .map(|f| f.len())
                            .sum();
                        let prealloc = (input_count + 10).max(20);
                        let next_file = state.versions.new_file_number();
                        for _ in 0..prealloc {
                            let _ = state.versions.new_file_number();
                        }
                        Some((comp, next_file))
                    }
                    None => None,
                }
            };
            // Lock released.

            let Some((mut comp_state, mut next_file)) = comp_info else {
                break;
            };

            // Step 2: Compaction I/O — no lock held.
            let oldest_snap = self.snapshots.oldest_sequence();
            let edit = compaction::compact(
                &self.dbname,
                &mut comp_state,
                &self.options,
                &mut next_file,
                oldest_snap,
            )?;

            Statistics::record(&self.stats.compactions, 1);

            // Throttle compaction I/O via rate limiter if configured.
            if let Some(ref limiter) = self.options.rate_limiter {
                let bytes_written: u64 = comp_state.output_files.iter()
                    .map(|f| f.file_size)
                    .sum();
                limiter.request(bytes_written as usize);
            }

            // Step 3: Apply the edit under lock.
            let apply_result = {
                let mut state = self.state.write();
                while state.versions.manifest_file_number() < next_file {
                    state.versions.new_file_number();
                }
                state.versions.log_and_apply(edit)
            };

            if let Err(e) = apply_result {
                // Clean up orphaned compaction output files.
                for f in &comp_state.output_files {
                    if let Err(e) = fs::remove_file(self.dbname.join(sst_file_name(f.number))) { log::warn!("failed to delete file: {}", e); }
                }
                return Err(e);
            }

            // Step 4: Cleanup input files outside lock.
            for files in &comp_state.input_files {
                for f in files {
                    self.table_cache.evict(f.number);
                    if let Err(e) = fs::remove_file(self.dbname.join(sst_file_name(f.number))) { log::warn!("failed to delete file: {}", e); }
                }
            }

            // Fsync directory after compaction creates new files.
            self.sync_dir();

            // Loop to check if more compaction is needed.
        }

        // If we hit the round limit and there's still work, re-schedule
        // so the background thread picks it up on the next iteration.
        if !self.shutting_down.load(AtomicOrdering::Acquire) {
            let state = self.state.read();
            if state.versions.pick_compaction().is_some() {
                let _ = self.bg_sender.send(BgWork::MaybeCompact);
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Recovery helpers
    // -----------------------------------------------------------------------

    fn flush_memtable_to_l0(
        dbname: &Path,
        mem: &MemTable,
        versions: &mut VersionSet,
        options: &Options,
    ) -> Result<()> {
        let file_number = versions.new_file_number();
        let sst_path = dbname.join(sst_file_name(file_number));
        let sst_file = fs::File::create(&sst_path)?;
        let mut builder = TableBuilder::new(sst_file, options.clone());

        let mut iter = mem.iter();
        iter.seek_to_first();

        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Vec<u8> = Vec::new();

        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            if smallest_key.is_none() {
                smallest_key = Some(key.to_vec());
            }
            largest_key = key.to_vec();
            builder.add(key, value)?;
            iter.next();
        }

        let file_size = match builder.finish() {
            Ok(size) => size,
            Err(e) => {
                if let Err(e) = fs::remove_file(&sst_path) { log::warn!("failed to delete file: {}", e); }
                return Err(e);
            }
        };

        if let (true, Some(sk)) = (file_size > 0, smallest_key) {
            let meta = FileMetaData {
                number: file_number,
                file_size,
                smallest_key: InternalKey::from_bytes(sk),
                largest_key: InternalKey::from_bytes(largest_key),
            };
            let mut edit = VersionEdit::new();
            edit.set_log_number(versions.log_number());
            edit.set_last_sequence(versions.last_sequence());
            edit.add_file(0, meta);
            versions.log_and_apply(edit)?;
        } else if let Err(e) = fs::remove_file(&sst_path) { log::warn!("failed to delete file: {}", e); }

        Ok(())
    }

    fn find_wal_files(
        dbname: &Path,
        min_number: FileNumber,
    ) -> Result<Vec<(FileNumber, PathBuf)>> {
        let mut wal_files = Vec::new();
        if !dbname.exists() {
            return Ok(wal_files);
        }
        for entry in fs::read_dir(dbname)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(num_str) = name_str.strip_suffix(".log") {
                if let Ok(num) = num_str.parse::<FileNumber>() {
                    if num >= min_number && num > 0 {
                        wal_files.push((num, entry.path()));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(num, _)| *num);
        Ok(wal_files)
    }

    fn replay_wal(
        wal_path: &Path,
        mem: &MemTable,
        max_sequence: &mut SequenceNumber,
        wal_recovery_mode: WalRecoveryMode,
    ) -> Result<()> {
        let file = fs::File::open(wal_path)?;
        let mut reader = crate::wal::WalReader::new(file);

        loop {
            let data = match reader.read_record() {
                Ok(Some(data)) => data,
                Ok(None) => break, // clean EOF
                Err(e) => match wal_recovery_mode {
                    WalRecoveryMode::TolerateCorruptedTailRecords => {
                        log::warn!("ignoring corrupted WAL tail: {}", e);
                        break; // treat as EOF
                    }
                    WalRecoveryMode::AbsoluteConsistency => {
                        return Err(e); // fail hard
                    }
                    WalRecoveryMode::SkipAnyCorruptedRecords => {
                        log::warn!("skipping corrupted WAL record: {}", e);
                        continue; // skip and try next
                    }
                },
            };

            if data.len() < 12 {
                continue;
            }

            let sequence = u64::from_le_bytes(
                data[0..8]
                    .try_into()
                    .map_err(|_| Error::corruption("truncated WAL batch header"))?,
            );
            let count = u32::from_le_bytes(
                data[8..12]
                    .try_into()
                    .map_err(|_| Error::corruption("truncated WAL batch header"))?,
            );

            let mut seq = sequence;
            let mut pos = 12;

            for _ in 0..count {
                if pos >= data.len() {
                    break;
                }
                let tag = data[pos];
                pos += 1;

                match ValueType::from_u8(tag) {
                    Some(ValueType::Value) => {
                        let (key_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| Error::corruption("truncated key len in WAL batch"))?;
                        pos += n;
                        let key_len = key_len as usize;
                        if pos + key_len > data.len() {
                            return Err(Error::corruption("truncated key in WAL batch"));
                        }
                        let key = &data[pos..pos + key_len];
                        pos += key_len;

                        let (val_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| Error::corruption("truncated value len in WAL batch"))?;
                        pos += n;
                        let val_len = val_len as usize;
                        if pos + val_len > data.len() {
                            return Err(Error::corruption("truncated value in WAL batch"));
                        }
                        let value = &data[pos..pos + val_len];
                        pos += val_len;

                        mem.add(seq, ValueType::Value, key, value);
                        seq += 1;
                    }
                    Some(ValueType::Deletion) => {
                        let (key_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| Error::corruption("truncated key len in WAL delete"))?;
                        pos += n;
                        let key_len = key_len as usize;
                        if pos + key_len > data.len() {
                            return Err(Error::corruption("truncated key in WAL delete"));
                        }
                        let key = &data[pos..pos + key_len];
                        pos += key_len;

                        mem.add(seq, ValueType::Deletion, key, b"");
                        seq += 1;
                    }
                    Some(ValueType::RangeDeletion) => {
                        // Range deletion: start_key + end_key
                        let (start_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| Error::corruption("truncated start key len in WAL range delete"))?;
                        pos += n;
                        let start_len = start_len as usize;
                        if pos + start_len > data.len() {
                            return Err(Error::corruption("truncated start key in WAL range delete"));
                        }
                        let start_key = &data[pos..pos + start_len];
                        pos += start_len;

                        let (end_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| Error::corruption("truncated end key len in WAL range delete"))?;
                        pos += n;
                        let end_len = end_len as usize;
                        if pos + end_len > data.len() {
                            return Err(Error::corruption("truncated end key in WAL range delete"));
                        }
                        let end_key = &data[pos..pos + end_len];
                        pos += end_len;

                        // Store the range tombstone: start_key is the key, end_key is the value.
                        mem.add(seq, ValueType::RangeDeletion, start_key, end_key);
                        seq += 1;
                    }
                    None => {
                        return Err(Error::corruption(format!(
                            "unknown value type {} in WAL batch",
                            tag
                        )));
                    }
                }
            }

            // Use the actual number of operations parsed (seq - sequence),
            // not the batch header count, in case the batch was truncated.
            let actual_count = seq - sequence;
            let end_seq = sequence + actual_count;
            if end_seq > *max_sequence {
                *max_sequence = end_seq;
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Checkpoint
    // -----------------------------------------------------------------------

    /// Create a lightweight checkpoint of the database using hard links.
    ///
    /// The checkpoint is a consistent snapshot that shares SST files with
    /// the original database via hard links (no data copying). Only the
    /// MANIFEST and CURRENT files are copied. WAL files are NOT included
    /// (the checkpoint represents the last flushed state).
    ///
    /// The database must be open. The checkpoint directory must not exist.
    pub fn checkpoint(&self, checkpoint_path: impl AsRef<Path>) -> Result<()> {
        let checkpoint_path = checkpoint_path.as_ref();
        if checkpoint_path.exists() {
            return Err(Error::invalid_argument(
                "checkpoint directory already exists",
            ));
        }

        // Flush to ensure all data is in SSTs.
        self.flush()?;

        let state = self.state.read();
        let version = state.versions.current();

        fs::create_dir_all(checkpoint_path)?;

        // Hard-link all SST files.
        for level in 0..version.num_levels {
            for file_meta in version.files_at_level(level) {
                let src = self.dbname.join(sst_file_name(file_meta.number));
                let dst = checkpoint_path.join(sst_file_name(file_meta.number));
                fs::hard_link(&src, &dst).or_else(|_| fs::copy(&src, &dst).map(|_| ()))?;
            }
        }

        // Copy MANIFEST (don't hard-link -- it may be written to later).
        let manifest_name = manifest_file_name(state.versions.manifest_file_number());
        let src = self.dbname.join(&manifest_name);
        let dst = checkpoint_path.join(&manifest_name);
        fs::copy(&src, &dst)?;

        // Write CURRENT with fsync for crash safety.
        {
            use std::io::Write;
            let current_path = checkpoint_path.join(CURRENT_FILE_NAME);
            let mut f = fs::File::create(&current_path)?;
            f.write_all(format!("{}\n", manifest_name).as_bytes())?;
            f.sync_all()?;
        }

        // Create empty LOCK file.
        fs::File::create(checkpoint_path.join(LOCK_FILE_NAME))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // RepairDB
    // -----------------------------------------------------------------------

    /// Attempt to repair a corrupted database.
    ///
    /// Scans the database directory for valid SST files, reconstructs a
    /// new MANIFEST from their metadata, and creates a fresh WAL. Data in
    /// corrupted SST files or WAL files is lost.
    ///
    /// The database must NOT be open.
    pub fn repair(options: Options, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(Error::invalid_argument("database path does not exist"));
        }

        let mut recovered_files: Vec<(u64, u64, Vec<u8>, Vec<u8>)> = Vec::new();
        let mut max_sequence: SequenceNumber = 0;

        // Scan for valid SST files.
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(num_str) = name.strip_suffix(".sst") {
                if let Ok(num) = num_str.parse::<u64>() {
                    let file_path = entry.path();
                    match fs::File::open(&file_path) {
                        Ok(f) => {
                            let file_size = f.metadata()?.len();
                            match crate::sst::TableReader::open(f, file_size, options.clone()) {
                                Ok(reader) => {
                                    let entries = match reader.iter_entries() {
                                        Ok(e) => e,
                                        Err(e) => {
                                            log::warn!(
                                                "repair: skipping SST {} (cannot iterate): {}",
                                                name, e
                                            );
                                            continue;
                                        }
                                    };
                                    // Track the maximum sequence number from actual
                                    // internal keys so new writes don't reuse
                                    // sequence numbers that already exist in SSTs.
                                    for (k, _) in &entries {
                                        let tag = extract_tag(k);
                                        let seq = tag_sequence(tag);
                                        if seq > max_sequence {
                                            max_sequence = seq;
                                        }
                                    }
                                    if let (Some(first), Some(last)) =
                                        (entries.first(), entries.last())
                                    {
                                        recovered_files.push((
                                            num,
                                            file_size,
                                            first.0.clone(),
                                            last.0.clone(),
                                        ));
                                        log::info!(
                                            "repair: recovered SST file {} ({} entries)",
                                            name,
                                            entries.len()
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::warn!("repair: skipping corrupt SST {}: {}", name, e);
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("repair: cannot open {}: {}", name, e);
                        }
                    }
                }
            }
        }

        // Find the max file number for next_file_number.
        let max_file_num = recovered_files
            .iter()
            .map(|(n, _, _, _)| *n)
            .max()
            .unwrap_or(0);

        // Build a VersionEdit with all recovered files at L0.
        let mut edit = VersionEdit::new();
        edit.set_comparator_name("leveldb.BytewiseComparator".to_string());
        edit.set_log_number(0);
        edit.set_next_file_number(max_file_num + 10);
        edit.set_last_sequence(max_sequence);

        for (num, file_size, smallest, largest) in &recovered_files {
            edit.add_file(
                0,
                FileMetaData {
                    number: *num,
                    file_size: *file_size,
                    smallest_key: InternalKey::from_bytes(smallest.clone()),
                    largest_key: InternalKey::from_bytes(largest.clone()),
                },
            );
        }

        // Delete old MANIFEST and WAL files.
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("MANIFEST-") || name.ends_with(".log") || name == "CURRENT" {
                let _ = fs::remove_file(entry.path());
            }
        }

        // Write new MANIFEST.
        let manifest_number = max_file_num + 1;
        let manifest_name_str = manifest_file_name(manifest_number);
        let manifest_path = path.join(&manifest_name_str);
        let file = fs::File::create(&manifest_path)?;
        let mut writer = crate::version::ManifestWriter::new(file);
        writer.add_edit(&edit)?;

        // Write CURRENT atomically.
        use std::io::Write;
        let current_path = path.join(CURRENT_FILE_NAME);
        let tmp_path = path.join("CURRENT.tmp");
        let mut f = fs::File::create(&tmp_path)?;
        f.write_all(format!("{}\n", manifest_name_str).as_bytes())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp_path, &current_path)?;

        log::info!(
            "repair: recovered {} SST files, new MANIFEST at {}",
            recovered_files.len(),
            manifest_name_str
        );
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.shutting_down.load(AtomicOrdering::Acquire) {
            let _ = self.close();
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Compute the successor of a prefix for upper-bound iteration.
///
/// Returns the smallest byte string that is greater than all strings
/// starting with `prefix`. For example, `prefix_successor(b"abc")` returns
/// `Some(b"abd")`. Returns `None` if the prefix is empty or all 0xFF bytes
/// (meaning all keys match).
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }
    // Find the last byte that is not 0xFF and increment it.
    let mut upper = prefix.to_vec();
    while let Some(&last) = upper.last() {
        if last < 0xFF {
            *upper.last_mut().unwrap() += 1;
            return Some(upper);
        }
        upper.pop();
    }
    // All bytes were 0xFF — no upper bound possible.
    None
}

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

enum SearchResult {
    Found(Vec<u8>, SequenceNumber),
    Deleted,
    NotFound,
}

struct MemTableInserter<'a> {
    mem: &'a MemTable,
    sequence: SequenceNumber,
}

impl<'a> WriteBatchHandler for MemTableInserter<'a> {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.mem.add(self.sequence, ValueType::Value, key, value);
        self.sequence += 1;
    }

    fn delete(&mut self, key: &[u8]) {
        self.mem.add(self.sequence, ValueType::Deletion, key, b"");
        self.sequence += 1;
    }

    fn delete_range(&mut self, start_key: &[u8], end_key: &[u8]) {
        self.mem
            .add(self.sequence, ValueType::RangeDeletion, start_key, end_key);
        self.sequence += 1;
    }
}

