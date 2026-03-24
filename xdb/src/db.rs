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
use crate::options::{Options, WriteOptions};
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
                Self::replay_wal(wal_path, &recovery_mem, &mut max_sequence)?;
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

    /// Insert a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(WriteOptions::default(), batch)
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(WriteOptions::default(), batch)
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

        let lookup = LookupKey::new(key, sequence);

        // Fast-path: check whether ANY range tombstones could exist.
        // Memtable flags are atomic booleans (no lock needed). If neither
        // memtable has range tombstones, we can defer the expensive SST
        // tombstone scan until we actually find a value -- and even then
        // we only need to check the specific reader that produced it plus
        // the memtable flags.
        let mem_has_tombstones = mem.has_range_tombstones()
            || imm.as_ref().is_some_and(|i| i.has_range_tombstones());

        // 1. Active memtable.
        if let Some(result) = mem.get(&lookup) {
            Statistics::record(&self.stats.memtable_hits, 1);
            return match result {
                Ok(value) => {
                    if mem_has_tombstones {
                        let range_tombstones = self.collect_range_tombstones_for_get(
                            key, &mem, imm.as_deref(), &version, sequence,
                        );
                        if Self::is_key_range_deleted(key, sequence, &range_tombstones) {
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
        if let Some(ref imm_table) = imm {
            if let Some(result) = imm_table.get(&lookup) {
                Statistics::record(&self.stats.memtable_hits, 1);
                return match result {
                    Ok(value) => {
                        if mem_has_tombstones {
                            let range_tombstones = self.collect_range_tombstones_for_get(
                                key, &mem, imm.as_deref(), &version, sequence,
                            );
                            if Self::is_key_range_deleted(key, sequence, &range_tombstones) {
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
                SearchResult::Found(value) => {
                    // Lazily check range tombstones only when a value is found.
                    if self.check_range_tombstones_for_found_key(
                        key, sequence, mem_has_tombstones, &mem, imm.as_deref(),
                        &version,
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
                SearchResult::Found(value) => {
                    if self.check_range_tombstones_for_found_key(
                        key, sequence, mem_has_tombstones, &mem, imm.as_deref(),
                        &version,
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
        sequence: SequenceNumber,
        mem_has_tombstones: bool,
        mem: &MemTable,
        imm: Option<&MemTable>,
        version: &Version,
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
        let range_tombstones = self.collect_range_tombstones_for_get(
            user_key, mem, imm, version, sequence,
        );
        Ok(Self::is_key_range_deleted(user_key, sequence, &range_tombstones))
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

        if let Some(ref mut wal) = state.wal {
            wal.add_record(batch.data())?;
            // Note: sync is under the lock because WalWriter::sync requires
            // &mut self. Moving the WAL to a separate Mutex would allow
            // sync outside the state lock (group commit). Acceptable for now
            // since sync_writes=false is the default.
            if write_opts.sync || self.options.sync_writes {
                wal.sync()?;
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

    /// Get a reference to the database statistics.
    pub fn stats(&self) -> &Statistics {
        &self.stats
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

    /// Check whether a user key is covered by any collected range tombstone.
    fn is_key_range_deleted(
        user_key: &[u8],
        sequence: SequenceNumber,
        tombstones: &[(Vec<u8>, Vec<u8>, SequenceNumber)],
    ) -> bool {
        for (start, end, tomb_seq) in tombstones {
            if user_key >= start.as_slice()
                && user_key < end.as_slice()
                && *tomb_seq <= sequence
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

                // Delete the old WAL ONLY after the SST is durable in the
                // MANIFEST. This prevents data loss on crash.
                let old_wal_path = self.dbname.join(wal_file_name(log_number));
                if let Err(e) = fs::remove_file(&old_wal_path) {
                    log::warn!("failed to delete old WAL: {}", e);
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

        // Fast reject via bloom filter (checked inside get_for_user_key,
        // but we check here first to record the stat).
        if !reader.bloom_may_contain(user_key) {
            Statistics::record(&self.stats.bloom_useful, 1);
            return Ok(SearchResult::NotFound);
        }

        match reader.get_for_user_key(user_key, sequence)? {
            Some(crate::sst::UserKeyResult::Found(value)) => {
                Statistics::record(&self.stats.bytes_read, value.len() as u64);
                Ok(SearchResult::Found(value))
            }
            Some(crate::sst::UserKeyResult::Deleted) => Ok(SearchResult::Deleted),
            None => Ok(SearchResult::NotFound),
        }
    }

    // -----------------------------------------------------------------------
    // Background compaction
    // -----------------------------------------------------------------------

    /// Background worker loop: receives work items and processes them.
    fn background_worker(weak: Weak<Db>, receiver: crossbeam_channel::Receiver<BgWork>) {
        while let Ok(work) = receiver.recv() {
            match work {
                BgWork::Shutdown => break,
                BgWork::Flush => {
                    if let Some(db) = weak.upgrade() {
                        if let Err(e) = db.do_background_flush() {
                            log::error!("background flush failed: {}", e);
                        }
                    }
                }
                BgWork::MaybeCompact => {
                    if let Some(db) = weak.upgrade() {
                        if let Err(e) = db.do_background_compaction() {
                            log::error!("background compaction failed: {}", e);
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

                // Delete old WAL only after SST is durable in MANIFEST.
                let old_wal_path = self.dbname.join(wal_file_name(log_number));
                if let Err(e) = fs::remove_file(&old_wal_path) {
                    log::warn!("failed to delete old WAL: {}", e);
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
                state.imm = Some(imm);
                Err(e)
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
            let edit = compaction::compact(
                &self.dbname,
                &mut comp_state,
                &self.options,
                &mut next_file,
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
    ) -> Result<()> {
        let file = fs::File::open(wal_path)?;
        let mut reader = crate::wal::WalReader::new(file);

        while let Some(data) = reader.read_record()? {
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
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.shutting_down.load(AtomicOrdering::Acquire) {
            let _ = self.close();
        }
    }
}

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

enum SearchResult {
    Found(Vec<u8>),
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

