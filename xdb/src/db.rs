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
use crate::version::VersionSet;
use crate::wal::WalWriter;

use crossbeam_channel::Sender;
use parking_lot::Mutex;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Weak};
use std::thread;

// ---------------------------------------------------------------------------
// Background work types
// ---------------------------------------------------------------------------

enum BgWork {
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
    state: Mutex<DbState>,
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
                let _ = fs::remove_file(wal_path);
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
            state: Mutex::new(DbState {
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

        // Spawn background compaction thread.
        let weak = Arc::downgrade(&db);
        let handle = thread::Builder::new()
            .name("xdb-bg".to_string())
            .spawn(move || Self::background_worker(weak, bg_receiver))
            .ok();
        *db._bg_handle.lock() = handle;

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
            let state = self.state.lock();
            (
                Arc::clone(&state.mem),
                state.imm.as_ref().map(Arc::clone),
                state.versions.current(),
                state.versions.last_sequence(),
            )
        };

        let lookup = LookupKey::new(key, sequence);

        // 1. Active memtable.
        if let Some(result) = mem.get(&lookup) {
            Statistics::record(&self.stats.memtable_hits, 1);
            return match result {
                Ok(value) => {
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
                SearchResult::Found(value) => return Ok(Some(value)),
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
                SearchResult::Found(value) => return Ok(Some(value)),
                SearchResult::Deleted => return Ok(None),
                SearchResult::NotFound => continue,
            }
        }

        Ok(None)
    }

    /// Apply a `WriteBatch` atomically.
    pub fn write(&self, write_opts: WriteOptions, mut batch: WriteBatch) -> Result<()> {
        if self.shutting_down.load(AtomicOrdering::Acquire) {
            return Err(Error::ShutdownInProgress);
        }

        let mut state = self.state.lock();
        self.make_room_for_write(&mut state)?;

        let last_seq = state.versions.last_sequence();
        let first_seq = last_seq + 1;
        batch.set_sequence(first_seq);
        state
            .versions
            .set_last_sequence(last_seq + batch.count() as u64);

        if let Some(ref mut wal) = state.wal {
            wal.add_record(batch.data())?;
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
        let mut state = self.state.lock();

        if state.mem.is_empty() {
            return Ok(());
        }

        let old_mem = std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
        state.imm = Some(old_mem);

        let wal_number = state.versions.new_file_number();
        let wal_path = self.dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;
        let old_wal_number = state.versions.log_number();
        state.wal = Some(WalWriter::new(wal_file));
        state.versions.set_log_number(wal_number);

        self.do_flush(&mut state)?;

        let old_wal_path = self.dbname.join(wal_file_name(old_wal_number));
        let _ = fs::remove_file(&old_wal_path);

        Ok(())
    }

    /// Create a point-in-time snapshot of the database.
    ///
    /// The snapshot can be used with `ReadOptions` to perform consistent reads.
    /// Drop or release the snapshot when no longer needed.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        let seq = self.state.lock().versions.last_sequence();
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
        let seq = self.state.lock().versions.last_sequence();
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

        let mut state = self.state.lock();
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

    /// Build a DbIterator at a given sequence number.
    fn iter_at_sequence(&self, sequence: SequenceNumber) -> DbIterator {
        let (mem, imm, version) = {
            let state = self.state.lock();
            (
                Arc::clone(&state.mem),
                state.imm.as_ref().map(Arc::clone),
                state.versions.current(),
            )
        };

        let mut children: Vec<Box<dyn XdbIterator>> = Vec::new();

        // Memtable entries.
        let mem_entries = Self::collect_memtable_entries(&mem);
        if !mem_entries.is_empty() {
            children.push(Box::new(VecIterator::new(mem_entries)));
        }

        // Immutable memtable entries.
        if let Some(ref imm) = imm {
            let imm_entries = Self::collect_memtable_entries(imm);
            if !imm_entries.is_empty() {
                children.push(Box::new(VecIterator::new(imm_entries)));
            }
        }

        // SST file iterators via table cache.
        for level in 0..version.num_levels {
            for file_meta in version.files_at_level(level) {
                if let Ok(reader) = self.table_cache.get_reader(
                    file_meta.number,
                    file_meta.file_size,
                ) {
                    children.push(Box::new(reader.iter()));
                }
            }
        }

        // Collect range tombstones from all sources.
        let mut range_tombstones = Vec::new();
        Self::collect_range_tombstones_from_mem(&mem, sequence, &mut range_tombstones);
        if let Some(ref imm) = imm {
            Self::collect_range_tombstones_from_mem(imm, sequence, &mut range_tombstones);
        }
        for level in 0..version.num_levels {
            for file_meta in version.files_at_level(level) {
                if let Ok(reader) = self.table_cache.get_reader(
                    file_meta.number,
                    file_meta.file_size,
                ) {
                    if let Ok(entries) = reader.iter_entries() {
                        for (k, v) in &entries {
                            if let Some(p) = ParsedInternalKey::from_bytes(k) {
                                if p.value_type == ValueType::RangeDeletion
                                    && p.sequence <= sequence
                                {
                                    range_tombstones.push((
                                        p.user_key.to_vec(),
                                        v.clone(),
                                        p.sequence,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        let merger = MergingIterator::new(children);
        DbIterator::with_range_tombstones(Box::new(merger), sequence, range_tombstones)
    }

    /// Collect range tombstones from a memtable.
    fn collect_range_tombstones_from_mem(
        mem: &MemTable,
        sequence: SequenceNumber,
        out: &mut Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>,
    ) {
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

    /// Collect all entries from a memtable into a Vec for use in iterators.
    fn collect_memtable_entries(mem: &MemTable) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::new();
        let mut iter = mem.iter();
        iter.seek_to_first();
        while iter.valid() {
            entries.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next();
        }
        entries
    }

    fn make_room_for_write(&self, state: &mut DbState) -> Result<()> {
        if state.mem.approximate_memory_usage() < self.options.write_buffer_size {
            return Ok(());
        }

        if state.imm.is_some() {
            self.do_flush(state)?;
        }

        let old_mem = std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
        state.imm = Some(old_mem);

        let wal_number = state.versions.new_file_number();
        let wal_path = self.dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;
        let old_wal_number = state.versions.log_number();
        state.wal = Some(WalWriter::new(wal_file));
        state.versions.set_log_number(wal_number);

        self.do_flush(state)?;

        let old_wal_path = self.dbname.join(wal_file_name(old_wal_number));
        let _ = fs::remove_file(&old_wal_path);

        Ok(())
    }

    /// Flush the immutable memtable to an L0 SST file.
    fn do_flush(&self, state: &mut DbState) -> Result<()> {
        let imm = match state.imm.take() {
            Some(m) => m,
            None => return Ok(()),
        };

        let file_number = state.versions.new_file_number();
        let sst_path = self.dbname.join(sst_file_name(file_number));
        let sst_file = fs::File::create(&sst_path)?;
        let mut builder = TableBuilder::new(sst_file, (*self.options).clone());

        let mut iter = imm.iter();
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

        if file_size > 0 && smallest_key.is_some() {
            let meta = FileMetaData {
                number: file_number,
                file_size,
                smallest_key: InternalKey::from_bytes(smallest_key.unwrap()),
                largest_key: InternalKey::from_bytes(largest_key),
            };
            let mut edit = VersionEdit::new();
            edit.set_log_number(state.versions.log_number());
            edit.add_file(0, meta);
            state.versions.log_and_apply(edit)?;
        } else {
            let _ = fs::remove_file(&sst_path);
        }

        Statistics::record(&self.stats.flushes, 1);

        // Schedule background compaction if needed.
        if state.versions.pick_compaction().is_some() && !state.bg_compaction_scheduled {
            state.bg_compaction_scheduled = true;
            let _ = self.bg_sender.send(BgWork::MaybeCompact);
        }

        Ok(())
    }

    /// Search a single SST file for a user key via the table cache.
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

        // Use the streaming iterator for the search.
        let ikey = InternalKey::new(user_key, sequence, ValueType::Value);
        let mut table_iter = reader.iter();
        table_iter.seek(ikey.as_bytes());

        while table_iter.valid() {
            let found_key = table_iter.key();
            if let Some(parsed) = ParsedInternalKey::from_bytes(found_key) {
                if parsed.user_key == user_key && parsed.sequence <= sequence {
                    match parsed.value_type {
                        ValueType::Value => {
                            let value = table_iter.value().to_vec();
                            Statistics::record(&self.stats.bytes_read, value.len() as u64);
                            return Ok(SearchResult::Found(value));
                        }
                        ValueType::Deletion | ValueType::RangeDeletion => {
                            return Ok(SearchResult::Deleted);
                        }
                    }
                }
                if parsed.user_key > user_key {
                    break;
                }
            } else {
                break;
            }
            table_iter.next();
        }

        Ok(SearchResult::NotFound)
    }

    // -----------------------------------------------------------------------
    // Background compaction
    // -----------------------------------------------------------------------

    /// Background worker loop: receives work items and processes them.
    fn background_worker(weak: Weak<Db>, receiver: crossbeam_channel::Receiver<BgWork>) {
        while let Ok(work) = receiver.recv() {
            match work {
                BgWork::Shutdown => break,
                BgWork::MaybeCompact => {
                    if let Some(db) = weak.upgrade() {
                        let _ = db.do_background_compaction();
                    }
                }
            }
        }
    }

    /// Run compaction in the background. Releases the lock during I/O.
    fn do_background_compaction(&self) -> Result<()> {
        use crate::compaction;

        loop {
            // Step 1: Pick compaction under lock.
            let comp_info = {
                let mut state = self.state.lock();
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
                        let next_file = state.versions.new_file_number();
                        for _ in 0..20 {
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

            // Step 3: Apply the edit under lock.
            {
                let mut state = self.state.lock();
                while state.versions.manifest_file_number() < next_file {
                    state.versions.new_file_number();
                }
                state.versions.log_and_apply(edit)?;
            }

            // Step 4: Cleanup outside lock.
            for files in &comp_state.input_files {
                for f in files {
                    self.table_cache.evict(f.number);
                    let _ = fs::remove_file(self.dbname.join(sst_file_name(f.number)));
                }
            }

            // Loop to check if more compaction is needed.
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

        let file_size = builder.finish()?;

        if file_size > 0 && smallest_key.is_some() {
            let meta = FileMetaData {
                number: file_number,
                file_size,
                smallest_key: InternalKey::from_bytes(smallest_key.unwrap()),
                largest_key: InternalKey::from_bytes(largest_key),
            };
            let mut edit = VersionEdit::new();
            edit.set_log_number(versions.log_number());
            edit.add_file(0, meta);
            versions.log_and_apply(edit)?;
        } else {
            let _ = fs::remove_file(&sst_path);
        }

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

            let sequence = u64::from_le_bytes(data[0..8].try_into().unwrap());
            let count = u32::from_le_bytes(data[8..12].try_into().unwrap());

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

            let end_seq = sequence + count as u64;
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

/// Simple vector-based iterator for memtable snapshots.
struct VecIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl VecIterator {
    fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        VecIterator { entries, pos: 0 }
    }
}

impl XdbIterator for VecIterator {
    fn valid(&self) -> bool {
        self.pos < self.entries.len()
    }
    fn seek_to_first(&mut self) {
        self.pos = 0;
    }
    fn seek(&mut self, target: &[u8]) {
        self.pos = self.entries.partition_point(|(k, _)| {
            compare_internal_key(k, target) == std::cmp::Ordering::Less
        });
    }
    fn next(&mut self) {
        if self.valid() {
            self.pos += 1;
        }
    }
    fn key(&self) -> &[u8] {
        &self.entries[self.pos].0
    }
    fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }
    fn seek_to_last(&mut self) {
        if self.entries.is_empty() {
            self.pos = 0;
        } else {
            self.pos = self.entries.len() - 1;
        }
    }
    fn prev(&mut self) {
        if self.pos > 0 {
            self.pos -= 1;
        } else {
            self.pos = self.entries.len(); // invalidate
        }
    }
}
