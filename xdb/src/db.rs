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
//! ## Phase 1 notes
//!
//! Background flush and compaction are performed synchronously in the
//! calling thread. Asynchronous background work will be added in Phase 2.

use crate::batch::{WriteBatch, WriteBatchHandler};
use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::options::{Options, WriteOptions};
use crate::sst::{TableBuilder, TableReader};
use crate::types::*;
use crate::version::edit::{FileMetaData, VersionEdit};
use crate::version::VersionSet;
use crate::wal::WalWriter;
use crate::cache::LruCache;
use crate::compaction;

use parking_lot::Mutex;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Db
// ---------------------------------------------------------------------------

/// The main database handle.
///
/// Thread-safe: can be shared across threads via `Arc<Db>`. All public
/// methods take `&self`. A single LOCK file prevents multiple processes
/// from opening the same database concurrently.
pub struct Db {
    /// Path to the database directory.
    dbname: PathBuf,
    /// Database configuration (immutable after open).
    options: Arc<Options>,
    /// Mutable state protected by a mutex.
    state: Mutex<DbState>,
    /// Block cache shared across all reads (if configured).
    #[allow(dead_code)]
    cache: Option<Arc<LruCache>>,
    /// Set to `true` when the database is shutting down.
    shutting_down: AtomicBool,
    /// Lock file handle kept open to prevent concurrent access.
    _lock_file: fs::File,
}

/// Internal mutable state of the database.
struct DbState {
    /// Active (mutable) memtable receiving writes.
    mem: Arc<MemTable>,
    /// Immutable memtable being flushed to disk (if any).
    imm: Option<Arc<MemTable>>,
    /// WAL writer for the active memtable.
    wal: Option<WalWriter>,
    /// Version management and MANIFEST tracking.
    versions: VersionSet,
    /// Whether a background flush is in progress (Phase 1: always false).
    #[allow(dead_code)]
    bg_flush_scheduled: bool,
    /// Whether a background compaction is in progress (Phase 1: always false).
    #[allow(dead_code)]
    bg_compaction_scheduled: bool,
}

impl Db {
    /// Open a database at the given path.
    ///
    /// If `options.create_if_missing` is set, the database directory will be
    /// created if it does not exist. If the directory exists and contains a
    /// valid MANIFEST, the database state is recovered. Otherwise a fresh
    /// database is initialized.
    pub fn open(options: Options, path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let dbname = path.as_ref().to_path_buf();

        // Create directory if needed.
        if options.create_if_missing {
            fs::create_dir_all(&dbname)?;
        }
        if !dbname.exists() {
            return Err(Error::invalid_argument(format!(
                "database directory {:?} does not exist and create_if_missing is false",
                dbname
            )));
        }

        // Acquire the LOCK file.
        let lock_path = dbname.join(LOCK_FILE_NAME);
        let lock_file = fs::File::create(&lock_path)?;

        // Create block cache if configured.
        let cache = if options.block_cache_capacity > 0 {
            Some(Arc::new(LruCache::new(options.block_cache_capacity)))
        } else {
            None
        };

        // Create VersionSet and attempt recovery.
        let mut versions = VersionSet::new(&dbname, options.clone());
        let recovered = versions.recover()?;

        if !recovered {
            // Fresh database: write an initial MANIFEST.
            let mut edit = VersionEdit::new();
            edit.set_comparator_name("leveldb.BytewiseComparator".to_string());
            edit.set_log_number(0);
            edit.set_next_file_number(2);
            edit.set_last_sequence(0);
            versions.log_and_apply(edit)?;
        }

        // Recover WAL files: replay any WAL files newer than the logged
        // log_number into a memtable, then flush to L0.
        let logged_log_number = versions.log_number();
        let wal_files = Self::find_wal_files(&dbname, logged_log_number)?;

        if !wal_files.is_empty() {
            let recovery_mem = MemTable::new();
            let mut max_sequence = versions.last_sequence();

            for (_wal_num, wal_path) in &wal_files {
                Self::replay_wal(wal_path, &recovery_mem, &mut max_sequence)?;
            }

            versions.set_last_sequence(max_sequence);

            // Flush recovered memtable to L0 if it has entries.
            if !recovery_mem.is_empty() {
                Self::flush_memtable_to_l0(
                    &dbname,
                    &recovery_mem,
                    &mut versions,
                    &options,
                )?;
            }

            // Delete old WAL files.
            for (_wal_num, wal_path) in &wal_files {
                let _ = fs::remove_file(wal_path);
            }
        }

        // Create a new WAL and memtable for fresh writes.
        let wal_number = versions.new_file_number();
        let wal_path = dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;
        let wal_writer = WalWriter::new(wal_file);
        versions.set_log_number(wal_number);

        let mem = Arc::new(MemTable::new());

        let state = DbState {
            mem,
            imm: None,
            wal: Some(wal_writer),
            versions,
            bg_flush_scheduled: false,
            bg_compaction_scheduled: false,
        };

        let db = Arc::new(Db {
            dbname,
            options: Arc::new(options),
            state: Mutex::new(state),
            cache,
            shutting_down: AtomicBool::new(false),
            _lock_file: lock_file,
        });

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

    /// Read a value by key.
    ///
    /// Searches the active memtable, the immutable memtable (if any), and
    /// then the SST files from newest to oldest.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.shutting_down.load(AtomicOrdering::Acquire) {
            return Err(Error::ShutdownInProgress);
        }

        // Snapshot state under the lock, then release for I/O.
        let (mem, imm, version, sequence) = {
            let state = self.state.lock();
            (
                Arc::clone(&state.mem),
                state.imm.as_ref().map(Arc::clone),
                state.versions.current(),
                state.versions.last_sequence(),
            )
        };

        // Build a lookup key for the user key at the current sequence.
        let lookup = LookupKey::new(key, sequence);

        // 1. Check the active memtable.
        if let Some(result) = mem.get(&lookup) {
            return match result {
                Ok(value) => Ok(Some(value)),
                Err(Error::NotFound(_)) => Ok(None),
                Err(e) => Err(e),
            };
        }

        // 2. Check the immutable memtable.
        if let Some(ref imm_table) = imm {
            if let Some(result) = imm_table.get(&lookup) {
                return match result {
                    Ok(value) => Ok(Some(value)),
                    Err(Error::NotFound(_)) => Ok(None),
                    Err(e) => Err(e),
                };
            }
        }

        // 3. Search SST files level by level.

        // L0: files may overlap; search all, newest first (already sorted by
        // file number descending in the Version).
        for file_meta in version.files_at_level(0) {
            // Check if the user key falls within this file's key range.
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

        // L1+: files are sorted and non-overlapping within each level.
        for level in 1..version.num_levels {
            let files = version.files_at_level(level);
            if files.is_empty() {
                continue;
            }

            // Binary search to find the file whose range might contain the key.
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
    ///
    /// Assigns sequence numbers, writes to the WAL, and applies mutations
    /// to the active memtable. If `write_opts.sync` is set, the WAL is
    /// fsynced before returning.
    pub fn write(&self, write_opts: WriteOptions, mut batch: WriteBatch) -> Result<()> {
        if self.shutting_down.load(AtomicOrdering::Acquire) {
            return Err(Error::ShutdownInProgress);
        }

        let mut state = self.state.lock();

        // Ensure there is room in the memtable.
        self.make_room_for_write(&mut state)?;

        // Assign sequence numbers.
        let last_seq = state.versions.last_sequence();
        let first_seq = last_seq + 1;
        batch.set_sequence(first_seq);
        state
            .versions
            .set_last_sequence(last_seq + batch.count() as u64);

        // Write to WAL.
        if let Some(ref mut wal) = state.wal {
            wal.add_record(batch.data())?;
            if write_opts.sync || self.options.sync_writes {
                wal.sync()?;
            }
        }

        // Apply to memtable.
        let mem = Arc::clone(&state.mem);
        let mut handler = MemTableInserter {
            mem: &mem,
            sequence: first_seq,
        };
        batch.iterate(&mut handler)?;

        Ok(())
    }

    /// Force flush the current memtable to disk.
    ///
    /// Swaps the active memtable to immutable, creates a new WAL and
    /// memtable, then flushes the immutable memtable to an L0 SST.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.lock();

        if state.mem.is_empty() {
            return Ok(());
        }

        // Swap memtable.
        let old_mem = std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
        state.imm = Some(old_mem);

        // Create new WAL.
        let wal_number = state.versions.new_file_number();
        let wal_path = self.dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;
        let old_wal_number = state.versions.log_number();
        state.wal = Some(WalWriter::new(wal_file));
        state.versions.set_log_number(wal_number);

        // Perform the flush.
        Self::do_flush(&self.dbname, &mut state, &self.options)?;

        // Delete old WAL.
        let old_wal_path = self.dbname.join(wal_file_name(old_wal_number));
        let _ = fs::remove_file(&old_wal_path);

        Ok(())
    }

    /// Graceful shutdown: flush any pending data and release resources.
    pub fn close(&self) -> Result<()> {
        if self
            .shutting_down
            .compare_exchange(
                false,
                true,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            )
            .is_err()
        {
            // Already shutting down.
            return Ok(());
        }

        // Flush any remaining memtable data.
        let mut state = self.state.lock();
        if !state.mem.is_empty() {
            let old_mem =
                std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
            state.imm = Some(old_mem);
            Self::do_flush(&self.dbname, &mut state, &self.options)?;
        }

        // Sync the WAL.
        if let Some(ref mut wal) = state.wal {
            wal.sync()?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Ensure there is room in the memtable for a write.
    ///
    /// If the memtable exceeds the write buffer size, it is swapped to
    /// immutable and flushed synchronously (Phase 1). A new WAL and
    /// memtable are created.
    fn make_room_for_write(&self, state: &mut DbState) -> Result<()> {
        if state.mem.approximate_memory_usage() < self.options.write_buffer_size {
            return Ok(());
        }

        // If there is already an immutable memtable being flushed, flush it
        // first (Phase 1: synchronous).
        if state.imm.is_some() {
            Self::do_flush(&self.dbname, state, &self.options)?;
        }

        // Swap active memtable to immutable.
        let old_mem =
            std::mem::replace(&mut state.mem, Arc::new(MemTable::new()));
        state.imm = Some(old_mem);

        // Create a new WAL file.
        let wal_number = state.versions.new_file_number();
        let wal_path = self.dbname.join(wal_file_name(wal_number));
        let wal_file = fs::File::create(&wal_path)?;
        let old_wal_number = state.versions.log_number();
        state.wal = Some(WalWriter::new(wal_file));
        state.versions.set_log_number(wal_number);

        // Flush the immutable memtable.
        Self::do_flush(&self.dbname, state, &self.options)?;

        // Delete the old WAL file.
        let old_wal_path = self.dbname.join(wal_file_name(old_wal_number));
        let _ = fs::remove_file(&old_wal_path);

        Ok(())
    }

    /// Flush the immutable memtable to an L0 SST file.
    ///
    /// Steps:
    /// 1. Iterate the immutable memtable
    /// 2. Write entries to a new SST file via `TableBuilder`
    /// 3. Create a `VersionEdit` adding the file at L0
    /// 4. Apply the edit to the version set
    /// 5. Clear the immutable memtable reference
    /// 6. Optionally trigger compaction
    fn do_flush(
        dbname: &Path,
        state: &mut DbState,
        options: &Options,
    ) -> Result<()> {
        let imm = match state.imm.take() {
            Some(m) => m,
            None => return Ok(()),
        };

        // Allocate a new SST file number.
        let file_number = state.versions.new_file_number();
        let sst_path = dbname.join(sst_file_name(file_number));
        let sst_file = fs::File::create(&sst_path)?;
        let mut builder = TableBuilder::new(sst_file, options.clone());

        // Iterate the memtable and write all entries.
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
            // Empty SST -- remove it.
            let _ = fs::remove_file(&sst_path);
        }

        // Try to trigger compaction.
        Self::maybe_do_compaction(dbname, state, options)?;

        Ok(())
    }

    /// Check if compaction is needed and perform it synchronously.
    fn maybe_do_compaction(
        dbname: &Path,
        state: &mut DbState,
        options: &Options,
    ) -> Result<()> {
        while let Some((level, _score)) = state.versions.pick_compaction() {
            let version = state.versions.current();
            let mut comp_state =
                compaction::pick_compaction_files(&version, level, options);

            // Skip if no input files were selected.
            if comp_state.input_files[0].is_empty() {
                break;
            }

            let mut next_file = state.versions.new_file_number();
            let edit = compaction::compact(
                dbname,
                &mut comp_state,
                options,
                &mut next_file,
            )?;

            // Advance the file number counter past whatever compact() used.
            while state.versions.manifest_file_number() < next_file {
                state.versions.new_file_number();
            }

            state.versions.log_and_apply(edit)?;

            // Delete input SST files that were compacted away.
            for files in comp_state.input_files.iter() {
                for f in files {
                    let path = dbname.join(sst_file_name(f.number));
                    let _ = fs::remove_file(&path);
                }
            }
        }

        Ok(())
    }

    /// Search a single SST file for a user key at a given sequence number.
    ///
    /// Opens the table, iterates entries to find a matching user key with
    /// sequence <= the requested sequence, and returns the result.
    fn search_sst_file(
        &self,
        file_meta: &FileMetaData,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<SearchResult> {
        let path = self.dbname.join(sst_file_name(file_meta.number));
        let f = fs::File::open(&path)?;
        let file_size = f.metadata()?.len();
        let reader =
            TableReader::open(f, file_size, (*self.options).clone())?;

        // Use iter_entries and scan for the user key. This is Phase 1;
        // a more efficient approach using the index block will come later.
        let entries = reader.iter_entries()?;
        for (found_key, found_value) in &entries {
            if let Some(parsed) = ParsedInternalKey::from_bytes(found_key) {
                if parsed.user_key == user_key && parsed.sequence <= sequence {
                    match parsed.value_type {
                        ValueType::Value => {
                            return Ok(SearchResult::Found(found_value.clone()));
                        }
                        ValueType::Deletion => {
                            return Ok(SearchResult::Deleted);
                        }
                    }
                }
                // If we've passed the user key in sort order, stop.
                if parsed.user_key > user_key {
                    break;
                }
            }
        }

        Ok(SearchResult::NotFound)
    }

    /// Flush a memtable directly to an L0 SST (used during WAL recovery).
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

    /// Find WAL files in the database directory with file numbers >= min_number.
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

        // Sort by file number so we replay in order.
        wal_files.sort_by_key(|(num, _)| *num);
        Ok(wal_files)
    }

    /// Replay a WAL file into a memtable, updating the max sequence number.
    fn replay_wal(
        wal_path: &Path,
        mem: &MemTable,
        max_sequence: &mut SequenceNumber,
    ) -> Result<()> {
        let file = fs::File::open(wal_path)?;
        let mut reader = crate::wal::WalReader::new(file);

        while let Some(data) = reader.read_record()? {
            if data.len() < 12 {
                // Too short for a valid WriteBatch header; skip.
                continue;
            }

            // Parse the WriteBatch header: sequence(8) + count(4).
            let sequence =
                u64::from_le_bytes(data[0..8].try_into().unwrap());
            let count =
                u32::from_le_bytes(data[8..12].try_into().unwrap());

            // Replay via manual parsing of the batch record format.
            let mut seq = sequence;
            let mut pos = 12; // skip header

            for _ in 0..count {
                if pos >= data.len() {
                    break;
                }

                let tag = data[pos];
                pos += 1;

                match ValueType::from_u8(tag) {
                    Some(ValueType::Value) => {
                        let (key_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| {
                                Error::corruption("truncated key len in WAL batch")
                            })?;
                        pos += n;
                        let key_len = key_len as usize;
                        if pos + key_len > data.len() {
                            return Err(Error::corruption(
                                "truncated key in WAL batch",
                            ));
                        }
                        let key = &data[pos..pos + key_len];
                        pos += key_len;

                        let (val_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| {
                                Error::corruption(
                                    "truncated value len in WAL batch",
                                )
                            })?;
                        pos += n;
                        let val_len = val_len as usize;
                        if pos + val_len > data.len() {
                            return Err(Error::corruption(
                                "truncated value in WAL batch",
                            ));
                        }
                        let value = &data[pos..pos + val_len];
                        pos += val_len;

                        mem.add(seq, ValueType::Value, key, value);
                        seq += 1;
                    }
                    Some(ValueType::Deletion) => {
                        let (key_len, n) = decode_varint32(&data[pos..])
                            .ok_or_else(|| {
                                Error::corruption(
                                    "truncated key len in WAL delete",
                                )
                            })?;
                        pos += n;
                        let key_len = key_len as usize;
                        if pos + key_len > data.len() {
                            return Err(Error::corruption(
                                "truncated key in WAL delete",
                            ));
                        }
                        let key = &data[pos..pos + key_len];
                        pos += key_len;

                        mem.add(seq, ValueType::Deletion, key, b"");
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
// SearchResult — outcome of searching a single SST file
// ---------------------------------------------------------------------------

/// Result of searching an SST file for a user key.
enum SearchResult {
    /// Found a live value.
    Found(Vec<u8>),
    /// Found a deletion tombstone (key was deleted).
    Deleted,
    /// Key not present in this file.
    NotFound,
}

// ---------------------------------------------------------------------------
// MemTableInserter — WriteBatchHandler that applies ops to a MemTable
// ---------------------------------------------------------------------------

/// Handler that inserts `WriteBatch` operations into a `MemTable`.
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
        self.mem
            .add(self.sequence, ValueType::Deletion, key, b"");
        self.sequence += 1;
    }
}
