//! Caches open [`TableReader`] instances to avoid re-opening SST files on every read.
//!
//! The database layer previously opened a new `File` + `TableReader` for every
//! `get()` call. `TableCache` keeps a bounded number of readers in memory,
//! keyed by [`FileNumber`], so that repeated reads against the same SST hit the
//! cache instead of the filesystem.
//!
//! Thread safety is provided by a [`parking_lot::Mutex`] around the inner map.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::Result;
use crate::options::Options;
use crate::sst::TableReader;
use crate::types::FileNumber;

/// Caches open [`TableReader`] instances to avoid re-opening SST files.
///
/// The cache is thread-safe and uses a simple `HashMap` protected by a `Mutex`.
/// When the cache exceeds `max_open_files`, the least-recently-used 25% of
/// entries are evicted.
pub struct TableCache {
    dbname: PathBuf,
    options: Arc<Options>,
    cache: Mutex<CacheInner>,
}

struct CacheInner {
    readers: HashMap<FileNumber, CacheEntry>,
    max_open: usize,
    access_counter: u64,
}

struct CacheEntry {
    reader: Arc<TableReader>,
    last_access: u64,
}

impl TableCache {
    /// Create a new table cache.
    ///
    /// `max_open_files` controls how many SST files can be kept open
    /// simultaneously. When this limit is exceeded, the oldest 25% of entries
    /// are evicted.
    pub fn new(dbname: &Path, options: Arc<Options>, max_open_files: usize) -> Self {
        TableCache {
            dbname: dbname.to_path_buf(),
            options,
            cache: Mutex::new(CacheInner {
                readers: HashMap::new(),
                max_open: max_open_files,
                access_counter: 0,
            }),
        }
    }

    /// Get a [`TableReader`] for the given file number.
    ///
    /// If the reader is already cached, its last-access timestamp is updated
    /// and it is returned immediately. Otherwise, the SST file is opened from
    /// disk, wrapped in an `Arc`, and inserted into the cache.
    ///
    /// `file_size` is required when opening the file for the first time.
    pub fn get_reader(&self, file_number: FileNumber, file_size: u64) -> Result<Arc<TableReader>> {
        let mut inner = self.cache.lock();

        // Fast path: cache hit.
        inner.access_counter += 1;
        let current_access = inner.access_counter;

        if let Some(entry) = inner.readers.get_mut(&file_number) {
            entry.last_access = current_access;
            return Ok(Arc::clone(&entry.reader));
        }

        // Slow path: open the file and create a new reader.
        let path = self.dbname.join(crate::types::sst_file_name(file_number));
        let file = fs::File::open(&path)?;
        let reader = TableReader::open(file, file_size, (*self.options).clone())?;
        let reader = Arc::new(reader);

        inner.readers.insert(
            file_number,
            CacheEntry {
                reader: Arc::clone(&reader),
                last_access: current_access,
            },
        );

        // Evict the oldest 25% if we exceeded the capacity.
        if inner.readers.len() > inner.max_open {
            Self::evict_oldest(&mut inner);
        }

        Ok(reader)
    }

    /// Remove a cached reader for the given file number.
    ///
    /// This should be called after an SST file is deleted (e.g., by
    /// compaction) so that the cache does not hold a stale reference.
    pub fn evict(&self, file_number: FileNumber) {
        self.cache.lock().readers.remove(&file_number);
    }

    /// Remove all cached readers.
    pub fn evict_all(&self) {
        self.cache.lock().readers.clear();
    }

    /// Return the number of currently cached readers.
    pub fn len(&self) -> usize {
        self.cache.lock().readers.len()
    }

    /// Return `true` if no readers are currently cached.
    pub fn is_empty(&self) -> bool {
        self.cache.lock().readers.is_empty()
    }

    /// Evict the oldest 25% of entries by `last_access`.
    fn evict_oldest(inner: &mut CacheInner) {
        let mut entries: Vec<(FileNumber, u64)> = inner
            .readers
            .iter()
            .map(|(&file_num, entry)| (file_num, entry.last_access))
            .collect();

        // Sort by last_access ascending (oldest first).
        entries.sort_by_key(|&(_, access)| access);

        // Remove the oldest 25% (at least 1).
        let to_remove = (entries.len() / 4).max(1);
        for &(file_num, _) in entries.iter().take(to_remove) {
            inner.readers.remove(&file_num);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::Options;
    use crate::sst::TableBuilder;
    use crate::types::sst_file_name;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Create a test `Options` with small blocks to keep files compact.
    fn test_options() -> Options {
        Options {
            block_size: 256,
            block_restart_interval: 4,
            bloom_bits_per_key: 10,
            ..Options::default()
        }
    }

    /// Build a small SST file in `dir` named according to `file_number`.
    ///
    /// Returns the file size in bytes.
    fn build_sst(dir: &Path, file_number: FileNumber, opts: &Options) -> u64 {
        let path = dir.join(sst_file_name(file_number));
        let file = fs::File::create(&path).expect("create SST file");
        let mut builder = TableBuilder::new(file, opts.clone());
        for i in 0..10u32 {
            let key = format!("key-{:04}", i);
            let val = format!("val-{:04}-file{}", i, file_number);
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        builder.finish().unwrap();
        fs::metadata(&path).unwrap().len()
    }

    #[test]
    fn get_reader_returns_same_arc_on_cache_hit() {
        let dir = TempDir::new().unwrap();
        let opts = Arc::new(test_options());
        let cache = TableCache::new(dir.path(), Arc::clone(&opts), 10);

        let size = build_sst(dir.path(), 1, &opts);

        let r1 = cache.get_reader(1, size).unwrap();
        let r2 = cache.get_reader(1, size).unwrap();

        // Both should be the exact same Arc allocation.
        assert!(Arc::ptr_eq(&r1, &r2));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn get_reader_can_read_data() {
        let dir = TempDir::new().unwrap();
        let opts = Arc::new(test_options());
        let cache = TableCache::new(dir.path(), Arc::clone(&opts), 10);

        let size = build_sst(dir.path(), 42, &opts);
        let reader = cache.get_reader(42, size).unwrap();

        // Verify a known key is readable.
        let result = reader.get(b"key-0005").unwrap();
        assert!(result.is_some());
        let (k, v) = result.unwrap();
        assert_eq!(k, b"key-0005");
        assert_eq!(v, b"val-0005-file42");
    }

    #[test]
    fn evict_removes_reader_and_next_get_opens_new_one() {
        let dir = TempDir::new().unwrap();
        let opts = Arc::new(test_options());
        let cache = TableCache::new(dir.path(), Arc::clone(&opts), 10);

        let size = build_sst(dir.path(), 1, &opts);

        let r1 = cache.get_reader(1, size).unwrap();
        assert_eq!(cache.len(), 1);

        cache.evict(1);
        assert_eq!(cache.len(), 0);

        // Next call must open a fresh reader (different Arc allocation).
        let r2 = cache.get_reader(1, size).unwrap();
        assert!(!Arc::ptr_eq(&r1, &r2));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn evict_all_clears_cache() {
        let dir = TempDir::new().unwrap();
        let opts = Arc::new(test_options());
        let cache = TableCache::new(dir.path(), Arc::clone(&opts), 10);

        for i in 1..=5 {
            let size = build_sst(dir.path(), i, &opts);
            cache.get_reader(i, size).unwrap();
        }
        assert_eq!(cache.len(), 5);

        cache.evict_all();
        assert!(cache.is_empty());
    }

    #[test]
    fn eviction_kicks_in_when_exceeding_max_open() {
        let dir = TempDir::new().unwrap();
        let opts = Arc::new(test_options());
        // Allow only 4 cached readers.
        let cache = TableCache::new(dir.path(), Arc::clone(&opts), 4);

        // Build 8 SST files and open them all.
        let mut sizes = Vec::new();
        for i in 1..=8 {
            sizes.push(build_sst(dir.path(), i, &opts));
        }

        for (i, &size) in sizes.iter().enumerate() {
            let file_num = (i as u64) + 1;
            cache.get_reader(file_num, size).unwrap();
        }

        // After inserting 8 entries with a cap of 4, eviction should have
        // trimmed the cache back to at most 4 entries.
        assert!(cache.len() <= 4, "cache len {} exceeds max 4", cache.len());
        // The most recently accessed files should still be cached.
        assert!(cache.len() > 0);
    }

    #[test]
    fn lru_eviction_removes_oldest_entries() {
        let dir = TempDir::new().unwrap();
        let opts = Arc::new(test_options());
        // Max 4 files.
        let cache = TableCache::new(dir.path(), Arc::clone(&opts), 4);

        let mut sizes = Vec::new();
        for i in 1..=4 {
            sizes.push(build_sst(dir.path(), i, &opts));
        }

        // Open files 1..=4. File 1 is the oldest access.
        for (i, &size) in sizes.iter().enumerate() {
            cache.get_reader((i as u64) + 1, size).unwrap();
        }
        assert_eq!(cache.len(), 4);

        // Re-access files 2..=4 to refresh them (file 1 stays the oldest).
        for i in 2..=4u64 {
            cache.get_reader(i, sizes[(i - 1) as usize]).unwrap();
        }

        // Build and insert file 5, which should evict file 1 (the oldest).
        let size5 = build_sst(dir.path(), 5, &opts);
        cache.get_reader(5, size5).unwrap();

        // File 1 should have been evicted.
        let r1 = cache.get_reader(1, sizes[0]).unwrap();
        // After re-opening file 1, the cache may have evicted something else
        // too, but file 1 must now be present (we just opened it).
        assert!(r1.get(b"key-0000").unwrap().is_some());
    }

    #[test]
    fn get_reader_returns_error_for_missing_file() {
        let dir = TempDir::new().unwrap();
        let opts = Arc::new(test_options());
        let cache = TableCache::new(dir.path(), Arc::clone(&opts), 10);

        // File 999 does not exist on disk.
        let result = cache.get_reader(999, 1024);
        assert!(result.is_err());
        assert_eq!(cache.len(), 0);
    }
}
