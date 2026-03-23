//! Database configuration.


// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/// Compression algorithm applied to SST data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression (fastest writes/reads, largest files).
    None,
    /// LZ4 compression (fast, moderate ratio).
    #[cfg(feature = "lz4")]
    Lz4,
    /// Zstandard compression (slower, better ratio).
    #[cfg(feature = "zstd")]
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}

// ---------------------------------------------------------------------------
// Options (DB-wide)
// ---------------------------------------------------------------------------

/// Configuration for opening or creating a database.
#[derive(Debug, Clone)]
pub struct Options {
    // -- lifecycle -----------------------------------------------------------
    /// Create the database directory if it does not exist.
    pub create_if_missing: bool,

    /// Return an error if the database already exists.
    pub error_if_exists: bool,

    // -- write buffer --------------------------------------------------------
    /// Approximate size (bytes) of the active memtable before it is flushed
    /// to an L0 SST file.  Default: 4 MiB.
    pub write_buffer_size: usize,

    /// Maximum number of memtables kept in memory (active + immutable).
    /// When the limit is hit writes stall until a flush completes.
    /// Default: 2.
    pub max_write_buffer_number: usize,

    // -- LSM structure -------------------------------------------------------
    /// Number of LSM levels.  Default: 7.
    pub num_levels: usize,

    /// Number of L0 files that triggers compaction into L1.  Default: 4.
    pub level0_compaction_trigger: usize,

    /// Maximum size (bytes) of Level-1.  Each subsequent level is
    /// `max_bytes_for_level_multiplier` times larger.  Default: 64 MiB.
    pub max_bytes_for_level_base: u64,

    /// Multiplier between levels.  Default: 10.
    pub max_bytes_for_level_multiplier: f64,

    /// Target size of a single SST file (bytes).  Default: 2 MiB.
    pub target_file_size_base: u64,

    /// Multiplier applied per level: actual target =
    /// `target_file_size_base * multiplier ^ (level - 1)`.  Default: 1
    /// (same size at every level).
    pub target_file_size_multiplier: u64,

    // -- SST / block format --------------------------------------------------
    /// Approximate size of a data block in an SST file.  Default: 4 KiB.
    pub block_size: usize,

    /// Number of keys between block restart points (prefix compression
    /// resets).  Default: 16.
    pub block_restart_interval: usize,

    /// Bits per key for the Bloom filter.  0 disables filters.  Default: 10.
    pub bloom_bits_per_key: usize,

    /// Compression applied to data blocks.
    pub compression: CompressionType,

    // -- cache ---------------------------------------------------------------
    /// Block cache capacity in bytes.  0 disables the cache.  Default: 8 MiB.
    pub block_cache_capacity: usize,

    /// Maximum number of open SST files kept in the table cache.  Default: 1000.
    pub max_open_files: usize,

    // -- background work -----------------------------------------------------
    /// Maximum concurrent background compactions.  Default: 1.
    pub max_background_compactions: usize,

    /// Maximum concurrent background flushes.  Default: 1.
    pub max_background_flushes: usize,

    // -- WAL -----------------------------------------------------------------
    /// If true, every write is fsynced to the WAL before returning.
    /// Default: false (rely on OS page cache for batching).
    pub sync_writes: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: false,
            error_if_exists: false,

            write_buffer_size: 4 * 1024 * 1024,
            max_write_buffer_number: 2,

            num_levels: 7,
            level0_compaction_trigger: 4,
            max_bytes_for_level_base: 64 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10.0,
            target_file_size_base: 2 * 1024 * 1024,
            target_file_size_multiplier: 1,

            block_size: 4096,
            block_restart_interval: 16,
            bloom_bits_per_key: 10,
            compression: CompressionType::None,

            block_cache_capacity: 8 * 1024 * 1024,
            max_open_files: 1000,

            max_background_compactions: 1,
            max_background_flushes: 1,

            sync_writes: false,
        }
    }
}

impl Options {
    /// Builder-style: create the DB if missing.
    pub fn create_if_missing(mut self, v: bool) -> Self {
        self.create_if_missing = v;
        self
    }

    /// Builder-style: set write buffer size.
    pub fn write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size = bytes;
        self
    }

    /// Builder-style: set block cache capacity.
    pub fn block_cache_capacity(mut self, bytes: usize) -> Self {
        self.block_cache_capacity = bytes;
        self
    }

    /// Builder-style: set bloom filter bits per key (0 = disabled).
    pub fn bloom_bits_per_key(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }

    /// Builder-style: set number of levels.
    pub fn num_levels(mut self, n: usize) -> Self {
        self.num_levels = n;
        self
    }

    /// Builder-style: set sync_writes.
    pub fn sync_writes(mut self, v: bool) -> Self {
        self.sync_writes = v;
        self
    }

    /// Builder-style: set compression type for data blocks.
    pub fn compression(mut self, c: CompressionType) -> Self {
        self.compression = c;
        self
    }

    /// Builder-style: set maximum number of open SST files in the table cache.
    pub fn max_open_files(mut self, n: usize) -> Self {
        self.max_open_files = n;
        self
    }

    /// Maximum size target for the given level (0-indexed, L0 is level 0).
    pub fn max_bytes_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            // L0 is not size-limited; it's file-count-limited.
            return u64::MAX;
        }
        let mut size = self.max_bytes_for_level_base;
        for _ in 1..level {
            size = (size as f64 * self.max_bytes_for_level_multiplier) as u64;
        }
        size
    }

    /// Target SST file size for the given level.
    pub fn target_file_size(&self, level: usize) -> u64 {
        let mut size = self.target_file_size_base;
        for _ in 1..level {
            size = size.saturating_mul(self.target_file_size_multiplier);
        }
        size
    }
}

// ---------------------------------------------------------------------------
// ReadOptions
// ---------------------------------------------------------------------------

/// Per-read configuration.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// If true, data read from the file system will *not* be cached in the
    /// block cache.  Useful for bulk scans.
    pub fill_cache: bool,

    /// If set, read as of this snapshot sequence number.
    pub snapshot: Option<u64>,
}

impl ReadOptions {
    pub fn new() -> Self {
        ReadOptions {
            fill_cache: true,
            snapshot: None,
        }
    }
}

// ---------------------------------------------------------------------------
// WriteOptions
// ---------------------------------------------------------------------------

/// Per-write configuration.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// If true, fsync the WAL before returning from the write.
    pub sync: bool,
}
