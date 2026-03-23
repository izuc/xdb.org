//! Database statistics for observability and tuning.
//!
//! All counters are lock-free atomics that can be read from any thread.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters tracking database operations and performance.
pub struct Statistics {
    // -- write path --
    /// Total number of write operations (puts + deletes).
    pub writes: AtomicU64,
    /// Total bytes written to the database (keys + values).
    pub bytes_written: AtomicU64,
    /// Sum of all batch sizes (for computing average batch size).
    pub batch_size_total: AtomicU64,

    // -- read path --
    /// Total number of read (get) operations.
    pub reads: AtomicU64,
    /// Total bytes read from the database (values returned).
    pub bytes_read: AtomicU64,

    // -- memtable --
    /// Number of reads satisfied by the memtable.
    pub memtable_hits: AtomicU64,
    /// Number of reads that missed the memtable.
    pub memtable_misses: AtomicU64,

    // -- cache --
    /// Number of block cache hits.
    pub cache_hits: AtomicU64,
    /// Number of block cache misses.
    pub cache_misses: AtomicU64,

    // -- bloom filter --
    /// Number of times the bloom filter avoided an unnecessary read.
    pub bloom_useful: AtomicU64,
    /// Number of times the bloom filter said "maybe" but the key was absent.
    pub bloom_not_useful: AtomicU64,

    // -- background --
    /// Number of memtable flushes to L0 SST files.
    pub flushes: AtomicU64,
    /// Number of compactions performed.
    pub compactions: AtomicU64,
    /// Total bytes read during compaction.
    pub compaction_bytes_read: AtomicU64,
    /// Total bytes written during compaction.
    pub compaction_bytes_written: AtomicU64,
}

impl Statistics {
    /// Create a new `Statistics` instance with all counters at zero.
    pub fn new() -> Self {
        Statistics {
            writes: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            batch_size_total: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            memtable_hits: AtomicU64::new(0),
            memtable_misses: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            bloom_useful: AtomicU64::new(0),
            bloom_not_useful: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            compaction_bytes_read: AtomicU64::new(0),
            compaction_bytes_written: AtomicU64::new(0),
        }
    }

    /// Increment a counter by the given amount.
    ///
    /// Uses `Relaxed` ordering since statistics are best-effort and do not
    /// need to synchronize with other memory operations.
    #[inline]
    pub fn record(counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    /// Read a counter's current value.
    ///
    /// Uses `Relaxed` ordering; the value may be slightly stale on other
    /// threads.
    #[inline]
    pub fn read(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.writes.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.batch_size_total.store(0, Ordering::Relaxed);
        self.reads.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.memtable_hits.store(0, Ordering::Relaxed);
        self.memtable_misses.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.bloom_useful.store(0, Ordering::Relaxed);
        self.bloom_not_useful.store(0, Ordering::Relaxed);
        self.flushes.store(0, Ordering::Relaxed);
        self.compactions.store(0, Ordering::Relaxed);
        self.compaction_bytes_read.store(0, Ordering::Relaxed);
        self.compaction_bytes_written.store(0, Ordering::Relaxed);
    }
}

impl Default for Statistics {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== xdb Statistics ===")?;
        writeln!(
            f,
            "Writes:              {}",
            self.writes.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Bytes written:       {}",
            self.bytes_written.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Reads:               {}",
            self.reads.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Bytes read:          {}",
            self.bytes_read.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Memtable hits:       {}",
            self.memtable_hits.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Memtable misses:     {}",
            self.memtable_misses.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Cache hits:          {}",
            self.cache_hits.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Cache misses:        {}",
            self.cache_misses.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Bloom useful:        {}",
            self.bloom_useful.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Bloom not useful:    {}",
            self.bloom_not_useful.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Flushes:             {}",
            self.flushes.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Compactions:         {}",
            self.compactions.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Compaction read:     {}",
            self.compaction_bytes_read.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "Compaction written:  {}",
            self.compaction_bytes_written.load(Ordering::Relaxed)
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_increments_correctly() {
        let stats = Statistics::new();

        Statistics::record(&stats.writes, 1);
        assert_eq!(Statistics::read(&stats.writes), 1);

        Statistics::record(&stats.writes, 5);
        assert_eq!(Statistics::read(&stats.writes), 6);

        Statistics::record(&stats.bytes_written, 1024);
        assert_eq!(Statistics::read(&stats.bytes_written), 1024);

        Statistics::record(&stats.bytes_written, 2048);
        assert_eq!(Statistics::read(&stats.bytes_written), 3072);
    }

    #[test]
    fn reset_zeros_all_counters() {
        let stats = Statistics::new();

        // Increment various counters.
        Statistics::record(&stats.writes, 10);
        Statistics::record(&stats.bytes_written, 500);
        Statistics::record(&stats.reads, 20);
        Statistics::record(&stats.bytes_read, 300);
        Statistics::record(&stats.memtable_hits, 15);
        Statistics::record(&stats.memtable_misses, 5);
        Statistics::record(&stats.cache_hits, 100);
        Statistics::record(&stats.cache_misses, 50);
        Statistics::record(&stats.bloom_useful, 80);
        Statistics::record(&stats.bloom_not_useful, 3);
        Statistics::record(&stats.flushes, 2);
        Statistics::record(&stats.compactions, 1);
        Statistics::record(&stats.compaction_bytes_read, 4096);
        Statistics::record(&stats.compaction_bytes_written, 2048);
        Statistics::record(&stats.batch_size_total, 42);

        // Reset everything.
        stats.reset();

        assert_eq!(Statistics::read(&stats.writes), 0);
        assert_eq!(Statistics::read(&stats.bytes_written), 0);
        assert_eq!(Statistics::read(&stats.batch_size_total), 0);
        assert_eq!(Statistics::read(&stats.reads), 0);
        assert_eq!(Statistics::read(&stats.bytes_read), 0);
        assert_eq!(Statistics::read(&stats.memtable_hits), 0);
        assert_eq!(Statistics::read(&stats.memtable_misses), 0);
        assert_eq!(Statistics::read(&stats.cache_hits), 0);
        assert_eq!(Statistics::read(&stats.cache_misses), 0);
        assert_eq!(Statistics::read(&stats.bloom_useful), 0);
        assert_eq!(Statistics::read(&stats.bloom_not_useful), 0);
        assert_eq!(Statistics::read(&stats.flushes), 0);
        assert_eq!(Statistics::read(&stats.compactions), 0);
        assert_eq!(Statistics::read(&stats.compaction_bytes_read), 0);
        assert_eq!(Statistics::read(&stats.compaction_bytes_written), 0);
    }

    #[test]
    fn display_formatting() {
        let stats = Statistics::new();
        Statistics::record(&stats.writes, 42);
        Statistics::record(&stats.reads, 100);
        Statistics::record(&stats.flushes, 3);

        let output = format!("{}", stats);

        assert!(output.contains("=== xdb Statistics ==="));
        assert!(output.contains("Writes:              42"));
        assert!(output.contains("Reads:               100"));
        assert!(output.contains("Flushes:             3"));
        assert!(output.contains("Compactions:         0"));
    }

    #[test]
    fn default_trait() {
        let stats = Statistics::default();

        assert_eq!(Statistics::read(&stats.writes), 0);
        assert_eq!(Statistics::read(&stats.reads), 0);
        assert_eq!(Statistics::read(&stats.flushes), 0);
        assert_eq!(Statistics::read(&stats.compactions), 0);
    }

    #[test]
    fn record_with_zero_is_noop() {
        let stats = Statistics::new();
        Statistics::record(&stats.writes, 5);
        Statistics::record(&stats.writes, 0);
        assert_eq!(Statistics::read(&stats.writes), 5);
    }

    #[test]
    fn multiple_counters_independent() {
        let stats = Statistics::new();

        Statistics::record(&stats.writes, 10);
        Statistics::record(&stats.reads, 20);
        Statistics::record(&stats.memtable_hits, 30);

        assert_eq!(Statistics::read(&stats.writes), 10);
        assert_eq!(Statistics::read(&stats.reads), 20);
        assert_eq!(Statistics::read(&stats.memtable_hits), 30);
    }
}
