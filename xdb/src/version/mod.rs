//! Version management for the LSM-tree.
//!
//! A `Version` is an immutable snapshot of which SST files exist at each
//! level. The `VersionSet` manages the sequence of versions and persists
//! changes to the MANIFEST file. Each change is described by a `VersionEdit`
//! which records added/deleted files and metadata updates.

pub mod edit;
pub mod manifest;

pub use edit::{FileMetaData, VersionEdit};
pub use manifest::{ManifestReader, ManifestWriter};

use crate::error::{Error, Result};
use crate::options::Options;
use crate::types::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

/// An immutable snapshot of the LSM-tree structure (which files at which levels).
///
/// Level 0 may have overlapping key ranges. Levels 1+ are sorted and
/// non-overlapping within each level.
#[derive(Clone, Debug)]
pub struct Version {
    /// Files at each level. Index 0 = L0, index 1 = L1, etc.
    pub files: Vec<Vec<FileMetaData>>,
    /// Number of levels in this version.
    pub num_levels: usize,
}

impl Version {
    /// Create an empty version with the given number of levels.
    pub fn new(num_levels: usize) -> Self {
        Version {
            files: vec![Vec::new(); num_levels],
            num_levels,
        }
    }

    /// Return the files at a given level.
    pub fn files_at_level(&self, level: usize) -> &[FileMetaData] {
        &self.files[level]
    }

    /// Total number of files across all levels.
    pub fn total_file_count(&self) -> usize {
        self.files.iter().map(|f| f.len()).sum()
    }

    /// Total bytes across all files at a given level.
    pub fn level_size(&self, level: usize) -> u64 {
        self.files[level].iter().map(|f| f.file_size).sum()
    }

    /// Find files at a level that overlap the given key range.
    ///
    /// For L0 all files are checked (they can overlap). For L1+ the sorted
    /// non-overlapping invariant allows an early exit once we pass the range.
    pub fn get_overlapping_files(
        &self,
        level: usize,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<&FileMetaData> {
        let files = &self.files[level];
        if level == 0 {
            // L0 files can overlap -- must check all of them.
            files
                .iter()
                .filter(|f| {
                    f.largest_key.as_bytes() >= smallest
                        && f.smallest_key.as_bytes() <= largest
                })
                .collect()
        } else {
            // L1+: files are sorted by smallest key and non-overlapping.
            // Collect files whose key range intersects [smallest, largest].
            let mut result = Vec::new();
            for f in files {
                if f.smallest_key.as_bytes() > largest {
                    break;
                }
                if f.largest_key.as_bytes() >= smallest {
                    result.push(f);
                }
            }
            result
        }
    }

    /// Apply a `VersionEdit` to produce a new `Version`.
    ///
    /// Deleted files are removed, new files are inserted, and levels 1+ are
    /// re-sorted by smallest key. L0 is sorted by file number descending
    /// (newest first).
    pub fn apply(&self, edit: &VersionEdit) -> Version {
        let mut new = self.clone();

        // Remove deleted files.
        for &(level, file_num) in &edit.deleted_files {
            if level < new.num_levels {
                new.files[level].retain(|f| f.number != file_num);
            }
        }

        // Add new files.
        for &(level, ref meta) in &edit.new_files {
            if level < new.num_levels {
                new.files[level].push(meta.clone());
            }
        }

        // Sort L1+ files by smallest key (non-overlapping invariant).
        for level in 1..new.num_levels {
            new.files[level].sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
        }

        // Sort L0 by file number descending (newest first for reads).
        new.files[0].sort_by(|a, b| b.number.cmp(&a.number));

        new
    }
}

// ---------------------------------------------------------------------------
// VersionSet
// ---------------------------------------------------------------------------

/// Manages the sequence of `Version`s and the MANIFEST file.
///
/// The `VersionSet` tracks the current version of the LSM-tree, allocates
/// file numbers, and persists all structural changes to a durable MANIFEST
/// file that can be replayed on recovery.
pub struct VersionSet {
    /// Path to the database directory.
    dbname: PathBuf,
    /// Database configuration.
    options: Options,
    /// The current (most recent) version.
    current: Arc<Version>,
    /// Next file number to allocate.
    next_file_number: FileNumber,
    /// Last sequence number written.
    last_sequence: SequenceNumber,
    /// Current WAL file number.
    log_number: FileNumber,
    /// File number of the current MANIFEST file.
    manifest_file_number: FileNumber,
    /// Writer for the current MANIFEST file (lazily opened).
    manifest_writer: Option<ManifestWriter>,
}

impl VersionSet {
    /// Create a new `VersionSet` with an empty initial version.
    ///
    /// `next_file_number` starts at 2 (file 1 is reserved for the initial
    /// MANIFEST).
    pub fn new(dbname: &Path, options: Options) -> Self {
        let num_levels = options.num_levels;
        VersionSet {
            dbname: dbname.to_path_buf(),
            options,
            current: Arc::new(Version::new(num_levels)),
            next_file_number: 2,
            last_sequence: 0,
            log_number: 0,
            manifest_file_number: 1,
            manifest_writer: None,
        }
    }

    /// Get the current version.
    pub fn current(&self) -> Arc<Version> {
        Arc::clone(&self.current)
    }

    /// Apply a `VersionEdit`: update the current version and write the edit
    /// to the MANIFEST file.
    ///
    /// If the MANIFEST writer has not been opened yet, a fresh MANIFEST is
    /// created with a full snapshot followed by the new edit.
    pub fn log_and_apply(&mut self, edit: VersionEdit) -> Result<()> {
        // Create new version by applying the edit.
        let new_version = self.current.apply(&edit);

        // Ensure we have a manifest writer.
        if self.manifest_writer.is_none() {
            self.write_snapshot()?;
        }

        // Write the edit to the MANIFEST.
        let writer = self.manifest_writer.as_mut().unwrap();
        writer.add_edit(&edit)?;

        // Install the new version.
        self.current = Arc::new(new_version);
        Ok(())
    }

    /// Recover from an existing MANIFEST file.
    ///
    /// Reads the CURRENT file to find the active MANIFEST, then replays all
    /// `VersionEdit`s to reconstruct the LSM-tree state. Returns `true` if
    /// recovery was successful, `false` if no CURRENT file exists (fresh DB).
    pub fn recover(&mut self) -> Result<bool> {
        let current_path = self.dbname.join(CURRENT_FILE_NAME);

        // If no CURRENT file, this is a fresh database.
        if !current_path.exists() {
            return Ok(false);
        }

        // Read the CURRENT file to get the MANIFEST filename.
        let manifest_name = fs::read_to_string(&current_path)
            .map_err(|e| Error::Io(e))?;
        let manifest_name = manifest_name.trim();
        if manifest_name.is_empty() {
            return Err(Error::corruption("empty CURRENT file"));
        }
        // Validate the CURRENT file contains a single valid MANIFEST name.
        if !manifest_name.starts_with("MANIFEST-") || manifest_name.contains('\n') {
            return Err(Error::corruption(format!(
                "invalid CURRENT file content: {:?}",
                manifest_name
            )));
        }

        let manifest_path = self.dbname.join(manifest_name);
        if !manifest_path.exists() {
            return Err(Error::corruption(format!(
                "MANIFEST file {} not found",
                manifest_name
            )));
        }

        // Open and read the MANIFEST.
        let file = fs::File::open(&manifest_path)?;
        let mut reader = ManifestReader::new(file);

        // Start with an empty version and replay all edits.
        let mut version = Version::new(self.options.num_levels);
        let mut log_number: Option<FileNumber> = None;
        let mut next_file_number: Option<FileNumber> = None;
        let mut last_sequence: Option<SequenceNumber> = None;

        while let Some(edit) = reader.read_edit()? {
            version = version.apply(&edit);

            if let Some(n) = edit.log_number {
                log_number = Some(n);
            }
            if let Some(n) = edit.next_file_number {
                next_file_number = Some(n);
            }
            if let Some(s) = edit.last_sequence {
                last_sequence = Some(s);
            }
        }

        // Apply recovered state.
        if let Some(n) = next_file_number {
            self.next_file_number = n;
        }
        if let Some(s) = last_sequence {
            self.last_sequence = s;
        }
        if let Some(n) = log_number {
            self.log_number = n;
        }

        // Parse the manifest file number from its name.
        // Format: "MANIFEST-NNNNNN"
        if let Some(num_str) = manifest_name.strip_prefix("MANIFEST-") {
            if let Ok(num) = num_str.parse::<u64>() {
                self.manifest_file_number = num;
            }
        }

        // Ensure next_file_number is at least one past any file we know about.
        self.mark_file_number_used(self.manifest_file_number);
        self.mark_file_number_used(self.log_number);
        for level_files in &version.files {
            for f in level_files {
                self.mark_file_number_used(f.number);
            }
        }

        self.current = Arc::new(version);
        Ok(true)
    }

    /// Allocate the next file number.
    pub fn new_file_number(&mut self) -> FileNumber {
        let num = self.next_file_number;
        self.next_file_number += 1;
        num
    }

    /// Return the last written sequence number.
    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    /// Set the last written sequence number.
    pub fn set_last_sequence(&mut self, seq: SequenceNumber) {
        self.last_sequence = seq;
    }

    /// Return the current WAL log file number.
    pub fn log_number(&self) -> FileNumber {
        self.log_number
    }

    /// Set the current WAL log file number.
    pub fn set_log_number(&mut self, num: FileNumber) {
        self.log_number = num;
    }

    /// Return the current MANIFEST file number.
    pub fn manifest_file_number(&self) -> FileNumber {
        self.manifest_file_number
    }

    /// Compute compaction scores for each level and return the level with
    /// the highest score, if that score exceeds the compaction threshold (1.0).
    ///
    /// - L0: score = num_files / level0_compaction_trigger
    /// - L1+: score = level_size / max_bytes_for_level
    ///
    /// Returns `Some((level, score))` if compaction is needed, `None` otherwise.
    pub fn pick_compaction(&self) -> Option<(usize, f64)> {
        let v = self.current();
        let mut best_level = None;
        let mut best_score = 1.0f64; // threshold: only compact if score >= 1.0

        // L0: score based on file count.
        let l0_score =
            v.files[0].len() as f64 / self.options.level0_compaction_trigger as f64;
        if l0_score >= best_score {
            best_level = Some(0);
            best_score = l0_score;
        }

        // L1+: score based on total level size vs max allowed.
        for level in 1..self.options.num_levels - 1 {
            let size = v.level_size(level);
            let max = self.options.max_bytes_for_level(level);
            let score = size as f64 / max as f64;
            if score >= best_score {
                best_level = Some(level);
                best_score = score;
            }
        }

        best_level.map(|l| (l, best_score))
    }

    /// Write a full snapshot of the current version to a new MANIFEST file.
    ///
    /// Creates a `VersionEdit` containing all files in the current version,
    /// plus metadata (log number, next file number, last sequence), and
    /// writes it as the first record. Updates the CURRENT pointer.
    fn write_snapshot(&mut self) -> Result<()> {
        let manifest_number = self.new_file_number();
        self.manifest_file_number = manifest_number;

        let manifest_name = manifest_file_name(manifest_number);
        let manifest_path = self.dbname.join(&manifest_name);
        let file = fs::File::create(&manifest_path)?;
        let mut writer = ManifestWriter::new(file);

        // Build a snapshot edit with all files.
        let mut edit = VersionEdit::new();
        edit.set_comparator_name("leveldb.BytewiseComparator".to_string());
        edit.set_log_number(self.log_number);
        edit.set_next_file_number(self.next_file_number);
        edit.set_last_sequence(self.last_sequence);

        let v = self.current();
        for (level, files) in v.files.iter().enumerate() {
            for f in files {
                edit.add_file(level, f.clone());
            }
        }

        writer.add_edit(&edit)?;
        self.manifest_writer = Some(writer);

        // Update the CURRENT file to point to this MANIFEST.
        self.set_current_file(&manifest_name)?;

        Ok(())
    }

    /// Atomically update the CURRENT file to point to the given MANIFEST.
    fn set_current_file(&self, manifest_name: &str) -> Result<()> {
        let current_path = self.dbname.join(CURRENT_FILE_NAME);
        let tmp_path = self.dbname.join("CURRENT.tmp");

        // Write to a temp file, then rename for atomicity.
        fs::write(&tmp_path, format!("{}\n", manifest_name))?;
        fs::rename(&tmp_path, &current_path)?;
        Ok(())
    }

    /// Ensure `next_file_number` is strictly greater than `num`.
    fn mark_file_number_used(&mut self, num: FileNumber) {
        if self.next_file_number <= num {
            self.next_file_number = num + 1;
        }
    }
}
