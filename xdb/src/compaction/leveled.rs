//! Leveled compaction logic.
//!
//! Leveled compaction merges files from level N into level N+1, keeping
//! each level (except L0) sorted and non-overlapping. When a level exceeds
//! its size budget, files are selected for compaction and merged with
//! overlapping files in the next level.

use crate::error::Result;
use crate::iterator::{MergingIterator, XdbIterator};
use crate::options::Options;
use crate::sst::{TableBuilder, TableReader};
use crate::types::*;
use crate::version::edit::{FileMetaData, VersionEdit};
use crate::version::Version;
use std::fs;
use std::path::Path;

// ---------------------------------------------------------------------------
// CompactionState
// ---------------------------------------------------------------------------

/// State for a single compaction operation.
///
/// Tracks the source level, the input files from both levels, and the
/// output files produced by the merge.
pub struct CompactionState {
    /// Source level (files are merged from here into `level + 1`).
    pub level: usize,
    /// Input files: `[0]` = files from `level`, `[1]` = files from `level + 1`.
    pub input_files: Vec<Vec<FileMetaData>>,
    /// Output files produced by the compaction.
    pub output_files: Vec<FileMetaData>,
}

// ---------------------------------------------------------------------------
// File selection
// ---------------------------------------------------------------------------

/// Pick files for compaction at the given level.
///
/// For L0, all files are selected (they can overlap). For L1+, the first
/// file is selected (a more sophisticated policy would pick the file with
/// the most overlap or largest size). Overlapping files in `level + 1` are
/// also collected.
pub fn pick_compaction_files(
    version: &Version,
    level: usize,
    options: &Options,
) -> CompactionState {
    let mut state = CompactionState {
        level,
        input_files: vec![Vec::new(), Vec::new()],
        output_files: Vec::new(),
    };

    if level == 0 {
        // For L0, compact ALL L0 files since they may overlap.
        state.input_files[0] = version.files[0].clone();
    } else {
        // For L1+, pick the first file. A smarter policy (e.g. pick the
        // largest or the one with the most overlap) is left for Phase 2.
        if let Some(file) = version.files[level].first() {
            state.input_files[0].push(file.clone());
        }
    }

    // Find overlapping files in level+1.
    if !state.input_files[0].is_empty() && level + 1 < options.num_levels {
        // Compute the key range across all input files.
        let (mut smallest, mut largest) = (
            state.input_files[0][0].smallest_key.as_bytes().to_vec(),
            state.input_files[0][0].largest_key.as_bytes().to_vec(),
        );
        for f in &state.input_files[0] {
            if f.smallest_key.as_bytes() < smallest.as_slice() {
                smallest = f.smallest_key.as_bytes().to_vec();
            }
            if f.largest_key.as_bytes() > largest.as_slice() {
                largest = f.largest_key.as_bytes().to_vec();
            }
        }

        let overlapping = version.get_overlapping_files(level + 1, &smallest, &largest);
        state.input_files[1] = overlapping.into_iter().cloned().collect();
    }

    state
}

// ---------------------------------------------------------------------------
// Compaction execution
// ---------------------------------------------------------------------------

/// Execute a compaction: merge input files, write output files, return a
/// `VersionEdit` describing the changes.
///
/// Input SST files from both levels are opened, merged via a
/// `MergingIterator`, and written into new output files at `level + 1`.
/// Duplicate user keys are collapsed (only the newest version is kept),
/// and deletion tombstones at the bottom level are dropped.
pub fn compact(
    dbname: &Path,
    state: &mut CompactionState,
    options: &Options,
    next_file_number: &mut FileNumber,
) -> Result<VersionEdit> {
    let mut edit = VersionEdit::new();

    // Collect range tombstones from input files.
    let mut range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new(); // (start, end, sequence)

    for files in &state.input_files {
        for file_meta in files {
            let path = dbname.join(sst_file_name(file_meta.number));
            let f = fs::File::open(&path)?;
            let file_size = f.metadata()?.len();
            let reader = TableReader::open(f, file_size, options.clone())?;
            let entries = reader.iter_entries()?;
            for (key, value) in &entries {
                if let Some(parsed) = ParsedInternalKey::from_bytes(key) {
                    if parsed.value_type == ValueType::RangeDeletion {
                        range_tombstones.push((
                            parsed.user_key.to_vec(),
                            value.clone(),
                            parsed.sequence,
                        ));
                    }
                }
            }
        }
    }

    // Open all input files and create iterators.
    let mut iters: Vec<Box<dyn XdbIterator>> = Vec::new();

    for files in &state.input_files {
        for file_meta in files {
            let path = dbname.join(sst_file_name(file_meta.number));
            let f = fs::File::open(&path)?;
            let file_size = f.metadata()?.len();
            let reader = TableReader::open(f, file_size, options.clone())?;
            let entries = reader.iter_entries()?;
            iters.push(Box::new(VecIterator::new(entries)));
        }
    }

    // Merge all iterators into a single sorted stream.
    let mut merger = MergingIterator::new(iters);
    merger.seek_to_first();

    // Write output files.
    let mut current_builder: Option<TableBuilder> = None;
    let mut current_file_number: FileNumber = 0;
    let mut current_smallest: Option<Vec<u8>> = None;
    let mut current_largest: Vec<u8> = Vec::new();
    let target_level = state.level + 1;
    let mut last_user_key: Vec<u8> = Vec::new();

    while merger.valid() {
        let key = merger.key();
        let value = merger.value();

        // Skip duplicate user keys: keep only the newest (first encountered).
        let user_key = extract_user_key(key);
        if user_key == last_user_key.as_slice() && !last_user_key.is_empty() {
            merger.next();
            continue;
        }
        last_user_key = user_key.to_vec();

        // Extract tag information for range tombstone and deletion checks.
        let tag = extract_tag(key);
        let seq = tag_sequence(tag);
        let vt = tag_value_type(tag);

        // Range tombstone entries themselves are passed through to output.
        if vt == Some(ValueType::RangeDeletion) {
            // Fall through to the add logic below.
        } else {
            // Check if this key is covered by a range tombstone.
            let mut is_range_deleted = false;
            for (start, end, tomb_seq) in &range_tombstones {
                if user_key >= start.as_slice()
                    && user_key < end.as_slice()
                    && *tomb_seq >= seq
                {
                    is_range_deleted = true;
                    break;
                }
            }
            if is_range_deleted {
                merger.next();
                continue;
            }
        }

        // Drop deletion tombstones at the bottom level where no older files
        // could still reference the key.
        if let Some(ValueType::Deletion) = vt {
            if target_level == options.num_levels - 1 {
                merger.next();
                continue;
            }
        }

        // Start a new output file if needed.
        if current_builder.is_none() {
            current_file_number = *next_file_number;
            *next_file_number += 1;
            let path = dbname.join(sst_file_name(current_file_number));
            let f = fs::File::create(&path)?;
            current_builder = Some(TableBuilder::new(f, options.clone()));
            current_smallest = Some(key.to_vec());
        }

        let builder = current_builder.as_mut().unwrap();
        builder.add(key, value)?;
        current_largest = key.to_vec();

        // Finish the output file if it exceeds the target size.
        if builder.file_size() >= options.target_file_size(target_level) {
            // Take the builder out of the Option so we can call finish(self).
            let finished_builder = current_builder.take().unwrap();
            let file_size = finished_builder.finish()?;
            let meta = FileMetaData {
                number: current_file_number,
                file_size,
                smallest_key: InternalKey::from_bytes(current_smallest.take().unwrap()),
                largest_key: InternalKey::from_bytes(current_largest.clone()),
            };
            state.output_files.push(meta.clone());
            edit.add_file(target_level, meta);
            // current_builder is already None after take().
        }

        merger.next();
    }

    // Finish the last output file.
    if let Some(finished_builder) = current_builder.take() {
        let file_size = finished_builder.finish()?;
        if file_size > 0 {
            let meta = FileMetaData {
                number: current_file_number,
                file_size,
                smallest_key: InternalKey::from_bytes(
                    current_smallest.unwrap_or_default(),
                ),
                largest_key: InternalKey::from_bytes(current_largest),
            };
            state.output_files.push(meta.clone());
            edit.add_file(target_level, meta);
        }
    }

    // Mark input files as deleted.
    for (i, files) in state.input_files.iter().enumerate() {
        let del_level = if i == 0 { state.level } else { state.level + 1 };
        for f in files {
            edit.delete_file(del_level, f.number);
        }
    }

    Ok(edit)
}

// ---------------------------------------------------------------------------
// VecIterator — simple in-memory iterator for compaction input
// ---------------------------------------------------------------------------

/// A vector-based iterator over pre-loaded key-value pairs.
///
/// Used to wrap the entries read from a `TableReader` so they can be fed
/// into a `MergingIterator`.
struct VecIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl VecIterator {
    fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        VecIterator {
            entries,
            pos: usize::MAX, // invalid until seek
        }
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
        self.pos = self
            .entries
            .partition_point(|(k, _)| compare_internal_key(k, target) == std::cmp::Ordering::Less);
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
}
