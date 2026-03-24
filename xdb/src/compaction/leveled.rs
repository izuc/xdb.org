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

    // Open all input files and collect range tombstones + streaming iterators
    // in a single pass. Uses TableReader::iter() (streaming) instead of
    // iter_entries() (loads all into Vec) to avoid OOM on large compactions.
    let mut range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
    let mut iters: Vec<Box<dyn XdbIterator>> = Vec::new();

    for files in &state.input_files {
        for file_meta in files {
            let path = dbname.join(sst_file_name(file_meta.number));
            let f = fs::File::open(&path)?;
            let file_size = f.metadata()?.len();
            let reader = TableReader::open(f, file_size, options.clone())?;
            // Use cached range tombstones (parsed at open time).
            for (start, end, tomb_seq) in reader.range_tombstones() {
                range_tombstones.push((start.clone(), end.clone(), *tomb_seq));
            }
            // Use streaming iterator — reads blocks on demand, not all into memory.
            iters.push(Box::new(reader.iter()));
        }
    }

    // Sort range tombstones by start key for efficient lookup.
    range_tombstones.sort_by(|a, b| a.0.cmp(&b.0));

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

        let user_key = extract_user_key(key);
        let tag = extract_tag(key);
        let seq = tag_sequence(tag);
        let vt = tag_value_type(tag);

        // Range tombstones are EXCLUDED from point-key deduplication.
        // A RangeDeletion with user_key "apple" (meaning "delete range
        // starting at apple") must not suppress a Put("apple") entry.
        let is_range_tombstone = vt == Some(ValueType::RangeDeletion);

        if !is_range_tombstone {
            // Skip duplicate user keys: keep only the newest (first encountered).
            if user_key == last_user_key.as_slice() && !last_user_key.is_empty() {
                merger.next();
                continue;
            }
            last_user_key = user_key.to_vec();

            // Check if this key is covered by a range tombstone.
            // Uses binary search on the sorted tombstone list to find
            // candidates efficiently instead of scanning all tombstones.
            let is_range_deleted = range_tombstones
                .partition_point(|(start, _, _)| start.as_slice() <= user_key)
                .checked_sub(1)
                .map(|idx| {
                    // Check tombstones from idx downward that could cover user_key.
                    (0..=idx).rev().any(|i| {
                        let (start, end, tomb_seq) = &range_tombstones[i];
                        if start.as_slice() > user_key {
                            return false;
                        }
                        user_key < end.as_slice() && *tomb_seq >= seq
                    })
                })
                .unwrap_or(false);

            if is_range_deleted {
                merger.next();
                continue;
            }
        }

        // Drop deletion and range-deletion tombstones at the bottom level
        // where no older files could still reference the key.
        if matches!(vt, Some(ValueType::Deletion) | Some(ValueType::RangeDeletion))
            && target_level == options.num_levels - 1
        {
            merger.next();
            continue;
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

