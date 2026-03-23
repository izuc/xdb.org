//! Compaction: merging overlapping SST files into fewer, sorted files.
//!
//! Currently implements leveled compaction. Size-tiered and FIFO strategies
//! may be added in future phases.

pub mod leveled;

pub use leveled::{compact, pick_compaction_files, CompactionState};
