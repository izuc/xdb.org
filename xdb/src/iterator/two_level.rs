//! Two-level iterator for SST table reads.
//!
//! Two-level iteration is handled directly by
//! [`TableIterator`](crate::sst::table_reader::TableIterator), which uses an
//! index block to locate data blocks and iterates within each one.
//!
//! This module is retained as a placeholder for a future standalone
//! `TwoLevelIterator` component if the logic is factored out of
//! `TableReader`.

#![allow(dead_code, unused_imports)]

use super::XdbIterator;

/// Placeholder for a future standalone two-level iterator.
///
/// Currently, `TableIterator` implements two-level iteration internally
/// (including `seek_to_last` / `prev` for reverse scanning).
pub struct TwoLevelIterator {
    _private: (),
}
