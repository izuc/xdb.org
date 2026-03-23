//! Two-level iterator for SST table reads.
//!
//! Uses an index-level iterator to locate data blocks, then iterates
//! within each data block.  This is used internally by `TableReader`.
//!
//! In Phase 1, `TableReader` handles two-level iteration inline rather
//! than through this module.  This module is reserved for Phase 2
//! optimization.

#![allow(dead_code, unused_imports)]

use super::XdbIterator;

/// Placeholder for Phase 2 two-level iterator.
///
/// Currently, `TableReader` implements two-level iteration internally.
/// This struct will be fleshed out when we factor the two-level logic
/// out of `TableReader` into a reusable component.
pub struct TwoLevelIterator {
    // Will be implemented in Phase 2.
    _private: (),
}
