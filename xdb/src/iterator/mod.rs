//! Iterator traits and implementations for xdb.
//!
//! All internal iterators implement [`XdbIterator`], which provides a
//! cursor-style interface over sorted key-value entries.  Keys are raw
//! byte slices containing encoded internal keys.

pub mod merge;
pub mod two_level;

pub use merge::MergingIterator;

/// The core iterator interface for xdb.
///
/// All internal iterators implement this trait.  Keys and values are raw
/// byte slices; for internal iterators the keys are encoded internal keys
/// (user_key + 8-byte sequence/type tag).
pub trait XdbIterator {
    /// Returns `true` if the iterator is positioned at a valid entry.
    fn valid(&self) -> bool;

    /// Position at the first entry (smallest key).
    fn seek_to_first(&mut self);

    /// Position at the first entry with key >= `target`.
    fn seek(&mut self, target: &[u8]);

    /// Advance to the next entry.
    ///
    /// # Panics
    ///
    /// Panics (or produces undefined results) if `valid()` is `false`.
    fn next(&mut self);

    /// The key of the current entry.
    ///
    /// # Panics
    ///
    /// Panics (or produces undefined results) if `valid()` is `false`.
    fn key(&self) -> &[u8];

    /// The value of the current entry.
    ///
    /// # Panics
    ///
    /// Panics (or produces undefined results) if `valid()` is `false`.
    fn value(&self) -> &[u8];
}
