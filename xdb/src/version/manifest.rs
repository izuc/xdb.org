//! MANIFEST file writer and reader.
//!
//! The MANIFEST records the history of `VersionEdit`s that describe the
//! evolution of the LSM-tree structure. It uses the same physical log format
//! as the WAL (see [`crate::wal`]) so that each edit is a single log record
//! with CRC integrity protection.

use crate::error::Result;
use crate::wal::{WalReader, WalWriter};
use super::edit::VersionEdit;
use std::fs::File;

/// Writes `VersionEdit`s to a MANIFEST file using the WAL log format.
///
/// Each edit is serialized via [`VersionEdit::encode`] and written as a
/// single log record. After each write the file is fsynced to ensure
/// durability.
pub struct ManifestWriter {
    writer: WalWriter,
}

impl ManifestWriter {
    /// Create a new `ManifestWriter` that writes to the given file.
    pub fn new(file: File) -> Self {
        ManifestWriter {
            writer: WalWriter::new(file),
        }
    }

    /// Serialize and append a `VersionEdit` to the MANIFEST.
    ///
    /// The edit is encoded, written as a log record, and then fsynced.
    pub fn add_edit(&mut self, edit: &VersionEdit) -> Result<()> {
        let data = edit.encode();
        self.writer.add_record(&data)?;
        self.writer.sync()
    }
}

/// Reads `VersionEdit`s from a MANIFEST file.
///
/// Records are read using [`WalReader`] and then decoded via
/// [`VersionEdit::decode`].
pub struct ManifestReader {
    reader: WalReader,
}

impl ManifestReader {
    /// Create a new `ManifestReader` that reads from the given file.
    pub fn new(file: File) -> Self {
        ManifestReader {
            reader: WalReader::new(file),
        }
    }

    /// Read and decode the next `VersionEdit` from the MANIFEST.
    ///
    /// Returns `Ok(Some(edit))` for each record, `Ok(None)` at EOF, or
    /// `Err` if the data is corrupt.
    pub fn read_edit(&mut self) -> Result<Option<VersionEdit>> {
        match self.reader.read_record()? {
            Some(data) => Ok(Some(VersionEdit::decode(&data)?)),
            None => Ok(None),
        }
    }
}
