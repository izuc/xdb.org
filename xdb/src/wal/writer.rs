//! WAL writer using the LevelDB/RocksDB physical log format.
//!
//! The log file is divided into fixed-size **blocks** of [`BLOCK_SIZE`] bytes.
//! Each logical record is stored as one or more **physical records**, each
//! carrying a 7-byte header:
//!
//! ```text
//! checksum : u32  (CRC32 of type + data, little-endian)
//! length   : u16  (little-endian, length of the data portion)
//! type     : u8   (FULL=1, FIRST=2, MIDDLE=3, LAST=4)
//! ```
//!
//! If fewer than [`HEADER_SIZE`] bytes remain in the current block the writer
//! emits zero-padding and advances to the next block.

use std::fs::File;
use std::io::{BufWriter, Write};

use crate::error::Result;

/// Size of a single block in the log file (32 KiB).
const BLOCK_SIZE: usize = 32768;

/// Size of the physical record header: CRC(4) + length(2) + type(1).
const HEADER_SIZE: usize = 7;

/// The entire logical record fits in one physical record.
const RECORD_TYPE_FULL: u8 = 1;

/// First fragment of a multi-part logical record.
const RECORD_TYPE_FIRST: u8 = 2;

/// Middle fragment(s) of a multi-part logical record.
const RECORD_TYPE_MIDDLE: u8 = 3;

/// Final fragment of a multi-part logical record.
const RECORD_TYPE_LAST: u8 = 4;

/// Writes logical records to a WAL file using the LevelDB/RocksDB log format.
///
/// Records that do not fit in the remaining space of the current block are
/// automatically split across multiple physical records (FIRST / MIDDLE / LAST).
pub struct WalWriter {
    writer: BufWriter<File>,
    /// Current byte offset within the current block (0..BLOCK_SIZE).
    block_offset: usize,
}

impl WalWriter {
    /// Create a new `WalWriter` that writes to `file`.
    ///
    /// The file should be opened in write (and typically append) mode.
    pub fn new(file: File) -> Self {
        WalWriter {
            writer: BufWriter::new(file),
            block_offset: 0,
        }
    }

    /// Append a logical record to the log.
    ///
    /// The data may be split into multiple physical records if it exceeds the
    /// remaining space in the current block.
    pub fn add_record(&mut self, data: &[u8]) -> Result<()> {
        let mut left = data.len();
        let mut data_ptr = 0;
        let mut begin = true;

        // We must emit at least one physical record even for zero-length data.
        loop {
            debug_assert!(
                self.block_offset <= BLOCK_SIZE,
                "WAL block_offset {} exceeds BLOCK_SIZE {}",
                self.block_offset,
                BLOCK_SIZE,
            );
            let leftover = BLOCK_SIZE - self.block_offset;

            // If fewer than HEADER_SIZE bytes remain, pad with zeros.
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    self.writer.write_all(&vec![0u8; leftover])?;
                }
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_length = std::cmp::min(left, avail);

            let end = fragment_length == left;
            let record_type = match (begin, end) {
                (true, true) => RECORD_TYPE_FULL,
                (true, false) => RECORD_TYPE_FIRST,
                (false, true) => RECORD_TYPE_LAST,
                (false, false) => RECORD_TYPE_MIDDLE,
            };

            self.emit_record(record_type, &data[data_ptr..data_ptr + fragment_length])?;

            data_ptr += fragment_length;
            left -= fragment_length;
            begin = false;

            if left == 0 {
                break;
            }
        }

        Ok(())
    }

    /// Flush and fsync the underlying file to durable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Write a single physical record (header + data fragment).
    fn emit_record(&mut self, record_type: u8, data: &[u8]) -> Result<()> {
        assert!(data.len() <= u16::MAX as usize);

        // CRC32 covers the type byte followed by the data fragment.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[record_type]);
        hasher.update(data);
        let crc = hasher.finalize();

        let length = data.len() as u16;

        // Header: crc(4 LE) + length(2 LE) + type(1)
        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&crc.to_le_bytes());
        header[4..6].copy_from_slice(&length.to_le_bytes());
        header[6] = record_type;

        self.writer.write_all(&header)?;
        self.writer.write_all(data)?;

        self.block_offset += HEADER_SIZE + data.len();
        Ok(())
    }
}
