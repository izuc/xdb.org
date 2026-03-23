//! WAL reader that reconstructs logical records from the physical log format.
//!
//! Reads back records written by [`super::WalWriter`].  Multi-fragment records
//! (FIRST / MIDDLE / LAST) are transparently reassembled into the original
//! logical record.

use std::fs::File;
use std::io::{BufReader, Read};

use crate::error::{Error, Result};

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

/// Reads logical records from a WAL file written in the LevelDB/RocksDB log
/// format.
///
/// Handles multi-fragment records transparently and verifies CRC32 checksums
/// on every physical record.
pub struct WalReader {
    reader: BufReader<File>,
    /// Contents of the current block (up to [`BLOCK_SIZE`] bytes).
    block_buf: Vec<u8>,
    /// Read position within `block_buf`.
    block_offset: usize,
    /// True once the underlying file has been fully read.
    eof: bool,
}

impl WalReader {
    /// Create a new `WalReader` that reads from `file`.
    pub fn new(file: File) -> Self {
        WalReader {
            reader: BufReader::new(file),
            block_buf: Vec::new(),
            block_offset: 0,
            eof: false,
        }
    }

    /// Read the next logical record from the log.
    ///
    /// Returns `Ok(Some(data))` for each record, `Ok(None)` at end-of-file,
    /// or `Err` if the data is corrupt (bad CRC, truncated record, etc.).
    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut result: Vec<u8> = Vec::new();
        let mut in_fragmented_record = false;

        loop {
            // Refill the block buffer when we have consumed everything.
            if self.block_offset >= self.block_buf.len() {
                if !self.read_next_block()? {
                    // EOF
                    if in_fragmented_record {
                        return Err(Error::corruption(
                            "WAL: partial record at end of file",
                        ));
                    }
                    return Ok(None);
                }
            }

            let remaining = self.block_buf.len() - self.block_offset;

            // If fewer than HEADER_SIZE bytes remain this is zero-padding;
            // skip to the next block.
            if remaining < HEADER_SIZE {
                self.block_offset = self.block_buf.len();
                continue;
            }

            // Parse the 7-byte header.
            let header_start = self.block_offset;
            let crc_stored = u32::from_le_bytes(
                self.block_buf[header_start..header_start + 4]
                    .try_into()
                    .unwrap(),
            );
            let length = u16::from_le_bytes(
                self.block_buf[header_start + 4..header_start + 6]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let record_type = self.block_buf[header_start + 6];

            let data_start = header_start + HEADER_SIZE;
            let data_end = data_start + length;

            if data_end > self.block_buf.len() {
                return Err(Error::corruption(
                    "WAL: physical record extends past end of block",
                ));
            }

            let data = &self.block_buf[data_start..data_end];

            // Verify CRC32.
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[record_type]);
            hasher.update(data);
            let crc_computed = hasher.finalize();

            if crc_stored != crc_computed {
                return Err(Error::corruption(format!(
                    "WAL: CRC mismatch (stored={:#010x}, computed={:#010x})",
                    crc_stored, crc_computed
                )));
            }

            // Advance past this physical record.
            self.block_offset = data_end;

            match record_type {
                RECORD_TYPE_FULL => {
                    if in_fragmented_record {
                        return Err(Error::corruption(
                            "WAL: FULL record inside fragmented record",
                        ));
                    }
                    return Ok(Some(data.to_vec()));
                }
                RECORD_TYPE_FIRST => {
                    if in_fragmented_record {
                        return Err(Error::corruption(
                            "WAL: FIRST record inside fragmented record",
                        ));
                    }
                    result = data.to_vec();
                    in_fragmented_record = true;
                }
                RECORD_TYPE_MIDDLE => {
                    if !in_fragmented_record {
                        return Err(Error::corruption(
                            "WAL: MIDDLE record without preceding FIRST",
                        ));
                    }
                    result.extend_from_slice(data);
                }
                RECORD_TYPE_LAST => {
                    if !in_fragmented_record {
                        return Err(Error::corruption(
                            "WAL: LAST record without preceding FIRST",
                        ));
                    }
                    result.extend_from_slice(data);
                    return Ok(Some(result));
                }
                _ => {
                    return Err(Error::corruption(format!(
                        "WAL: unknown record type {}",
                        record_type
                    )));
                }
            }
        }
    }

    /// Read the next block from the file into `block_buf`.
    ///
    /// Returns `true` if any bytes were read, `false` on EOF.
    fn read_next_block(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }

        self.block_buf.resize(BLOCK_SIZE, 0);
        let mut total_read = 0;

        // A single `read` call may return fewer bytes than requested, so we
        // loop until we fill the block or hit EOF.
        while total_read < BLOCK_SIZE {
            let n = self.reader.read(&mut self.block_buf[total_read..])?;
            if n == 0 {
                self.eof = true;
                break;
            }
            total_read += n;
        }

        if total_read == 0 {
            self.block_buf.clear();
            return Ok(false);
        }

        // Truncate to actual bytes read (the last block may be short).
        self.block_buf.truncate(total_read);
        self.block_offset = 0;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalWriter;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    /// Helper: open the temp file for writing, then re-open for reading.
    fn writer_for(tmp: &NamedTempFile) -> WalWriter {
        let file = OpenOptions::new()
            .write(true)
            .open(tmp.path())
            .unwrap();
        WalWriter::new(file)
    }

    fn reader_for(tmp: &NamedTempFile) -> WalReader {
        let file = OpenOptions::new()
            .read(true)
            .open(tmp.path())
            .unwrap();
        WalReader::new(file)
    }

    #[test]
    fn single_small_record() {
        let tmp = NamedTempFile::new().unwrap();
        let mut w = writer_for(&tmp);
        w.add_record(b"hello world").unwrap();
        w.sync().unwrap();

        let mut r = reader_for(&tmp);
        let rec = r.read_record().unwrap().expect("expected a record");
        assert_eq!(rec, b"hello world");
        assert!(r.read_record().unwrap().is_none(), "expected EOF");
    }

    #[test]
    fn record_spanning_multiple_blocks() {
        let tmp = NamedTempFile::new().unwrap();
        let mut w = writer_for(&tmp);

        // Create data larger than one block to force fragmentation.
        let big = vec![0xABu8; BLOCK_SIZE * 3];
        w.add_record(&big).unwrap();
        w.sync().unwrap();

        let mut r = reader_for(&tmp);
        let rec = r.read_record().unwrap().expect("expected a record");
        assert_eq!(rec, big);
        assert!(r.read_record().unwrap().is_none());
    }

    #[test]
    fn multiple_records() {
        let tmp = NamedTempFile::new().unwrap();
        let mut w = writer_for(&tmp);

        let messages: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("record number {}", i).into_bytes())
            .collect();

        for msg in &messages {
            w.add_record(msg).unwrap();
        }
        w.sync().unwrap();

        let mut r = reader_for(&tmp);
        for expected in &messages {
            let rec = r.read_record().unwrap().expect("expected a record");
            assert_eq!(&rec, expected);
        }
        assert!(r.read_record().unwrap().is_none());
    }

    #[test]
    fn crc_corruption_detected() {
        let tmp = NamedTempFile::new().unwrap();
        let mut w = writer_for(&tmp);
        w.add_record(b"important data").unwrap();
        w.sync().unwrap();

        // Corrupt one byte of the data portion (byte index 7 is the first
        // data byte, right after the 7-byte header).
        {
            let mut raw = std::fs::read(tmp.path()).unwrap();
            assert!(raw.len() > HEADER_SIZE);
            raw[HEADER_SIZE] ^= 0xFF; // flip all bits
            std::fs::write(tmp.path(), &raw).unwrap();
        }

        let mut r = reader_for(&tmp);
        let err = r.read_record().unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("CRC mismatch"),
            "expected CRC mismatch error, got: {}",
            msg
        );
    }

    #[test]
    fn empty_record() {
        let tmp = NamedTempFile::new().unwrap();
        let mut w = writer_for(&tmp);
        w.add_record(b"").unwrap();
        w.sync().unwrap();

        let mut r = reader_for(&tmp);
        let rec = r.read_record().unwrap().expect("expected a record");
        assert!(rec.is_empty());
        assert!(r.read_record().unwrap().is_none());
    }

    #[test]
    fn record_exactly_filling_block() {
        let tmp = NamedTempFile::new().unwrap();
        let mut w = writer_for(&tmp);

        // Data that exactly fills the first block (block minus header).
        let exact = vec![0x42u8; BLOCK_SIZE - HEADER_SIZE];
        w.add_record(&exact).unwrap();
        // A second small record should land in the next block.
        w.add_record(b"second").unwrap();
        w.sync().unwrap();

        let mut r = reader_for(&tmp);
        let rec1 = r.read_record().unwrap().expect("expected first record");
        assert_eq!(rec1, exact);
        let rec2 = r.read_record().unwrap().expect("expected second record");
        assert_eq!(rec2, b"second");
        assert!(r.read_record().unwrap().is_none());
    }

    #[test]
    fn padding_when_header_does_not_fit() {
        let tmp = NamedTempFile::new().unwrap();
        let mut w = writer_for(&tmp);

        // Fill the block so that exactly (HEADER_SIZE - 1) bytes remain,
        // forcing the next record to start on a new block with zero padding.
        let fill_size = BLOCK_SIZE - HEADER_SIZE - (HEADER_SIZE - 1);
        let filler = vec![0xCCu8; fill_size];
        w.add_record(&filler).unwrap();
        w.add_record(b"after pad").unwrap();
        w.sync().unwrap();

        let mut r = reader_for(&tmp);
        let rec1 = r.read_record().unwrap().expect("expected filler record");
        assert_eq!(rec1, filler);
        let rec2 = r.read_record().unwrap().expect("expected second record");
        assert_eq!(rec2, b"after pad");
        assert!(r.read_record().unwrap().is_none());
    }
}
