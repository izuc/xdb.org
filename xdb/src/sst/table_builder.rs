//! SST TableBuilder -- writes sorted key-value pairs into a block-based SST file.
//!
//! ## SST file layout
//!
//! ```text
//! [data block 0] [trailer 0]
//! [data block 1] [trailer 1]
//! ...
//! [filter block]  [trailer]    (bloom filter for all keys)
//! [metaindex block] [trailer]  (maps "filter.bloom" -> filter block handle)
//! [index block] [trailer]      (maps last_key_in_block -> data block handle)
//! [footer: 48 bytes]
//! ```
//!
//! Each block trailer is 5 bytes: `compression_type: u8` + `checksum: u32 LE`
//! (CRC32 of block data concatenated with the compression_type byte).

use std::fs::File;
use std::io::{BufWriter, Write};

use crate::error::Result;
use crate::options::Options;
use crate::sst::block::BlockBuilder;
use crate::sst::bloom::BloomFilter;
use crate::sst::compression;
use crate::sst::footer::{BlockHandle, Footer, FOOTER_SIZE};

/// Size of the block trailer appended after every block on disk.
const BLOCK_TRAILER_SIZE: usize = 5;

/// Builds a complete SST file from a sorted sequence of key-value pairs.
///
/// Usage: create a `TableBuilder`, call [`add`](Self::add) for each entry in
/// sorted order, then call [`finish`](Self::finish) to finalize the file and
/// retrieve the total file size.
pub struct TableBuilder {
    writer: BufWriter<File>,
    offset: u64,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    filter_keys: Vec<Vec<u8>>,
    last_key: Vec<u8>,
    num_entries: u64,
    options: Options,
    closed: bool,
    pending_handle: Option<BlockHandle>,
}

impl TableBuilder {
    /// Create a new `TableBuilder` that writes to the given file.
    pub fn new(file: File, options: Options) -> Self {
        let restart_interval = options.block_restart_interval;
        TableBuilder {
            writer: BufWriter::new(file),
            offset: 0,
            data_block: BlockBuilder::new(restart_interval),
            index_block: BlockBuilder::new(1), // index entries are never prefix-compressed
            filter_keys: Vec::new(),
            last_key: Vec::new(),
            num_entries: 0,
            options,
            closed: false,
            pending_handle: None,
        }
    }

    /// Add a key-value pair. Keys **must** be added in sorted order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.closed, "add() called after finish()");

        // If a previous data block was flushed, write an index entry for it.
        if let Some(handle) = self.pending_handle.take() {
            // Use the last key of the flushed block as the index key.
            // A shortest-separator optimization can be added later.
            let mut handle_encoding = Vec::new();
            handle.encode(&mut handle_encoding);
            self.index_block.add(&self.last_key, &handle_encoding);
        }

        // Collect the key for the bloom filter.
        self.filter_keys.push(key.to_vec());

        // Add to the current data block.
        self.data_block.add(key, value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.num_entries += 1;

        // Flush the data block if it is large enough.
        if self.data_block.estimated_size() >= self.options.block_size {
            self.flush_data_block()?;
        }

        Ok(())
    }

    /// Finalize the SST file. Returns the total file size in bytes.
    pub fn finish(mut self) -> Result<u64> {
        // Flush any remaining data block.
        if !self.data_block.is_empty() {
            self.flush_data_block()?;
        }

        // Write the final index entry for the last data block.
        if let Some(handle) = self.pending_handle.take() {
            let mut handle_encoding = Vec::new();
            handle.encode(&mut handle_encoding);
            self.index_block.add(&self.last_key, &handle_encoding);
        }

        // ---- filter block (bloom filter for all keys) ----
        let filter_handle = if self.options.bloom_bits_per_key > 0 && !self.filter_keys.is_empty()
        {
            let bloom = BloomFilter::new(self.options.bloom_bits_per_key);
            let key_refs: Vec<&[u8]> = self.filter_keys.iter().map(|k| k.as_slice()).collect();
            let filter_data = bloom.build(&key_refs);
            Some(self.write_block(&filter_data)?)
        } else {
            None
        };

        // ---- metaindex block ----
        let mut metaindex_builder = BlockBuilder::new(1);
        if let Some(fh) = filter_handle {
            let mut handle_encoding = Vec::new();
            fh.encode(&mut handle_encoding);
            metaindex_builder.add(b"filter.bloom", &handle_encoding);
        }
        let metaindex_data = metaindex_builder.finish().to_vec();
        let metaindex_handle = self.write_block(&metaindex_data)?;

        // ---- index block ----
        let index_data = self.index_block.finish().to_vec();
        let index_handle = self.write_block(&index_data)?;

        // ---- footer ----
        let footer = Footer {
            metaindex_handle,
            index_handle,
        };
        let footer_bytes = footer.encode();
        debug_assert_eq!(footer_bytes.len(), FOOTER_SIZE);
        self.writer.write_all(&footer_bytes)?;
        self.offset += FOOTER_SIZE as u64;

        self.writer.flush()?;
        self.closed = true;

        Ok(self.offset)
    }

    /// Number of entries added so far.
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Approximate file size written so far.
    pub fn file_size(&self) -> u64 {
        self.offset
    }

    // -- internal helpers ---------------------------------------------------

    /// Flush the current data block to disk.
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let block_data = self.data_block.finish().to_vec();
        let handle = self.write_block(&block_data)?;
        self.pending_handle = Some(handle);
        self.data_block.reset();
        Ok(())
    }

    /// Write a block (data bytes) plus its 5-byte trailer to the file.
    ///
    /// The block data is compressed according to `self.options.compression`
    /// before writing. The CRC32 covers the compressed data plus the
    /// compression type byte.
    ///
    /// Returns the [`BlockHandle`] pointing to the written block.
    fn write_block(&mut self, block_data: &[u8]) -> Result<BlockHandle> {
        // Compress the block data.
        let compressed = compression::compress(block_data, self.options.compression)?;

        let handle = BlockHandle::new(self.offset, compressed.len() as u64);

        // Write compressed block data.
        self.writer.write_all(&compressed)?;

        // Compute CRC32 of compressed_data + compression_type byte.
        let compression_type = compression::compression_type_to_byte(self.options.compression);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&compressed);
        hasher.update(&[compression_type]);
        let checksum = hasher.finalize();

        // Write trailer: compression_type (1 byte) + checksum (4 bytes LE).
        self.writer.write_all(&[compression_type])?;
        self.writer.write_all(&checksum.to_le_bytes())?;

        self.offset += compressed.len() as u64 + BLOCK_TRAILER_SIZE as u64;

        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::table_reader::TableReader;
    use tempfile::NamedTempFile;

    fn default_options() -> Options {
        Options {
            block_size: 256,
            block_restart_interval: 4,
            bloom_bits_per_key: 10,
            ..Options::default()
        }
    }

    #[test]
    fn build_empty_table() {
        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.reopen().unwrap();
        let builder = TableBuilder::new(file, default_options());
        assert_eq!(builder.num_entries(), 0);
        let size = builder.finish().unwrap();
        assert!(size > 0, "even an empty table should have a footer");
    }

    #[test]
    fn build_and_read_small_table() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Build.
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut builder = TableBuilder::new(file, default_options());
            for i in 0..50u32 {
                let key = format!("key-{:04}", i).into_bytes();
                let val = format!("val-{:04}", i).into_bytes();
                builder.add(&key, &val).unwrap();
            }
            let size = builder.finish().unwrap();
            assert!(size > 0);
        }

        // Read.
        let file = std::fs::File::open(&path).unwrap();
        let meta = file.metadata().unwrap();
        let reader = TableReader::open(file, meta.len(), default_options()).unwrap();

        let entries = reader.iter_entries().unwrap();
        assert_eq!(entries.len(), 50);
        for (i, (k, v)) in entries.iter().enumerate() {
            let expected_key = format!("key-{:04}", i).into_bytes();
            let expected_val = format!("val-{:04}", i).into_bytes();
            assert_eq!(k, &expected_key);
            assert_eq!(v, &expected_val);
        }
    }

    #[test]
    fn build_table_with_many_blocks() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let opts = Options {
            block_size: 64, // very small blocks to force many flushes
            block_restart_interval: 2,
            bloom_bits_per_key: 10,
            ..Options::default()
        };

        {
            let file = std::fs::File::create(&path).unwrap();
            let mut builder = TableBuilder::new(file, opts.clone());
            for i in 0..200u32 {
                let key = format!("k{:06}", i).into_bytes();
                let val = format!("v{:06}", i).into_bytes();
                builder.add(&key, &val).unwrap();
            }
            builder.finish().unwrap();
        }

        let file = std::fs::File::open(&path).unwrap();
        let meta = file.metadata().unwrap();
        let reader = TableReader::open(file, meta.len(), opts).unwrap();

        let entries = reader.iter_entries().unwrap();
        assert_eq!(entries.len(), 200);
    }

    #[test]
    fn point_lookup() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let file = std::fs::File::create(&path).unwrap();
            let mut builder = TableBuilder::new(file, default_options());
            for i in 0..100u32 {
                let key = format!("key-{:04}", i).into_bytes();
                let val = format!("val-{:04}", i).into_bytes();
                builder.add(&key, &val).unwrap();
            }
            builder.finish().unwrap();
        }

        let file = std::fs::File::open(&path).unwrap();
        let meta = file.metadata().unwrap();
        let reader = TableReader::open(file, meta.len(), default_options()).unwrap();

        // Existing key.
        let result = reader.get(b"key-0042").unwrap();
        assert!(result.is_some());
        let (k, v) = result.unwrap();
        assert_eq!(k, b"key-0042");
        assert_eq!(v, b"val-0042");

        // Non-existing key.
        let result = reader.get(b"no-such-key").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn no_bloom_filter() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let opts = Options {
            bloom_bits_per_key: 0,
            block_size: 256,
            block_restart_interval: 4,
            ..Options::default()
        };

        {
            let file = std::fs::File::create(&path).unwrap();
            let mut builder = TableBuilder::new(file, opts.clone());
            builder.add(b"aaa", b"111").unwrap();
            builder.add(b"bbb", b"222").unwrap();
            builder.finish().unwrap();
        }

        let file = std::fs::File::open(&path).unwrap();
        let meta = file.metadata().unwrap();
        let reader = TableReader::open(file, meta.len(), opts).unwrap();

        let result = reader.get(b"aaa").unwrap();
        assert_eq!(result, Some((b"aaa".to_vec(), b"111".to_vec())));
    }
}
