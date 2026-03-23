//! Block builder and reader with prefix compression.
//!
//! A block is the basic unit of storage within an SST file. Keys are stored
//! with prefix compression: consecutive keys that share a common prefix only
//! encode the differing suffix. Restart points at regular intervals reset
//! the prefix state so that binary search can jump into the middle of a block.
//!
//! ## On-disk format
//!
//! ```text
//! [entry 0] [entry 1] ... [entry N-1]
//! [restart_offset_0: u32 LE] [restart_offset_1: u32 LE] ...
//! [num_restarts: u32 LE]
//! ```
//!
//! Each entry:
//! ```text
//! shared_key_length:   varint32
//! unshared_key_length: varint32
//! value_length:        varint32
//! key_delta:           [unshared_key_length bytes]
//! value:               [value_length bytes]
//! ```

use crate::error;
use crate::types::{decode_varint32, encode_varint32};

// ---------------------------------------------------------------------------
// BlockBuilder
// ---------------------------------------------------------------------------

/// Incrementally builds a block of sorted key-value entries with prefix
/// compression.
///
/// Keys **must** be added in sorted (non-decreasing) order.
pub struct BlockBuilder {
    data: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
    finished: bool,
}

impl BlockBuilder {
    /// Create a new block builder.
    ///
    /// `restart_interval` controls how many entries appear between restart
    /// points. A value of 16 means every 16th entry is a restart point.
    pub fn new(restart_interval: usize) -> Self {
        let restart_interval = if restart_interval < 1 {
            1
        } else {
            restart_interval
        };
        BlockBuilder {
            data: Vec::new(),
            restarts: Vec::new(),
            counter: 0,
            restart_interval,
            last_key: Vec::new(),
            finished: false,
        }
    }

    /// Add a key-value pair. Keys must be added in sorted order.
    ///
    /// # Panics
    ///
    /// Panics if called after [`finish`](Self::finish).
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished, "cannot add to a finished block");

        let shared = if self.counter % self.restart_interval == 0 {
            // Restart point: reset prefix compression and record offset.
            self.restarts.push(self.data.len() as u32);
            0
        } else {
            // Compute shared prefix length with previous key.
            shared_prefix_len(&self.last_key, key)
        };

        let non_shared = key.len() - shared;

        // Encode entry header as three varints.
        encode_varint32(&mut self.data, shared as u32);
        encode_varint32(&mut self.data, non_shared as u32);
        encode_varint32(&mut self.data, value.len() as u32);

        // Encode key delta (the unshared suffix) and value.
        self.data.extend_from_slice(&key[shared..]);
        self.data.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    /// Finalize the block and return a reference to the complete block data.
    ///
    /// After calling this, no more entries may be added.
    pub fn finish(&mut self) -> &[u8] {
        // If no entries were added, ensure we have at least one restart
        // so the format is valid.
        if self.restarts.is_empty() {
            self.restarts.push(0);
        }

        // Write restart offsets.
        for &offset in &self.restarts {
            self.data.extend_from_slice(&offset.to_le_bytes());
        }
        // Write number of restarts.
        self.data
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());

        self.finished = true;
        &self.data
    }

    /// Estimated current size of the block data in bytes (before finish).
    pub fn estimated_size(&self) -> usize {
        let num_restarts = if self.restarts.is_empty() {
            1
        } else {
            self.restarts.len()
        };
        self.data.len() + num_restarts * 4 + 4
    }

    /// Returns `true` if no entries have been added.
    pub fn is_empty(&self) -> bool {
        self.counter == 0
    }

    /// Reset the builder to start a fresh block.
    pub fn reset(&mut self) {
        self.data.clear();
        self.restarts.clear();
        self.counter = 0;
        self.last_key.clear();
        self.finished = false;
    }
}

/// Compute the length of the common prefix between two byte slices.
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// BlockReader
// ---------------------------------------------------------------------------

/// Reads a block produced by [`BlockBuilder`].
///
/// Owns the raw block data and provides an iterator over its entries.
pub struct BlockReader {
    data: Vec<u8>,
    restarts_offset: usize,
    num_restarts: u32,
}

impl BlockReader {
    /// Parse a block from raw bytes (the bytes returned by
    /// [`BlockBuilder::finish`], without the 5-byte on-disk trailer).
    pub fn new(data: Vec<u8>) -> error::Result<Self> {
        if data.len() < 4 {
            return Err(error::Error::corruption(
                "block too short to contain num_restarts",
            ));
        }

        let num_restarts =
            u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());

        let restarts_byte_len = (num_restarts as usize) * 4 + 4;
        if restarts_byte_len > data.len() {
            return Err(error::Error::corruption(format!(
                "num_restarts ({}) implies {} bytes but block is only {} bytes",
                num_restarts, restarts_byte_len, data.len()
            )));
        }

        let restarts_offset = data.len() - restarts_byte_len;

        Ok(BlockReader {
            data,
            restarts_offset,
            num_restarts,
        })
    }

    /// Create an iterator over the entries in this block.
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator {
            block: self,
            current: 0,
            key: Vec::new(),
            value_offset: 0,
            value_len: 0,
            restart_index: 0,
            valid: false,
        }
    }

    /// Number of restart points in this block.
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    /// Get the byte offset stored at the given restart index.
    fn restart_offset(&self, index: usize) -> usize {
        assert!((index as u32) < self.num_restarts);
        let pos = self.restarts_offset + index * 4;
        u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap()) as usize
    }

    /// The byte offset where entry data ends (start of restart array).
    fn data_end(&self) -> usize {
        self.restarts_offset
    }
}

// ---------------------------------------------------------------------------
// BlockIterator
// ---------------------------------------------------------------------------

/// Iterates over the key-value entries in a [`BlockReader`].
pub struct BlockIterator<'a> {
    block: &'a BlockReader,
    current: usize,
    key: Vec<u8>,
    value_offset: usize,
    value_len: usize,
    restart_index: usize,
    valid: bool,
}

impl<'a> BlockIterator<'a> {
    /// Returns `true` if the iterator is positioned at a valid entry.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Position the iterator at the first entry.
    pub fn seek_to_first(&mut self) {
        self.seek_to_restart(0);
        self.parse_next_entry();
    }

    /// Seek to the first entry whose key is >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        let num = self.block.num_restarts as usize;
        let mut left = 0usize;
        let mut right = num;

        // Binary search over restart points.
        while left < right {
            let mid = left + (right - left) / 2;
            let key_at_restart = self.decode_restart_key(mid);
            if key_at_restart.as_slice() <= target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // `left` is the first restart whose key > target; we want left - 1.
        let restart = if left > 0 { left - 1 } else { 0 };

        // Seek to that restart point and linear scan forward.
        self.seek_to_restart(restart);
        loop {
            self.parse_next_entry();
            if !self.valid {
                return;
            }
            if self.key.as_slice() >= target {
                return;
            }
        }
    }

    /// Advance to the next entry.
    ///
    /// Must only be called when the iterator is valid.
    pub fn next(&mut self) {
        assert!(self.valid, "next() called on invalid iterator");
        self.parse_next_entry();
    }

    /// Returns the current key. Only valid when [`valid`](Self::valid) is true.
    pub fn key(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.key
    }

    /// Returns the current value. Only valid when [`valid`](Self::valid) is true.
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.block.data[self.value_offset..self.value_offset + self.value_len]
    }

    // -- internal helpers ---------------------------------------------------

    /// Position `current` at the given restart point without parsing.
    fn seek_to_restart(&mut self, restart_index: usize) {
        self.restart_index = restart_index;
        let offset = if (restart_index as u32) < self.block.num_restarts {
            self.block.restart_offset(restart_index)
        } else {
            self.block.data_end()
        };
        self.current = offset;
        self.key.clear();
        self.valid = false;
    }

    /// Parse the entry at `self.current` and advance `current` past it.
    fn parse_next_entry(&mut self) {
        if self.current >= self.block.data_end() {
            self.valid = false;
            return;
        }

        let data = &self.block.data[self.current..self.block.data_end()];

        let (shared, n1) = match decode_varint32(data) {
            Some(v) => v,
            None => {
                self.valid = false;
                return;
            }
        };
        let (non_shared, n2) = match decode_varint32(&data[n1..]) {
            Some(v) => v,
            None => {
                self.valid = false;
                return;
            }
        };
        let (value_len, n3) = match decode_varint32(&data[n1 + n2..]) {
            Some(v) => v,
            None => {
                self.valid = false;
                return;
            }
        };

        let header_len = n1 + n2 + n3;
        let shared = shared as usize;
        let non_shared = non_shared as usize;
        let value_len = value_len as usize;

        let needed = header_len + non_shared + value_len;
        if needed > data.len() {
            self.valid = false;
            return;
        }

        // Rebuild the full key: keep `shared` bytes, append delta.
        self.key.truncate(shared);
        self.key
            .extend_from_slice(&data[header_len..header_len + non_shared]);

        self.value_offset = self.current + header_len + non_shared;
        self.value_len = value_len;

        self.current += needed;

        // Track restart region.
        while self.restart_index + 1 < self.block.num_restarts as usize
            && self.block.restart_offset(self.restart_index + 1) <= self.current
        {
            self.restart_index += 1;
        }

        self.valid = true;
    }

    /// Decode the key stored at a specific restart point (shared must be 0).
    fn decode_restart_key(&self, restart_index: usize) -> Vec<u8> {
        let offset = self.block.restart_offset(restart_index);
        let data = &self.block.data[offset..self.block.data_end()];

        let (_shared, n1) = match decode_varint32(data) {
            Some(v) => v,
            None => return Vec::new(),
        };
        let (non_shared, n2) = match decode_varint32(&data[n1..]) {
            Some(v) => v,
            None => return Vec::new(),
        };
        let (_value_len, n3) = match decode_varint32(&data[n1 + n2..]) {
            Some(v) => v,
            None => return Vec::new(),
        };

        let header_len = n1 + n2 + n3;
        let non_shared = non_shared as usize;
        if header_len + non_shared > data.len() {
            return Vec::new();
        }

        data[header_len..header_len + non_shared].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a block from a slice of (key, value) pairs.
    fn build_block(entries: &[(&[u8], &[u8])], restart_interval: usize) -> Vec<u8> {
        let mut builder = BlockBuilder::new(restart_interval);
        for &(k, v) in entries {
            builder.add(k, v);
        }
        builder.finish().to_vec()
    }

    #[test]
    fn build_and_read_single_entry() {
        let data = build_block(&[(b"key1", b"val1")], 16);
        let reader = BlockReader::new(data).unwrap();
        let mut iter = reader.iter();

        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"val1");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn build_and_read_many_entries_in_order() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..200u32)
            .map(|i| {
                (
                    format!("key-{:06}", i).into_bytes(),
                    format!("val-{:06}", i).into_bytes(),
                )
            })
            .collect();
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let data = build_block(&entry_refs, 16);
        let reader = BlockReader::new(data).unwrap();
        let mut iter = reader.iter();
        iter.seek_to_first();

        for (k, v) in &entries {
            assert!(iter.valid(), "expected valid entry for key {:?}", k);
            assert_eq!(iter.key(), k.as_slice());
            assert_eq!(iter.value(), v.as_slice());
            iter.next();
        }
        assert!(!iter.valid());
    }

    #[test]
    fn seek_exact_match() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100u32)
            .map(|i| {
                (
                    format!("key-{:04}", i).into_bytes(),
                    format!("val-{}", i).into_bytes(),
                )
            })
            .collect();
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let data = build_block(&entry_refs, 16);
        let reader = BlockReader::new(data).unwrap();

        let mut iter = reader.iter();
        iter.seek(b"key-0050");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key-0050");
        assert_eq!(iter.value(), b"val-50");
    }

    #[test]
    fn seek_between_keys() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..50u32)
            .map(|i| {
                (
                    format!("key-{:04}", i * 2).into_bytes(),
                    format!("v{}", i).into_bytes(),
                )
            })
            .collect();
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let data = build_block(&entry_refs, 4);
        let reader = BlockReader::new(data).unwrap();

        let mut iter = reader.iter();
        iter.seek(b"key-0005"); // between key-0004 and key-0006
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key-0006");
    }

    #[test]
    fn seek_before_first() {
        let data = build_block(&[(b"bbb", b"2"), (b"ccc", b"3")], 16);
        let reader = BlockReader::new(data).unwrap();
        let mut iter = reader.iter();

        iter.seek(b"aaa");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"bbb");
    }

    #[test]
    fn seek_past_last() {
        let data = build_block(&[(b"aaa", b"1"), (b"bbb", b"2")], 16);
        let reader = BlockReader::new(data).unwrap();
        let mut iter = reader.iter();

        iter.seek(b"zzz");
        assert!(!iter.valid());
    }

    #[test]
    fn prefix_compression_works() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100u32)
            .map(|i| {
                (
                    format!("shared-prefix-long-enough-{:04}", i).into_bytes(),
                    b"v".to_vec(),
                )
            })
            .collect();
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let compressed = build_block(&entry_refs, 16);
        let uncompressed = build_block(&entry_refs, 1);

        assert!(
            compressed.len() < uncompressed.len(),
            "prefix compression should reduce block size: {} vs {}",
            compressed.len(),
            uncompressed.len()
        );

        let reader = BlockReader::new(compressed).unwrap();
        let mut iter = reader.iter();
        iter.seek_to_first();
        for (k, v) in &entries {
            assert!(iter.valid());
            assert_eq!(iter.key(), k.as_slice());
            assert_eq!(iter.value(), v.as_slice());
            iter.next();
        }
    }

    #[test]
    fn empty_block() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.is_empty());
        let data = builder.finish().to_vec();

        let reader = BlockReader::new(data).unwrap();
        let mut iter = reader.iter();
        iter.seek_to_first();
        assert!(!iter.valid());
    }

    #[test]
    fn restart_interval_one() {
        let entries: Vec<(&[u8], &[u8])> =
            vec![(b"abc", b"1"), (b"abd", b"2"), (b"abe", b"3"), (b"xyz", b"4")];
        let data = build_block(&entries, 1);
        let reader = BlockReader::new(data).unwrap();

        let mut iter = reader.iter();
        iter.seek_to_first();
        for &(k, v) in &entries {
            assert!(iter.valid());
            assert_eq!(iter.key(), k);
            assert_eq!(iter.value(), v);
            iter.next();
        }
        assert!(!iter.valid());
    }

    #[test]
    fn estimated_size_grows() {
        let mut builder = BlockBuilder::new(16);
        let s0 = builder.estimated_size();
        builder.add(b"key", b"value");
        let s1 = builder.estimated_size();
        assert!(s1 > s0);
    }

    #[test]
    fn reset_clears_state() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"a", b"1");
        builder.add(b"b", b"2");
        builder.reset();
        assert!(builder.is_empty());

        builder.add(b"x", b"10");
        let data = builder.finish().to_vec();
        let reader = BlockReader::new(data).unwrap();
        let mut iter = reader.iter();
        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"x");
        assert_eq!(iter.value(), b"10");
        iter.next();
        assert!(!iter.valid());
    }
}
