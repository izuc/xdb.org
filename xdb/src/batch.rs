//! `WriteBatch` — a collection of put/delete mutations applied atomically.
//!
//! ## Wire format
//!
//! ```text
//! [sequence_number: u64 LE] [count: u32 LE] [records...]
//! ```
//!
//! Each record:
//! ```text
//! [value_type: u8] [key_len: varint32] [key_bytes] [value_len: varint32] [value_bytes]
//! ```
//! For deletions the value_len/value_bytes fields are omitted.

use crate::error;
use crate::types::{decode_varint32, encode_varint32, SequenceNumber, ValueType};

/// Size of the fixed header: 8 bytes sequence + 4 bytes count.
const HEADER_SIZE: usize = 12;

/// A batch of mutations (puts and deletes) to be applied atomically.
///
/// Internally the batch is stored as a byte buffer with a 12-byte header
/// followed by a sequence of variable-length records.
pub struct WriteBatch {
    data: Vec<u8>,
    count: u32,
}

impl WriteBatch {
    /// Create an empty batch with a zeroed header.
    pub fn new() -> Self {
        let data = vec![0u8; HEADER_SIZE];
        // sequence = 0 (bytes 0..8), count = 0 (bytes 8..12) — already zeroed.
        WriteBatch { data, count: 0 }
    }

    /// Append a `Put(key, value)` operation to the batch.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.count += 1;
        self.set_count_in_header(self.count);

        self.data.push(ValueType::Value as u8);
        encode_varint32(&mut self.data, key.len() as u32);
        self.data.extend_from_slice(key);
        encode_varint32(&mut self.data, value.len() as u32);
        self.data.extend_from_slice(value);
    }

    /// Append a `Delete(key)` operation to the batch.
    pub fn delete(&mut self, key: &[u8]) {
        self.count += 1;
        self.set_count_in_header(self.count);

        self.data.push(ValueType::Deletion as u8);
        encode_varint32(&mut self.data, key.len() as u32);
        self.data.extend_from_slice(key);
    }

    /// Append a `DeleteRange(start_key, end_key)` operation to the batch.
    ///
    /// Marks all keys in `[start_key, end_key)` as deleted.
    pub fn delete_range(&mut self, start_key: &[u8], end_key: &[u8]) {
        self.count += 1;
        self.set_count_in_header(self.count);

        self.data.push(ValueType::RangeDeletion as u8);
        encode_varint32(&mut self.data, start_key.len() as u32);
        self.data.extend_from_slice(start_key);
        encode_varint32(&mut self.data, end_key.len() as u32);
        self.data.extend_from_slice(end_key);
    }

    /// Remove all buffered operations and reset the header.
    pub fn clear(&mut self) {
        self.data.truncate(HEADER_SIZE);
        self.data[..HEADER_SIZE].fill(0);
        self.count = 0;
    }

    /// Number of operations in the batch.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Raw byte representation (header + records).
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Overwrite the count stored in the header.
    pub fn set_count(&mut self, count: u32) {
        self.count = count;
        self.set_count_in_header(count);
    }

    /// Set the sequence number in the header.
    pub fn set_sequence(&mut self, seq: SequenceNumber) {
        self.data[0..8].copy_from_slice(&seq.to_le_bytes());
    }

    /// Read the sequence number from the header.
    pub fn sequence(&self) -> SequenceNumber {
        u64::from_le_bytes(self.data[0..8].try_into().unwrap())
    }

    /// Iterate over all operations, calling `handler` for each.
    ///
    /// Returns an error if the batch data is malformed.
    pub fn iterate<H: WriteBatchHandler>(&self, handler: &mut H) -> error::Result<()> {
        if self.data.len() < HEADER_SIZE {
            return Err(error::Error::corruption("batch too short"));
        }

        let count = u32::from_le_bytes(self.data[8..12].try_into().unwrap());
        let mut pos = HEADER_SIZE;
        let mut found = 0u32;

        while pos < self.data.len() {
            let tag = self.data[pos];
            pos += 1;

            match ValueType::from_u8(tag) {
                Some(ValueType::Value) => {
                    let (key, value) = Self::decode_put_record(&self.data, &mut pos)?;
                    handler.put(key, value);
                }
                Some(ValueType::Deletion) => {
                    let key = Self::decode_delete_record(&self.data, &mut pos)?;
                    handler.delete(key);
                }
                Some(ValueType::RangeDeletion) => {
                    // Range deletion: start_key + end_key (with bounds checks)
                    let (start_len, n) = decode_varint32(&self.data[pos..])
                        .ok_or_else(|| error::Error::corruption("truncated varint in batch"))?;
                    pos += n;
                    let start_len = start_len as usize;
                    if pos + start_len > self.data.len() {
                        return Err(error::Error::corruption(
                            "truncated start_key in range deletion",
                        ));
                    }
                    let start_key = &self.data[pos..pos + start_len];
                    pos += start_len;
                    let (end_len, n) = decode_varint32(&self.data[pos..])
                        .ok_or_else(|| error::Error::corruption("truncated varint in batch"))?;
                    pos += n;
                    let end_len = end_len as usize;
                    if pos + end_len > self.data.len() {
                        return Err(error::Error::corruption(
                            "truncated end_key in range deletion",
                        ));
                    }
                    let end_key = &self.data[pos..pos + end_len];
                    pos += end_len;
                    handler.delete_range(start_key, end_key);
                }
                None => {
                    return Err(error::Error::corruption(format!(
                        "unknown value type tag: {}",
                        tag
                    )));
                }
            }

            found += 1;
        }

        if found != count {
            return Err(error::Error::corruption(format!(
                "batch count mismatch: header says {} but found {}",
                count, found
            )));
        }

        Ok(())
    }

    /// Create a `WriteBatch` from raw data (header + records).
    ///
    /// The count is parsed from the header bytes 8..12. This is useful for
    /// reconstructing a batch from a serialised copy (e.g. Python bindings).
    pub fn from_data(data: Vec<u8>) -> Self {
        let count = if data.len() >= 12 {
            u32::from_le_bytes(data[8..12].try_into().unwrap())
        } else {
            0
        };
        WriteBatch { data, count }
    }

    /// Approximate in-memory size of this batch in bytes.
    pub fn approximate_size(&self) -> usize {
        self.data.len()
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Write `count` into header bytes 8..12.
    fn set_count_in_header(&mut self, count: u32) {
        self.data[8..12].copy_from_slice(&count.to_le_bytes());
    }

    /// Decode a Put record: `key_len(varint) key_bytes value_len(varint) value_bytes`.
    fn decode_put_record<'a>(
        data: &'a [u8],
        pos: &mut usize,
    ) -> error::Result<(&'a [u8], &'a [u8])> {
        let key = Self::decode_length_prefixed_slice(data, pos)?;
        let value = Self::decode_length_prefixed_slice(data, pos)?;
        Ok((key, value))
    }

    /// Decode a Delete record: `key_len(varint) key_bytes`.
    fn decode_delete_record<'a>(
        data: &'a [u8],
        pos: &mut usize,
    ) -> error::Result<&'a [u8]> {
        Self::decode_length_prefixed_slice(data, pos)
    }

    /// Read a varint32-prefixed byte slice from `data` starting at `*pos`,
    /// advancing `*pos` past the consumed bytes.
    fn decode_length_prefixed_slice<'a>(
        data: &'a [u8],
        pos: &mut usize,
    ) -> error::Result<&'a [u8]> {
        let remaining = &data[*pos..];
        let (len, n) = decode_varint32(remaining)
            .ok_or_else(|| error::Error::corruption("truncated varint in batch"))?;
        let len = len as usize;
        *pos += n;
        if *pos + len > data.len() {
            return Err(error::Error::corruption("truncated data in batch"));
        }
        let slice = &data[*pos..*pos + len];
        *pos += len;
        Ok(slice)
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for consumers that process each operation in a [`WriteBatch`].
pub trait WriteBatchHandler {
    /// Called for each `Put(key, value)` in the batch.
    fn put(&mut self, key: &[u8], value: &[u8]);
    /// Called for each `Delete(key)` in the batch.
    fn delete(&mut self, key: &[u8]);
    /// Called for each `DeleteRange(start_key, end_key)` in the batch.
    ///
    /// The default implementation is a no-op for backwards compatibility.
    #[allow(unused_variables)]
    fn delete_range(&mut self, start_key: &[u8], end_key: &[u8]) {}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple handler that records operations for verification.
    struct Recorder {
        ops: Vec<Op>,
    }

    #[derive(Debug, PartialEq)]
    enum Op {
        Put(Vec<u8>, Vec<u8>),
        Delete(Vec<u8>),
        DeleteRange(Vec<u8>, Vec<u8>),
    }

    impl WriteBatchHandler for Recorder {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.ops.push(Op::Put(key.to_vec(), value.to_vec()));
        }
        fn delete(&mut self, key: &[u8]) {
            self.ops.push(Op::Delete(key.to_vec()));
        }
        fn delete_range(&mut self, start_key: &[u8], end_key: &[u8]) {
            self.ops
                .push(Op::DeleteRange(start_key.to_vec(), end_key.to_vec()));
        }
    }

    #[test]
    fn put_and_delete_roundtrip() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key3");

        assert_eq!(batch.count(), 3);
        assert!(batch.approximate_size() > HEADER_SIZE);

        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();

        assert_eq!(
            recorder.ops,
            vec![
                Op::Put(b"key1".to_vec(), b"value1".to_vec()),
                Op::Put(b"key2".to_vec(), b"value2".to_vec()),
                Op::Delete(b"key3".to_vec()),
            ]
        );
    }

    #[test]
    fn iterate_produces_correct_operations() {
        let mut batch = WriteBatch::new();
        batch.delete(b"a");
        batch.put(b"b", b"B");
        batch.delete(b"c");
        batch.put(b"d", b"D");

        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();

        assert_eq!(recorder.ops.len(), 4);
        assert_eq!(recorder.ops[0], Op::Delete(b"a".to_vec()));
        assert_eq!(recorder.ops[1], Op::Put(b"b".to_vec(), b"B".to_vec()));
        assert_eq!(recorder.ops[2], Op::Delete(b"c".to_vec()));
        assert_eq!(recorder.ops[3], Op::Put(b"d".to_vec(), b"D".to_vec()));
    }

    #[test]
    fn sequence_number_get_set() {
        let mut batch = WriteBatch::new();
        assert_eq!(batch.sequence(), 0);

        batch.set_sequence(42);
        assert_eq!(batch.sequence(), 42);

        batch.set_sequence(u64::MAX);
        assert_eq!(batch.sequence(), u64::MAX);
    }

    #[test]
    fn clear_resets_state() {
        let mut batch = WriteBatch::new();
        batch.set_sequence(100);
        batch.put(b"key", b"value");
        batch.delete(b"key2");

        assert_eq!(batch.count(), 2);
        assert!(batch.approximate_size() > HEADER_SIZE);

        batch.clear();

        assert_eq!(batch.count(), 0);
        assert_eq!(batch.sequence(), 0);
        assert_eq!(batch.approximate_size(), HEADER_SIZE);

        // Iterating an empty batch should produce no operations.
        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();
        assert!(recorder.ops.is_empty());
    }

    #[test]
    fn set_count() {
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.put(b"b", b"2");
        assert_eq!(batch.count(), 2);

        // Overwrite count (useful when replaying from WAL).
        batch.set_count(99);
        assert_eq!(batch.count(), 99);
        // The header bytes should reflect the new count.
        let header_count = u32::from_le_bytes(batch.data()[8..12].try_into().unwrap());
        assert_eq!(header_count, 99);
    }

    #[test]
    fn empty_batch_iterate() {
        let batch = WriteBatch::new();
        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();
        assert!(recorder.ops.is_empty());
    }

    #[test]
    fn large_key_value() {
        let big_key = vec![0xABu8; 1024];
        let big_value = vec![0xCDu8; 4096];

        let mut batch = WriteBatch::new();
        batch.put(&big_key, &big_value);

        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();

        assert_eq!(recorder.ops.len(), 1);
        assert_eq!(recorder.ops[0], Op::Put(big_key, big_value));
    }

    #[test]
    fn delete_range_roundtrip() {
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"val_a");
        batch.delete_range(b"b", b"d");
        batch.delete(b"e");

        assert_eq!(batch.count(), 3);

        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();

        assert_eq!(
            recorder.ops,
            vec![
                Op::Put(b"a".to_vec(), b"val_a".to_vec()),
                Op::DeleteRange(b"b".to_vec(), b"d".to_vec()),
                Op::Delete(b"e".to_vec()),
            ]
        );
    }

    #[test]
    fn delete_range_only() {
        let mut batch = WriteBatch::new();
        batch.delete_range(b"start", b"end");

        assert_eq!(batch.count(), 1);

        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();

        assert_eq!(recorder.ops.len(), 1);
        assert_eq!(
            recorder.ops[0],
            Op::DeleteRange(b"start".to_vec(), b"end".to_vec())
        );
    }

    #[test]
    fn multiple_delete_ranges() {
        let mut batch = WriteBatch::new();
        batch.delete_range(b"a", b"c");
        batch.delete_range(b"m", b"z");

        assert_eq!(batch.count(), 2);

        let mut recorder = Recorder { ops: Vec::new() };
        batch.iterate(&mut recorder).unwrap();

        assert_eq!(
            recorder.ops,
            vec![
                Op::DeleteRange(b"a".to_vec(), b"c".to_vec()),
                Op::DeleteRange(b"m".to_vec(), b"z".to_vec()),
            ]
        );
    }
}
