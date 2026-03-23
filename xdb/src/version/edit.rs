//! Version edits track changes to the LSM-tree structure.
//!
//! A `VersionEdit` describes a delta between two `Version`s: which SST files
//! were added and which were removed. Edits are serialized to the MANIFEST
//! file using a tagged varint encoding.

use crate::error::{Error, Result};
use crate::types::*;

// ---------------------------------------------------------------------------
// Tag constants for the on-disk encoding
// ---------------------------------------------------------------------------

const TAG_COMPARATOR: u32 = 1;
const TAG_LOG_NUMBER: u32 = 2;
const TAG_NEXT_FILE_NUMBER: u32 = 3;
const TAG_LAST_SEQUENCE: u32 = 4;
const TAG_DELETED_FILE: u32 = 5;
const TAG_NEW_FILE: u32 = 6;

// ---------------------------------------------------------------------------
// FileMetaData
// ---------------------------------------------------------------------------

/// Metadata for a single SST file within a level.
#[derive(Debug, Clone)]
pub struct FileMetaData {
    /// Unique file number used to derive the on-disk file name.
    pub number: FileNumber,
    /// Total size of the SST file in bytes.
    pub file_size: u64,
    /// Smallest internal key in the file.
    pub smallest_key: InternalKey,
    /// Largest internal key in the file.
    pub largest_key: InternalKey,
}

// ---------------------------------------------------------------------------
// VersionEdit
// ---------------------------------------------------------------------------

/// A delta describing changes to a `Version`.
///
/// Contains metadata updates (log number, sequence number, etc.) as well as
/// the set of files added and removed from each level.
#[derive(Debug, Clone, Default)]
pub struct VersionEdit {
    /// Name of the comparator used by this DB (set once on creation).
    pub comparator_name: Option<String>,
    /// Current WAL file number.
    pub log_number: Option<FileNumber>,
    /// Next file number to be allocated.
    pub next_file_number: Option<FileNumber>,
    /// Last sequence number written.
    pub last_sequence: Option<SequenceNumber>,
    /// Files added: `(level, metadata)`.
    pub new_files: Vec<(usize, FileMetaData)>,
    /// Files removed: `(level, file_number)`.
    pub deleted_files: Vec<(usize, FileNumber)>,
}

impl VersionEdit {
    /// Create an empty version edit.
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the comparator name.
    pub fn set_comparator_name(&mut self, name: String) {
        self.comparator_name = Some(name);
    }

    /// Set the current WAL log number.
    pub fn set_log_number(&mut self, num: FileNumber) {
        self.log_number = Some(num);
    }

    /// Set the next file number to allocate.
    pub fn set_next_file_number(&mut self, num: FileNumber) {
        self.next_file_number = Some(num);
    }

    /// Set the last written sequence number.
    pub fn set_last_sequence(&mut self, seq: SequenceNumber) {
        self.last_sequence = Some(seq);
    }

    /// Record that an SST file was added at the given level.
    pub fn add_file(&mut self, level: usize, meta: FileMetaData) {
        self.new_files.push((level, meta));
    }

    /// Record that an SST file was removed from the given level.
    pub fn delete_file(&mut self, level: usize, file_number: FileNumber) {
        self.deleted_files.push((level, file_number));
    }

    /// Encode this edit to bytes for writing to the MANIFEST file.
    ///
    /// Each field is encoded as `tag(varint32) | field_data`. Tags:
    /// - 1: comparator_name (length-prefixed string)
    /// - 2: log_number (varint64)
    /// - 3: next_file_number (varint64)
    /// - 4: last_sequence (varint64)
    /// - 5: deleted_file — level(varint32) + file_number(varint64)
    /// - 6: new_file — level(varint32) + file_number(varint64) + file_size(varint64)
    ///      + smallest_key(length-prefixed) + largest_key(length-prefixed)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        if let Some(ref name) = self.comparator_name {
            encode_varint32(&mut buf, TAG_COMPARATOR);
            encode_length_prefixed(&mut buf, name.as_bytes());
        }

        if let Some(num) = self.log_number {
            encode_varint32(&mut buf, TAG_LOG_NUMBER);
            encode_varint64(&mut buf, num);
        }

        if let Some(num) = self.next_file_number {
            encode_varint32(&mut buf, TAG_NEXT_FILE_NUMBER);
            encode_varint64(&mut buf, num);
        }

        if let Some(seq) = self.last_sequence {
            encode_varint32(&mut buf, TAG_LAST_SEQUENCE);
            encode_varint64(&mut buf, seq);
        }

        for &(level, file_number) in &self.deleted_files {
            encode_varint32(&mut buf, TAG_DELETED_FILE);
            encode_varint32(&mut buf, level as u32);
            encode_varint64(&mut buf, file_number);
        }

        for (level, ref meta) in &self.new_files {
            encode_varint32(&mut buf, TAG_NEW_FILE);
            encode_varint32(&mut buf, *level as u32);
            encode_varint64(&mut buf, meta.number);
            encode_varint64(&mut buf, meta.file_size);
            encode_length_prefixed(&mut buf, meta.smallest_key.as_bytes());
            encode_length_prefixed(&mut buf, meta.largest_key.as_bytes());
        }

        buf
    }

    /// Decode a `VersionEdit` from bytes previously produced by [`encode`].
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut edit = VersionEdit::new();
        let mut pos = 0;

        while pos < data.len() {
            let (tag, n) = decode_varint32(&data[pos..])
                .ok_or_else(|| Error::corruption("truncated tag in VersionEdit"))?;
            pos += n;

            match tag {
                TAG_COMPARATOR => {
                    let (name_bytes, n) = decode_length_prefixed(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated comparator name"))?;
                    let name = String::from_utf8(name_bytes.to_vec())
                        .map_err(|_| Error::corruption("invalid UTF-8 in comparator name"))?;
                    edit.comparator_name = Some(name);
                    pos += n;
                }
                TAG_LOG_NUMBER => {
                    let (val, n) = decode_varint64(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated log_number"))?;
                    edit.log_number = Some(val);
                    pos += n;
                }
                TAG_NEXT_FILE_NUMBER => {
                    let (val, n) = decode_varint64(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated next_file_number"))?;
                    edit.next_file_number = Some(val);
                    pos += n;
                }
                TAG_LAST_SEQUENCE => {
                    let (val, n) = decode_varint64(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated last_sequence"))?;
                    edit.last_sequence = Some(val);
                    pos += n;
                }
                TAG_DELETED_FILE => {
                    let (level, n) = decode_varint32(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated deleted_file level"))?;
                    pos += n;
                    let (file_number, n) = decode_varint64(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated deleted_file number"))?;
                    pos += n;
                    edit.deleted_files.push((level as usize, file_number));
                }
                TAG_NEW_FILE => {
                    let (level, n) = decode_varint32(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated new_file level"))?;
                    pos += n;
                    let (file_number, n) = decode_varint64(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated new_file number"))?;
                    pos += n;
                    let (file_size, n) = decode_varint64(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated new_file size"))?;
                    pos += n;
                    let (smallest_bytes, n) = decode_length_prefixed(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated new_file smallest_key"))?;
                    pos += n;
                    let (largest_bytes, n) = decode_length_prefixed(&data[pos..])
                        .ok_or_else(|| Error::corruption("truncated new_file largest_key"))?;
                    pos += n;

                    let meta = FileMetaData {
                        number: file_number,
                        file_size,
                        smallest_key: InternalKey::from_bytes(smallest_bytes.to_vec()),
                        largest_key: InternalKey::from_bytes(largest_bytes.to_vec()),
                    };
                    edit.new_files.push((level as usize, meta));
                }
                _ => {
                    return Err(Error::corruption(format!(
                        "unknown tag {} in VersionEdit",
                        tag
                    )));
                }
            }
        }

        Ok(edit)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_edit_roundtrip() {
        let edit = VersionEdit::new();
        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();
        assert!(decoded.comparator_name.is_none());
        assert!(decoded.log_number.is_none());
        assert!(decoded.next_file_number.is_none());
        assert!(decoded.last_sequence.is_none());
        assert!(decoded.new_files.is_empty());
        assert!(decoded.deleted_files.is_empty());
    }

    #[test]
    fn metadata_fields_roundtrip() {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name("leveldb.BytewiseComparator".to_string());
        edit.set_log_number(42);
        edit.set_next_file_number(100);
        edit.set_last_sequence(9999);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(
            decoded.comparator_name.as_deref(),
            Some("leveldb.BytewiseComparator")
        );
        assert_eq!(decoded.log_number, Some(42));
        assert_eq!(decoded.next_file_number, Some(100));
        assert_eq!(decoded.last_sequence, Some(9999));
    }

    #[test]
    fn new_files_roundtrip() {
        let mut edit = VersionEdit::new();
        let meta = FileMetaData {
            number: 7,
            file_size: 1024,
            smallest_key: InternalKey::new(b"aaa", 10, ValueType::Value),
            largest_key: InternalKey::new(b"zzz", 50, ValueType::Value),
        };
        edit.add_file(2, meta);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.new_files.len(), 1);
        let (level, ref m) = decoded.new_files[0];
        assert_eq!(level, 2);
        assert_eq!(m.number, 7);
        assert_eq!(m.file_size, 1024);
        assert_eq!(m.smallest_key.user_key(), b"aaa");
        assert_eq!(m.largest_key.user_key(), b"zzz");
    }

    #[test]
    fn deleted_files_roundtrip() {
        let mut edit = VersionEdit::new();
        edit.delete_file(0, 3);
        edit.delete_file(1, 15);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.deleted_files.len(), 2);
        assert_eq!(decoded.deleted_files[0], (0, 3));
        assert_eq!(decoded.deleted_files[1], (1, 15));
    }

    #[test]
    fn complex_edit_roundtrip() {
        let mut edit = VersionEdit::new();
        edit.set_comparator_name("test".to_string());
        edit.set_log_number(5);
        edit.set_next_file_number(20);
        edit.set_last_sequence(500);
        edit.delete_file(0, 1);
        edit.delete_file(0, 2);
        edit.add_file(
            1,
            FileMetaData {
                number: 10,
                file_size: 2048,
                smallest_key: InternalKey::new(b"key1", 100, ValueType::Value),
                largest_key: InternalKey::new(b"key9", 200, ValueType::Value),
            },
        );
        edit.add_file(
            1,
            FileMetaData {
                number: 11,
                file_size: 4096,
                smallest_key: InternalKey::new(b"keya", 300, ValueType::Value),
                largest_key: InternalKey::new(b"keyz", 400, ValueType::Deletion),
            },
        );

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.comparator_name.as_deref(), Some("test"));
        assert_eq!(decoded.log_number, Some(5));
        assert_eq!(decoded.next_file_number, Some(20));
        assert_eq!(decoded.last_sequence, Some(500));
        assert_eq!(decoded.deleted_files.len(), 2);
        assert_eq!(decoded.new_files.len(), 2);
        assert_eq!(decoded.new_files[0].1.number, 10);
        assert_eq!(decoded.new_files[1].1.number, 11);
        assert_eq!(decoded.new_files[1].1.file_size, 4096);
    }

    #[test]
    fn large_values_roundtrip() {
        let mut edit = VersionEdit::new();
        edit.set_last_sequence(u64::MAX >> 8); // MAX_SEQUENCE_NUMBER
        edit.set_next_file_number(u64::MAX);
        edit.add_file(
            6,
            FileMetaData {
                number: u64::MAX,
                file_size: u64::MAX,
                smallest_key: InternalKey::new(&[0xFF; 256], MAX_SEQUENCE_NUMBER, ValueType::Value),
                largest_key: InternalKey::new(&[0xFF; 256], MAX_SEQUENCE_NUMBER, ValueType::Value),
            },
        );

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();
        assert_eq!(decoded.last_sequence, Some(u64::MAX >> 8));
        assert_eq!(decoded.next_file_number, Some(u64::MAX));
        assert_eq!(decoded.new_files[0].1.number, u64::MAX);
    }

    #[test]
    fn decode_invalid_tag_returns_error() {
        // Encode a single byte with an unknown tag value (99).
        let mut buf = Vec::new();
        encode_varint32(&mut buf, 99);
        assert!(VersionEdit::decode(&buf).is_err());
    }
}
