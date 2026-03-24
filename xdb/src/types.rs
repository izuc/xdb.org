//! Core types: internal keys, sequence numbers, varint encoding.

use std::cmp::Ordering;

// ---------------------------------------------------------------------------
// Fundamental type aliases
// ---------------------------------------------------------------------------

/// Monotonically increasing 56-bit counter stamped on every write.
pub type SequenceNumber = u64;

/// Monotonically increasing counter for file names (SST, WAL, MANIFEST).
pub type FileNumber = u64;

/// Maximum value that fits in 56 bits (7 bytes).
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1u64 << 56) - 1;

// ---------------------------------------------------------------------------
// ValueType — the operation stored alongside each key
// ---------------------------------------------------------------------------

/// Discriminant stored in the low byte of an internal key's 8-byte tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    /// A tombstone (deletion marker).
    Deletion = 0,
    /// A normal put.
    Value = 1,
    /// A range deletion tombstone: the user key is the start and the value
    /// holds the exclusive end key.
    RangeDeletion = 2,
}

impl ValueType {
    /// Try to convert a raw byte into a `ValueType`.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(ValueType::Deletion),
            1 => Some(ValueType::Value),
            2 => Some(ValueType::RangeDeletion),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// InternalKey — the on-disk / in-memory key representation
// ---------------------------------------------------------------------------

/// Owned internal key: `user_key ++ pack(sequence, value_type)`.
///
/// The trailing 8-byte **tag** encodes `(sequence << 8) | value_type`.
/// Ordering: user_key ascending, then sequence **descending**, then type
/// descending.  This means the *newest* version of a key sorts first.
#[derive(Clone, Debug)]
pub struct InternalKey {
    data: Vec<u8>,
}

impl InternalKey {
    /// Build an internal key from parts.
    pub fn new(user_key: &[u8], seq: SequenceNumber, vt: ValueType) -> Self {
        let mut data = Vec::with_capacity(user_key.len() + 8);
        data.extend_from_slice(user_key);
        let tag = (seq << 8) | (vt as u64);
        data.extend_from_slice(&tag.to_le_bytes());
        InternalKey { data }
    }

    /// Raw bytes (user_key + 8-byte tag).
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Decode into borrowed parts.
    #[inline]
    pub fn parse(&self) -> Option<ParsedInternalKey<'_>> {
        ParsedInternalKey::from_bytes(&self.data)
    }

    /// Convenience: extract just the user key portion.
    #[inline]
    pub fn user_key(&self) -> &[u8] {
        &self.data[..self.data.len().saturating_sub(8)]
    }

    pub fn from_bytes(data: Vec<u8>) -> Self {
        InternalKey { data }
    }

    pub fn encoded_len(&self) -> usize {
        self.data.len()
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for InternalKey {}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_internal_key(&self.data, &other.data)
    }
}

// ---------------------------------------------------------------------------
// ParsedInternalKey — borrowed view into an encoded internal key
// ---------------------------------------------------------------------------

/// Borrowed/parsed view of an internal key.
#[derive(Debug, Clone, Copy)]
pub struct ParsedInternalKey<'a> {
    pub user_key: &'a [u8],
    pub sequence: SequenceNumber,
    pub value_type: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    /// Decode from a byte slice that contains `user_key ++ tag(8)`.
    pub fn from_bytes(data: &'a [u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let tag_offset = data.len() - 8;
        let tag = u64::from_le_bytes(data[tag_offset..].try_into().ok()?);
        let vt = ValueType::from_u8((tag & 0xFF) as u8)?;
        Some(ParsedInternalKey {
            user_key: &data[..tag_offset],
            sequence: tag >> 8,
            value_type: vt,
        })
    }
}

// ---------------------------------------------------------------------------
// LookupKey — convenience for point queries
// ---------------------------------------------------------------------------

/// Maximum size for inline (stack-allocated) lookup key storage.
///
/// Covers keys up to ~243 user-key bytes without a heap allocation.
/// `256 = 5 (varint32 max) + 243 (user key) + 8 (tag)`.
const LOOKUP_KEY_INLINE_CAP: usize = 256;

/// A helper that encodes the key used for memtable lookups.
///
/// Format: `klength(varint32) | user_key | tag(8)` where
/// `klength = user_key.len() + 8`.
///
/// Uses inline stack storage for small keys (the common case) to avoid
/// a heap allocation on every point lookup.
pub struct LookupKey {
    /// Inline storage for small keys (common case -- no heap allocation).
    inline: [u8; LOOKUP_KEY_INLINE_CAP],
    /// If the key doesn't fit inline, spill to heap.
    heap: Option<Vec<u8>>,
    /// Total number of bytes written (varint + user_key + tag).
    len: usize,
    /// Offset where the internal key starts (after the varint length prefix).
    key_start: usize,
}

impl LookupKey {
    pub fn new(user_key: &[u8], sequence: SequenceNumber) -> Self {
        let internal_len = user_key.len() + 8;
        assert!(
            internal_len <= u32::MAX as usize,
            "user key too large ({} bytes)",
            user_key.len()
        );
        let varint_len = varint32_length(internal_len as u32);
        let total_len = varint_len + internal_len;
        let tag = (sequence << 8) | (ValueType::Value as u64);

        if total_len <= LOOKUP_KEY_INLINE_CAP {
            // Fast path: write directly into stack buffer.
            let mut inline = [0u8; LOOKUP_KEY_INLINE_CAP];
            // Encode varint32 inline.
            let mut pos = 0;
            let mut v = internal_len as u32;
            loop {
                if v < 0x80 {
                    inline[pos] = v as u8;
                    pos += 1;
                    break;
                }
                inline[pos] = (v as u8) | 0x80;
                v >>= 7;
                pos += 1;
            }
            let key_start = pos;
            inline[pos..pos + user_key.len()].copy_from_slice(user_key);
            pos += user_key.len();
            inline[pos..pos + 8].copy_from_slice(&tag.to_le_bytes());
            pos += 8;
            LookupKey {
                inline,
                heap: None,
                len: pos,
                key_start,
            }
        } else {
            // Rare path: key too large, spill to heap.
            let mut data = Vec::with_capacity(total_len);
            encode_varint32(&mut data, internal_len as u32);
            let key_start = data.len();
            data.extend_from_slice(user_key);
            data.extend_from_slice(&tag.to_le_bytes());
            let len = data.len();
            LookupKey {
                inline: [0u8; LOOKUP_KEY_INLINE_CAP],
                heap: Some(data),
                len,
                key_start,
            }
        }
    }

    /// Return a reference to the underlying storage bytes.
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        match self.heap {
            Some(ref v) => &v[..self.len],
            None => &self.inline[..self.len],
        }
    }

    /// Full memtable key (length-prefixed internal key).
    #[inline]
    pub fn memtable_key(&self) -> &[u8] {
        self.as_bytes()
    }

    /// Just the internal key portion (user_key + tag).
    #[inline]
    pub fn internal_key(&self) -> &[u8] {
        &self.as_bytes()[self.key_start..]
    }

    /// Just the user key.
    #[inline]
    pub fn user_key(&self) -> &[u8] {
        let bytes = self.as_bytes();
        &bytes[self.key_start..bytes.len() - 8]
    }
}

// ---------------------------------------------------------------------------
// Internal key comparison
// ---------------------------------------------------------------------------

/// Compare two encoded internal keys.
///
/// Order: user_key ascending, then sequence **descending**, then type
/// descending.
pub fn compare_internal_key(a: &[u8], b: &[u8]) -> Ordering {
    let a_ukey = &a[..a.len().saturating_sub(8)];
    let b_ukey = &b[..b.len().saturating_sub(8)];

    match a_ukey.cmp(b_ukey) {
        Ordering::Equal => {}
        other => return other,
    }

    // Both have 8-byte tags; compare sequence|type *descending*.
    let a_tag = if a.len() >= 8 {
        u64::from_le_bytes(a[a.len() - 8..].try_into().unwrap())
    } else {
        0
    };
    let b_tag = if b.len() >= 8 {
        u64::from_le_bytes(b[b.len() - 8..].try_into().unwrap())
    } else {
        0
    };

    // Higher tag (newer sequence) should come first → reverse order.
    b_tag.cmp(&a_tag)
}

/// Extract the user key from an encoded internal key slice.
#[inline]
pub fn extract_user_key(internal_key: &[u8]) -> &[u8] {
    &internal_key[..internal_key.len().saturating_sub(8)]
}

/// Extract the 8-byte tag from an encoded internal key slice.
#[inline]
pub fn extract_tag(internal_key: &[u8]) -> u64 {
    if internal_key.len() >= 8 {
        u64::from_le_bytes(internal_key[internal_key.len() - 8..].try_into().unwrap())
    } else {
        0
    }
}

/// Extract sequence number from a tag.
#[inline]
pub fn tag_sequence(tag: u64) -> SequenceNumber {
    tag >> 8
}

/// Extract value type from a tag.
#[inline]
pub fn tag_value_type(tag: u64) -> Option<ValueType> {
    ValueType::from_u8((tag & 0xFF) as u8)
}

// ---------------------------------------------------------------------------
// Varint encoding (LEB128-style, compatible with LevelDB / RocksDB)
// ---------------------------------------------------------------------------

/// Encode a u32 as a variable-length integer (1–5 bytes).
pub fn encode_varint32(buf: &mut Vec<u8>, mut value: u32) {
    loop {
        if value < 0x80 {
            buf.push(value as u8);
            return;
        }
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
}

/// Decode a varint32 from the start of `data`.
/// Returns `(value, bytes_consumed)` or `None` on truncation.
pub fn decode_varint32(data: &[u8]) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        if i >= 5 {
            return None; // varint32 uses at most 5 bytes
        }
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None // ran out of data
}

/// Encode a u64 as a variable-length integer (1–10 bytes).
pub fn encode_varint64(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        if value < 0x80 {
            buf.push(value as u8);
            return;
        }
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
}

/// Decode a varint64 from the start of `data`.
/// Returns `(value, bytes_consumed)` or `None` on truncation.
pub fn decode_varint64(data: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift > 63 {
            return None; // overflow
        }
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None // ran out of data
}

/// Return how many bytes a varint32 encoding would occupy.
pub fn varint32_length(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// Return how many bytes a varint64 encoding would occupy.
pub fn varint64_length(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// Encode a length-prefixed byte slice: `varint(len) | bytes`.
pub fn encode_length_prefixed(buf: &mut Vec<u8>, data: &[u8]) {
    encode_varint32(buf, data.len() as u32);
    buf.extend_from_slice(data);
}

/// Decode a length-prefixed byte slice.
/// Returns `(slice, bytes_consumed)` or `None`.
pub fn decode_length_prefixed(data: &[u8]) -> Option<(&[u8], usize)> {
    let (len, n) = decode_varint32(data)?;
    let len = len as usize;
    if n + len > data.len() {
        return None;
    }
    Some((&data[n..n + len], n + len))
}

// ---------------------------------------------------------------------------
// Fixed-width encoding helpers
// ---------------------------------------------------------------------------

/// Encode a u32 in little-endian.
pub fn encode_fixed32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Decode a u32 from little-endian bytes.
pub fn decode_fixed32(data: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(data.get(..4)?.try_into().ok()?))
}

/// Encode a u64 in little-endian.
pub fn encode_fixed64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Decode a u64 from little-endian bytes.
pub fn decode_fixed64(data: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(data.get(..8)?.try_into().ok()?))
}

// ---------------------------------------------------------------------------
// File naming helpers
// ---------------------------------------------------------------------------

/// Format an SST file name: `{number:06}.sst`
pub fn sst_file_name(number: FileNumber) -> String {
    format!("{:06}.sst", number)
}

/// Format a WAL file name: `{number:06}.log`
pub fn wal_file_name(number: FileNumber) -> String {
    format!("{:06}.log", number)
}

/// Format a MANIFEST file name: `MANIFEST-{number:06}`
pub fn manifest_file_name(number: FileNumber) -> String {
    format!("MANIFEST-{:06}", number)
}

/// The CURRENT file records which MANIFEST is active.
pub const CURRENT_FILE_NAME: &str = "CURRENT";

/// The LOCK file prevents concurrent access.
pub const LOCK_FILE_NAME: &str = "LOCK";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint32_roundtrip() {
        for &v in &[0u32, 1, 127, 128, 16383, 16384, u32::MAX] {
            let mut buf = Vec::new();
            encode_varint32(&mut buf, v);
            let (decoded, n) = decode_varint32(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(n, buf.len());
            assert_eq!(n, varint32_length(v));
        }
    }

    #[test]
    fn varint64_roundtrip() {
        for &v in &[0u64, 1, 127, 128, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint64(&mut buf, v);
            let (decoded, n) = decode_varint64(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(n, buf.len());
            assert_eq!(n, varint64_length(v));
        }
    }

    #[test]
    fn internal_key_ordering() {
        // Same user key, different sequences → newer (higher seq) first.
        let k1 = InternalKey::new(b"hello", 100, ValueType::Value);
        let k2 = InternalKey::new(b"hello", 200, ValueType::Value);
        assert!(k2 < k1); // 200 is newer → sorts before 100

        // Different user keys → lexicographic.
        let ka = InternalKey::new(b"aaa", 1, ValueType::Value);
        let kb = InternalKey::new(b"bbb", 1, ValueType::Value);
        assert!(ka < kb);
    }

    #[test]
    fn parsed_internal_key_roundtrip() {
        let ik = InternalKey::new(b"mykey", 42, ValueType::Deletion);
        let p = ik.parse().unwrap();
        assert_eq!(p.user_key, b"mykey");
        assert_eq!(p.sequence, 42);
        assert_eq!(p.value_type, ValueType::Deletion);
    }

    #[test]
    fn length_prefixed_roundtrip() {
        let mut buf = Vec::new();
        encode_length_prefixed(&mut buf, b"hello world");
        let (decoded, n) = decode_length_prefixed(&buf).unwrap();
        assert_eq!(decoded, b"hello world");
        assert_eq!(n, buf.len());
    }

    #[test]
    fn file_names() {
        assert_eq!(sst_file_name(1), "000001.sst");
        assert_eq!(wal_file_name(42), "000042.log");
        assert_eq!(manifest_file_name(3), "MANIFEST-000003");
    }
}
