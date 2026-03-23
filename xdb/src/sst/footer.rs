//! Block handles and SST file footer.
//!
//! The footer occupies the last [`FOOTER_SIZE`] bytes of every SST file and
//! contains two [`BlockHandle`]s (metaindex and index) plus a magic number
//! for format identification.

use crate::error;
use crate::types::{decode_varint64, encode_varint64, varint64_length};

/// Total footer size in bytes: 2 block handles (max 20 bytes each) + 8-byte magic.
pub const FOOTER_SIZE: usize = 48;

/// Magic number written at the end of every xdb SST file.
/// Encodes "xdbsst01" as a little-endian u64.
pub const MAGIC_NUMBER: u64 = 0x_78_64_62_73_73_74_30_31;

// ---------------------------------------------------------------------------
// BlockHandle
// ---------------------------------------------------------------------------

/// Pointer to a block within an SST file (offset + size).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockHandle {
    /// Byte offset of the block from the start of the file.
    pub offset: u64,
    /// Size of the block data in bytes (excludes the 5-byte trailer).
    pub size: u64,
}

impl BlockHandle {
    /// Create a new block handle.
    pub fn new(offset: u64, size: u64) -> Self {
        BlockHandle { offset, size }
    }

    /// Encode as two varints appended to `buf`.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint64(buf, self.offset);
        encode_varint64(buf, self.size);
    }

    /// Decode from the start of `data`.
    ///
    /// Returns `(handle, bytes_consumed)` or `None` if the data is truncated.
    pub fn decode(data: &[u8]) -> Option<(Self, usize)> {
        let (offset, n1) = decode_varint64(data)?;
        let (size, n2) = decode_varint64(&data[n1..])?;
        Some((BlockHandle { offset, size }, n1 + n2))
    }

    /// Number of bytes this handle would occupy when varint-encoded.
    pub fn encoded_len(&self) -> usize {
        varint64_length(self.offset) + varint64_length(self.size)
    }
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

/// The last [`FOOTER_SIZE`] bytes of an SST file.
///
/// Layout:
/// ```text
/// [metaindex_handle varint] [index_handle varint] [zero-padding] [magic: 8 bytes LE]
/// ```
#[derive(Debug, Clone)]
pub struct Footer {
    /// Handle to the metaindex block (maps filter names to block handles).
    pub metaindex_handle: BlockHandle,
    /// Handle to the index block (maps last-key-per-block to data block handles).
    pub index_handle: BlockHandle,
}

impl Footer {
    /// Encode the footer into a fixed-size byte array.
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);
        self.metaindex_handle.encode(&mut buf);
        self.index_handle.encode(&mut buf);

        // Zero-pad up to FOOTER_SIZE - 8 bytes.
        let pad_target = FOOTER_SIZE - 8;
        debug_assert!(buf.len() <= pad_target);
        buf.resize(pad_target, 0);

        // Write magic number as u64 LE.
        buf.extend_from_slice(&MAGIC_NUMBER.to_le_bytes());

        debug_assert_eq!(buf.len(), FOOTER_SIZE);
        let mut out = [0u8; FOOTER_SIZE];
        out.copy_from_slice(&buf);
        out
    }

    /// Decode a footer from exactly [`FOOTER_SIZE`] bytes.
    pub fn decode(data: &[u8; FOOTER_SIZE]) -> error::Result<Self> {
        // Verify magic number at the end.
        let magic_start = FOOTER_SIZE - 8;
        let magic = u64::from_le_bytes(data[magic_start..].try_into().unwrap());
        if magic != MAGIC_NUMBER {
            return Err(error::Error::corruption(format!(
                "bad SST footer magic: expected {:#018x}, got {:#018x}",
                MAGIC_NUMBER, magic
            )));
        }

        // Decode the two block handles from the start.
        let (metaindex_handle, n1) = BlockHandle::decode(data)
            .ok_or_else(|| error::Error::corruption("truncated metaindex handle in footer"))?;
        let (index_handle, _n2) = BlockHandle::decode(&data[n1..])
            .ok_or_else(|| error::Error::corruption("truncated index handle in footer"))?;

        Ok(Footer {
            metaindex_handle,
            index_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_handle_encode_decode_roundtrip() {
        let cases = [
            BlockHandle::new(0, 0),
            BlockHandle::new(1, 1),
            BlockHandle::new(127, 255),
            BlockHandle::new(4096, 65536),
            BlockHandle::new(u64::MAX / 2, u64::MAX / 3),
        ];

        for handle in &cases {
            let mut buf = Vec::new();
            handle.encode(&mut buf);
            assert_eq!(buf.len(), handle.encoded_len());

            let (decoded, consumed) = BlockHandle::decode(&buf).unwrap();
            assert_eq!(decoded, *handle);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn block_handle_decode_truncated_returns_none() {
        assert!(BlockHandle::decode(&[]).is_none());
        assert!(BlockHandle::decode(&[0x80]).is_none());
    }

    #[test]
    fn footer_encode_decode_roundtrip() {
        let footer = Footer {
            metaindex_handle: BlockHandle::new(100, 200),
            index_handle: BlockHandle::new(300, 400),
        };
        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.metaindex_handle, footer.metaindex_handle);
        assert_eq!(decoded.index_handle, footer.index_handle);
    }

    #[test]
    fn footer_decode_bad_magic() {
        let mut data = [0u8; FOOTER_SIZE];
        data[FOOTER_SIZE - 8..].copy_from_slice(&0xDEADBEEFu64.to_le_bytes());
        let result = Footer::decode(&data);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("magic"));
    }

    #[test]
    fn footer_with_large_handles() {
        let footer = Footer {
            metaindex_handle: BlockHandle::new(1_000_000, 500_000),
            index_handle: BlockHandle::new(2_000_000, 100_000),
        };
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.metaindex_handle, footer.metaindex_handle);
        assert_eq!(decoded.index_handle, footer.index_handle);
    }

    #[test]
    fn footer_with_zero_handles() {
        let footer = Footer {
            metaindex_handle: BlockHandle::default(),
            index_handle: BlockHandle::default(),
        };
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.metaindex_handle, BlockHandle::new(0, 0));
        assert_eq!(decoded.index_handle, BlockHandle::new(0, 0));
    }
}
