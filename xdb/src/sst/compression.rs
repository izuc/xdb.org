//! Block compression and decompression helpers.

use crate::options::CompressionType;
use crate::error::{Error, Result};

/// Compression type byte stored in the block trailer.
pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_LZ4: u8 = 1;
pub const COMPRESSION_ZSTD: u8 = 2;

/// Map a CompressionType to its on-disk byte.
pub fn compression_type_to_byte(ct: CompressionType) -> u8 {
    match ct {
        CompressionType::None => COMPRESSION_NONE,
        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => COMPRESSION_LZ4,
        #[cfg(feature = "zstd")]
        CompressionType::Zstd => COMPRESSION_ZSTD,
    }
}

/// Compress a block of data.
pub fn compress(data: &[u8], ct: CompressionType) -> Result<Vec<u8>> {
    match ct {
        CompressionType::None => Ok(data.to_vec()),
        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        #[cfg(feature = "zstd")]
        CompressionType::Zstd => {
            zstd::encode_all(data, 3) // level 3 is a good balance
                .map_err(|e| Error::internal(format!("zstd compress error: {}", e)))
        }
    }
}

/// Decompress a block of data based on the compression type byte.
pub fn decompress(data: &[u8], compression_byte: u8) -> Result<Vec<u8>> {
    match compression_byte {
        COMPRESSION_NONE => Ok(data.to_vec()),
        COMPRESSION_LZ4 => {
            #[cfg(feature = "lz4")]
            {
                lz4_flex::decompress_size_prepended(data)
                    .map_err(|e| Error::corruption(format!("lz4 decompress error: {}", e)))
            }
            #[cfg(not(feature = "lz4"))]
            {
                Err(Error::not_supported("lz4 compression not enabled"))
            }
        }
        COMPRESSION_ZSTD => {
            #[cfg(feature = "zstd")]
            {
                zstd::decode_all(data)
                    .map_err(|e| Error::corruption(format!("zstd decompress error: {}", e)))
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err(Error::not_supported("zstd compression not enabled"))
            }
        }
        other => Err(Error::corruption(format!("unknown compression type: {}", other))),
    }
}
