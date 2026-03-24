//! Bloom filter for fast negative lookups in SST files.
//!
//! Uses double-hashing derived from a single hash: `h(i) = h1 + i * h2`.
//! The filter data format is `[filter_bits: N bytes] [k: u8]`, where the
//! last byte stores the number of hash probes.

/// Bloom filter configuration.
///
/// Builds compact bit-vector filters and checks membership.
/// False positives are possible; false negatives are not.
pub struct BloomFilter {
    bits_per_key: usize,
    k: usize, // number of hash probes
}

/// Simple hash function similar to LevelDB's bloom hash.
#[inline]
fn bloom_hash(key: &[u8]) -> u32 {
    let mut h: u32 = 0;
    for &b in key {
        h = h.wrapping_mul(0x5bd1e995).wrapping_add(b as u32);
        h ^= h >> 15;
    }
    h
}

impl BloomFilter {
    /// Create a new `BloomFilter` with the given bits-per-key setting.
    ///
    /// A higher value means fewer false positives but larger filters.
    /// 10 bits/key yields roughly a 1% false-positive rate.
    pub fn new(bits_per_key: usize) -> Self {
        // k = bits_per_key * ln(2) ~ bits_per_key * 0.69, clamped to [1, 30].
        let k = ((bits_per_key as f64) * 0.69) as usize;
        let k = k.clamp(1, 30);
        BloomFilter { bits_per_key, k }
    }

    /// Build a filter from a list of keys.
    ///
    /// Returns the filter data bytes. The last byte is `k` (number of probes).
    pub fn build(&self, keys: &[&[u8]]) -> Vec<u8> {
        let num_keys = keys.len();
        // Number of bits, with a minimum of 8 (1 byte).
        let num_bits = std::cmp::max(8, num_keys * self.bits_per_key);
        // Round up to whole bytes.
        let num_bytes = num_bits.div_ceil(8);
        let num_bits = num_bytes * 8;

        let mut data = vec![0u8; num_bytes];

        for key in keys {
            let mut h = bloom_hash(key);
            let delta = h.rotate_left(15);
            for _ in 0..self.k {
                let bit_pos = (h as usize) % num_bits;
                data[bit_pos / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        // Append k as the last byte.
        data.push(self.k as u8);
        data
    }

    /// Check if a key might exist in the filter.
    ///
    /// Returns `false` if the key is **definitely not** present.
    /// Returns `true` if the key **might** be present (possible false positive).
    #[inline]
    pub fn may_contain(filter_data: &[u8], key: &[u8]) -> bool {
        if filter_data.is_empty() {
            return false;
        }

        let k = *filter_data.last().unwrap() as usize;
        if k == 0 || k > 30 {
            // Treat as a degenerate filter: assume everything matches.
            return true;
        }

        let bits_data = &filter_data[..filter_data.len() - 1];
        if bits_data.is_empty() {
            return false;
        }

        let num_bits = bits_data.len() * 8;

        let mut h = bloom_hash(key);
        let delta = h.rotate_left(15);
        for _ in 0..k {
            let bit_pos = (h as usize) % num_bits;
            if bits_data[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_keys_pass_may_contain() {
        let bf = BloomFilter::new(10);
        let keys: Vec<&[u8]> = vec![
            b"hello",
            b"world",
            b"foo",
            b"bar",
            b"baz",
            b"xdb-is-great",
        ];
        let filter = bf.build(&keys);

        for key in &keys {
            assert!(
                BloomFilter::may_contain(&filter, key),
                "key {:?} should be found in filter",
                std::str::from_utf8(key)
            );
        }
    }

    #[test]
    fn empty_filter_returns_false() {
        assert!(!BloomFilter::may_contain(&[], b"anything"));
    }

    #[test]
    fn single_key() {
        let bf = BloomFilter::new(10);
        let filter = bf.build(&[b"only-key"]);
        assert!(BloomFilter::may_contain(&filter, b"only-key"));
    }

    #[test]
    fn false_positive_rate_below_two_percent() {
        let bf = BloomFilter::new(10);

        // Insert 1000 keys with a known pattern.
        let inserted: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("key-{:06}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = inserted.iter().map(|k| k.as_slice()).collect();
        let filter = bf.build(&key_refs);

        // Test 10000 keys that were NOT inserted.
        let mut false_positives = 0u32;
        let test_count = 10_000u32;
        for i in 0..test_count {
            let key = format!("miss-{:06}", i).into_bytes();
            if BloomFilter::may_contain(&filter, &key) {
                false_positives += 1;
            }
        }

        let rate = false_positives as f64 / test_count as f64;
        assert!(
            rate < 0.02,
            "false positive rate {:.4} exceeds 2%",
            rate
        );
    }

    #[test]
    fn k_is_clamped() {
        let bf_low = BloomFilter::new(0);
        assert_eq!(bf_low.k, 1);

        let bf_high = BloomFilter::new(100);
        assert_eq!(bf_high.k, 30);
    }

    #[test]
    fn large_key_set() {
        let bf = BloomFilter::new(10);
        let keys: Vec<Vec<u8>> = (0..5000u32)
            .map(|i| i.to_le_bytes().to_vec())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = bf.build(&key_refs);

        // Every inserted key must be found.
        for key in &keys {
            assert!(BloomFilter::may_contain(&filter, key));
        }
    }
}
