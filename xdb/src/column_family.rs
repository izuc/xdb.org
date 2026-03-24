//! Column family support for xdb.
//!
//! Column families provide logical table namespacing within a single xdb
//! database. Internally, each column family is assigned a 1-byte ID that
//! is prepended to every key, ensuring complete isolation between families
//! while sharing a single LSM tree for efficiency.
//!
//! The API matches RocksDB's column family interface so that switching
//! from `rocksdb::` to `xdb::` requires minimal code changes.

use std::collections::HashMap;

/// A handle to a column family in the database.
///
/// Obtained via [`Db::cf_handle()`]. Column family handles are lightweight
/// (just a name and 1-byte ID) and can be cloned freely.
#[derive(Clone, Debug)]
pub struct ColumnFamily {
    pub(crate) name: String,
    pub(crate) id: u8,
}

impl ColumnFamily {
    /// The name of this column family.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Prepend this column family's ID byte to a key.
    #[inline]
    pub(crate) fn prefixed_key(&self, key: &[u8]) -> Vec<u8> {
        let mut prefixed = Vec::with_capacity(1 + key.len());
        prefixed.push(self.id);
        prefixed.extend_from_slice(key);
        prefixed
    }

    /// Strip the CF prefix byte from a key, returning the original user key.
    #[inline]
    pub fn strip_prefix(key: &[u8]) -> &[u8] {
        if key.is_empty() { key } else { &key[1..] }
    }
}

/// Descriptor for creating or opening a column family.
///
/// Matches the RocksDB `ColumnFamilyDescriptor` API.
pub struct ColumnFamilyDescriptor {
    name: String,
    // Options per-CF are accepted but currently ignored (xdb uses
    // database-wide options). This field exists for API compatibility.
    _options: crate::options::Options,
}

impl ColumnFamilyDescriptor {
    /// Create a new column family descriptor.
    pub fn new<S: Into<String>>(name: S, options: crate::options::Options) -> Self {
        ColumnFamilyDescriptor {
            name: name.into(),
            _options: options,
        }
    }

    /// The name of this column family.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Iterator mode for creating iterators, matching RocksDB's `IteratorMode`.
#[derive(Copy, Clone, Debug)]
pub enum IteratorMode<'a> {
    /// Start from the beginning (smallest key).
    Start,
    /// Start from the end (largest key).
    End,
    /// Start from the given key in the given direction.
    From(&'a [u8], Direction),
}

/// Iterator direction.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Reverse,
}

/// Manages the mapping between column family names and their internal IDs.
///
/// ID 0 is always reserved for "default". User-created CFs get IDs 1..255.
pub(crate) struct ColumnFamilyMap {
    name_to_id: HashMap<String, u8>,
    families: Vec<ColumnFamily>,
}

impl ColumnFamilyMap {
    /// Create a new map from a list of CF descriptors.
    ///
    /// Panics if more than 255 column families are requested.
    pub fn new(descriptors: &[ColumnFamilyDescriptor]) -> Self {
        let mut name_to_id = HashMap::new();
        let mut families = Vec::new();

        // Ensure "default" is always ID 0.
        let mut has_default = false;
        let mut next_id: u8 = 0;

        for desc in descriptors {
            if desc.name == "default" {
                has_default = true;
            }
        }

        if has_default {
            // Assign IDs in descriptor order, with "default" getting 0.
            for desc in descriptors {
                let id = if desc.name == "default" {
                    0
                } else {
                    next_id += 1;
                    if next_id == 0 {
                        // Wrapped around — too many CFs.
                        next_id = 255;
                    }
                    next_id
                };
                name_to_id.insert(desc.name.clone(), id);
                families.push(ColumnFamily {
                    name: desc.name.clone(),
                    id,
                });
            }
        } else {
            // No explicit "default" — add it as ID 0.
            name_to_id.insert("default".to_string(), 0);
            families.push(ColumnFamily {
                name: "default".to_string(),
                id: 0,
            });
            for desc in descriptors {
                next_id += 1;
                name_to_id.insert(desc.name.clone(), next_id);
                families.push(ColumnFamily {
                    name: desc.name.clone(),
                    id: next_id,
                });
            }
        }

        assert!(
            families.len() <= 256,
            "xdb supports at most 256 column families"
        );

        ColumnFamilyMap {
            name_to_id,
            families,
        }
    }

    /// Look up a column family by name.
    pub fn get(&self, name: &str) -> Option<&ColumnFamily> {
        let id = self.name_to_id.get(name)?;
        self.families.iter().find(|cf| cf.id == *id)
    }
}
