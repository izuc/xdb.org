use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;

use xdb_core as xdb_dep;

/// Helper to convert xdb errors into Python RuntimeError.
fn to_pyerr(e: xdb_dep::Error) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// Python wrapper around the xdb database handle.
#[pyclass]
struct Database {
    db: Option<Arc<xdb_dep::Db>>,
}

#[pymethods]
impl Database {
    #[new]
    #[pyo3(signature = (path, create_if_missing=true, write_buffer_size=None, bloom_bits_per_key=None))]
    fn new(
        path: &str,
        create_if_missing: bool,
        write_buffer_size: Option<usize>,
        bloom_bits_per_key: Option<usize>,
    ) -> PyResult<Self> {
        let mut opts = xdb_dep::Options::default();
        opts.create_if_missing = create_if_missing;
        if let Some(size) = write_buffer_size {
            opts.write_buffer_size = size;
        }
        if let Some(bits) = bloom_bits_per_key {
            opts.bloom_bits_per_key = bits;
        }
        let db = xdb_dep::Db::open(opts, path).map_err(to_pyerr)?;
        Ok(Database { db: Some(db) })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.get_db()?.put(key, value).map_err(to_pyerr)
    }

    fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        self.get_db()?.get(key).map_err(to_pyerr)
    }

    fn delete(&self, key: &[u8]) -> PyResult<()> {
        self.get_db()?.delete(key).map_err(to_pyerr)
    }

    fn delete_range(&self, start_key: &[u8], end_key: &[u8]) -> PyResult<()> {
        self.get_db()?
            .delete_range(start_key, end_key)
            .map_err(to_pyerr)
    }

    fn write(&self, batch: &PyWriteBatch) -> PyResult<()> {
        let b = batch.clone_batch()?;
        self.get_db()?
            .write(xdb_dep::WriteOptions::default(), b)
            .map_err(to_pyerr)
    }

    fn flush(&self) -> PyResult<()> {
        self.get_db()?.flush().map_err(to_pyerr)
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some(db) = self.db.take() {
            db.close().map_err(to_pyerr)?;
        }
        Ok(())
    }

    fn snapshot(&self) -> PyResult<PySnapshot> {
        let db = self.get_db()?.clone();
        let snap = db.snapshot();
        Ok(PySnapshot {
            snapshot: Some(snap),
            db,
        })
    }

    fn iter(&self) -> PyResult<PyIterator> {
        let iter = self.get_db()?.iter();
        Ok(PyIterator {
            iter: Some(iter),
            started: false,
        })
    }

    fn iter_with_snapshot(&self, snap: &PySnapshot) -> PyResult<PyIterator> {
        let snapshot = snap
            .snapshot
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("snapshot released"))?;
        let iter = self.get_db()?.iter_with_snapshot(snapshot);
        Ok(PyIterator {
            iter: Some(iter),
            started: false,
        })
    }

    fn stats(&self) -> PyResult<String> {
        Ok(format!("{}", self.get_db()?.stats()))
    }

    fn __iter__(&self) -> PyResult<PyIterator> {
        let mut iter = self.get_db()?.iter();
        iter.seek_to_first();
        Ok(PyIterator {
            iter: Some(iter),
            started: true,
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<()> {
        self.close()
    }
}

impl Database {
    fn get_db(&self) -> PyResult<&Arc<xdb_dep::Db>> {
        self.db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("database is closed"))
    }
}

// ---------------------------------------------------------------------------
// WriteBatch
// ---------------------------------------------------------------------------

/// Python wrapper around `xdb::WriteBatch`.
#[pyclass]
struct PyWriteBatch {
    batch: Option<xdb_dep::WriteBatch>,
}

#[pymethods]
impl PyWriteBatch {
    #[new]
    fn new() -> Self {
        PyWriteBatch {
            batch: Some(xdb_dep::WriteBatch::new()),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.get_batch_mut()?.put(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> PyResult<()> {
        self.get_batch_mut()?.delete(key);
        Ok(())
    }

    fn delete_range(&mut self, start_key: &[u8], end_key: &[u8]) -> PyResult<()> {
        self.get_batch_mut()?.delete_range(start_key, end_key);
        Ok(())
    }

    fn clear(&mut self) -> PyResult<()> {
        self.get_batch_mut()?.clear();
        Ok(())
    }

    fn count(&self) -> u32 {
        self.batch.as_ref().map_or(0, |b| b.count())
    }
}

impl PyWriteBatch {
    fn get_batch_mut(&mut self) -> PyResult<&mut xdb_dep::WriteBatch> {
        self.batch
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("batch already consumed"))
    }

    /// Clone the batch data so the Python batch object can be reused after
    /// `db.write(batch)`.
    fn clone_batch(&self) -> PyResult<xdb_dep::WriteBatch> {
        let b = self
            .batch
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("batch already consumed"))?;
        Ok(xdb_dep::WriteBatch::from_data(b.data().to_vec()))
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// Python wrapper around `xdb::Snapshot`.
#[pyclass]
struct PySnapshot {
    snapshot: Option<Arc<xdb_dep::Snapshot>>,
    db: Arc<xdb_dep::Db>,
}

#[pymethods]
impl PySnapshot {
    fn release(&mut self) {
        if let Some(snap) = self.snapshot.take() {
            self.db.release_snapshot(&snap);
        }
    }
}

impl Drop for PySnapshot {
    fn drop(&mut self) {
        self.release();
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

/// Python wrapper around `xdb::DbIterator`.
///
/// Marked `unsendable` because `DbIterator` contains trait objects that are
/// not `Send`.
#[pyclass(unsendable)]
struct PyIterator {
    iter: Option<xdb_dep::DbIterator>,
    started: bool,
}

#[pymethods]
impl PyIterator {
    fn valid(&self) -> PyResult<bool> {
        let iter = self.get_iter()?;
        Ok(iter.valid())
    }

    fn seek_to_first(&mut self) -> PyResult<()> {
        self.get_iter_mut()?.seek_to_first();
        self.started = true;
        Ok(())
    }

    fn seek_to_last(&mut self) -> PyResult<()> {
        self.get_iter_mut()?.seek_to_last();
        self.started = true;
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> PyResult<()> {
        self.get_iter_mut()?.seek(target);
        self.started = true;
        Ok(())
    }

    #[pyo3(name = "next")]
    fn next_entry(&mut self) -> PyResult<()> {
        self.get_iter_mut()?.next();
        Ok(())
    }

    fn prev(&mut self) -> PyResult<()> {
        self.get_iter_mut()?.prev();
        Ok(())
    }

    fn key(&self) -> PyResult<Option<Vec<u8>>> {
        let iter = self.get_iter()?;
        if iter.valid() {
            Ok(Some(iter.key().to_vec()))
        } else {
            Ok(None)
        }
    }

    fn value(&self) -> PyResult<Option<Vec<u8>>> {
        let iter = self.get_iter()?;
        if iter.valid() {
            Ok(Some(iter.value().to_vec()))
        } else {
            Ok(None)
        }
    }

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(Vec<u8>, Vec<u8>)>> {
        let iter = self
            .iter
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("iterator has been invalidated"))?;
        if !self.started {
            iter.seek_to_first();
            self.started = true;
        } else if iter.valid() {
            iter.next();
        }
        if iter.valid() {
            Ok(Some((iter.key().to_vec(), iter.value().to_vec())))
        } else {
            Ok(None)
        }
    }
}

impl PyIterator {
    fn get_iter(&self) -> PyResult<&xdb_dep::DbIterator> {
        self.iter
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("iterator has been invalidated"))
    }

    fn get_iter_mut(&mut self) -> PyResult<&mut xdb_dep::DbIterator> {
        self.iter
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("iterator has been invalidated"))
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

/// The Python module.
#[pymodule]
fn xdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<PyWriteBatch>()?;
    m.add_class::<PySnapshot>()?;
    m.add_class::<PyIterator>()?;
    Ok(())
}
