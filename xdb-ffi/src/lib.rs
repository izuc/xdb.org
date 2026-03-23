//! C FFI bindings for xdb.
//!
//! This module provides a C-compatible API for xdb, allowing it to be
//! embedded in C, C++, Go, and other languages via FFI.
//!
//! # Error Handling
//!
//! Functions that can fail take a `*mut *mut c_char` parameter called `errptr`.
//! On success, `*errptr` is set to null. On failure, `*errptr` is set to a
//! heap-allocated C string describing the error. The caller must free it with
//! `xdb_free()`.
//!
//! # Memory Management
//!
//! - Objects returned by `_create` functions must be freed with the
//!   corresponding `_destroy` function.
//! - `char*` values returned by `xdb_get` must be freed with `xdb_free()`.
//! - Error strings must be freed with `xdb_free()`.
//! - Pointers from `xdb_iterator_key` / `xdb_iterator_value` are borrowed
//!   and valid only until the next iterator operation. Do NOT free them.
//!
//! Internally, all heap buffers returned to C callers are prefixed with a
//! hidden `usize` header that stores the allocation length. `xdb_free`
//! reads this header to reconstruct the `Box<[u8]>` and drop it properly.

#![allow(non_camel_case_types)]

use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use std::slice;
use std::sync::Arc;

use xdb::{Db, DbIterator, Options, WriteBatch, WriteOptions};

// ---------------------------------------------------------------------------
// Opaque types
// ---------------------------------------------------------------------------

pub struct xdb_t {
    db: Arc<Db>,
}

pub struct xdb_options_t {
    opts: Options,
}

pub struct xdb_writebatch_t {
    batch: WriteBatch,
}

pub struct xdb_iterator_t {
    iter: DbIterator,
}

// ---------------------------------------------------------------------------
// Internal allocation helpers
//
// Every buffer returned to C is laid out as:
//
//   [ usize: data_len ] [ data bytes ... ]
//                        ^--- pointer returned to caller
//
// xdb_free() subtracts sizeof(usize) from the pointer, reads the length,
// reconstructs the Box<[u8]>, and drops it.
// ---------------------------------------------------------------------------

const HEADER: usize = std::mem::size_of::<usize>();

/// Allocate a buffer with a hidden length prefix, copy `data` into it,
/// and return a pointer to the data portion (past the header).
fn heap_alloc(data: &[u8]) -> *mut c_char {
    let total = HEADER + data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&data.len().to_ne_bytes());
    buf.extend_from_slice(data);
    debug_assert_eq!(buf.len(), total);
    let boxed = buf.into_boxed_slice();
    let base = Box::into_raw(boxed) as *mut u8;
    // SAFETY: base is valid for `total` bytes; we return base + HEADER.
    unsafe { base.add(HEADER) as *mut c_char }
}

/// Reconstruct the `Box<[u8]>` from a pointer previously returned by
/// `heap_alloc` and drop it, releasing the memory.
///
/// # Safety
///
/// `ptr` must have been returned by `heap_alloc` (or be null).
unsafe fn heap_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    let data_ptr = ptr as *mut u8;
    let base = data_ptr.sub(HEADER);
    let len_bytes: [u8; HEADER] =
        slice::from_raw_parts(base, HEADER).try_into().unwrap();
    let data_len = usize::from_ne_bytes(len_bytes);
    let total = HEADER + data_len;
    // Reconstruct the original Box<[u8]> and drop it.
    let _ = Box::from_raw(slice::from_raw_parts_mut(base, total));
}

// ---------------------------------------------------------------------------
// Helper: set / clear error string
// ---------------------------------------------------------------------------

unsafe fn set_error(errptr: *mut *mut c_char, err: xdb::Error) {
    if !errptr.is_null() {
        let msg = err.to_string();
        // Allocate a null-terminated copy via heap_alloc. We include the
        // null terminator in the data so C can treat it as a C string.
        let mut bytes = msg.into_bytes();
        bytes.push(0); // null terminator
        *errptr = heap_alloc(&bytes);
    }
}

unsafe fn clear_error(errptr: *mut *mut c_char) {
    if !errptr.is_null() {
        *errptr = ptr::null_mut();
    }
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn xdb_options_create() -> *mut xdb_options_t {
    Box::into_raw(Box::new(xdb_options_t {
        opts: Options::default(),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn xdb_options_destroy(opts: *mut xdb_options_t) {
    if !opts.is_null() {
        drop(Box::from_raw(opts));
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_options_set_create_if_missing(
    opts: *mut xdb_options_t,
    v: u8,
) {
    if !opts.is_null() {
        (*opts).opts.create_if_missing = v != 0;
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_options_set_write_buffer_size(
    opts: *mut xdb_options_t,
    size: usize,
) {
    if !opts.is_null() {
        (*opts).opts.write_buffer_size = size;
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_options_set_bloom_bits_per_key(
    opts: *mut xdb_options_t,
    bits: usize,
) {
    if !opts.is_null() {
        (*opts).opts.bloom_bits_per_key = bits;
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_options_set_max_open_files(
    opts: *mut xdb_options_t,
    n: usize,
) {
    if !opts.is_null() {
        (*opts).opts.max_open_files = n;
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_options_set_block_cache_capacity(
    opts: *mut xdb_options_t,
    size: usize,
) {
    if !opts.is_null() {
        (*opts).opts.block_cache_capacity = size;
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_options_set_sync_writes(
    opts: *mut xdb_options_t,
    v: u8,
) {
    if !opts.is_null() {
        (*opts).opts.sync_writes = v != 0;
    }
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn xdb_open(
    opts: *const xdb_options_t,
    path: *const c_char,
    errptr: *mut *mut c_char,
) -> *mut xdb_t {
    clear_error(errptr);

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(
                errptr,
                xdb::Error::InvalidArgument("invalid UTF-8 path".into()),
            );
            return ptr::null_mut();
        }
    };

    let options = if opts.is_null() {
        Options::default().create_if_missing(true)
    } else {
        (*opts).opts.clone()
    };

    match Db::open(options, path_str) {
        Ok(db) => Box::into_raw(Box::new(xdb_t { db })),
        Err(e) => {
            set_error(errptr, e);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_close(db: *mut xdb_t) {
    if !db.is_null() {
        let db_box = Box::from_raw(db);
        let _ = db_box.db.close();
    }
}

// ---------------------------------------------------------------------------
// Point operations
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn xdb_put(
    db: *mut xdb_t,
    key: *const c_char,
    keylen: usize,
    val: *const c_char,
    vallen: usize,
    errptr: *mut *mut c_char,
) {
    clear_error(errptr);
    if db.is_null() || key.is_null() || val.is_null() {
        return;
    }
    let key_slice = slice::from_raw_parts(key as *const u8, keylen);
    let val_slice = slice::from_raw_parts(val as *const u8, vallen);

    if let Err(e) = (*db).db.put(key_slice, val_slice) {
        set_error(errptr, e);
    }
}

/// Returns a heap-allocated copy of the value. Caller must free with
/// `xdb_free()`. Sets `*vallen` to the length of the value.
/// Returns NULL if the key is not found (this is not an error).
#[no_mangle]
pub unsafe extern "C" fn xdb_get(
    db: *mut xdb_t,
    key: *const c_char,
    keylen: usize,
    vallen: *mut usize,
    errptr: *mut *mut c_char,
) -> *mut c_char {
    clear_error(errptr);
    if db.is_null() || key.is_null() {
        return ptr::null_mut();
    }
    let key_slice = slice::from_raw_parts(key as *const u8, keylen);

    match (*db).db.get(key_slice) {
        Ok(Some(value)) => {
            if !vallen.is_null() {
                *vallen = value.len();
            }
            heap_alloc(&value)
        }
        Ok(None) => {
            if !vallen.is_null() {
                *vallen = 0;
            }
            ptr::null_mut()
        }
        Err(e) => {
            set_error(errptr, e);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_delete(
    db: *mut xdb_t,
    key: *const c_char,
    keylen: usize,
    errptr: *mut *mut c_char,
) {
    clear_error(errptr);
    if db.is_null() || key.is_null() {
        return;
    }
    let key_slice = slice::from_raw_parts(key as *const u8, keylen);

    if let Err(e) = (*db).db.delete(key_slice) {
        set_error(errptr, e);
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_delete_range(
    db: *mut xdb_t,
    start_key: *const c_char,
    start_keylen: usize,
    end_key: *const c_char,
    end_keylen: usize,
    errptr: *mut *mut c_char,
) {
    clear_error(errptr);
    if db.is_null() || start_key.is_null() || end_key.is_null() {
        return;
    }
    let start = slice::from_raw_parts(start_key as *const u8, start_keylen);
    let end = slice::from_raw_parts(end_key as *const u8, end_keylen);

    if let Err(e) = (*db).db.delete_range(start, end) {
        set_error(errptr, e);
    }
}

// ---------------------------------------------------------------------------
// WriteBatch
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn xdb_writebatch_create() -> *mut xdb_writebatch_t {
    Box::into_raw(Box::new(xdb_writebatch_t {
        batch: WriteBatch::new(),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn xdb_writebatch_destroy(batch: *mut xdb_writebatch_t) {
    if !batch.is_null() {
        drop(Box::from_raw(batch));
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_writebatch_put(
    batch: *mut xdb_writebatch_t,
    key: *const c_char,
    keylen: usize,
    val: *const c_char,
    vallen: usize,
) {
    if batch.is_null() || key.is_null() || val.is_null() {
        return;
    }
    let key_slice = slice::from_raw_parts(key as *const u8, keylen);
    let val_slice = slice::from_raw_parts(val as *const u8, vallen);
    (*batch).batch.put(key_slice, val_slice);
}

#[no_mangle]
pub unsafe extern "C" fn xdb_writebatch_delete(
    batch: *mut xdb_writebatch_t,
    key: *const c_char,
    keylen: usize,
) {
    if batch.is_null() || key.is_null() {
        return;
    }
    let key_slice = slice::from_raw_parts(key as *const u8, keylen);
    (*batch).batch.delete(key_slice);
}

#[no_mangle]
pub unsafe extern "C" fn xdb_writebatch_clear(batch: *mut xdb_writebatch_t) {
    if !batch.is_null() {
        (*batch).batch.clear();
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_writebatch_count(
    batch: *const xdb_writebatch_t,
) -> u32 {
    if batch.is_null() {
        0
    } else {
        (*batch).batch.count()
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_write(
    db: *mut xdb_t,
    batch: *mut xdb_writebatch_t,
    errptr: *mut *mut c_char,
) {
    clear_error(errptr);
    if db.is_null() || batch.is_null() {
        return;
    }
    // Take the batch out and replace with an empty one so the caller
    // can reuse the xdb_writebatch_t handle.
    let batch_val =
        std::mem::replace(&mut (*batch).batch, WriteBatch::new());

    if let Err(e) = (*db).db.write(WriteOptions::default(), batch_val) {
        set_error(errptr, e);
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn xdb_create_iterator(
    db: *mut xdb_t,
) -> *mut xdb_iterator_t {
    if db.is_null() {
        return ptr::null_mut();
    }
    let iter = (*db).db.iter();
    Box::into_raw(Box::new(xdb_iterator_t { iter }))
}

#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_destroy(iter: *mut xdb_iterator_t) {
    if !iter.is_null() {
        drop(Box::from_raw(iter));
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_valid(
    iter: *const xdb_iterator_t,
) -> u8 {
    if iter.is_null() {
        0
    } else {
        (*iter).iter.valid() as u8
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_seek_to_first(
    iter: *mut xdb_iterator_t,
) {
    if !iter.is_null() {
        (*iter).iter.seek_to_first();
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_seek_to_last(
    iter: *mut xdb_iterator_t,
) {
    if !iter.is_null() {
        (*iter).iter.seek_to_last();
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_seek(
    iter: *mut xdb_iterator_t,
    key: *const c_char,
    keylen: usize,
) {
    if iter.is_null() || key.is_null() {
        return;
    }
    let key_slice = slice::from_raw_parts(key as *const u8, keylen);
    (*iter).iter.seek(key_slice);
}

#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_next(iter: *mut xdb_iterator_t) {
    if !iter.is_null() {
        (*iter).iter.next();
    }
}

#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_prev(iter: *mut xdb_iterator_t) {
    if !iter.is_null() {
        (*iter).iter.prev();
    }
}

/// Returns a pointer to the current key. The pointer is borrowed and valid
/// only until the next iterator operation. Do NOT free this pointer.
#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_key(
    iter: *const xdb_iterator_t,
    keylen: *mut usize,
) -> *const c_char {
    if iter.is_null() || !(*iter).iter.valid() {
        return ptr::null();
    }
    let key = (*iter).iter.key();
    if !keylen.is_null() {
        *keylen = key.len();
    }
    key.as_ptr() as *const c_char
}

/// Returns a pointer to the current value. The pointer is borrowed and valid
/// only until the next iterator operation. Do NOT free this pointer.
#[no_mangle]
pub unsafe extern "C" fn xdb_iterator_value(
    iter: *const xdb_iterator_t,
    vallen: *mut usize,
) -> *const c_char {
    if iter.is_null() || !(*iter).iter.valid() {
        return ptr::null();
    }
    let val = (*iter).iter.value();
    if !vallen.is_null() {
        *vallen = val.len();
    }
    val.as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn xdb_flush(
    db: *mut xdb_t,
    errptr: *mut *mut c_char,
) {
    clear_error(errptr);
    if !db.is_null() {
        if let Err(e) = (*db).db.flush() {
            set_error(errptr, e);
        }
    }
}

/// Free a pointer previously allocated by xdb (error strings, `xdb_get`
/// return values). This function is a no-op if `ptr` is null.
///
/// Do NOT pass iterator key/value pointers to this function -- those are
/// borrowed, not owned.
#[no_mangle]
pub unsafe extern "C" fn xdb_free(ptr: *mut c_void) {
    heap_free(ptr);
}
