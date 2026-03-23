/*
 * xdb -- C FFI bindings for the xdb embedded key-value store.
 *
 * Memory management:
 *   - Objects created by xdb_*_create() must be freed with xdb_*_destroy().
 *   - Error strings (*errptr) must be freed with xdb_free().
 *   - Values returned by xdb_get() must be freed with xdb_free().
 *   - Pointers from xdb_iterator_key/value are borrowed -- do NOT free them.
 */

#ifndef XDB_H
#define XDB_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque types */
typedef struct xdb_t xdb_t;
typedef struct xdb_options_t xdb_options_t;
typedef struct xdb_writebatch_t xdb_writebatch_t;
typedef struct xdb_iterator_t xdb_iterator_t;

/* Options */
xdb_options_t* xdb_options_create(void);
void xdb_options_destroy(xdb_options_t* opts);
void xdb_options_set_create_if_missing(xdb_options_t* opts, unsigned char v);
void xdb_options_set_write_buffer_size(xdb_options_t* opts, size_t size);
void xdb_options_set_bloom_bits_per_key(xdb_options_t* opts, size_t bits);
void xdb_options_set_max_open_files(xdb_options_t* opts, size_t n);
void xdb_options_set_block_cache_capacity(xdb_options_t* opts, size_t size);
void xdb_options_set_sync_writes(xdb_options_t* opts, unsigned char v);

/* Database lifecycle */
xdb_t* xdb_open(const xdb_options_t* opts, const char* path, char** errptr);
void xdb_close(xdb_t* db);
void xdb_flush(xdb_t* db, char** errptr);

/* Point operations */
void xdb_put(xdb_t* db, const char* key, size_t keylen,
             const char* val, size_t vallen, char** errptr);
char* xdb_get(xdb_t* db, const char* key, size_t keylen,
              size_t* vallen, char** errptr);
void xdb_delete(xdb_t* db, const char* key, size_t keylen, char** errptr);
void xdb_delete_range(xdb_t* db,
                      const char* start_key, size_t start_keylen,
                      const char* end_key, size_t end_keylen,
                      char** errptr);

/* WriteBatch */
xdb_writebatch_t* xdb_writebatch_create(void);
void xdb_writebatch_destroy(xdb_writebatch_t* batch);
void xdb_writebatch_put(xdb_writebatch_t* batch,
                        const char* key, size_t keylen,
                        const char* val, size_t vallen);
void xdb_writebatch_delete(xdb_writebatch_t* batch,
                           const char* key, size_t keylen);
void xdb_writebatch_clear(xdb_writebatch_t* batch);
unsigned int xdb_writebatch_count(const xdb_writebatch_t* batch);
void xdb_write(xdb_t* db, xdb_writebatch_t* batch, char** errptr);

/* Iterator */
xdb_iterator_t* xdb_create_iterator(xdb_t* db);
void xdb_iterator_destroy(xdb_iterator_t* iter);
unsigned char xdb_iterator_valid(const xdb_iterator_t* iter);
void xdb_iterator_seek_to_first(xdb_iterator_t* iter);
void xdb_iterator_seek_to_last(xdb_iterator_t* iter);
void xdb_iterator_seek(xdb_iterator_t* iter, const char* key, size_t keylen);
void xdb_iterator_next(xdb_iterator_t* iter);
void xdb_iterator_prev(xdb_iterator_t* iter);
const char* xdb_iterator_key(const xdb_iterator_t* iter, size_t* keylen);
const char* xdb_iterator_value(const xdb_iterator_t* iter, size_t* vallen);

/* Memory management */
void xdb_free(void* ptr);

#ifdef __cplusplus
}
#endif

#endif /* XDB_H */
