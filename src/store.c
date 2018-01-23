/* store.c
   Rémi Attab (remi.attab@gmail.com), 02 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"
#include "htable.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <assert.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>


// -----------------------------------------------------------------------------
// impl
// -----------------------------------------------------------------------------

#include "vals.c"
#include "index.c"
#include "coder.c"

// -----------------------------------------------------------------------------
// store
// -----------------------------------------------------------------------------

/* version 6 introduces reverse lookup, and massive db format changes */
static const uint32_t version = 6;

static const uint32_t magic = 0x4C4C4952;
static const uint64_t stamp = 0xFFFFFFFFFFFFFFFFUL;
/* version 6 can not support older dbs -- they'll need to be updated */
static const uint32_t supported_versions[] = { 6 };

struct rill_packed header
{
    uint32_t magic;
    uint32_t version;

    uint64_t ts;
    uint64_t quant;

    uint64_t pairs;

    uint64_t data_a_off;
    uint64_t data_b_off;

    uint64_t index_a_off;
    uint64_t index_b_off;

    uint64_t reserved[2];

    uint64_t stamp;
};

struct rill_store
{
    int fd;
    const char *file;

    void *vma;
    size_t vma_len;

    struct header *head;
    struct vals *vals;

    uint8_t *data_a;
    uint8_t *data_b;
    struct index *index_a;
    struct index *index_b;
    uint8_t *end;
};

struct rill_space
{
    size_t header_bytes;
    size_t index_bytes[2];
    size_t pairs_bytes[2];
};


// -----------------------------------------------------------------------------
// coder
// -----------------------------------------------------------------------------

static struct encoder store_encoder(
        struct rill_store *store,
        struct indexer *indexer,
        struct vals* vals,
        uint64_t offset)
{
    return make_encoder(
            store->vma + offset,
            store->vma + store->vma_len,
            vals,
            indexer);
}

static struct decoder store_decoder_at(
        struct rill_store *store,
        size_t key_idx,
        uint64_t curr_off,
        enum rill_col column)
{
    size_t offset = 0;
    size_t offset_end = 0;
    struct index* lookup = NULL;
    struct index* index = NULL;

    switch (column) {
    case rill_col_a:
        lookup = store->index_b;
        index  = store->index_a;
        offset = store->head->data_a_off;
        offset_end = store->head->data_b_off;
        break;
    case rill_col_b:
        lookup = store->index_a;
        index  = store->index_b;
        offset = store->head->data_b_off;
        offset_end = store->vma_len;
        break;
    default:
        rill_fail("improper rill col passed: %d", column);
        break;
    }

    return make_decoder_at(
            store->vma + offset + curr_off,
            store->vma + offset_end,
            lookup,
            index,
            key_idx);
}

static struct decoder store_decoder(
        struct rill_store *store,
        enum rill_col column)
{
    return store_decoder_at(store, 0, 0, column);
}

// -----------------------------------------------------------------------------
// vma
// -----------------------------------------------------------------------------

static inline void vma_will_need(struct rill_store *store)
{
    if (madvise(store->vma, store->vma_len, MADV_WILLNEED) == -1)
        rill_fail("unable to madvise '%s'", store->file);
}

static inline void vma_dont_need(struct rill_store *store)
{
    if (madvise(store->vma, store->vma_len, MADV_DONTNEED) == -1)
        rill_fail("unable to madvise '%s'", store->file);
}


// -----------------------------------------------------------------------------
// reader
// -----------------------------------------------------------------------------

static bool is_supported_version(uint32_t version)
{
    for (size_t i = 0; i < array_len(supported_versions); ++i)
        if (version == supported_versions[i]) return true;
    return false;
}

struct rill_store *rill_store_open(const char *file)
{
    struct rill_store *store = calloc(1, sizeof(*store));
    if (!store) {
        rill_fail("unable to allocate memory for '%s'", file);
        goto fail_alloc_struct;
    }

    store->file = strndup(file, PATH_MAX);
    if (!store->file) {
        rill_fail("unable to allocate memory for '%s'", file);
        goto fail_alloc_file;
    }

    struct stat stat_ret = {0};
    if (stat(file, &stat_ret) == -1) {
        rill_fail_errno("unable to stat '%s'", file);
        goto fail_stat;
    }

    size_t len = stat_ret.st_size;
    if (len < sizeof(struct header)) {
        rill_fail("invalid size for '%s'", file);
        goto fail_size;
    }

    store->vma_len = to_vma_len(len);

    store->fd = open(file, O_RDONLY);
    if (store->fd == -1) {
        rill_fail_errno("unable to open '%s'", file);
        goto fail_open;
    }

    store->vma = mmap(NULL, store->vma_len, PROT_READ, MAP_SHARED, store->fd, 0);
    if (store->vma == MAP_FAILED) {
        rill_fail_errno("unable to mmap '%s' of len '%lu'", file, store->vma_len);
        goto fail_mmap;
    }

    store->head = store->vma;
    store->index_a = (void *) ((uintptr_t) store->vma + store->head->index_a_off);
    store->index_b = (void *) ((uintptr_t) store->vma + store->head->index_b_off);
    store->data_a = (void *) ((uintptr_t) store->vma + store->head->data_a_off);
    store->data_b = (void *) ((uintptr_t) store->vma + store->head->data_b_off);
    store->end = (void *) ((uintptr_t) store->vma + store->vma_len);

    if (store->head->magic != magic) {
        rill_fail("invalid magic '0x%x' for '%s'", store->head->magic, file);
        goto fail_magic;
    }

    if (!is_supported_version(store->head->version)) {
        rill_fail("invalid version '%u' for '%s'", store->head->version, file);
        goto fail_version;
    }

    if (store->head->stamp != stamp) {
        rill_fail("invalid stamp '%lx' for '%s'", store->head->stamp, file);
        goto fail_stamp;
    }

    return store;

  fail_version:
  fail_magic:
  fail_stamp:
    munmap(store->vma, store->vma_len);
  fail_mmap:
    close(store->fd);
  fail_open:
  fail_size:
  fail_stat:
    free((char *) store->file);
  fail_alloc_file:
    free(store);
  fail_alloc_struct:
    return NULL;
}

void rill_store_close(struct rill_store *store)
{
    munmap(store->vma, store->vma_len);
    close(store->fd);
    free((char *) store->file);
    free(store);
}

bool rill_store_rm(struct rill_store *store)
{
    if (unlink(store->file) == -1) {
        rill_fail_errno("unable to unlink '%s'", store->file);
        return false;
    }

    rill_store_close(store);
    return true;
}


// -----------------------------------------------------------------------------
// writer
// -----------------------------------------------------------------------------

static bool writer_open(
        struct rill_store *store,
        const char *file,
        struct vals *vals,
        struct vals *inverted_vals,
        size_t pairs,
        rill_ts_t ts,
        size_t quant)
{
    store->file = file;

    store->fd = open(file, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (store->fd == -1) {
        rill_fail_errno("unable to open '%s'", file);
        goto fail_open;
    }

    size_t len =
        sizeof(struct header) +
        indexer_cap(inverted_vals->len) +
        indexer_cap(vals->len) +
        coder_cap(vals->len, pairs) +
        coder_cap(inverted_vals->len, pairs);

    if (ftruncate(store->fd, len) == -1) {
        rill_fail_errno("unable to resize '%s'", file);
        goto fail_truncate;
    }

    store->vma_len = to_vma_len(len);
    store->vma = mmap(NULL, store->vma_len, PROT_WRITE | PROT_READ, MAP_SHARED, store->fd, 0);
    if (store->vma == MAP_FAILED) {
        rill_fail_errno("unable to mmap '%s'", file);
        goto fail_mmap;
    }

    store->head = store->vma;
    store->end = (void *) ((uintptr_t) store->vma + store->vma_len);

    *store->head = (struct header) {
        .magic = magic,
        .version = version,
        .ts = ts,
        .quant = quant,
    };

    return true;

    munmap(store->vma, store->vma_len);
  fail_mmap:
  fail_truncate:
    close(store->fd);
  fail_open:
    return false;
}

static void writer_flush_indices(
    struct rill_store *store,
    struct indexer *indexer_a,
    struct indexer *indexer_b)
{
    const size_t indexer_a_size = indexer_cap(indexer_a->len);
    const size_t indexer_b_size = indexer_cap(indexer_b->len);

    indexer_write(indexer_a, store->index_a, indexer_a_size);
    indexer_write(indexer_b, store->index_b, indexer_b_size);
}

static void writer_close(
    struct rill_store *store, size_t len)
{
    if (len) {
        assert(len <= store->vma_len);
        if (ftruncate(store->fd, len) == -1)
            rill_fail_errno("unable to resize '%s'", store->file);

        if (fdatasync(store->fd) == -1)
            rill_fail_errno("unable to fdatasync data '%s'", store->file);

        // Indicate that the file has been fully written and is ready for
        // use. An additional sync is required for the stamp to ensure that the
        // data is...
        // - ... properly persisted before we delete it (durability)
        // - ... only persisted after all the data has been persisted (ordering)
        store->head->stamp = stamp;
        if (fdatasync(store->fd) == -1)
            rill_fail_errno("unable to fdatasync stamp '%s'", store->file);
    }
    else if (unlink(store->file) == -1)
        rill_fail_errno("unable to unlink '%s'", store->file);

    munmap(store->vma, store->vma_len);
    close(store->fd);
}

static void init_store_offsets(
    struct rill_store* store, size_t vals, size_t inverse_vals)
{
    store->head->index_a_off = sizeof(struct header);
    store->head->index_b_off = store->head->index_a_off + indexer_cap(inverse_vals);
    store->head->data_a_off = store->head->index_b_off + indexer_cap(vals);

    store->index_a = (void *) ((uintptr_t) store->vma + store->head->index_a_off);
    store->index_b = (void *) ((uintptr_t) store->vma + store->head->index_b_off);
    store->data_a = (void *) ((uintptr_t) store->vma + store->head->data_a_off);
}

static void prepare_col_b_offsets(
    struct rill_store* store, struct encoder* coder_a)
{
    store->head->data_b_off = store->head->data_a_off + coder_off(coder_a);
    store->data_b = (void *) ((uintptr_t) store->vma + store->head->data_b_off);
}

bool rill_store_write(
        const char *file,
        rill_ts_t ts,
        size_t quant,
        struct rill_pairs *pairs)
{
    rill_pairs_compact(pairs);
    if (!pairs->len) return true;

    struct vals *vals = vals_cols_from_pairs(pairs, rill_col_b);
    if (!vals) goto fail_vals;
    struct vals *invert_vals = vals_cols_from_pairs(pairs, rill_col_a);
    if (!invert_vals) goto fail_invert_vals;

    struct rill_store store = {0};
    if (!writer_open(&store, file, vals, invert_vals,
                     pairs->len, ts, quant)) {
        rill_fail("unable to create '%s'", file);
        goto fail_open;
    }

    struct indexer *indexer_a = indexer_alloc(invert_vals->len);
    if (!indexer_a) goto fail_indexer_a_alloc;
    struct indexer *indexer_b = indexer_alloc(vals->len);
    if (!indexer_b) goto fail_indexer_b_alloc;

    init_store_offsets(&store, vals->len, invert_vals->len);

    struct encoder coder_a =
        store_encoder(&store, indexer_a, vals, store.head->data_a_off);

    for (size_t i = 0; i < pairs->len; ++i) {
        if (!coder_encode(&coder_a, &pairs->data[i])) goto fail_encode_a;
    }
    if (!coder_finish(&coder_a)) goto fail_encode_a;

    prepare_col_b_offsets(&store, &coder_a);

    struct encoder coder_b =
        store_encoder(&store, indexer_b, invert_vals, store.head->data_b_off);

    rill_pairs_invert(pairs);
    rill_pairs_compact(pairs); /* recompact mainly for sort */

    for (size_t i = 0; i < pairs->len; ++i) {
        if (!coder_encode(&coder_b, &pairs->data[i])) goto fail_encode_b;
    }
    if (!coder_finish(&coder_b)) goto fail_encode_b;

    writer_flush_indices(&store, indexer_a, indexer_b);

    store.head->pairs = coder_a.pairs;

    writer_close(&store, store.head->data_b_off + coder_off(&coder_b));

    coder_close(&coder_a);
    coder_close(&coder_b);

    indexer_free(indexer_a);
    indexer_free(indexer_b);

    free(vals);
    free(invert_vals);

    return true;

  fail_encode_b:
    coder_close(&coder_b);
  fail_encode_a:
    coder_close(&coder_a);
    indexer_free(indexer_b);
  fail_indexer_b_alloc:
    indexer_free(indexer_a);
  fail_indexer_a_alloc:
    writer_close(&store, 0);
    free(invert_vals);
  fail_invert_vals:
  fail_open:
    free(vals);
  fail_vals:
    return false;
}

static struct vals *vals_merge_from_index(
    struct vals *vals, struct index *merge)
{
    assert(merge);

    if (!vals) {
        size_t len = sizeof(*vals) + sizeof(vals->data[0]) * merge->len;
        vals = calloc(1, len);
        if (!vals) {
            rill_fail("unable to allocate memory for vals: %lu", merge->len);
            return NULL;
        }

        for (size_t i = 0; i < merge->len; ++i)
            vals->data[i] = merge->data[i].key;

        vals->len = merge->len;

        return vals;
    }

    vals = realloc(vals,
            sizeof(*vals) + sizeof(vals->data[0]) * (vals->len + merge->len));

    if (!vals) {
        rill_fail("unable to allocate memory for vals: %lu + %lu",
                vals->len, merge->len);
        return NULL;
    }

    for (size_t i = 0; i < merge->len; ++i)
        vals->data[vals->len + i] = merge->data[i].key;

    vals->len += merge->len;

    vals_compact(vals);

    return vals;
}


static bool merge_with_config(
    struct encoder* coder,
    struct rill_store** list,
    size_t list_len,
    enum rill_col col)
{
    struct rill_kv kvs[list_len];

    struct decoder decoders[list_len];

    size_t it_len = 0;
    for (size_t i = 0; i < list_len; ++i) {
        if (!list[i]) continue;
        decoders[it_len] = store_decoder(list[i], col);
        it_len++;
    }
    assert(it_len);

    for (size_t i = 0; i < it_len; ++i) {
        if (!(coder_decode(&decoders[i], &kvs[i]))) goto fail_decoder;
    }

    struct rill_kv prev = {0};

    while (it_len > 0) {
        size_t target = 0;

        for (size_t i = 1; i < it_len; ++i) {
            if (rill_kv_cmp(&kvs[i], &kvs[target]) < 0)
                target = i;
        }

        struct rill_kv *kv = &kvs[target];
        struct decoder *decoder = &decoders[target];

        if (rill_likely(rill_kv_nil(&prev) || rill_kv_cmp(&prev, kv) < 0)) {
            if (!coder_encode(coder, kv)) goto fail_decoder;
            prev = *kv;
        }

        if (!coder_decode(decoder, kv)) goto fail_decoder;
        if (rill_unlikely(rill_kv_nil(kv))) {
            memmove(kvs + target,
                    kvs + target + 1,
                    (it_len - target - 1) * sizeof(kvs[0]));
            memmove(decoders + target,
                    decoders + target + 1,
                    (it_len - target - 1) * sizeof(decoders[0]));
            it_len--;
        }
    }

    return true;

  fail_decoder:
    return false;
}

bool rill_store_merge(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_store **list, size_t list_len)
{
    assert(list_len > 1);

    size_t pairs = 0;
    struct vals *vals = NULL;
    struct vals *invert_vals = NULL;

    for (size_t i = 0; i < list_len; ++i) {
        if (!list[i]) continue;
        vma_will_need(list[i]);

        struct vals *ret = vals_merge_from_index(vals, list[i]->index_b);
        struct vals *iret = vals_merge_from_index(invert_vals, list[i]->index_a);

        pairs += list[i]->head->pairs;

        if (ret) vals = ret; else goto fail_vals;
        if (iret) invert_vals = iret; else goto fail_invert_vals;
    }

    struct rill_store store = {0};
    if (!writer_open(&store, file, vals, invert_vals,
                     pairs, ts, quant)) {
        rill_fail("unable to create '%s'", file);
        goto fail_open;
    }

    init_store_offsets(&store, vals->len, invert_vals->len);

    struct indexer *indexer_a = indexer_alloc(invert_vals->len);
    if (!indexer_a) goto fail_index_a;

    struct indexer *indexer_b = indexer_alloc(vals->len);
    if (!indexer_b) goto fail_index_b;

    struct encoder encoder_a = store_encoder(&store, indexer_a, vals, store.head->data_a_off);
    if (!merge_with_config(&encoder_a, list, list_len, rill_col_a)) goto fail_merge_with_config_a;
    if (!coder_finish(&encoder_a)) goto fail_coder_a;

    prepare_col_b_offsets(&store, &encoder_a);

    struct encoder encoder_b =
        store_encoder(&store, indexer_b, invert_vals, store.head->data_b_off);

    if (!merge_with_config(&encoder_b, list, list_len, rill_col_b)) goto fail_merge_with_config_b;
    if (!coder_finish(&encoder_b)) goto fail_coder_b;

    writer_flush_indices(&store, indexer_a, indexer_b);

    store.head->pairs = encoder_a.pairs;

    writer_close(&store, store.head->data_b_off + coder_off(&encoder_b));

    for (size_t i = 0; i < list_len; ++i)
        if (list[i]) vma_dont_need(list[i]);

    coder_close(&encoder_a);
    coder_close(&encoder_b);
    indexer_free(indexer_a);
    indexer_free(indexer_b);
    free(vals);
    free(invert_vals);
    return true;

  fail_coder_b:
    coder_close(&encoder_b);
  fail_coder_a:
  fail_merge_with_config_a:
    coder_close(&encoder_a);
    writer_close(&store, 0);
    indexer_free(indexer_a);
  fail_merge_with_config_b:
  fail_index_b:
    free(indexer_a);
  fail_index_a:
  fail_open:
  fail_invert_vals:
    free(invert_vals);
  fail_vals:
    free(vals);
    return false;
}


// -----------------------------------------------------------------------------
// scan
// -----------------------------------------------------------------------------

const char * rill_store_file(const struct rill_store *store)
{
    return store->file;
}

unsigned rill_store_version(const struct rill_store *store)
{
    return store->head->version;
}

rill_ts_t rill_store_ts(const struct rill_store *store)
{
    return store->head->ts;
}

size_t rill_store_quant(const struct rill_store *store)
{
    return store->head->quant;
}

size_t rill_store_keys_count(const struct rill_store *store, enum rill_col column)
{
    assert(column == rill_col_a || column == rill_col_b);
    const struct index*
        ix = column == rill_col_a ? store->index_a : store->index_b;
    return ix->len;
}

size_t rill_store_pairs(const struct rill_store *store)
{
    return store->head->pairs;
}

size_t rill_store_index_len(const struct rill_store *store, enum rill_col col)
{
    assert(col == rill_col_a || col == rill_col_b);
    return col == rill_col_a ? store->index_a->len : store->index_b->len;
}

static struct rill_pairs *store_query_key_or_value(
        struct rill_store *store,
        rill_key_t key,
        struct rill_pairs *out,
        enum rill_col column)
{
    struct rill_pairs *result = out;
    size_t key_idx = 0;
    uint64_t off = 0;
    struct index *ix =
        column == rill_col_a ? store->index_a : store->index_b;

    if (!index_find(ix, key, &key_idx, &off)) return result;

    struct rill_kv kv = {0};
    struct decoder coder = store_decoder_at(store, key_idx, off, column);

    while (true) {
        if (!coder_decode(&coder, &kv)) goto fail;
        if (rill_kv_nil(&kv)) break;
        if (kv.key != key) break;

        result = rill_pairs_push(result, kv.key, kv.val);
        if (!result) goto fail;
    }

    return result;

  fail:
    // \todo potentially leaking result
    return NULL;
}

struct rill_pairs *rill_store_query_key(
        struct rill_store *store, rill_val_t key, struct rill_pairs *out)
{
    return store_query_key_or_value(store, key, out, rill_col_a);
}

struct rill_pairs *rill_store_query_value(
        struct rill_store *store, rill_val_t key, struct rill_pairs *out)
{
    return store_query_key_or_value(store, key, out, rill_col_b);
}

size_t rill_store_keys(
    const struct rill_store *store, rill_key_t *out, size_t cap,
    enum rill_col column)
{
    assert(column == rill_col_a || column == rill_col_b);

    const struct index* ix =
        column == rill_col_a ? store->index_a : store->index_b;

    size_t len = cap < ix->len ? cap : ix->len;

    for (size_t i = 0; i < len; ++i)
        out[i] = ix->data[i].key;

    return len;
}


struct rill_store_it { struct decoder decoder; };

struct rill_store_it *rill_store_begin(
        struct rill_store *store, enum rill_col column)
{
    struct rill_store_it *it = calloc(1, sizeof(*it));
    if (!it) return NULL;

    it->decoder = store_decoder(store, column);
    return it;
}

void rill_store_it_free(struct rill_store_it *it)
{
    free(it);
}

bool rill_store_it_next(struct rill_store_it *it, struct rill_kv *kv)
{
    return coder_decode(&it->decoder, kv);
}

struct rill_space* rill_store_space(struct rill_store* store)
{
    struct rill_space *ret = calloc(1, sizeof(*ret));

    *ret =  (struct rill_space) {
        .header_bytes = sizeof(*store->head),
        .index_bytes[rill_col_a] = store->head->index_b_off - store->head->index_a_off,
        .index_bytes[rill_col_b] = store->head->data_a_off - store->head->index_b_off,
        .pairs_bytes[rill_col_a] = store->head->data_b_off - store->head->data_a_off,
        .pairs_bytes[rill_col_b] = store->vma_len - store->head->data_b_off,
    };

    return ret;
}

size_t rill_store_space_header(struct rill_space* space) {
    return space->header_bytes;
}
size_t rill_store_space_index(struct rill_space* space, enum rill_col col) {
    assert(col == rill_col_a || col == rill_col_b);
    return space->index_bytes[col];
}
size_t rill_store_space_pairs(struct rill_space* space, enum rill_col col) {
    assert(col == rill_col_a || col == rill_col_b);
    return space->pairs_bytes[col];
}
