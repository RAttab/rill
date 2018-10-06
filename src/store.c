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

#include "index.c"
#include "vals.c"
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

    uint64_t rows;

    uint64_t data_off[rill_cols];
    uint64_t index_off[rill_cols];

    uint64_t __unused[2];

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

    uint8_t *data[rill_cols];
    struct index *index[rill_cols];
    uint8_t *end;
};



// -----------------------------------------------------------------------------
// coder
// -----------------------------------------------------------------------------

static inline void *store_ptr(struct rill_store *store, uint64_t off)
{
    return (void *) ((uintptr_t) store->vma + off);
}

static struct encoder store_encoder(
        struct rill_store *store,
        enum rill_col col,
        struct vals *vals[rill_cols])
{
    enum rill_col other_col = rill_col_flip(col);

    size_t start = store->head->data_off[col];
    size_t end = store->vma_len;

    return make_encoder(
            store->vma + start,
            store->vma + end,
            vals[other_col],
            store->index[col]);
}

static struct decoder store_decoder_at(
        const struct rill_store *store,
        enum rill_col col,
        size_t key_idx,
        uint64_t off)
{
    enum rill_col other_col = rill_col_flip(col);

    struct index *index = store->index[col];
    struct index *lookup = store->index[other_col];

    size_t start = store->head->data_off[col];
    size_t end = col == rill_col_a ?
        store->head->data_off[other_col] : store->vma_len;

    return make_decoder_at(
            store->vma + start + off,
            store->vma + end,
            lookup, index,
            key_idx);
}

static struct decoder store_decoder(
        const struct rill_store *store, enum rill_col col)
{
    return store_decoder_at(store, col, 0, 0);
}


// -----------------------------------------------------------------------------
// open
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
        rill_fail("invalid size '%lu' for '%s'", len, file);
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
    for (size_t col = 0; col < rill_cols; ++col) {
        store->index[col] = store_ptr(store, store->head->index_off[col]);
        store->data[col] = store_ptr(store, store->head->data_off[col]);
    }
    store->end = store_ptr(store, store->vma_len);

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
        struct vals *vals[rill_cols],
        size_t rows,
        rill_ts_t ts,
        size_t quant)
{
    store->file = file;

    store->fd = open(file, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (store->fd == -1) {
        rill_fail_errno("unable to open '%s'", file);
        goto fail_open;
    }

    size_t len = sizeof(struct header);
    for (size_t col = 0; col < rill_cols; ++col) {
        len += index_cap(vals[col]->len);
        len += coder_cap(vals[col]->len, rows);
    }

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
    store->end = store_ptr(store, store->vma_len);

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

static void writer_offsets_init(
        struct rill_store *store, struct vals *vals[rill_cols])
{
    uint64_t off = sizeof(struct header);

    store->head->index_off[rill_col_a] = off;
    store->index[rill_col_a] = store_ptr(store, off);

    off += index_cap(vals[rill_col_a]->len);

    store->head->index_off[rill_col_b] = off;
    store->index[rill_col_b] = store_ptr(store, off);

    off += index_cap(vals[rill_col_b]->len);

    store->head->data_off[rill_col_a] = off;
    store->data[rill_col_a] = store_ptr(store, off);
}

static void writer_offsets_finish(struct rill_store *store, size_t off)
{
    store->head->data_off[rill_col_b] = store->head->data_off[rill_col_a] + off;
    store->data[rill_col_b] = store_ptr(store, off);
}

bool rill_store_write(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_rows *rows)
{
    rill_rows_compact(rows);
    if (!rows->len) return true;

    struct vals *vals[rill_cols] = {0};
    for (size_t col = 0; col < rill_cols; ++col) {
        vals[col] = vals_for_col(rows, col);
        if (!vals[col]) goto fail_vals;
    }

    struct rill_store store = {0};
    if (!writer_open(&store, file, vals, rows->len, ts, quant)) goto fail_open;

    writer_offsets_init(&store, vals);

    struct encoder coder_a = store_encoder(&store, rill_col_a, vals);
    for (size_t i = 0; i < rows->len; ++i) {
        if (!coder_encode(&coder_a, &rows->data[i])) goto fail_encode_a;
    }
    if (!coder_finish(&coder_a)) goto fail_encode_a;

    writer_offsets_finish(&store, coder_off(&coder_a));
    rill_rows_invert(rows);

    struct encoder coder_b = store_encoder(&store, rill_col_b, vals);
    for (size_t i = 0; i < rows->len; ++i) {
        if (!coder_encode(&coder_b, &rows->data[i])) goto fail_encode_b;
    }
    if (!coder_finish(&coder_b)) goto fail_encode_b;

    store.head->rows = rows->len;
    writer_close(&store, store.head->data_off[rill_col_b] + coder_off(&coder_b));

    coder_close(&coder_a);
    coder_close(&coder_b);

    for (size_t col = 0; col < rill_cols; ++col)
        free(vals[col]);

    return true;

  fail_encode_b:
    coder_close(&coder_b);
  fail_encode_a:
    coder_close(&coder_a);
    writer_close(&store, 0);
  fail_open:
    for (size_t col = 0; col < rill_cols; ++col) free(vals[col]);
  fail_vals:
    return false;
}


static bool store_merge_col(
        struct rill_store** list,
        size_t list_len,
        enum rill_col col,
        struct encoder* coder)
{
    struct rill_row rows[list_len];
    struct decoder decoders[list_len];

    size_t it_len = 0;
    for (size_t i = 0; i < list_len; ++i) {
        if (!list[i]) continue;
        decoders[it_len] = store_decoder(list[i], col);
        it_len++;
    }
    assert(it_len);

    for (size_t i = 0; i < it_len; ++i) {
        if (!(coder_decode(&decoders[i], &rows[i]))) goto fail_decoder;
    }

    struct rill_row prev = {0};
    while (it_len > 0) {
        size_t target = 0;

        for (size_t i = 1; i < it_len; ++i) {
            if (rill_row_cmp(&rows[i], &rows[target]) < 0)
                target = i;
        }

        struct rill_row *row = &rows[target];
        struct decoder *decoder = &decoders[target];

        if (rill_likely(rill_row_nil(&prev) || rill_row_cmp(&prev, row) < 0)) {
            if (!coder_encode(coder, row)) goto fail_decoder;
            prev = *row;
        }

        if (!coder_decode(decoder, row)) goto fail_decoder;
        if (rill_unlikely(rill_row_nil(row))) {
            memmove(rows + target,
                    rows + target + 1,
                    (it_len - target - 1) * sizeof(rows[0]));
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

    size_t rows = 0;
    struct vals *vals[rill_cols] = {0};

    for (size_t i = 0; i < list_len; ++i) {
        if (!list[i]) continue;

        for (size_t col = 0; col < rill_cols; ++col) {
            struct vals *ret = vals_add_index(vals[col], list[i]->index[col]);
            if (!ret) goto fail_vals;
            vals[col] = ret;
        }

        rows += list[i]->head->rows;
    }

    struct rill_store store = {0};
    if (!writer_open(&store, file, vals, rows, ts, quant)) goto fail_open;

    writer_offsets_init(&store, vals);

    struct encoder encoder_a = store_encoder(&store, rill_col_a, vals);
    if (!store_merge_col(list, list_len, rill_col_a, &encoder_a)) goto fail_coder_a;
    if (!coder_finish(&encoder_a)) goto fail_coder_a;

    writer_offsets_finish(&store, coder_off(&encoder_a));

    struct encoder encoder_b = store_encoder(&store, rill_col_b, vals);
    if (!store_merge_col(list, list_len, rill_col_b, &encoder_b)) goto fail_coder_b;
    if (!coder_finish(&encoder_b)) goto fail_coder_b;

    store.head->rows = encoder_a.rows;
    writer_close(&store, store.head->data_off[rill_col_b] + coder_off(&encoder_b));

    coder_close(&encoder_a);
    coder_close(&encoder_b);

    for (size_t col = 0; col < rill_cols; ++col) free(vals[col]);
    return true;

    coder_close(&encoder_b);
  fail_coder_b:
    coder_close(&encoder_a);
  fail_coder_a:
    writer_close(&store, 0);
  fail_open:
  fail_vals:
    for (size_t col = 0; col < rill_cols; ++col) free(vals[col]);
    return false;
}


// -----------------------------------------------------------------------------
// header info
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

size_t rill_store_rows(const struct rill_store *store)
{
    return store->head->rows;
}


// -----------------------------------------------------------------------------
// query
// -----------------------------------------------------------------------------

size_t rill_store_vals_count(const struct rill_store *store, enum rill_col col)
{
    return store->index[col]->len;
}

size_t rill_store_vals(
        const struct rill_store *store,
        enum rill_col col,
        rill_val_t *out,
        size_t cap)
{
    const struct index* index = store->index[col];
    size_t len = cap < index->len ? cap : index->len;

    for (size_t i = 0; i < len; ++i)
        out[i] = index->data[i].key;

    return len;
}


bool rill_store_query(
        const struct rill_store *store,
        enum rill_col col,
        rill_val_t key,
        struct rill_rows *out)
{
    uint64_t off = 0;
    size_t key_idx = 0;
    if (!index_find(store->index[col], key, &key_idx, &off)) return true;

    struct rill_row row = {0};
    struct decoder coder = store_decoder_at(store, col, key_idx, off);

    while (true) {
        if (!coder_decode(&coder, &row)) return false;
        if (rill_row_nil(&row) || row.a != key) break;

        if (!rill_rows_push(out, row.a, row.b)) return false;
    }

    return true;
}


// -----------------------------------------------------------------------------
// iterators
// -----------------------------------------------------------------------------

struct rill_store_it { struct decoder decoder; };

struct rill_store_it *rill_store_begin(
        const struct rill_store *store, enum rill_col col)
{
    struct rill_store_it *it = calloc(1, sizeof(*it));
    if (!it) return NULL;

    it->decoder = store_decoder(store, col);
    return it;
}

void rill_store_it_free(struct rill_store_it *it)
{
    free(it);
}

bool rill_store_it_next(struct rill_store_it *it, struct rill_row *row)
{
    return coder_decode(&it->decoder, row);
}


// -----------------------------------------------------------------------------
// stats
// -----------------------------------------------------------------------------

void rill_store_stats(
        const struct rill_store *store, struct rill_store_stats *out)
{
    *out = (struct rill_store_stats) {
        .header_bytes = sizeof(*store->head),

        .index_bytes[rill_col_a] = store->head->index_off[rill_col_b] -
                                   store->head->index_off[rill_col_a],
        .index_bytes[rill_col_b] = store->head->data_off[rill_col_a] -
                                   store->head->index_off[rill_col_b],

        .rows_bytes[rill_col_a] = store->head->data_off[rill_col_b] -
                                  store->head->data_off[rill_col_a],
        .rows_bytes[rill_col_b] = store->vma_len -
                                  store->head->data_off[rill_col_b],
    };
}
