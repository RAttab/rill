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

static const uint32_t magic = 0x4C4C4952;
static const uint64_t stamp = 0xFFFFFFFFFFFFFFFFUL;
static const uint32_t version = 5;
static const uint32_t supported_versions[] = { 5 };

struct rill_packed header
{
    uint32_t magic;
    uint32_t version;

    uint64_t ts;
    uint64_t quant;

    uint64_t keys;
    uint64_t pairs;

    uint64_t vals_off;
    uint64_t data_off;
    uint64_t index_off;

    uint64_t reserved[3]; // for future use

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
    uint8_t *data;
    struct index *index;
    uint8_t *end;
};


// -----------------------------------------------------------------------------
// coder
// -----------------------------------------------------------------------------

static struct encoder store_encoder(
        struct rill_store *store, struct indexer *indexer)
{
    return make_encoder(
            store->vma + store->head->data_off,
            store->vma + store->vma_len,
            store->vals, indexer);
}

static struct decoder store_decoder(struct rill_store *store)
{
    return make_decoder(
            store->vma + store->head->data_off,
            store->vma + store->vma_len,
            store->vals, store->index);
}

static struct decoder store_decoder_at(
        struct rill_store *store, size_t key_idx, uint64_t off)
{
    return make_decoder_at(
            store->vma + store->head->data_off + off,
            store->vma + store->vma_len,
            store->vals, store->index, key_idx);
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
    store->vals = (void *) ((uintptr_t) store->vma + store->head->vals_off);
    store->data = (void *) ((uintptr_t) store->vma + store->head->data_off);
    store->index = (void *) ((uintptr_t) store->vma + store->head->index_off);
    store->end = (void *) ((uintptr_t) store->vma + store->vma_len);

    if (store->head->magic != magic) {
        rill_fail("invalid magic '0x%x' for '%s'", store->head->magic, file);
        goto fail_magic;
    }

    if (!is_supported_version(store->head->version)) {
        rill_fail("invalid version '%du' for '%s'", store->head->version, file);
        goto fail_version;
    }

    if (store->head->version >= 4) {
        if (store->head->stamp != stamp) {
            rill_fail("invalid stamp '%p' for '%s'", (void *) store->head->stamp, file);
            goto fail_stamp;
        }
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
        struct vals *vals, size_t pairs,
        rill_ts_t ts, size_t quant)
{
    store->file = file;

    store->fd = open(file, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (store->fd == -1) {
        rill_fail_errno("unable to open '%s'", file);
        goto fail_open;
    }

    size_t len =
        sizeof(struct header) +
        vals_cap(vals) +
        coder_cap(vals->len, pairs) +
        indexer_cap(pairs);

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
    store->vals = (void *) ((uintptr_t) store->vma + sizeof(struct header));
    store->end = (void *) ((uintptr_t) store->vma + store->vma_len);

    *store->head = (struct header) {
        .magic = magic,
        .version = version,
        .ts = ts,
        .quant = quant,
        .vals_off = sizeof(struct header),
    };

    return true;

    munmap(store->vma, store->vma_len);
  fail_mmap:
  fail_truncate:
    close(store->fd);
  fail_open:
    return false;
}

static struct encoder writer_begin(
        struct rill_store *store,
        const struct vals *vals,
        struct indexer *indexer)
{
    size_t len = sizeof(*vals) + sizeof(vals->data[0]) * vals->len;
    assert(store->head->vals_off + len < store->vma_len);

    memcpy(store->vals, vals, len);

    store->head->data_off = store->head->vals_off + len;
    store->data = (void *) ((uintptr_t) store->vma + store->head->data_off);

    return store_encoder(store, indexer);
}

static void writer_close(
        struct rill_store *store, struct indexer *indexer, size_t len)
{
    if (len) {
        store->head->index_off = len;
        store->index = (void *) ((uintptr_t) store->vma + store->head->index_off);
        len += indexer_write(indexer, store->index);

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

bool rill_store_write(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_pairs *pairs)
{
    rill_pairs_compact(pairs);
    if (!pairs->len) return true;

    struct vals *vals = vals_from_pairs(pairs);
    if (!vals) goto fail_vals;

    struct rill_store store = {0};
    if (!writer_open(&store, file, vals, pairs->len, ts, quant)) {
        rill_fail("unable to create '%s'", file);
        goto fail_open;
    }

    struct indexer *indexer = indexer_alloc(pairs->len);
    struct encoder coder = writer_begin(&store, vals, indexer);

    for (size_t i = 0; i < pairs->len; ++i) {
        if (!coder_encode(&coder, &pairs->data[i])) goto fail_encode;
    }
    if (!coder_finish(&coder)) goto fail_encode;

    store.head->keys = coder.keys;
    store.head->pairs = coder.pairs;

    writer_close(&store, indexer, store.head->data_off + coder_off(&coder));

    coder_close(&coder);
    indexer_free(indexer);
    free(vals);
    return true;

  fail_encode:
    coder_close(&coder);
    writer_close(&store, indexer, 0);
    indexer_free(indexer);
  fail_open:
    free(vals);
  fail_vals:
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
    struct it {
        struct rill_kv kv;
        struct decoder decoder;
    } its[list_len];

    size_t it_len = 0;
    for (size_t i = 0; i < list_len; ++i) {
        if (!list[i]) continue;
        vma_will_need(list[i]);

        struct vals *ret = vals_merge(vals, list[i]->vals);
        if (ret) { vals = ret; } else { goto fail_vals; }

        its[it_len].decoder = store_decoder(list[i]);
        pairs += list[i]->head->pairs;
        it_len++;
    }
    assert(it_len);

    struct rill_store store = {0};
    if (!writer_open(&store, file, vals, pairs, ts, quant)) {
        rill_fail("unable to create '%s'", file);
        goto fail_open;
    }

    struct indexer *indexer = indexer_alloc(pairs);
    struct encoder encoder = writer_begin(&store, vals, indexer);

    for (size_t i = 0; i < it_len; ++i) {
        if (!(coder_decode(&its[i].decoder, &its[i].kv))) goto fail_coder;
    }

    struct rill_kv prev = {0};

    while (it_len > 0) {
        size_t target = 0;
        for (size_t i = 1; i < it_len; ++i) {
            if (rill_kv_cmp(&its[i].kv, &its[target].kv) < 0)
                target = i;
        }

        struct it *it = &its[target];
        if (rill_likely(rill_kv_nil(&prev) || rill_kv_cmp(&prev, &it->kv) < 0)) {
            prev = it->kv;
            if (!coder_encode(&encoder, &it->kv)) goto fail_coder;
        }

        if (!coder_decode(&it->decoder, &it->kv)) goto fail_coder;
        if (rill_unlikely(rill_kv_nil(&it->kv))) {
            memmove(its + target,
                    its + target + 1,
                    (it_len - target - 1) * sizeof(its[0]));
            it_len--;
        }
    }

    store.head->keys = encoder.keys;
    store.head->pairs = encoder.pairs;

    if (!coder_finish(&encoder)) goto fail_coder;
    writer_close(&store, indexer, store.head->data_off + coder_off(&encoder));

    for (size_t i = 0; i < list_len; ++i) {
        if (list[i]) vma_dont_need(list[i]);
    }

    coder_close(&encoder);
    indexer_free(indexer);
    free(vals);
    return true;

  fail_coder:
    coder_close(&encoder);
    writer_close(&store, indexer, 0);
    indexer_free(indexer);
  fail_open:
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

rill_ts_t rill_store_ts(const struct rill_store *store)
{
    return store->head->ts;
}

size_t rill_store_quant(const struct rill_store *store)
{
    return store->head->quant;
}

size_t rill_store_vals(const struct rill_store *store)
{
    return store->vals->len;
}

size_t rill_store_pairs(const struct rill_store *store)
{
    return store->head->pairs;
}


struct rill_pairs *rill_store_query_key(
        struct rill_store *store, rill_key_t key, struct rill_pairs *out)
{
    struct rill_pairs *result = out;

    size_t key_idx = 0;
    uint64_t off = 0;
    if (!index_find(store->index, key, &key_idx, &off)) return result;

    struct rill_kv kv = {0};
    struct decoder coder = store_decoder_at(store, key_idx, off);

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


struct rill_pairs *rill_store_scan_keys(
        struct rill_store *store,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out)
{
    vma_will_need(store);

    struct rill_kv kv = {0};
    struct rill_pairs *result = out;
    struct decoder coder = store_decoder(store);

    while (true) {
        if (!coder_decode(&coder, &kv)) goto fail;
        if (rill_kv_nil(&kv)) break;

        for (size_t j = 0; j < len; ++j) {
            if (kv.key != keys[j]) continue;

            result = rill_pairs_push(result, kv.key, kv.val);
            if (!result) goto fail;
        }
    }

    vma_dont_need(store);
    return result;

  fail:
    // \todo potentially leaking result
    vma_dont_need(store);
    return NULL;
}

struct rill_pairs *rill_store_scan_vals(
        struct rill_store *store,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out)
{
    if (!vals_contains(store->vals, vals, len)) return out;

    vma_will_need(store);

    struct rill_kv kv = {0};
    struct rill_pairs *result = out;
    struct decoder coder = store_decoder(store);

    size_t i = 0;
    rill_key_t current = 0;

    while (true) {
        if (!coder_decode(&coder, &kv)) goto fail;
        if (rill_kv_nil(&kv)) break;

        if (current != kv.key) { i = 0; current = kv.key; }
        while (i < len && vals[i] < kv.val) ++i;

        if (vals[i] == kv.val) {
            result = rill_pairs_push(result, kv.key, kv.val);
            if (!result) goto fail;
        }
    }

    vma_dont_need(store);
    return result;

  fail:
    // \todo potentially leaking result
    vma_dont_need(store);
    return NULL;
}

size_t rill_store_dump_vals(
        const struct rill_store *store, rill_val_t *out, size_t cap)
{
    size_t len = cap < store->vals->len ? cap : store->vals->len;
    memcpy(out, store->vals->data, len * sizeof(*out));
    return len;
}


struct rill_store_it { struct decoder decoder; };

struct rill_store_it *rill_store_begin(struct rill_store *store)
{
    struct rill_store_it *it = calloc(1, sizeof(*it));
    if (!it) return NULL;

    it->decoder = store_decoder(store);
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


void rill_store_print_head(struct rill_store *store)
{
    printf("%s\n", store->file);
    printf("magic:   0x%x\n", store->head->magic);
    printf("version: %u\n", store->head->version);
    printf("stamp:   %p\n", (void *) store->head->stamp);
    printf("ts:      %lu\n", store->head->ts);
    printf("quant:   %lu\n", store->head->quant);
    printf("keys:    %lu\n", store->head->keys);
    printf("vals:    %lu\n", store->vals->len);
    printf("pairs:   %lu\n", store->head->pairs);
}

void rill_store_print(struct rill_store *store)
{
    vma_will_need(store);

    struct rill_kv kv = {0};
    struct decoder coder = store_decoder(store);

    const rill_key_t no_key = -1ULL;
    rill_key_t key = no_key;

    for (size_t i = 0; i < store->head->pairs; ++i) {
        if (!coder_decode(&coder, &kv)) goto fail;
        if (rill_kv_nil(&kv)) break;

        if (kv.key == key) printf(", %lu", kv.val);
        else {
            if (key != no_key) printf("]\n");
            printf("%p: [ %lu", (void *) kv.key, kv.val);
            key = kv.key;
        }
    }

    printf(" ]\n");

  fail:
    vma_dont_need(store);
}
