/* store.c
   Rémi Attab (remi.attab@gmail.com), 02 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

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
// utils
// -----------------------------------------------------------------------------

static const size_t page_len = 4096;

static inline size_t to_vma_len(size_t len)
{
    if (!(len % page_len)) return len;
    return (len & ~(page_len - 1)) + page_len;
}


// -----------------------------------------------------------------------------
// store
// -----------------------------------------------------------------------------

static const uint32_t version = 1;
static const uint32_t magic = 0x4C4C4952;

struct rill_packed header
{
    uint32_t magic;
    uint32_t version;

    uint64_t pairs;
    uint64_t ts;
    uint64_t quant;
};

struct rill_store
{
    int fd;
    const char *file;

    void *vma;
    size_t vma_len;

    struct header *head;
    struct rill_kv *data;
};


// -----------------------------------------------------------------------------
// reader
// -----------------------------------------------------------------------------

struct rill_store *rill_store_open(const char *file)
{
    struct rill_store *store = calloc(1, sizeof(*store));
    if (!store) {
        fail("unable to allocate memory for '%s'", file);
        goto fail_alloc_struct;
    }

    store->file = strndup(file, NAME_MAX);
    if (!store->file) {
        fail("unable to allocate memory for '%s'", file);
        goto fail_alloc_file;
    }

    struct stat stat_ret = {0};
    if (stat(file, &stat_ret) == -1) {
        fail_errno("unable to stat '%s'", file);
        goto fail_stat;
    }

    size_t len = stat_ret.st_size;
    if (len < sizeof(struct header)) {
        fail("invalid size for '%s'", file);
        goto fail_size;
    }

    store->vma_len = to_vma_len(len);

    store->fd = open(file, O_RDONLY);
    if (store->fd == -1) {
        fail_errno("unable to open '%s'", file);
        goto fail_open;
    }

    store->vma = mmap(NULL, store->vma_len, PROT_READ, MAP_SHARED, store->fd, 0);
    if (store->vma == MAP_FAILED) {
        fail_errno("[reader] unable to mmap '%s'", file);
        goto fail_mmap;
    }

    store->head = store->vma;
    store->data = (void *) ((uintptr_t) store->vma + sizeof(struct header));

    if (store->head->magic != magic) {
        fail("invalid magic '0x%x' for '%s'", store->head->magic, file);
        goto fail_magic;
    }

    if (store->head->version != version) {
        fail("unknown version '%du' for '%s'", store->head->version, file);
        goto fail_version;
    }

    size_t expected = sizeof(struct header) + sizeof(struct rill_kv) * store->head->pairs;
    if (expected != len) {
        fail("invalid file size '%lu != %lu' for '%s'", len, expected, file);
        goto fail_len;
    }

    return store;

  fail_len:
  fail_version:
  fail_magic:
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
        fail_errno("unable to unlink '%s'", store->file);
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
        const char *file, size_t cap,
        rill_ts_t ts, size_t quant)
{
    store->file = file;

    store->fd = open(file, O_RDWR | O_CREAT | O_EXCL, 0640);
    if (store->fd == -1) {
        fail_errno("unable to open '%s'", file);
        goto fail_open;
    }

    size_t len = sizeof(struct header) + sizeof(struct rill_kv) * cap;
    if (ftruncate(store->fd, len) == -1) {
        fail_errno("unable to resize '%s'", file);
        goto fail_truncate;
    }

    store->vma_len = to_vma_len(len);
    store->vma = mmap(NULL, store->vma_len, PROT_WRITE | PROT_READ, MAP_SHARED, store->fd, 0);
    if (store->vma == MAP_FAILED) {
        fail_errno("unable to mmap '%s'", file);
        goto fail_mmap;
    }

    store->head = store->vma;
    store->data = (void *) ((uintptr_t) store->vma + sizeof(struct header));

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


static void writer_close(struct rill_store *store, size_t pairs)
{
    store->head->pairs = pairs;

    munmap(store->vma, store->vma_len);

    size_t len = sizeof(struct header) + sizeof(struct rill_kv) * pairs;
    if (ftruncate(store->fd, len) == -1)
        fail_errno("unable to resize '%s'", store->file);

    if (fdatasync(store->fd) == -1)
        fail_errno("unable to fsync '%s'", store->file);

    close(store->fd);
}

bool rill_store_write(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_pairs *pairs)
{
    rill_pairs_compact(pairs);

    struct rill_store store = {0};
    if (!writer_open(&store, file, pairs->len, ts, quant)) {
        fail("unable to create '%s'", file);
        goto fail_open;
    }

    for (size_t i = 0; i < pairs->len; ++i) {
        store.data[i].key = pairs->data[i].key;
        store.data[i].val = pairs->data[i].val;
    }

    writer_close(&store, pairs->len);
    return true;

  fail_open:
    return false;
}

bool rill_store_merge(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_store **list, size_t list_len)
{
    assert(list_len > 1);

    size_t cap = 0;
    struct { struct rill_kv *it, *end; } its[list_len];

    size_t it_len = 0;
    for (size_t i = 0; i < list_len; ++i) {
        if (!list[i]) continue;

        cap += list[i]->head->pairs;
        its[it_len].it = list[i]->data;
        its[it_len].end = list[i]->data + list[i]->head->pairs;
        it_len++;
    }

    struct rill_store store = {0};
    if (!writer_open(&store, file, cap, ts, quant)) {
        fail("unable to create '%s'", file);
        goto fail_open;
    }

    size_t pairs = 0;
    struct rill_kv *current = store.data;

    while (it_len > 0) {
        size_t target = 0;
        for (size_t i = 1; i < it_len; ++i) {
            if (rill_kv_cmp(its[i].it, its[target].it) < 0)
                target = i;
        }

        if (rill_kv_cmp(current, its[target].it) < 0) {
            pairs++;
            current++;
            *current = *its[target].it;
        }

        its[target].it++;
        if (its[target].it == its[target].end) {
            memmove(its + target, its + target + 1, (it_len - target - 1) * sizeof(its[0]));
            it_len--;
        }
    }

    writer_close(&store, pairs);
    return true;

  fail_open:
    return false;
}


// -----------------------------------------------------------------------------
// scan
// -----------------------------------------------------------------------------

const char * rill_store_file(struct rill_store *store)
{
    return store->file;
}

rill_ts_t rill_store_ts(struct rill_store *store)
{
    return store->head->ts;
}

size_t rill_store_quant(struct rill_store *store)
{
    return store->head->quant;
}

static inline void vma_will_need(struct rill_store *store)
{
    if (madvise(store->vma, store->vma_len, MADV_WILLNEED) == -1)
        fail("unable to madvise '%s'", store->file);
}

static inline void vma_dont_need(struct rill_store *store)
{
    if (madvise(store->vma, store->vma_len, MADV_DONTNEED) == -1)
        fail("unable to madvise '%s'", store->file);
}

bool rill_store_scan_key(
        struct rill_store *store,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out)
{
    vma_will_need(store);

    for (size_t i = 0; i < store->head->pairs; ++i) {
        struct rill_kv *kv = &store->data[i];

        for (size_t j = 0; j < len; ++j) {
            if (kv->key != keys[j]) continue;
            if (!rill_pairs_push(out, kv->key, kv->val)) return false;
        }
    }

    vma_dont_need(store);
    return true;
}

bool rill_store_scan_val(
        struct rill_store *store,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out)
{
    vma_will_need(store);

    for (size_t i = 0; i < store->head->pairs; ++i) {
        struct rill_kv *kv = &store->data[i];

        for (size_t j = 0; j < len; ++j) {
            if (kv->val != vals[j]) continue;
            if (!rill_pairs_push(out, kv->key, kv->val)) return false;
        }
    }

    vma_dont_need(store);
    return true;
}

void rill_store_print_head(struct rill_store *store)
{
    fprintf(stderr, "magic:   0x%x\n", store->head->magic);
    fprintf(stderr, "version: %u\n", store->head->version);
    fprintf(stderr, "pairs:   %lu\n", store->head->pairs);
    fprintf(stderr, "ts:      %lu\n", store->head->ts);
    fprintf(stderr, "quant:   %lu\n", store->head->quant);
}

void rill_store_print(struct rill_store *store)
{
    vma_will_need(store);

    const rill_key_t no_key = -1ULL;
    rill_key_t key = no_key;

    for (size_t i = 0; i < store->head->pairs; ++i) {
        struct rill_kv *kv = &store->data[i];

        if (kv->key == key) fprintf(stderr, ", %lu", kv->val);
        else {
            if (key != no_key) fprintf(stderr, "]\n");
            fprintf(stderr, "%p: [ %lu", (void *) kv->key, kv->val);
            key = kv->key;
        }
    }

    fprintf(stderr, " ]\n");

    vma_dont_need(store);
}
