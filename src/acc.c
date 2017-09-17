/* acc.c
   Rémi Attab (remi.attab@gmail.com), 16 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>


// -----------------------------------------------------------------------------
// acc
// -----------------------------------------------------------------------------

static const uint32_t version = 1;
static const uint32_t magic = 0x43434152;

struct rill_packed header
{
    uint32_t magic;
    uint32_t version;

    uint64_t len;

    atomic_size_t read;
    atomic_size_t write;
};

struct rill_packed kv
{
    uint64_t key, val;
};

struct rill_acc
{
    int fd;
    const char *dir;

    void *vma;
    size_t vma_len;

    struct header *head;
    struct kv *data;
};

enum { min_cap = 32 };

struct rill_acc *rill_acc_open(const char *dir, size_t cap)
{
    if (cap != rill_acc_read_only && cap < min_cap) cap = min_cap;

    struct rill_acc *acc = calloc(1, sizeof(*acc));
    if (!acc) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_struct;
    }

    acc->dir = strndup(dir, PATH_MAX);
    if (!acc->dir) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_dir;
    }

    if (mkdir(dir, 0775) == -1 && errno != EEXIST) {
        fail_errno("unable to open create dir '%s'", dir);
        goto fail_mkdir;
    }

    char file[PATH_MAX];
    snprintf(file, sizeof(file), "%s/acc", dir);

    bool create = false;
    struct stat stat_ret = {0};
    if (stat(file, &stat_ret) == -1) {
        if (errno != ENOENT) {
            fail_errno("unable to stat '%s'", file);
            goto fail_stat;
        }

        if (cap == rill_acc_read_only) return false;

        create = true;
        acc->fd = open(file, O_RDWR | O_CREAT | O_EXCL | O_NOATIME, 0644);
    }
    else acc->fd = open(file, O_RDWR);

    if (acc->fd == -1) {
        fail_errno("unable to create '%s'", file);
        goto fail_open;
    }

    acc->vma_len = to_vma_len(sizeof(*acc->head) + cap * sizeof(*acc->data));
    if (create) {
        if (ftruncate(acc->fd, acc->vma_len) == -1) {
            fail_errno("unable to ftruncate '%s' to len '%lu'", file, acc->vma_len);
            goto fail_truncate;
        }
    }

    acc->vma = mmap(NULL, acc->vma_len, PROT_READ | PROT_WRITE, MAP_SHARED, acc->fd, 0);
    if (acc->vma == MAP_FAILED) {
        fail_errno("unable to mmap '%s' of len '%lu'", file, acc->vma_len);
        goto fail_mmap;
    }

    acc->head = acc->vma;
    acc->data = (void *) (acc->head + 1);

    if (create) {
        acc->head->magic = magic;
        acc->head->version = version;
        acc->head->len = cap;
    }
    else {
        if (acc->head->magic != magic) {
            fail("invalid magic '0x%x' for '%s'", acc->head->magic, file);
            goto fail_magic;
        }

        if (acc->head->version != version) {
            fail("unknown version '%du' for '%s'", acc->head->version, file);
            goto fail_version;
        }
    }

    return acc;

  fail_version:
  fail_magic:
    munmap(acc->vma, acc->vma_len);
  fail_mmap:
  fail_truncate:
    close(acc->fd);
  fail_open:
  fail_stat:
  fail_mkdir:
    free((char *) acc->dir);
  fail_alloc_dir:
    free(acc);
  fail_alloc_struct:
    return NULL;
}

void rill_acc_close(struct rill_acc *acc)
{
    munmap(acc->vma, acc->vma_len);
    close(acc->fd);
    free((char *) acc->dir);
    free(acc);
}

void rill_acc_ingest(struct rill_acc *acc, rill_key_t key, rill_val_t val)
{
    size_t read = atomic_load_explicit(&acc->head->read, memory_order_relaxed);
    size_t index = read % acc->head->len;

    acc->data[index].key = key;
    acc->data[index].val = val;

    atomic_store_explicit(&acc->head->read, read + 1, memory_order_release);
}

bool rill_acc_write(struct rill_acc *acc, const char *file, rill_ts_t now)
{
    struct rill_pairs *pairs = rill_pairs_new(acc->head->len);
    if (!pairs) {
        fail("unable to allocate pairs for len '%lu'", acc->head->len);
        return false;
    }

    size_t start = atomic_load_explicit(&acc->head->write, memory_order_acquire);
    size_t end = atomic_load_explicit(&acc->head->read, memory_order_acquire);
    if (start == end) return true;
    assert(start < end);

    if (end - start >= acc->head->len) {
        printf("acc lost '%lu' events\n", (end - start) - acc->head->len);
        size_t leeway = min_cap / 2; // to avoid contentention between reader and writer
        start = end - acc->head->len + leeway;
    }

    struct rill_pairs *ret = NULL;
    for (size_t i = start; i < end; ++i) {
        size_t index = i % acc->head->len;

        ret = rill_pairs_push(pairs, acc->data[index].key, acc->data[index].val);
        assert(ret == pairs);
    }

    if (!rill_store_write(file, now, 0, pairs)) {
        fail("unable to write acc file '%s'", file);
        goto fail_write;
    }

    atomic_store_explicit(&acc->head->write, end, memory_order_release);
    return true;

  fail_write:
    free(pairs);
    return false;
}
