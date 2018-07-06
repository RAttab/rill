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

struct rill_packed row
{
    uint64_t a, b;
};

struct rill_acc
{
    int fd;
    const char *dir;

    void *vma;
    size_t vma_len;

    struct header *head;
    struct row *data;
};

enum { min_cap = 32 };

struct rill_acc *rill_acc_open(const char *dir, size_t cap)
{
    if (cap != rill_acc_read_only && cap < min_cap) cap = min_cap;

    // Add enough leeway to avoid contention between the reader and the writer.
    // some might say this is an excessive amount of leeway but I don't care.
    cap *= 2;

    struct rill_acc *acc = calloc(1, sizeof(*acc));
    if (!acc) {
        rill_fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_struct;
    }

    acc->dir = strndup(dir, PATH_MAX);
    if (!acc->dir) {
        rill_fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_dir;
    }

    if (mkdir(dir, 0775) == -1 && errno != EEXIST) {
        rill_fail_errno("unable to open create dir '%s'", dir);
        goto fail_mkdir;
    }

    char file[PATH_MAX];
    snprintf(file, sizeof(file), "%s/acc", dir);

    bool create = false;
    struct stat stat_ret = {0};
    if (stat(file, &stat_ret) == -1) {
        if (errno != ENOENT) {
            rill_fail_errno("unable to stat '%s'", file);
            goto fail_stat;
        }

        if (cap == rill_acc_read_only) goto fail_read_only;

        create = true;
        acc->fd = open(file, O_RDWR | O_CREAT | O_EXCL | O_NOATIME, 0644);
    }
    else acc->fd = open(file, O_RDWR);

    if (acc->fd == -1) {
        rill_fail_errno("unable to create '%s'", file);
        goto fail_open;
    }

    if (create) {
        acc->vma_len = to_vma_len(sizeof(*acc->head) + cap * sizeof(*acc->data));
        if (ftruncate(acc->fd, acc->vma_len) == -1) {
            rill_fail_errno("unable to ftruncate '%s' to len '%lu'", file, acc->vma_len);
            goto fail_truncate;
        }
    }
    else {
        size_t len = stat_ret.st_size;
        if (len < sizeof(struct header)) {
            rill_fail("invalid size for '%s'", file);
            goto fail_size;
        }

        acc->vma_len = to_vma_len(len);
    }

    int prot = PROT_READ | PROT_WRITE;
    int flags = MAP_SHARED | MAP_POPULATE;
    acc->vma = mmap(NULL, acc->vma_len, prot, flags, acc->fd, 0);
    if (acc->vma == MAP_FAILED) {
        rill_fail_errno("unable to mmap '%s' of len '%lu'", file, acc->vma_len);
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
            rill_fail("invalid magic '0x%x' for '%s'", acc->head->magic, file);
            goto fail_magic;
        }

        if (acc->head->version != version) {
            rill_fail("unknown version '%du' for '%s'", acc->head->version, file);
            goto fail_version;
        }
    }

    return acc;

  fail_version:
  fail_magic:
    munmap(acc->vma, acc->vma_len);
  fail_mmap:
  fail_size:
  fail_truncate:
    close(acc->fd);
  fail_open:
  fail_read_only:
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

void rill_acc_ingest(struct rill_acc *acc, rill_val_t a, rill_val_t b)
{
    assert(a && b);

    size_t write = atomic_load_explicit(&acc->head->write, memory_order_relaxed);
    size_t index = write % acc->head->len;
    struct row *row = &acc->data[index];

    row->a = a;
    row->b = b;

    atomic_store_explicit(&acc->head->write, write + 1, memory_order_release);
}

bool rill_acc_write(struct rill_acc *acc, const char *file, rill_ts_t now)
{
    size_t start = atomic_load_explicit(&acc->head->read, memory_order_acquire);
    size_t end = atomic_load_explicit(&acc->head->write, memory_order_acquire);
    if (start == end) return true;
    assert(start < end);

    if (end - start > acc->head->len) {
        printf("acc lost '%lu' events: read=%lu, write=%lu, cap=%lu\n",
                (end - start) - acc->head->len, start, end, acc->head->len);
        start = end - acc->head->len;
    }

    struct rill_rows rows = {0};
    if (!rill_rows_reserve(&rows, end - start)) goto fail_rows_reserve;

    for (size_t i = start; i < end; ++i) {
        size_t index = i % acc->head->len;
        struct row *row = &acc->data[index];

        if (!rill_rows_push(&rows, row->a, row->b)) goto fail_rows_push;
    }

    if (!rill_store_write(file, now, 0, &rows)) {
        rill_fail("unable to write acc file '%s'", file);
        goto fail_write;
    }

    atomic_store_explicit(&acc->head->read, end, memory_order_release);

    rill_rows_free(&rows);
    return true;

  fail_write:
  fail_rows_push:
    rill_rows_free(&rows);
  fail_rows_reserve:
    return false;
}
