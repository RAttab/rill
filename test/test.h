/* test.h
   Rémi Attab (remi.attab@gmail.com), 11 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#pragma once

#include "rill.h"
#include "utils.h"
#include "htable.h"
#include "rng.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>


// -----------------------------------------------------------------------------
// rows
// -----------------------------------------------------------------------------

struct rill_row row(rill_val_t key, rill_val_t val)
{
    return (struct rill_row) { .key = key, .val = val };
}

#define make_pair(...)                                          \
    ({                                                          \
        struct rill_row rows[] = { __VA_ARGS__ };               \
        make_pair_impl(rows, sizeof(rows) / sizeof(rows[0]));   \
    })

struct rill_rows *make_pair_impl(const struct rill_row *row, size_t len)
{
    struct rill_rows *rows = rill_rows_new(len);
    for (size_t i = 0; i < len; ++i)
        rows = rill_rows_push(rows, row[i].key, row[i].val);
    return rows;
}

enum { rng_range_key = 500, rng_range_val = 100 };

struct rill_rows *make_rng_rows(struct rng *rng)
{
    enum { len = 1000 };
    struct rill_rows *rows = rill_rows_new(len);

    for (size_t i = 0; i < len; ++i) {
        uint64_t key = rng_gen_range(rng, 1, rng_range_key);
        uint64_t val = rng_gen_range(rng, 1, rng_range_val);
        rows = rill_rows_push(rows, key, val);
        assert(rows);
    }

    rill_rows_compact(rows);

    return rows;
}


// -----------------------------------------------------------------------------
// rm
// -----------------------------------------------------------------------------

void rm(const char *path)
{
    DIR *dir = opendir(path);
    if (!dir) return;

    struct dirent *entry;
    while (true) {
        if (!(entry = readdir(dir))) break;
        else if (entry->d_type != DT_REG) continue;

        char file[PATH_MAX];
        snprintf(file, sizeof(file), "%s/%s", path, entry->d_name);
        unlink(file);
    }

    closedir(dir);
    rmdir(path);
}
