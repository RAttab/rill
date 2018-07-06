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

struct rill_row row(rill_val_t a, rill_val_t b)
{
    return (struct rill_row) { .a = a, .b = b };
}

#define make_rows(...)                                          \
    ({                                                          \
        struct rill_row rows[] = { __VA_ARGS__ };               \
        make_rows_impl(rows, sizeof(rows) / sizeof(rows[0]));   \
    })

struct rill_rows make_rows_impl(const struct rill_row *rows, size_t len)
{
    struct rill_rows result = {0};
    assert(rill_rows_reserve(&result, len));

    for (size_t i = 0; i < len; ++i)
        assert(rill_rows_push(&result, rows[i].a, rows[i].b));

    return result;
}

enum { rng_range_a = 250, rng_range_b = 100 };

struct rill_rows make_rng_rows(struct rng *rng)
{
    enum { len = 1000 };
    struct rill_rows rows = {0};
    rill_rows_reserve(&rows, len);

    for (size_t i = 0; i < len; ++i) {
        uint64_t a = rng_gen_range(rng, 1, rng_range_a);
        uint64_t b = rng_gen_range(rng, 1, rng_range_b);
        rill_rows_push(&rows, a, b);
    }

    rill_rows_compact(&rows);
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


// -----------------------------------------------------------------------------
// hexdump
// -----------------------------------------------------------------------------

void hexdump(const uint8_t *buffer, size_t len)
{
    for (size_t i = 0; i < len;) {
        printf("%6p: ", (void *) i);
        for (size_t j = 0; j < 16 && i < len; ++i, ++j) {
            if (j % 2 == 0) printf(" ");
            printf("%02x", buffer[i]);
        }
        printf("\n");
    }
}
