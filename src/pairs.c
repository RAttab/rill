/* rows.c
   Rémi Attab (remi.attab@gmail.com), 02 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


// -----------------------------------------------------------------------------
// row
// -----------------------------------------------------------------------------

extern inline bool rill_row_nil(const struct rill_row *);
extern inline int rill_row_cmp(const struct rill_row *, const struct rill_row *);


// -----------------------------------------------------------------------------
// rows
// -----------------------------------------------------------------------------

static size_t adjust_cap(size_t cap, size_t len)
{
    while (len > cap) cap *= 2;
    return cap;
}

struct rill_rows *rill_rows_new(size_t cap)
{
    cap = adjust_cap(1, cap);

    struct rill_rows *rows =
        calloc(1, sizeof(*rows) + cap * sizeof(rows->data[0]));
    if (!rows) {
        rill_fail("unable to alloc rows: cap=%lu", cap);
        return NULL;
    }

    rows->cap = cap;
    return rows;
}


void rill_rows_free(struct rill_rows *rows)
{
    free(rows);
}


void rill_rows_clear(struct rill_rows *rows)
{
    rows->len = 0;
}

struct rill_rows *rill_rows_reserve(struct rill_rows *rows, size_t cap)
{
    if (rill_likely(cap <= rows->cap)) return rows;
    cap = adjust_cap(rows->cap, cap);

    rows = realloc(rows, sizeof(*rows) + cap * sizeof(rows->data[0]));
    if (!rows) {
        rill_fail("unable to realloc rows: cap=%lu", cap);
        return NULL;
    }

    rows->cap = cap;
    return rows;
}

struct rill_rows *rill_rows_push(
        struct rill_rows *rows, rill_val_t key, rill_val_t val)
{
    assert(key && val && rows);

    rows = rill_rows_reserve(rows, rows->len + 1);
    if (!rows) return NULL;

    rows->data[rows->len] = (struct rill_row) { .key = key, .val = val };
    rows->len++;

    return rows;
}

static int row_cmp(const void *lhs, const void *rhs)
{
    return rill_row_cmp(lhs, rhs);
}

void rill_rows_compact(struct rill_rows *rows)
{
    if (rows->len <= 1) return;
    qsort(rows->data, rows->len, sizeof(*rows->data), &row_cmp);

    size_t j = 0;
    for (size_t i = 1; i < rows->len; ++i) {
        if (!rill_row_cmp(&rows->data[i], &rows->data[j])) continue;
        ++j;
        if (j != i) rows->data[j] = rows->data[i];
    }

    assert(j + 1 <= rows->len);
    rows->len = j + 1;
}

void rill_rows_print(const struct rill_rows *rows)
{
    const rill_val_t no_key = -1ULL;
    rill_val_t key = no_key;

    printf("rows(%p, %lu, %lu):\n", (void *) rows, rows->len, rows->cap);

    for (size_t i = 0; i < rows->len; ++i) {
        const struct rill_row *row = &rows->data[i];

        if (row->key == key) printf(", %lu", row->val);
        else {
            if (key != no_key) printf("]\n");
            printf("  %p: [ %lu", (void *) row->key, row->val);
            key = row->key;
        }
    }

    if (rows->len) printf(" ]\n");
}

void rill_rows_invert(struct rill_rows* rows)
{
    for (size_t i = 0; i < rows->len; ++i) {
        rows->data[i] = (struct rill_row) {
            .key = rows->data[i].val,
            .val = rows->data[i].key,
        };
    }
}
