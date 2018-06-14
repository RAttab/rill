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
    return cap;
}

void rill_rows_free(struct rill_rows *rows)
{
    free(rows->data);
}

void rill_rows_clear(struct rill_rows *rows)
{
    rows->len = 0;
}

bool rill_rows_reserve(struct rill_rows *rows, size_t cap)
{
    if (rill_likely(cap <= rows->cap)) return true;

    size_t new_cap = rows->cap;
    while (new_cap < cap) new_cap *= 2;

    rows->data = realloc(rows->data, new_cap * sizeof(rows->data[0]));
    if (!rows->data) {
        rill_fail("unable to realloc rows: cap=%lu", new_cap);
        return false;
    }

    rows->cap = new_cap;
    return true;
}

bool rill_rows_push(struct rill_rows *rows, rill_val_t a, rill_val_t b)
{
    assert(a && b);
    if (!rill_rows_reserve(rows, rows->len + 1)) return false;

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

void rill_rows_invert(struct rill_rows* rows)
{
    for (size_t i = 0; i < rows->len; ++i) {
        rows->data[i] = (struct rill_row) {
            .a = rows->data[i].b,
            .b = rows->data[i].a,
        };
    }

    qsort(rows->data, rows->len, sizeof(*rows->data), &row_cmp);
}

void rill_rows_print(const struct rill_rows *rows)
{
    const rill_val_t no_key = -1ULL;
    rill_val_t key = no_key;

    printf("rows(%lu, %lu):\n", rows->len, rows->cap);

    for (size_t i = 0; i < rows->len; ++i) {
        const struct rill_row *row = &rows->data[i];

        if (row->key == key) printf(", %p", row->b);
        else {
            if (key != no_key) printf("]\n");
            printf("  %p: [ %p", (void *) row->a, row->b);
            key = row->key;
        }
    }

    if (rows->len) printf(" ]\n");
}
