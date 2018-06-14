/* rill.c
   Rémi Attab (remi.attab@gmail.com), 03 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <assert.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>


// -----------------------------------------------------------------------------
// rill
// -----------------------------------------------------------------------------

struct rill_query
{
    const char *dir;

    size_t len;
    struct rill_store *list[1024];
};

struct rill_query * rill_query_open(const char *dir)
{
    struct rill_query *query = calloc(1, sizeof(*query));
    if (!query) {
        rill_fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_struct;
    }

    query->dir = strndup(dir, PATH_MAX);
    if (!query->dir) {
        rill_fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_dir;
    }

    size_t cap = sizeof(query->list) / sizeof(query->list[0]);
    query->len = rill_scan_dir(query->dir, query->list, cap);

    return query;

    free((char *) query->dir);
  fail_alloc_dir:
    free(query);
  fail_alloc_struct:
    return NULL;
}

void rill_query_close(struct rill_query *query)
{
    for (size_t i = 0; i < query->len; ++i)
        rill_store_close(query->list[i]);

    free((char *) query->dir);
    free(query);
}

struct rill_rows *rill_query_key(
        const struct rill_query *query, rill_val_t key, struct rill_rows *out)
{
    if (!key) return out;

    struct rill_rows *result = out;
    for (size_t i = 0; i < query->len; ++i) {
        result = rill_store_query_key(query->list[i], key, result);
        if (!result) return NULL;
    }

    rill_rows_compact(result);
    return result;
}

struct rill_rows *rill_query_keys(
        const struct rill_query *query,
        const rill_val_t *keys, size_t len,
        struct rill_rows *out)
{
    if (!len) return out;

    struct rill_rows *result = out;
    for (size_t i = 0; i < query->len; ++i) {
        for (size_t j = 0; i < len; ++j) {
            result = rill_store_query_key(query->list[i], keys[j], result);
            if (!result) return NULL;
        }
    }

    rill_rows_compact(result);
    return result;
}

static int compare_rill_values(const void *v1, const void *v2) {
    const rill_val_t rv1 = *(rill_val_t*)v1;
    const rill_val_t rv2 = *(rill_val_t*)v2;

    if (rv1 > rv2) return 1;
    if (rv1 < rv2) return -1;
    return 0;
}

struct rill_rows *rill_query_vals(
        const struct rill_query *query,
        const rill_val_t *vals, size_t len,
        struct rill_rows *out)
{
    if (!len) return out;

    rill_val_t *sorted = malloc(sizeof(vals[0]) * len);
    if (!sorted) goto fail_alloc;

    memcpy(sorted, vals, sizeof(vals[0]) * len);
    qsort(sorted, len, sizeof(vals[0]), compare_rill_values);

    struct rill_rows *result = out;
    for (size_t i = 0; i < query->len; ++i) {
        for (size_t j = 0; j < len; ++j) {
            result = rill_store_query_value(query->list[i], sorted[j], result);
            if (!result) goto fail_scan;
        }
    }

    rill_rows_compact(result);
    free(sorted);
    return result;

  fail_scan:
    free(sorted);
  fail_alloc:
    // \todo potentially leaking result
    return NULL;
}

struct rill_rows *rill_query_all(
    const struct rill_query *query, enum rill_col col)
{
    struct rill_rows *result = rill_rows_new(1);
    for (size_t i = 0; i < query->len; ++i) {
        size_t rows = rill_store_rows(query->list[i]);
        result = rill_rows_reserve(result, result->len + rows);
        if (!result) goto fail_scan;

        struct rill_store_it *it = rill_store_begin(query->list[i], col);
        if (!it) goto fail_scan;

        struct rill_row row;
        while (true) {
            if (!rill_store_it_next(it, &row)) {
                rill_store_it_free(it);
                goto fail_scan;
            }
            if (rill_row_nil(&row)) break;

            result = rill_rows_push(result, row.key, row.val);
        }
        rill_store_it_free(it);
    }

    rill_rows_compact(result);
    return result;

  fail_scan:
    free(result);
    return NULL;

}
