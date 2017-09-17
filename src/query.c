/* rill.c
   Rémi Attab (remi.attab@gmail.com), 03 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <assert.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>



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
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_struct;
    }

    query->dir = strndup(dir, PATH_MAX);
    if (!query->dir) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_dir;
    }

    DIR *dir_handle = opendir(dir);
    if (!dir_handle) {
        fail_errno("unable to open dir '%s'", dir);
        goto fail_dir;
    }

    struct dirent *entry;
    while ((entry = readdir(dir_handle))) {
        if (entry->d_type != DT_REG) continue;
        if (!strcmp(entry->d_name, "acc")) continue;

        char file[PATH_MAX];
        snprintf(file, sizeof(file), "%s/%s", query->dir, entry->d_name);

        query->list[query->len] = rill_store_open(file);
        if (!query->list[query->len]) continue;

        query->len++;
    }

    closedir(dir_handle);

    return query;

    closedir(dir_handle);
  fail_dir:
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

struct rill_pairs *rill_query_key(
        struct rill_query *query,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out)
{
    if (!len) return out;

    struct rill_pairs *result = out;
    for (size_t i = 0; i < query->len; ++i) {
        result = rill_store_scan_key(query->list[i], keys, len, result);
        if (!result) return NULL;
    }

    rill_pairs_compact(result);
    return result;
}

struct rill_pairs *rill_query_val(
        struct rill_query *query,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out)
{
    if (!len) return out;

    struct rill_pairs *result = out;
    for (size_t i = 0; i < query->len; ++i) {
        result = rill_store_scan_val(query->list[i], vals, len, result);
        if (!result) return result;
    }

    rill_pairs_compact(result);
    return result;
}
