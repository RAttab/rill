/* pairs.c
   Rémi Attab (remi.attab@gmail.com), 02 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


// -----------------------------------------------------------------------------
// pairs
// -----------------------------------------------------------------------------

void rill_pairs_free(struct rill_pairs *pairs)
{
    free(pairs->data);
}

static bool resize(struct rill_pairs *pairs, size_t len)
{
    if (len <= pairs->cap) return true;

    size_t cap = pairs->cap ? pairs->cap : 1;
    while (cap < len) cap *= 2;

    void *ret = realloc(pairs->data, cap);
    if (!ret) return false;

    pairs->data = ret;
    pairs->cap = cap;

    return true;
}

bool rill_pairs_reset(struct rill_pairs *pairs, size_t cap)
{
    pairs->len = 0;
    return resize(pairs, cap);
}

bool rill_pairs_push(struct rill_pairs *pairs, rill_key_t key, rill_val_t val)
{
    if (!resize(pairs, pairs->len + 1)) return false;

    pairs->data[pairs->len] = (struct rill_kv) { .key = key, .val = val };
    pairs->len++;

    return true;
}

static int kv_cmp(const void *lhs, const void *rhs)
{
    return rill_kv_cmp(lhs, rhs);
}

void rill_pairs_compact(struct rill_pairs *pairs)
{
    if (pairs->len <= 1) return;
    qsort(pairs->data, pairs->len, sizeof(*pairs->data), &kv_cmp);

    size_t j = 0;
    for (size_t i = 1; i < pairs->len; ++i) {
        if (!rill_kv_cmp(&pairs->data[i], &pairs->data[j])) continue;
        ++j;
        if (j != i) pairs->data[j] = pairs->data[i];
    }

    assert(j + 1 <= pairs->len);
    pairs->len = j + 1;
}

bool rill_pairs_scan_key(
        const struct rill_pairs *pairs,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out)
{
    for (size_t i = 0; i < pairs->len; ++i) {
        struct rill_kv *kv = &pairs->data[i];

        for (size_t j = 0; j < len; ++j) {
            if (kv->key != keys[j]) continue;
            if (!rill_pairs_push(out, kv->key, kv->val)) return false;
        }
    }

    return true;
}

bool rill_pairs_scan_val(
        const struct rill_pairs *pairs,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out)
{
    for (size_t i = 0; i < pairs->len; ++i) {
        struct rill_kv *kv = &pairs->data[i];

        for (size_t j = 0; j < len; ++j) {
            if (kv->val != vals[j]) continue;
            if (!rill_pairs_push(out, kv->key, kv->val)) return false;
        }
    }

    return true;
}

void rill_pairs_print(const struct rill_pairs *pairs)
{
    const rill_key_t no_key = -1ULL;
    rill_key_t key = no_key;

    for (size_t i = 0; i < pairs->len; ++i) {
        struct rill_kv *kv = &pairs->data[i];

        if (kv->key == key) fprintf(stderr, "%lu, ", kv->val);
        else {
            if (key != no_key) fprintf(stderr, "]\n");
            fprintf(stderr, "%p: [ %lu", (void *) kv->key, kv->val);
            key = kv->key;
        }
    }

    fprintf(stderr, "]\n");
}
