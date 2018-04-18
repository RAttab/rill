/* pairs.c
   Rémi Attab (remi.attab@gmail.com), 02 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


// -----------------------------------------------------------------------------
// kv
// -----------------------------------------------------------------------------

extern inline bool rill_kv_nil(const struct rill_kv *);
extern inline int rill_kv_cmp(const struct rill_kv *, const struct rill_kv *);


// -----------------------------------------------------------------------------
// pairs
// -----------------------------------------------------------------------------

static size_t adjust_cap(size_t cap, size_t len)
{
    while (len > cap) cap *= 2;
    return cap;
}

struct rill_pairs *rill_pairs_new(size_t cap)
{
    cap = adjust_cap(1, cap);

    struct rill_pairs *pairs =
        calloc(1, sizeof(*pairs) + cap * sizeof(pairs->data[0]));
    if (!pairs) {
        rill_fail("unable to alloc pairs: cap=%lu", cap);
        return NULL;
    }

    pairs->cap = cap;
    return pairs;
}


void rill_pairs_free(struct rill_pairs *pairs)
{
    free(pairs);
}


void rill_pairs_clear(struct rill_pairs *pairs)
{
    pairs->len = 0;
}

struct rill_pairs *rill_pairs_reserve(struct rill_pairs *pairs, size_t cap)
{
    if (rill_likely(cap <= pairs->cap)) return pairs;
    cap = adjust_cap(pairs->cap, cap);

    pairs = realloc(pairs, sizeof(*pairs) + cap * sizeof(pairs->data[0]));
    if (!pairs) {
        rill_fail("unable to realloc pairs: cap=%lu", cap);
        return NULL;
    }

    pairs->cap = cap;
    return pairs;
}

struct rill_pairs *rill_pairs_push(
        struct rill_pairs *pairs, rill_key_t key, rill_val_t val)
{
    assert(key && val && pairs);

    pairs = rill_pairs_reserve(pairs, pairs->len + 1);
    if (!pairs) return NULL;

    pairs->data[pairs->len] = (struct rill_kv) { .key = key, .val = val };
    pairs->len++;

    return pairs;
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

void rill_pairs_print(const struct rill_pairs *pairs)
{
    const rill_key_t no_key = -1ULL;
    rill_key_t key = no_key;

    printf("pairs(%p, %lu, %lu):\n", (void *) pairs, pairs->len, pairs->cap);

    for (size_t i = 0; i < pairs->len; ++i) {
        const struct rill_kv *kv = &pairs->data[i];

        if (kv->key == key) printf(", %lu", kv->val);
        else {
            if (key != no_key) printf("]\n");
            printf("  %p: [ %lu", (void *) kv->key, kv->val);
            key = kv->key;
        }
    }

    if (pairs->len) printf(" ]\n");
}

void rill_pairs_invert(struct rill_pairs* pairs)
{
    for (size_t i = 0; i < pairs->len; ++i) {
        pairs->data[i] = (struct rill_kv) {
            .key = pairs->data[i].val,
            .val = pairs->data[i].key,
        };
    }
}
