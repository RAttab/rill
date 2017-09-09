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


// -----------------------------------------------------------------------------
// utils
// -----------------------------------------------------------------------------

struct rill_kv kv(rill_key_t key, rill_val_t val)
{
    return (struct rill_kv) { .key = key, .val = val };
}

#define make_pair(...)                                          \
    ({                                                          \
        struct rill_kv kvs[] = { __VA_ARGS__ };                 \
        make_pair_impl(kvs, sizeof(kvs) / sizeof(kvs[0]));      \
    })

struct rill_pairs make_pair_impl(const struct rill_kv *kv, size_t len)
{
    struct rill_pairs pairs = {0};
    for (size_t i = 0; i < len; ++i)
        rill_pairs_push(&pairs, kv[i].key, kv[i].val);
    return pairs;
}
