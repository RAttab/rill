/* rill_invert.c
   Rémi Attab (remi.attab@gmail.com), 02 Nov 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>

enum { shards = 10 };

static struct rill_pairs *make_pairs(struct rill_store *store)
{
    size_t cap = (rill_store_pairs(store) - 1) / shards + 1;
    size_t len = sizeof(struct rill_pairs) + cap * sizeof(struct rill_kv);
    len = (len / page_len + 1) * page_len;

    // glibc's malloc chokes on large allocation for whatever reason...
    struct rill_pairs *pairs =
        mmap(0, len, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (pairs == MAP_FAILED) {
        rill_fail_errno("unable mmap '%p' bytes", (void *) len);
        rill_exit(1);
    }

    pairs->cap = cap;
    return pairs;
}

int main(int argc, const char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "rill_invert <src> <dst>\n");
        exit(1);
    }

    fprintf(stderr, "opening '%s'...\n", argv[1]);
    struct rill_store *src = rill_store_open(argv[1]);
    if (!src) rill_exit(1);

    struct rill_pairs *pairs = make_pairs(src);

    size_t shard_i = 0;
    struct rill_store *shard[shards] = {0};

    rill_ts_t ts = rill_store_ts(src), quant = rill_store_quant(src);

    fprintf(stderr, "reading '%lu' pairs in shards of '%lu'...\n",
            rill_store_pairs(src), pairs->cap);

    struct rill_kv kv;
    struct rill_store_it *it = rill_store_begin(src);

    printf("%lu: reading...\n", shard_i);

    do {
        if (!rill_store_it_next(it, &kv)) rill_exit(1);
        if (!rill_kv_nil(&kv))
            pairs = rill_pairs_push(pairs, kv.val, kv.key);

        if (pairs->len == pairs->cap || rill_kv_nil(&kv)) {
            printf("%lu: compacting...\n", shard_i);

            rill_pairs_compact(pairs);

            char path[256];
            snprintf(path, sizeof(path), "%s.%lu", argv[2], shard_i);
            printf("%lu: writing '%s'...\n", shard_i, path);

            if (!rill_store_write(path, ts, quant, pairs)) rill_exit(1);

            shard[shard_i] = rill_store_open(path);
            if (!shard[shard_i]) rill_exit(1);

            shard_i++;
            rill_pairs_clear(pairs);
            printf("%lu: reading...\n", shard_i);
        }

    } while (!rill_kv_nil(&kv));

    fprintf(stderr, "merging to '%s'...\n", argv[2]);
    if (!rill_store_merge(argv[2], ts, quant, shard, shards)) rill_exit(1);

    for (size_t i = 0; i < shards; ++i) rill_store_rm(shard[i]);

    return 0;
}
