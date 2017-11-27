/* dump.c
   Rémi Attab (remi.attab@gmail.com), 07 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>

void usage()
{
    fprintf(stderr, "rill_dump [-h] [-k] [-v] [-p] <file>\n");
    exit(1);
}

int main(int argc, char **argv)
{
    bool header = false;
    bool keys = false;
    bool vals = false;
    bool pairs = false;

    int opt = 0;
    while ((opt = getopt(argc, argv, "+hkvp")) != -1) {
        switch (opt) {
        case 'h': header = true; break;
        case 'k': keys = true; break;
        case 'v': vals = true; break;
        case 'p': pairs = true; break;
        default:
            fprintf(stderr, "unknown argument: %c\n", opt);
            usage();
        }
    }

    if (!header && !keys && !vals && !pairs) usage();
    if (optind >= argc) usage();

    struct rill_store *store = rill_store_open(argv[optind]);
    if (!store) rill_exit(1);

    if (header) {
        printf("file:    %s\n", rill_store_file(store));
        printf("version: %u\n", rill_store_version(store));
        printf("ts:      %lu\n", rill_store_ts(store));
        printf("quant:   %lu\n", rill_store_quant(store));
        printf("keys:    %lu\n", rill_store_keys(store));
        printf("vals:    %lu\n", rill_store_vals(store));
        printf("pairs:   %lu\n", rill_store_pairs(store));
    }

    if (keys) {
        size_t key_len = rill_store_keys(store);
        rill_key_t *keys = calloc(key_len, sizeof(*keys));
        (void) rill_store_dump_keys(store, keys, key_len);

        printf("keys:\n");
        for (size_t i = 0; i < key_len; ++i)
            printf("  %p\n", (void *) keys[i]);
    }

    if (vals) {
        size_t val_len = rill_store_vals(store);
        rill_val_t *vals = calloc(val_len, sizeof(*vals));
        (void) rill_store_dump_vals(store, vals, val_len);

        printf("vals:\n");
        for (size_t i = 0; i < val_len; ++i)
            printf("  %p\n", (void *) vals[i]);
    }

    if (pairs) {
        struct rill_kv kv;
        struct rill_store_it *it = rill_store_begin(store);

        printf("pairs:\n");
        while (rill_store_it_next(it, &kv))
            printf("  %p %p\n", (void *) kv.key, (void *) kv.val);

        rill_store_it_free(it);
    }

    rill_store_close(store);
    return 0;
}
