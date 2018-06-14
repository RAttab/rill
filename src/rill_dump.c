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
    fprintf(stderr, "rill_dump [-h] [-k] [-p] [-m] -<a|b> <file>\n");
    exit(1);
}

int main(int argc, char **argv)
{
    bool header = false;
    bool key = false;
    bool rows = false;
    bool a = false;
    bool b = false;
    bool space = false;

    int opt = 0;
    while ((opt = getopt(argc, argv, "+habkpm")) != -1) {
        switch (opt) {
        case 'h': header = true; break;
        case 'k': key = true; break;
        case 'p': rows = true; break;
        case 'a': a = true; break;
        case 'b': b = true; break;
        case 'm': space = true; break;
        default:
            fprintf(stderr, "unknown argument: %c\n", opt);
            usage();
        }
    }

    if (!header && !a && !b && !a && !rows && !key && !space) usage();
    if (optind >= argc) usage();

    struct rill_store *store = rill_store_open(argv[optind]);
    if (!store) rill_exit(1);

    if (header) {
        printf("file:        %s\n", rill_store_file(store));
        printf("version:     %u\n", rill_store_version(store));
        printf("ts:          %lu\n", rill_store_ts(store));
        printf("quant:       %lu\n", rill_store_quant(store));
        printf("keys data a: %zu\n", rill_store_keys_count(store, rill_col_a));
        printf("keys data b: %zu\n", rill_store_keys_count(store, rill_col_b));
        printf("rows:       %lu\n", rill_store_rows(store));
        printf("index a len: %zu\n", rill_store_index_len(store, rill_col_a));
        printf("index b len: %zu\n", rill_store_index_len(store, rill_col_b));
    }

    if ((key || rows) && !a && !b) {
        fprintf(stderr, "you need to specify column a or b\n");
        return -1;
    }

    if (key) {
        const enum rill_col col = a ? rill_col_a : rill_col_b;
        const size_t keys_len = rill_store_keys_count(store, col);
        rill_val_t *keys = calloc(keys_len, sizeof(*keys));

        (void) rill_store_keys(store, keys, keys_len, col);

        printf("vals %c:\n", col ? 'b' : 'a');

        for (size_t i = 0; i < keys_len; ++i)
            printf("  0x%lx\n", keys[i]);
    }

    if (rows) {
        struct rill_row row = {0};
        const enum rill_col col = a ? rill_col_a : rill_col_b;
        struct rill_store_it *it = rill_store_begin(store, col);

        printf("rows %c:\n", a ? 'a' : 'b');
        while (rill_store_it_next(it, &row)) {
            if (rill_row_nil(&row)) break;
            printf("  0x%lx 0x%lx\n", row.key, row.val);
        }

        rill_store_it_free(it);
    }

    if (space) {
        struct rill_space* space = rill_store_space(store);

        printf(
            "size stats  : %s\n"
            "header size : %zu\n"
            "index a size: %zu\n"
            "index b size: %zu\n"
            "data a size : %zu\n"
            "data b size : %zu\n",
            rill_store_file(store),
            rill_store_space_header(space),
            rill_store_space_index(space, rill_col_a),
            rill_store_space_index(space, rill_col_b),
            rill_store_space_rows(space, rill_col_a),
            rill_store_space_rows(space, rill_col_b));

        free(space);
    }

    rill_store_close(store);
    return 0;
}
