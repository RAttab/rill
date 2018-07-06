/* dump.c
   Rémi Attab (remi.attab@gmail.com), 07 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>


// -----------------------------------------------------------------------------
// dump
// -----------------------------------------------------------------------------

static void dump_headers(struct rill_store *store)
{
    printf("file:    %s\n", rill_store_file(store));
    printf("version: %u\n", rill_store_version(store));
    printf("ts:      %lu\n", rill_store_ts(store));
    printf("quant:   %lu\n", rill_store_quant(store));
    printf("rows:    %lu\n", rill_store_rows(store));
    printf("vals[a]: %zu\n", rill_store_vals_count(store, rill_col_a));
    printf("vals[b]: %zu\n", rill_store_vals_count(store, rill_col_b));
}

static void dump_stats(struct rill_store *store)
{
    struct rill_store_stats stats = {0};
    rill_store_stats(store, &stats);

    printf("file:     %s\n", rill_store_file(store));
    printf("header:   %zu\n", stats.header_bytes);
    printf("index[a]: %zu\n", stats.index_bytes[rill_col_a]);
    printf("index[b]: %zu\n", stats.index_bytes[rill_col_b]);
    printf("rows[a]:  %zu\n", stats.rows_bytes[rill_col_a]);
    printf("rows[b]:  %zu\n", stats.rows_bytes[rill_col_b]);
}

static void dump_vals(struct rill_store *store, enum rill_col col)
{
    const size_t vals_len = rill_store_vals_count(store, col);
    rill_val_t *vals = calloc(vals_len, sizeof(*vals));

    (void) rill_store_vals(store, col, vals, vals_len);

    for (size_t i = 0; i < vals_len; ++i)
        printf("0x%lx\n", vals[i]);

    free(vals);
}

static void dump_rows(struct rill_store *store, enum rill_col col)
{
    struct rill_store_it *it = rill_store_begin(store, col);
    struct rill_row row = {0};

    while (rill_store_it_next(it, &row)) {
        if (rill_row_nil(&row)) break;
        printf("0x%lx 0x%lx\n", row.a, row.b);
    }

    rill_store_it_free(it);
}


// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

static void usage()
{
    fprintf(stderr, "rill_dump -<h|s> <file>\n");
    fprintf(stderr, "rill_dump -<v|r> -<a|b> <file>\n");
    exit(1);
}

int main(int argc, char **argv)
{
    bool headers = false;
    bool stats = false;
    bool vals = false;
    bool rows = false;
    bool a = false;
    bool b = false;

    int opt = 0;
    while ((opt = getopt(argc, argv, "+hsvrab")) != -1) {
        switch (opt) {
        case 'h': headers = true; break;
        case 's': stats = true; break;
        case 'v': vals = true; break;
        case 'r': rows = true; break;
        case 'a': a = true; break;
        case 'b': b = true; break;
        default:
            fprintf(stderr, "unknown argument: %c\n", opt);
            usage();
        }
    }

    if (optind >= argc) usage();

    struct rill_store *store = rill_store_open(argv[optind]);
    if (!store) rill_exit(1);

    if (!headers && !stats && !vals && !rows) usage();
    
    if (headers) dump_headers(store);
    if (stats) dump_stats(store);

    if ((a && b) || (!a && !b)) usage();
    enum rill_col col = a ? rill_col_a : rill_col_b;

    if (vals) dump_vals(store, col);
    if (rows) dump_rows(store, col);

    rill_store_close(store);
    return 0;
}
