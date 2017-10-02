/* rill_query.c
   Rémi Attab (remi.attab@gmail.com), 01 Oct 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdlib.h>
#include <getopt.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

bool is_file(const char *path)
{
    struct stat st = {0};
    stat(path, &st);
    return S_ISREG(st.st_mode);
}

void usage()
{
    fprintf(stderr, "rill_query [-k <key>|-v <val>] <db>\n");
    exit(1);
}

int main(int argc, char *argv[])
{
    rill_key_t key = 0;
    rill_val_t val = 0;

    int opt = 0;
    while ((opt = getopt(argc, argv, "k:v:")) != -1) {
        switch (opt) {
        case 'k': key = atoi(optarg); break;
        case 'v': val = atoi(optarg); break;
        default: usage(); exit(1);
        }
    }

    if (key && val) { usage(); }
    if (!key && !val) { usage(); }
    if (optind >= argc) { usage(); }

    const char *db = argv[optind];
    struct rill_pairs *pairs = rill_pairs_new(100);

    if (is_file(db)) {
        struct rill_store *store = rill_store_open(db);
        if (!store) rill_exit(1);

        if (key) pairs = rill_store_query_key(store, key, pairs);
        else pairs = rill_store_scan_vals(store, &val, 1, pairs);

        rill_store_close(store);
    }
    else {
        struct rill_query *query = rill_query_open(db);
        if (!query) rill_exit(1);

        if (key) pairs = rill_query_key(query, key, pairs);
        else pairs = rill_query_vals(query, &val, 1, pairs);

        rill_query_close(query);
    }

    if (!pairs) rill_exit(1);

    for (size_t i = 0; i < pairs->len; ++i) {
        if (key) printf("%lu\n", pairs->data[i].val);
        else printf("%p\n", (void *) pairs->data[i].key);
    }

    rill_pairs_free(pairs);
    return 0;
}
