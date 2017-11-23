/* rill_merge.c
   Rémi Attab (remi.attab@gmail.com), 23 Nov 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <getopt.h>
#include <stdlib.h>
#include <limits.h>


void usage()
{
    fprintf(stderr, "rill_ingest -t <ts> -q <quant> -o <output> <input...>\n");
    exit(1);
}

int main(int argc, char **argv)
{
    rill_ts_t ts = 0;
    rill_ts_t quant = 0;
    char *output = NULL;

    int opt = 0;
    while ((opt = getopt(argc, argv, "+t:q:o:")) != -1) {
        switch (opt) {
        case 't': ts = atol(optarg); break;
        case 'q': quant = atol(optarg); break;
        case 'o': output = optarg; break;
        default: usage();
        }
    }

    if (!ts || !quant || !output) usage();
    if (optind >= argc) usage();

    struct rill_store *merge[64] = {0};

    for (; optind < argc; optind++) {
        struct rill_store *store = rill_store_open(argv[optind]);
        if (!store) rill_exit(1);

        for (size_t i = 0; i < 64; ++i) {
            if (!merge[i]) { merge[i] = store; break; }

            printf("merging: %lu\n", i);

            char out[PATH_MAX];
            snprintf(out, sizeof(out), "%s.rill.%lu", argv[optind], i);

            struct rill_store *list[2] = { store, merge[i] };
            if (!rill_store_merge(out, ts, quant, list, 2)) rill_exit(1);

            store = rill_store_open(out);
            if (!store) rill_exit(1);

            merge[i] = NULL;
            rill_store_rm(list[0]);
            rill_store_rm(list[1]);
        }
    }

    if (!rill_store_merge(output, ts, quant, merge, 64)) rill_exit(1);
    for (size_t i = 0; i < 64; ++i) {
        if (!merge[i]) continue;
        rill_store_rm(merge[i]);
    }

    return 0;
}
