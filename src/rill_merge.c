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

    size_t len = argc - optind;
    struct rill_store *stores[len];
    for (size_t i = 0; i < len; i++, optind++) {
        stores[i] = rill_store_open(argv[optind]);
        if (!stores[i]) rill_exit(1);
    }

    if (!rill_store_merge(output, ts, quant, stores, len))
        rill_exit(1);

    for (size_t i = 0; i < len; ++i)
        rill_store_rm(stores[i]);

    return 0;
}
