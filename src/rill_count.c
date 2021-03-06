/* rill_count.c
   Rémi Attab (remi.attab@gmail.com), 07 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>


// -----------------------------------------------------------------------------
// count
// -----------------------------------------------------------------------------

static void count(struct rill_store *store, enum rill_col col)
{
    struct rill_row row;
    struct rill_store_it *it = rill_store_begin(store, col);

    rill_val_t key = 0;
    size_t count = 0;
    while (rill_store_it_next(it, &row)) {
        if (rill_row_nil(&row)) break;

        if (row.a == key) count++;
        else {
            if (key) printf("%lu %p\n", count, (void *) key);
            count = 1;
            key = row.a;
        }
    }

    rill_store_it_free(it);
}


// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

static void usage()
{
    fprintf(stderr, "rill_count -<a|b> <file>\n");
    exit(1);
}

int main(int argc, char **argv)
{
    if (argc != 3) usage();

    int opt = 0;
    bool col_a = false, col_b = false;

    while ((opt = getopt(argc, argv, "+ab")) != -1) {
        switch(opt) {
        case 'a': col_a = true; break;
        case 'b': col_b = true; break;
        default: usage();
        }
    }

    if (optind >= argc) usage();

    enum rill_col col;
    if (!rill_args_col(col_a, col_b, &col)) usage();
    
    struct rill_store *store = rill_store_open(argv[optind]);
    if (!store) rill_exit(1);

    count(store, col);

    rill_store_close(store);
    return 0;
}
