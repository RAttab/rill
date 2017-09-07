/* bench.c
   Rémi Attab (remi.attab@gmail.com), 04 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    struct rill *db = rill_open("db");
    if (!db) return 1;

    enum { n = 100 };

    {
        rill_key_t keys[100];
        for (size_t i = 0; i < 100; ++i) keys[i] = i;

        struct rill_pairs out = {0};
        rill_query_key(db, keys, n, &out);

        rill_pairs_print(&out);
    }

    {
        rill_val_t vals[100];
        for (size_t i = 0; i < 100; ++i) vals[i] = i;

        struct rill_pairs out = {0};
        rill_query_val(db, vals, n, &out);

        rill_pairs_print(&out);
    }

    rill_close(db);
    return 0;
}
