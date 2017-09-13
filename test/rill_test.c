/* rill_test.c
   Rémi Attab (remi.attab@gmail.com), 13 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "test.h"


// -----------------------------------------------------------------------------
// rotate
// -----------------------------------------------------------------------------

bool test_rotate(void)
{
    const char *dir = "test.rotate.db";

    rm(dir);
    struct rill *db = rill_open(dir);

    const uint64_t key = 1;

    for (rill_ts_t ts = 0; ts < 13 * month; ts += 1 * hour) {
        rill_ingest(db, key, ts + 1);
        rill_rotate(db, ts);
    }
    rill_rotate(db, 13 * month);

    {
        struct rill_pairs *pairs = rill_query_key(db, &key, 1, rill_pairs_new(1));

        size_t i = 0;
        for (rill_ts_t ts = 0; ts < 13 * month; ts += 1 * hour) {
            assert(pairs->data[i].key == key);
            assert(pairs->data[i].val == ts + 1);
            ++i;
        }

        rill_pairs_free(pairs);
    }

    // \todo this is bad and doesn't properly expire things.
    for (size_t i = 1; i <= 6; ++i)
        rill_rotate(db, (13 + i) * month);

    {
        struct rill_pairs *pairs = rill_query_key(db, &key, 1, rill_pairs_new(1));

        for (size_t i = 0; i < pairs->len; ++i) {
            assert(pairs->data[i].key == key);
            assert(pairs->data[i].val >= (5 * month) + 1);
        }

        rill_pairs_free(pairs);
    }

    rill_close(db);
    rm(dir);

    return true;
}


// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    bool ret = true;

    ret = ret && test_rotate();

    return ret ? 0 : 1;
}
