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

    const uint64_t key = 1;
    enum { step = 10 * min };

    {
        struct rill_acc *acc = rill_acc_open(dir, 1);

        for (rill_ts_t ts = 0; ts < 13 * month; ts += step) {
            rill_acc_ingest(acc, key, ts + 1);
            rill_rotate(dir, ts);
        }

        rill_acc_close(acc);
        rill_rotate(dir, 13 * month);
    }

    {
        struct rill_query *query = rill_query_open(dir);
        struct rill_pairs *pairs = rill_query_key(query, &key, 1, rill_pairs_new(1));
        rill_query_close(query);

        size_t i = 0;
        for (rill_ts_t ts = 0; ts < 13 * month; ts += step) {
            assert(pairs->data[i].key == key);
            assert(pairs->data[i].val == ts + 1);
            ++i;
        }

        rill_pairs_free(pairs);
    }

    for (size_t i = 1; i <= 6; ++i)
        rill_rotate(dir, (13 + i) * month);

    {
        struct rill_query *query = rill_query_open(dir);
        struct rill_pairs *pairs = rill_query_key(query, &key, 1, rill_pairs_new(1));
        rill_query_close(query);

        for (size_t i = 0; i < pairs->len; ++i) {
            assert(pairs->data[i].key == key);
            assert(pairs->data[i].val >= (5 * month) + 1);
        }

        rill_pairs_free(pairs);
    }

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
