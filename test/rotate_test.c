/* rill_test.c
   Rémi Attab (remi.attab@gmail.com), 13 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "test.h"

// -----------------------------------------------------------------------------
// rotate
// -----------------------------------------------------------------------------

void acc_dump(struct rill_acc *acc, const char *dir, rill_ts_t ts)
{
    char file[PATH_MAX];
    snprintf(file, sizeof(file), "%s/%010lu.rill", dir, ts);

    if (!rill_acc_write(acc, file, ts)) rill_abort();
}

bool test_rotate(void)
{
    const char *dir = "test.rotate.db";

    rm(dir);

    const uint64_t key = 1;
    enum { step = 10 * min_secs };

    struct rill_acc *acc = rill_acc_open(dir, 1);

    {

        for (rill_ts_t ts = 0; ts < expire_secs; ts += step) {
            rill_acc_ingest(acc, key, ts + 1);
            acc_dump(acc, dir, ts);
            rill_rotate(dir, ts, expire_secs);
        }

        acc_dump(acc, dir, expire_secs);
        rill_rotate(dir, expire_secs, expire_secs);
    }

    {
        struct rill_query *query = rill_query_open(dir);
        struct rill_pairs *pairs = rill_query_keys(query, &key, 1, rill_pairs_new(1));
        rill_query_close(query);

        size_t i = 0;
        for (rill_ts_t ts = 0; ts < expire_secs; ts += step) {
            assert(pairs->data[i].key == key);
            assert(pairs->data[i].val == ts + 1);
            ++i;
        }

        rill_pairs_free(pairs);
    }

    for (size_t i = 1; i <= 6; ++i) {
        rill_ts_t ts = (months_in_expire + i) * month_secs;
        acc_dump(acc, dir, ts);
        rill_rotate(dir, ts, expire_secs);
    }

    rill_acc_close(acc);

    {
        struct rill_query *query = rill_query_open(dir);
        struct rill_pairs *pairs = rill_query_keys(query, &key, 1, rill_pairs_new(1));
        rill_query_close(query);

        for (size_t i = 0; i < pairs->len; ++i) {
            assert(pairs->data[i].key == key);
            assert(pairs->data[i].val >= (5 * month_secs) + 1);
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
