#include "test.h"

bool test_sequence()
{
    const char* name = "test.query.sequence.rill";
    unlink(name);

    const size_t max_keys = 1000;
    const size_t max_values = 100;
    struct rill_pairs* pairs = rill_pairs_new(1024);

    for (size_t i = 1; i < max_keys + 1; ++i)
      for (size_t j = 1; j < max_values + 1; ++j)
        pairs = rill_pairs_push(pairs, i, j);

    rill_store_write(name, 666, 666, pairs);
    rill_pairs_free(pairs);

    struct rill_store* store = rill_store_open(name);

    {
        struct rill_pairs* result = rill_pairs_new(256);

        for (size_t i = 1; i < max_keys + 1; ++i) {
            result = rill_store_query_key(store, i, result);
            rill_pairs_compact(result);

            assert(result->len == max_values);
            for (size_t x = 0; x < max_values; ++x)
                assert(result->data[x].key == i &&
                       result->data[x].val == x + 1);

            rill_pairs_clear(result);
        }

        rill_pairs_free(result);
    }

    {
        struct rill_pairs* result = rill_pairs_new(256);

        for (size_t i = 1; i < max_values + 1; ++i) {
            result = rill_store_query_value(store, i, result);
            rill_pairs_compact(result);

            assert(result->len == max_keys);
            for (size_t x = 0; x < max_keys; ++x)
                assert(result->data[x].key == i &&
                       result->data[x].val == x + 1);

            rill_pairs_clear(result);
        }

        rill_pairs_free(result);
    }

    rill_store_close(store);

    unlink(name);

    unlink(name);
    return true;
}

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    return test_sequence() ? 0 : 1;
}
