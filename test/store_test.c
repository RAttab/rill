/* coder_test.c
   Rémi Attab (remi.attab@gmail.com), 11 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "test.h"


// -----------------------------------------------------------------------------
// utils
// -----------------------------------------------------------------------------

static struct rill_store *make_store(const char *name, struct rill_pairs *pairs)
{
    unlink(name);
    assert(rill_store_write(name, 0, 0, pairs));

    struct rill_store *store = rill_store_open(name);
    assert(store);

    return store;
}

struct list { size_t len; uint64_t data[]; };

#define make_list(...)                                          \
    ({                                                          \
        uint64_t list[] = { __VA_ARGS__ };                      \
        make_list_impl(list, sizeof(list) / sizeof(list[0]));   \
    })

static struct list *make_list_impl(uint64_t *data, size_t len)
{
    struct list *list = calloc(1, sizeof(struct list) + sizeof(data[0]) * len);

    list->len = len;
    memcpy(list->data, data, sizeof(data[0]) * len);

    return list;
}

static struct list *make_rng_list(struct rng *rng, uint64_t max)
{
    struct list *list = calloc(1, sizeof(struct list) + sizeof(list->data[0]) * max);

    for (uint64_t val = 0; val < max; ++val) {
        if (rng_gen(rng) > rng_max() / 2) continue;

        list->data[list->len] = val;
        list->len++;
    }

    return list;
}


// -----------------------------------------------------------------------------
// query_key
// -----------------------------------------------------------------------------

static struct rill_pairs* duplicate_pairs(struct rill_pairs* pairs)
{
    struct rill_pairs* copy = rill_pairs_new(pairs->len);
    for (size_t i = 0; i < pairs->len; ++i)
        rill_pairs_push(copy, pairs->data[i].key, pairs->data[i].val);
    return copy;
}

static void check_query_key(struct rill_pairs *pairs)
{
    struct rill_pairs *expected = duplicate_pairs(pairs);
    struct rill_store *store = make_store("test.store.query_key", pairs);
    struct rill_pairs *result = rill_pairs_new(128);

    rill_pairs_compact(pairs);
    rill_pairs_compact(expected);

    for (size_t i = 0; i < expected->len;) {
        rill_pairs_clear(result);
        result = rill_store_query_key(store, expected->data[i].key, result);

        assert(expected->len - i >= result->len);
        for (size_t j = 0; j < result->len; ++j, ++i)
            assert(!rill_kv_cmp(&expected->data[i], &result->data[j]));
    }

    free(result);
    rill_store_close(store);
    rill_pairs_free(pairs);
    rill_pairs_free(expected);
}

bool test_query_key(void)
{
    check_query_key(make_pair(kv(1, 10)));
    check_query_key(make_pair(kv(1, 10), kv(2, 20)));
    check_query_key(make_pair(kv(1, 10), kv(1, 20), kv(2, 20)));
    check_query_key(make_pair(kv(1, 10), kv(1, 20), kv(1, 20), kv(1, 30)));

    struct rng rng = rng_make(0);
    for (size_t iterations = 0; iterations < 10; ++iterations)
        check_query_key(make_rng_pairs(&rng));

    return true;
}


// -----------------------------------------------------------------------------
// scan_keys
// -----------------------------------------------------------------------------

static void check_scan_keys(
        struct rill_store *store, struct rill_pairs *pairs, struct list *keys)
{
    struct rill_pairs *result = rill_pairs_new(128);
    rill_pairs_compact(pairs);

    for (size_t i = 0; i < keys->len; ++i)
        result = rill_store_query_key(store, keys->data[i], result);

    struct rill_pairs *exp = rill_pairs_new(128);
    for (size_t i = 0; i < pairs->len; ++i) {
        for (size_t j = 0; j < keys->len; ++j) {
            struct rill_kv *kv = &pairs->data[i];
            if (kv->key == keys->data[j]) exp = rill_pairs_push(exp, kv->key, kv->val);
        }
    }

    assert(exp->len == result->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(!rill_kv_cmp(&exp->data[i], &result->data[i]));

    free(exp);
    free(result);
    free(keys);
}

bool test_scan_keys(void)
{
    static const char *name = "test.store.scan_keys";

    {
        struct rill_pairs *pairs = make_pair(kv(2, 10));
        struct rill_pairs *copy = duplicate_pairs(pairs);
        struct rill_store *store = make_store(name, pairs);

        check_scan_keys(store, copy, make_list(1));
        check_scan_keys(store, copy, make_list(2));
        check_scan_keys(store, copy, make_list(3));
        check_scan_keys(store, copy, make_list(1, 2));
        check_scan_keys(store, copy, make_list(2, 3));
        check_scan_keys(store, copy, make_list(1, 3));

        rill_store_close(store);
        free(copy);
        free(pairs);
    }


    {
        struct rill_pairs *pairs = make_pair(kv(2, 10), kv(3, 10), kv(3, 20), kv(4, 30));
        struct rill_pairs *copy = duplicate_pairs(pairs);
        struct rill_store *store = make_store(name, pairs);

        check_scan_keys(store, copy, make_list(1));
        check_scan_keys(store, copy, make_list(3));
        check_scan_keys(store, copy, make_list(5));
        check_scan_keys(store, copy, make_list(1, 3));
        check_scan_keys(store, copy, make_list(3, 5));
        check_scan_keys(store, copy, make_list(2, 3));
        check_scan_keys(store, copy, make_list(2, 3, 4));

        rill_store_close(store);
        free(copy);
        free(pairs);
    }

    {
        struct rng rng = rng_make(0);
        struct rill_pairs *pairs = make_rng_pairs(&rng);
        struct rill_pairs *copy = duplicate_pairs(pairs);
        struct rill_store *store = make_store(name, pairs);

        for (size_t iterations = 0; iterations < 10; ++iterations)
            check_scan_keys(store, copy, make_rng_list(&rng, rng_range_key));

        rill_store_close(store);
        free(pairs);
        free(copy);
    }

    return true;
}


// -----------------------------------------------------------------------------
// scan_vals
// -----------------------------------------------------------------------------

static void check_scan_vals(
        struct rill_store *store, struct rill_pairs *pairs, struct list *vals)
{
    struct rill_pairs *result = rill_pairs_new(128);
    rill_pairs_compact(pairs);

    for (size_t i = 0; i < vals->len; ++i)
        result = rill_store_query_value(store, vals->data[i], result);

    struct rill_pairs *exp = rill_pairs_new(128);
    for (size_t i = 0; i < pairs->len; ++i) {
        for (size_t j = 0; j < vals->len; ++j) {
            struct rill_kv *kv = &pairs->data[i];
            if (kv->val == vals->data[j])
                exp = rill_pairs_push(exp, kv->val, kv->key);
        }
    }

    rill_pairs_compact(exp);

    assert(exp->len == result->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(!rill_kv_cmp(&exp->data[i], &result->data[i]));

    free(exp);
    free(result);
    free(vals);
}

bool test_scan_vals(void)
{
    static const char *name = "test.store.scan_vals";

    {
        struct rill_pairs *pairs = make_pair(kv(2, 20));
        struct rill_pairs *copy = duplicate_pairs(pairs);
        struct rill_store *store = make_store(name, pairs);

        check_scan_vals(store, copy, make_list(10));
        check_scan_vals(store, copy, make_list(20));
        check_scan_vals(store, copy, make_list(30));
        check_scan_vals(store, copy, make_list(10, 20));
        check_scan_vals(store, copy, make_list(20, 30));
        check_scan_vals(store, copy, make_list(10, 30));

        rill_store_close(store);
        free(pairs);
        free(copy);
    }

    {
        struct rill_pairs *pairs = make_pair(kv(2, 20), kv(3, 20), kv(3, 30), kv(4, 40));
        struct rill_pairs *copy = duplicate_pairs(pairs);
        struct rill_store *store = make_store(name, pairs);

        check_scan_vals(store, copy, make_list(10));
        check_scan_vals(store, copy, make_list(20));
        check_scan_vals(store, copy, make_list(30));
        check_scan_vals(store, copy, make_list(50));
        check_scan_vals(store, copy, make_list(10, 20));
        check_scan_vals(store, copy, make_list(20, 40));
        check_scan_vals(store, copy, make_list(20, 50));
        check_scan_vals(store, copy, make_list(20, 30, 40));

        rill_store_close(store);
        free(pairs);
        free(copy);
    }

    {
        struct rng rng = rng_make(0);
        struct rill_pairs *pairs = make_rng_pairs(&rng);
        struct rill_pairs *copy = duplicate_pairs(pairs);
        struct rill_store *store = make_store(name, pairs);

        for (size_t iterations = 0; iterations < 10; ++iterations)
            check_scan_vals(store, copy, make_rng_list(&rng, rng_range_val));

        rill_store_close(store);
        free(pairs);
        free(copy);
    }

    return true;
}


// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    bool ret = true;

    ret = ret && test_query_key();
    ret = ret && test_scan_keys();
    ret = ret && test_scan_vals();

    return ret ? 0 : 1;
}
