/* coder_test.c
   Rémi Attab (remi.attab@gmail.com), 11 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "test.h"


// -----------------------------------------------------------------------------
// utils
// -----------------------------------------------------------------------------

static struct rill_store *make_store(const char *name, struct rill_rows *rows)
{
    unlink(name);
    assert(rill_store_write(name, 0, 0, rows));

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

static struct rill_rows* duplicate_rows(struct rill_rows* rows)
{
    struct rill_rows* copy = rill_rows_new(rows->len);
    for (size_t i = 0; i < rows->len; ++i)
        rill_rows_push(copy, rows->data[i].key, rows->data[i].val);
    return copy;
}

static void check_query_key(struct rill_rows *rows)
{
    struct rill_rows *expected = duplicate_rows(rows);
    struct rill_store *store = make_store("test.store.query_key", rows);
    struct rill_rows *result = rill_rows_new(128);

    rill_rows_compact(rows);
    rill_rows_compact(expected);

    for (size_t i = 0; i < expected->len;) {
        rill_rows_clear(result);
        result = rill_store_query_key(store, expected->data[i].key, result);

        assert(expected->len - i >= result->len);
        for (size_t j = 0; j < result->len; ++j, ++i)
            assert(!rill_row_cmp(&expected->data[i], &result->data[j]));
    }

    free(result);
    rill_store_close(store);
    rill_rows_free(rows);
    rill_rows_free(expected);
}

bool test_query_key(void)
{
    check_query_key(make_pair(row(1, 10)));
    check_query_key(make_pair(row(1, 10), row(2, 20)));
    check_query_key(make_pair(row(1, 10), row(1, 20), row(2, 20)));
    check_query_key(make_pair(row(1, 10), row(1, 20), row(1, 20), row(1, 30)));

    struct rng rng = rng_make(0);
    for (size_t iterations = 0; iterations < 10; ++iterations)
        check_query_key(make_rng_rows(&rng));

    return true;
}


// -----------------------------------------------------------------------------
// scan_keys
// -----------------------------------------------------------------------------

static void check_scan_keys(
        struct rill_store *store, struct rill_rows *rows, struct list *keys)
{
    struct rill_rows *result = rill_rows_new(128);
    rill_rows_compact(rows);

    for (size_t i = 0; i < keys->len; ++i)
        result = rill_store_query_key(store, keys->data[i], result);

    struct rill_rows *exp = rill_rows_new(128);
    for (size_t i = 0; i < rows->len; ++i) {
        for (size_t j = 0; j < keys->len; ++j) {
            struct rill_row *row = &rows->data[i];
            if (row->key == keys->data[j]) exp = rill_rows_push(exp, row->key, row->val);
        }
    }

    assert(exp->len == result->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(!rill_row_cmp(&exp->data[i], &result->data[i]));

    free(exp);
    free(result);
    free(keys);
}

bool test_scan_keys(void)
{
    static const char *name = "test.store.scan_keys";

    {
        struct rill_rows *rows = make_pair(row(2, 10));
        struct rill_rows *copy = duplicate_rows(rows);
        struct rill_store *store = make_store(name, rows);

        check_scan_keys(store, copy, make_list(1));
        check_scan_keys(store, copy, make_list(2));
        check_scan_keys(store, copy, make_list(3));
        check_scan_keys(store, copy, make_list(1, 2));
        check_scan_keys(store, copy, make_list(2, 3));
        check_scan_keys(store, copy, make_list(1, 3));

        rill_store_close(store);
        free(copy);
        free(rows);
    }


    {
        struct rill_rows *rows = make_pair(row(2, 10), row(3, 10), row(3, 20), row(4, 30));
        struct rill_rows *copy = duplicate_rows(rows);
        struct rill_store *store = make_store(name, rows);

        check_scan_keys(store, copy, make_list(1));
        check_scan_keys(store, copy, make_list(3));
        check_scan_keys(store, copy, make_list(5));
        check_scan_keys(store, copy, make_list(1, 3));
        check_scan_keys(store, copy, make_list(3, 5));
        check_scan_keys(store, copy, make_list(2, 3));
        check_scan_keys(store, copy, make_list(2, 3, 4));

        rill_store_close(store);
        free(copy);
        free(rows);
    }

    {
        struct rng rng = rng_make(0);
        struct rill_rows *rows = make_rng_rows(&rng);
        struct rill_rows *copy = duplicate_rows(rows);
        struct rill_store *store = make_store(name, rows);

        for (size_t iterations = 0; iterations < 10; ++iterations)
            check_scan_keys(store, copy, make_rng_list(&rng, rng_range_key));

        rill_store_close(store);
        free(rows);
        free(copy);
    }

    return true;
}


// -----------------------------------------------------------------------------
// scan_vals
// -----------------------------------------------------------------------------

static void check_scan_vals(
        struct rill_store *store, struct rill_rows *rows, struct list *vals)
{
    struct rill_rows *result = rill_rows_new(128);
    rill_rows_compact(rows);

    for (size_t i = 0; i < vals->len; ++i)
        result = rill_store_query_value(store, vals->data[i], result);

    struct rill_rows *exp = rill_rows_new(128);
    for (size_t i = 0; i < rows->len; ++i) {
        for (size_t j = 0; j < vals->len; ++j) {
            struct rill_row *row = &rows->data[i];
            if (row->val == vals->data[j])
                exp = rill_rows_push(exp, row->val, row->key);
        }
    }

    rill_rows_compact(exp);

    assert(exp->len == result->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(!rill_row_cmp(&exp->data[i], &result->data[i]));

    free(exp);
    free(result);
    free(vals);
}

bool test_scan_vals(void)
{
    static const char *name = "test.store.scan_vals";

    {
        struct rill_rows *rows = make_pair(row(2, 20));
        struct rill_rows *copy = duplicate_rows(rows);
        struct rill_store *store = make_store(name, rows);

        check_scan_vals(store, copy, make_list(10));
        check_scan_vals(store, copy, make_list(20));
        check_scan_vals(store, copy, make_list(30));
        check_scan_vals(store, copy, make_list(10, 20));
        check_scan_vals(store, copy, make_list(20, 30));
        check_scan_vals(store, copy, make_list(10, 30));

        rill_store_close(store);
        free(rows);
        free(copy);
    }

    {
        struct rill_rows *rows = make_pair(row(2, 20), row(3, 20), row(3, 30), row(4, 40));
        struct rill_rows *copy = duplicate_rows(rows);
        struct rill_store *store = make_store(name, rows);

        check_scan_vals(store, copy, make_list(10));
        check_scan_vals(store, copy, make_list(20));
        check_scan_vals(store, copy, make_list(30));
        check_scan_vals(store, copy, make_list(50));
        check_scan_vals(store, copy, make_list(10, 20));
        check_scan_vals(store, copy, make_list(20, 40));
        check_scan_vals(store, copy, make_list(20, 50));
        check_scan_vals(store, copy, make_list(20, 30, 40));

        rill_store_close(store);
        free(rows);
        free(copy);
    }

    {
        struct rng rng = rng_make(0);
        struct rill_rows *rows = make_rng_rows(&rng);
        struct rill_rows *copy = duplicate_rows(rows);
        struct rill_store *store = make_store(name, rows);

        for (size_t iterations = 0; iterations < 10; ++iterations)
            check_scan_vals(store, copy, make_rng_list(&rng, rng_range_val));

        rill_store_close(store);
        free(rows);
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
