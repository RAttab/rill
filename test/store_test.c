/* coder_test.c
   Rémi Attab (remi.attab@gmail.com), 11 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "test.h"
#include "store.c"


// -----------------------------------------------------------------------------
// utils
// -----------------------------------------------------------------------------

static struct rill_store *make_store(const char *name, struct rill_rows *rows)
{
    unlink(name);
    assert(rill_store_write(name, 0, 0, rows));

    struct rill_store *store = rill_store_open(name);
    if (!store) rill_abort();

    return store;
}


// -----------------------------------------------------------------------------
// query
// -----------------------------------------------------------------------------

static void check_query(struct rill_rows rows)
{
    struct rill_rows expected = {0};
    rill_rows_copy(&rows, &expected);
    rill_rows_compact(&expected);

    struct rill_store *store = make_store("test.store.query", &rows);
    struct rill_rows result = {0};

    for (size_t col = 0; col < rill_cols; ++col) {
        for (size_t i = 0; i < expected.len;) {
            rill_rows_clear(&result);
            assert(rill_store_query(store, col, expected.data[i].a, &result));

            assert(expected.len - i >= result.len);
            for (size_t j = 0; j < result.len; ++j, ++i)
                assert(!rill_row_cmp(&expected.data[i], &result.data[j]));
        }

        rill_rows_invert(&expected); // setup for next iteration.
    }

    rill_store_close(store);
    rill_rows_free(&rows);
    rill_rows_free(&expected);
    rill_rows_free(&result);
}

bool test_query(void)
{
    check_query(make_rows(row(1, 10)));
    check_query(make_rows(row(1, 10), row(2, 20)));
    check_query(make_rows(row(1, 10), row(1, 20), row(2, 20)));
    check_query(make_rows(row(1, 10), row(1, 20), row(1, 20), row(1, 30)));

    struct rng rng = rng_make(0);
    for (size_t iterations = 0; iterations < 10; ++iterations)
        check_query(make_rng_rows(&rng));

    return true;
}


// -----------------------------------------------------------------------------
// vals
// -----------------------------------------------------------------------------

static void check_vals(struct rill_rows rows)
{
    struct vals *exp[2] = {
        vals_for_col(&rows, rill_col_a),
        vals_for_col(&rows, rill_col_b),
    };

    struct rill_store *store = make_store("test.store.vals", &rows);

    for (size_t col = 0; col < rill_cols; ++col) {
        size_t len = rill_store_vals_count(store, col);
        rill_val_t *vals = calloc(len, sizeof(*vals));

        assert(rill_store_vals(store, col, vals, len) == len);

        for (size_t i = 0; i < len; ++i)
            assert(vals[i] == exp[col]->data[i]);

        free(exp[col]);
        free(vals);
    }
}

bool test_vals(void)
{
    check_vals(make_rows(row(1, 10)));
    check_vals(make_rows(row(1, 10), row(1, 20)));
    check_vals(make_rows(row(1, 10), row(2, 10)));
    check_vals(make_rows(row(1, 10), row(1, 20), row(2, 10), row(2, 20)));
    check_vals(make_rows(row(1, 10), row(1, 20), row(2, 20), row(3, 30)));

    struct rng rng = rng_make(0);
    for (size_t iterations = 0; iterations < 10; ++iterations)
        check_vals(make_rng_rows(&rng));

    return true;
}


// -----------------------------------------------------------------------------
// it
// -----------------------------------------------------------------------------

static void check_it(struct rill_rows rows)
{
    struct rill_rows expected = {0};
    rill_rows_copy(&rows, &expected);
    rill_rows_compact(&expected);

    struct rill_store *store = make_store("test.store.query", &rows);

    for (size_t col = 0; col < rill_cols; ++col) {
        struct rill_store_it *it = rill_store_begin(store, col);

        struct rill_row row = {0};
        for (size_t i = 0; i < expected.len; ++i) {
            assert(rill_store_it_next(it, &row));
            assert(!rill_row_cmp(&expected.data[i], &row));
        }

        assert(rill_store_it_next(it, &row));
        assert(rill_row_nil(&row));

        rill_store_it_free(it);

        rill_rows_invert(&expected); // setup for next iteration.
    }

    rill_store_close(store);
    rill_rows_free(&rows);
    rill_rows_free(&expected);
}

bool test_it(void)
{
    check_it(make_rows(row(1, 10)));
    check_it(make_rows(row(1, 10), row(2, 20)));
    check_it(make_rows(row(1, 10), row(1, 20), row(2, 20)));
    check_it(make_rows(row(1, 10), row(1, 20), row(1, 20), row(1, 30)));

    struct rng rng = rng_make(0);
    for (size_t iterations = 0; iterations < 10; ++iterations)
        check_it(make_rng_rows(&rng));

    return true;
}


// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    bool ret = true;

    ret = ret && test_query();
    ret = ret && test_vals();
    ret = ret && test_it();

    return ret ? 0 : 1;
}
