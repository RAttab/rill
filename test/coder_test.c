/* coder_test.c
   Rémi Attab (remi.attab@gmail.com), 11 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "test.h"

#include "store.c"


// -----------------------------------------------------------------------------
// utils
// -----------------------------------------------------------------------------

static struct index *index_alloc(size_t rows)
{
    return calloc(1, index_cap(rows));
}


// -----------------------------------------------------------------------------
// leb128
// -----------------------------------------------------------------------------

static void check_leb128(uint64_t val)
{
    uint8_t data[10] = {0};

    {
        uint8_t *it = leb128_encode(data, val);
        size_t len = (uintptr_t) it - (uintptr_t) data;
             if (val < (1UL <<  7)) assert(len == 1);
        else if (val < (1UL << 14)) assert(len == 2);
        else if (val < (1UL << 21)) assert(len == 3);
        else if (val < (1UL << 28)) assert(len == 4);
        else if (val < (1UL << 35)) assert(len == 5);
        else if (val < (1UL << 42)) assert(len == 6);
        else if (val < (1UL << 49)) assert(len == 7);
        else if (val < (1UL << 56)) assert(len == 8);
        else if (val < (1UL << 63)) assert(len == 9);
        else                        assert(len == 10);
    }

    {
        uint8_t *it = data;
        uint64_t result = 0;
        assert(leb128_decode(&it, it + sizeof(data), &result));
        assert(val == result);
    }
}

bool test_leb128(void)
{
    check_leb128(0);
    for (size_t i = 0; i < 64; ++i) check_leb128(1UL << i);

    struct rng rng = rng_make(0);
    for (size_t i = 0; i < 64; ++i) {
        for (size_t j = 0; j < 100; ++j)
            check_leb128(rng_gen_range(&rng, 0, 1UL << i));
    }

    return true;
}


// -----------------------------------------------------------------------------
// vals
// -----------------------------------------------------------------------------

#define make_vals(...)                                          \
    ({                                                          \
        rill_val_t vals[] = { __VA_ARGS__ };                    \
        make_vals_impl(vals, sizeof(vals) / sizeof(vals[0]));   \
    })

#define make_index(...)                                 \
    ({                                                  \
        rill_val_t vals[] = { __VA_ARGS__ };            \
        size_t len = sizeof(vals) / sizeof(vals[0]);    \
        struct index *index = index_alloc(len);         \
        for (size_t i = 0; i < len; ++i)                \
            index_put(index, vals[i], 1);               \
        index;                                          \
    })

static struct vals *make_vals_impl(rill_val_t *list, size_t len)
{
    struct vals *vals = calloc(1, sizeof(struct vals) + sizeof(list[0]) * len);

    vals->len = len;
    memcpy(vals->data, list, sizeof(list[0]) * len);

    vals_compact(vals);
    return vals;
}

static void check_vals(struct rill_rows rows, struct vals *exp)
{
    struct vals *vals = vals_for_col(&rows, rill_col_b);

    assert(vals->len == exp->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(vals->data[i] == exp->data[i]);

    vals_rev_t rev = {0};
    vals_rev_make(vals, &rev);

    for (size_t i = 0; i < exp->len; ++i) {
        size_t index = vals_vtoi(&rev, exp->data[i]);
        assert(vals->data[index - 1] == exp->data[i]);
    }

    free(vals);
    free(exp);
    rill_rows_free(&rows);
    htable_reset(&rev);
}

static void check_vals_merge(struct vals *a, struct index *b, struct vals *exp)
{
    struct vals *result = vals_add_index(a, b);

    assert(result->len == exp->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(result->data[i] == exp->data[i]);

    free(result);
    free(b);
    free(exp);
}

bool test_vals(void)
{
    check_vals(make_rows(row(1, 10)), make_vals(10));

    check_vals(make_rows(row(1, 10), row(1, 10)), make_vals(10));
    check_vals(make_rows(row(1, 10), row(2, 10)), make_vals(10));

    check_vals(make_rows(row(1, 10), row(1, 20)), make_vals(10, 20));
    check_vals(make_rows(row(1, 10), row(2, 20)), make_vals(10, 20));

    check_vals(make_rows(row(2, 20), row(1, 10)), make_vals(10, 20));
    check_vals(make_rows(row(1, 20), row(1, 10)), make_vals(10, 20));

    check_vals_merge(make_vals(10), make_index(10), make_vals(10));
    check_vals_merge(make_vals(10), make_index(20), make_vals(10, 20));

    check_vals_merge(NULL, make_index(10, 20), make_vals(10, 20));
    check_vals_merge(make_vals(10, 20), make_index(20), make_vals(10, 20));
    check_vals_merge(make_vals(10, 20), make_index(20, 30), make_vals(10, 20, 30));
    check_vals_merge(make_vals(10, 20), make_index(20, 30, 40, 50, 60),
                     make_vals(10, 20, 30, 40, 50, 60));

    return true;
}


// -----------------------------------------------------------------------------
// coder
// -----------------------------------------------------------------------------

static struct index *lookup_alloc(struct vals *vals)
{
    struct index *lookup = calloc(1, index_cap(vals->len));
    lookup->len = vals->len;

    for (size_t i = 0; i < lookup->len; ++i)
        lookup->data[i].key = vals->data[i];

    return lookup;
}

static void check_coder(struct rill_rows rows)
{
    rill_rows_compact(&rows);

    struct vals *vals[2] = {
        vals_for_col(&rows, rill_col_a),
        vals_for_col(&rows, rill_col_b),
    };

    struct index *index = index_alloc(vals[rill_col_a]->len);
    struct index *lookup = lookup_alloc(vals[rill_col_b]);

    size_t cap = coder_cap(vals[rill_col_b]->len, rows.len);
    uint8_t *buffer = calloc(1, cap);

    size_t len = 0;
    {
        struct encoder coder =
            make_encoder(buffer, buffer + cap, vals[rill_col_b], index);

        for (size_t i = 0; i < rows.len; ++i)
            assert(coder_encode(&coder, &rows.data[i]));

        assert(coder_finish(&coder));
        len = coder.it - buffer;
    }

    if (false) {
        printf("input: "); rill_rows_print(&rows);

        printf("buffer: start=%p, len=%lu\n", (void *) buffer, len);
        hexdump(buffer, cap);

        printf("index: [ ");
        for (size_t i = 0; i < index->len; ++i) {
            struct index_kv *row = &index->data[i];
            printf("{%p, %p} ", (void *) row->key, (void *) row->off);
        }
        printf("]\n");

        printf("lookup: [ ");
        for (size_t i = 0; i < lookup->len; ++i) {
            struct index_kv *row = &lookup->data[i];
            printf("%p ", (void *) row->key);
        }
        printf("]\n");
    }

    {
        struct decoder coder =
            make_decoder_at(buffer, buffer + len, lookup, index, 0);

        struct rill_row row = {0};
        for (size_t i = 0; i < rows.len; ++i) {
            assert(coder_decode(&coder, &row));
            assert(rill_row_cmp(&row, &rows.data[i]) == 0);
        }

        assert(coder_decode(&coder, &row));
        assert(rill_row_nil(&row));
    }

    for (size_t i = 0; i < rows.len; ++i) {
        size_t key_idx; uint64_t off;
        assert(index_find(index, rows.data[i].a, &key_idx, &off));
        struct decoder coder = make_decoder_at(
                buffer + off, buffer + len, lookup, index, key_idx);

        struct rill_row row = {0};
        do {
            assert(coder_decode(&coder, &row));
            assert(row.a == rows.data[i].a);
        } while (row.b != rows.data[i].b);
    }

    free(buffer);
    free(lookup);
    free(index);
    for (size_t col = 0; col < rill_cols; ++col) free(vals[col]);
    rill_rows_free(&rows);
}


bool test_coder(void)
{
    check_coder(make_rows(row(1, 10)));
    check_coder(make_rows(row(1, 10), row(1, 20)));
    check_coder(make_rows(row(1, 10), row(2, 20)));
    check_coder(make_rows(row(1, 10), row(1, 20), row(2, 30)));
    check_coder(make_rows(row(1, 10), row(1, 20), row(2, 10)));

    struct rng rng = rng_make(0);
    for (size_t iterations = 0; iterations < 100; ++iterations)
        check_coder(make_rng_rows(&rng));

    return true;
}


// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    bool ret = true;

    ret = ret && test_leb128();
    ret = ret && test_vals();
    ret = ret && test_coder();

    return ret ? 0 : 1;
}
