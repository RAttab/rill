/* coder_test.c
   Rémi Attab (remi.attab@gmail.com), 11 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "test.h"

#include "coder.c"


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

static struct vals *make_vals_impl(rill_val_t *list, size_t len)
{
    struct vals *vals = calloc(1, sizeof(struct vals) + sizeof(list[0]) * len);

    vals->len = len;
    for (size_t i = 0; i < len; ++i) vals->data[i] = list[i];

    vals_compact(vals);
    return vals;
}

static void check_vals(struct rill_pairs pairs, struct vals *exp)
{
    struct vals *vals = vals_from_pairs(&pairs);

    assert(vals->len == exp->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(vals->data[i] == exp->data[i]);

    vals_rev_t rev = {0};
    vals_rev_make(vals, &rev);

    for (size_t i = 0; i < exp->len; ++i) {
        size_t index = vals_vtoi(&rev, exp->data[i]);
        assert(vals_itov(vals, index) == exp->data[i]);
    }

    free(vals);
    free(exp);
}

static void check_vals_merge(struct vals *a, struct vals *b, struct vals *exp)
{
    struct vals *result = vals_merge(a, b);

    assert(result->len == exp->len);
    for (size_t i = 0; i < exp->len; ++i)
        assert(result->data[i] == exp->data[i]);

    free(a);
    free(b);
}

bool test_vals(void)
{
    check_vals(make_pair(kv(1, 10)), make_vals(10));

    check_vals(make_pair(kv(1, 10), kv(1, 10)), make_vals(10));
    check_vals(make_pair(kv(1, 10), kv(2, 10)), make_vals(10));

    check_vals(make_pair(kv(1, 10), kv(1, 20)), make_vals(10, 20));
    check_vals(make_pair(kv(1, 10), kv(2, 20)), make_vals(10, 20));

    check_vals(make_pair(kv(2, 20), kv(1, 10)), make_vals(10, 20));
    check_vals(make_pair(kv(1, 20), kv(1, 10)), make_vals(10, 20));

    check_vals_merge(make_vals(10), make_vals(10), make_vals(10));
    check_vals_merge(make_vals(10), make_vals(20), make_vals(10, 20));

    check_vals_merge(make_vals(10, 20), make_vals(20), make_vals(10, 20));
    check_vals_merge(make_vals(10, 20), make_vals(20, 30), make_vals(10, 20, 30));

    return true;
}


// -----------------------------------------------------------------------------
// coder
// -----------------------------------------------------------------------------

void check_coder(struct rill_pairs pairs)
{
    rill_pairs_compact(&pairs);

    size_t cap = (pairs.len + 1) * (sizeof(pairs.data[0]) + 3);
    uint8_t *buffer = calloc(1, cap);
    struct vals *vals = vals_from_pairs(&pairs);

    size_t len = 0;
    {
        struct coder coder = make_encoder(vals, buffer, buffer + cap);
        for (size_t i = 0; i < pairs.len; ++i)
            assert(coder_encode(&coder, &pairs.data[i]));
        assert(coder_finish(&coder));

        len = coder.it - buffer;
        assert(len <= cap);
    }

    /* printf("buffer: start=%p, len=%lu\n", (void *) buffer, len); */
    /* for (size_t i = 0; i < cap;) { */
    /*     printf("%6p: ", (void *) i); */
    /*     for (size_t j = 0; j < 16 && i < cap; ++i, ++j) { */
    /*         if (j % 2 == 0) printf(" "); */
    /*         printf("%02x", buffer[i]); */
    /*     } */
    /*     printf("\n"); */
    /* } */

    {
        struct coder coder = make_decoder(vals, buffer, buffer + len);

        struct rill_kv kv = {0};
        for (size_t i = 0; i < pairs.len; ++i) {
            assert(coder_decode(&coder, &kv));
            assert(rill_kv_cmp(&kv, &pairs.data[i]) == 0);
        }

        assert(coder_decode(&coder, &kv));
        assert(rill_kv_nil(&kv));
    }

    free(vals);
}


bool test_coder(void)
{
    check_coder(make_pair(kv(1, 10)));
    check_coder(make_pair(kv(1, 10), kv(1, 20)));
    check_coder(make_pair(kv(1, 10), kv(2, 20)));
    check_coder(make_pair(kv(1, 10), kv(1, 20), kv(2, 30)));
    check_coder(make_pair(kv(1, 10), kv(1, 20), kv(2, 10)));

    struct rng rng = rng_make(0);
    for (size_t iterations = 0; iterations < 100; ++iterations) {

        struct rill_pairs pairs = {0};
        for (size_t i = 0; i < 1000; ++i) {
            uint64_t key = rng_gen_range(&rng, 1, 500);
            uint64_t val = rng_gen_range(&rng, 1, 100);
            rill_pairs_push(&pairs, key, val);
        }

        check_coder(pairs);
        rill_pairs_free(&pairs);
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

    ret = ret && test_leb128();
    ret = ret && test_vals();
    ret = ret && test_coder();

    return ret ? 0 : 1;
}
