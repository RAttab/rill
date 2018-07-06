#include "test.h"

#include "index.c"

// -----------------------------------------------------------------------------
// utils
// -----------------------------------------------------------------------------

static struct index *index_alloc(size_t rows)
{
    struct index *index = calloc(1, index_cap(rows));

    assert(index);
    assert(index->len == 0);

    return index;
}


// -----------------------------------------------------------------------------
// test_index_build
// -----------------------------------------------------------------------------

static bool test_index_build(void)
{
    enum { rows = 10 };

    struct index *index = index_alloc(rows);

    rill_val_t data[rows] = {0};
    for (size_t i = 1; i < rows; ++i) data[i] = data[i - 1] += 2;

    for (size_t i = 0; i < rows; i++)
        index_put(index, data[i], i);

    assert(index->len == rows);
    for (size_t i = 0; i < index->len; i++)
        assert(index_get(index, i) == data[i]);

    assert(index_get(index, index->len) == 0);

    free(index);
    return true;
}


// -----------------------------------------------------------------------------
// test_index_lookup
// -----------------------------------------------------------------------------

static struct index *make_index(rill_val_t *data, size_t n)
{
    struct index *index = index_alloc(n);
    for (size_t i = 0; i < n; i++)
        index_put(index, data[i], i);

    return index;
}

#define index_from_keys(...)                                \
    ({                                                      \
        rill_val_t keys[] = { __VA_ARGS__ };                \
        make_index(keys, sizeof(keys) / sizeof(keys[0]));   \
    })

#define assert_found(index, ...) {                                  \
    rill_val_t keys[] = { __VA_ARGS__ };                            \
    size_t key_idx;                                                 \
    uint64_t val;                                                   \
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {   \
        assert(index_find(index, keys[i], &key_idx, &val));         \
        assert(key_idx == i);                                       \
        assert(val == i);                                           \
    }                                                               \
}

#define assert_not_found(index, ...) {                          \
    rill_val_t keys[] = { __VA_ARGS__ };                        \
    size_t key_idx;                                             \
    uint64_t val;                                               \
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) \
        assert(!index_find(index, keys[i], &key_idx, &val));    \
}

bool test_index_lookup(void)
{
    struct index *index;

    index = index_from_keys(0, 3, 6, 9, 12, 15, 18, 21, 24, 27);
    assert_found(index, 0, 3, 6, 9, 12, 15, 18, 21, 24, 27);
    assert_not_found(index, 1, 5, 8, 10, 14, 17, 20, 22, 25, 100);
    free(index);

    index = index_from_keys(0, 3, 4, 5, 6, 7, 8, 9, 12, 27);
    assert_found(index, 0, 3, 4, 5, 6, 7, 8, 9, 12, 27);
    free(index);

    index = index_from_keys(0, 3, 12, 13, 14, 15, 16, 17, 18, 27);
    assert_found(index, 0, 3, 12, 13, 14, 15, 16, 17, 18, 27);
    free(index);

    return true;
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    bool ret = true;

    ret = ret && test_index_build();
    ret = ret && test_index_lookup();

    return ret ? 0 : 1;
}
