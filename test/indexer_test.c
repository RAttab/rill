#include "test.h"

#include "index.c"

enum {
    CAP = 10
};

bool test_indexer_build(void)
{
    struct indexer *indexer = indexer_alloc(CAP);
    assert(indexer);
    assert(indexer->len == 0);
    assert(indexer->cap == CAP);

    rill_key_t data[CAP] = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18};
    for (size_t i = 0; i < CAP; i++)
        indexer_put(indexer, data[i], i);
    assert(indexer->len == indexer->cap);

    const size_t size = indexer_cap(CAP);
    struct index *index = calloc(1, size);
    assert(index);

    size_t n_written = indexer_write(indexer, index);
    assert(n_written == size);
    assert(index->slope == (data[CAP - 1] - data[0]) / CAP);

    indexer_free(indexer);

    for (size_t i = 0; i < index->len; i++)
        assert(index_get(index, i) == data[i]);
    assert(index_get(index, index->len) == 0);

    free(index);
    
    return true;
}

static struct index *make_index(rill_key_t *data, size_t n) 
{
    struct indexer *indexer = indexer_alloc(n);
    for (size_t i = 0; i < n; i++)
        indexer_put(indexer, data[i], i);

    struct index *index = calloc(1, indexer_cap(n));
    indexer_write(indexer, index);
    indexer_free(indexer);

    return index;
}

#define index_from_keys(...)                                \
    ({                                                      \
        rill_key_t keys[] = { __VA_ARGS__ };                \
        make_index(keys, sizeof(keys) / sizeof(keys[0]));   \
    })

#define assert_found(index, ...) {                                  \
    rill_key_t keys[] = { __VA_ARGS__ };                            \
    size_t key_idx;                                                 \
    uint64_t val;                                                   \
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {   \
        assert(index_find(index, keys[i], &key_idx, &val));         \
        assert(key_idx == i);                                       \
        assert(val == i);                                           \
    }                                                               \
}

#define assert_not_found(index, ...) {                          \
    rill_key_t keys[] = { __VA_ARGS__ };                        \
    size_t key_idx;                                             \
    uint64_t val;                                               \
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) \
        assert(!index_find(index, keys[i], &key_idx, &val));    \
}

bool test_indexer_lookup(void)
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

    ret = ret && test_indexer_build();
    ret = ret && test_indexer_lookup();

    return ret ? 0 : 1;
}
