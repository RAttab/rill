/* index.c
   Rémi Attab (remi.attab@gmail.com), 24 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/


// -----------------------------------------------------------------------------
// config
// -----------------------------------------------------------------------------

struct rill_packed index_kv
{
    rill_key_t key;
    uint64_t off;
};

struct rill_packed index
{
    uint64_t len;
    uint64_t slope;
    struct index_kv data[];
};

// -----------------------------------------------------------------------------
// indexer
// -----------------------------------------------------------------------------

struct indexer
{
    size_t len, cap;
    struct index_kv kvs[];
};

static size_t indexer_cap(size_t pairs)
{
    return sizeof(struct index) + pairs * sizeof(struct index_kv);
}


static struct indexer *indexer_alloc(size_t cap)
{
    assert(cap);

    struct indexer *indexer = calloc(1, sizeof(*indexer) + cap * sizeof(indexer->kvs[0]));
    if (!indexer) {
        rill_fail("unable to allocate indexer: %lu", cap);
        return NULL;
    }

    indexer->cap = cap;
    return indexer;
}

static void indexer_free(struct indexer *indexer)
{
    free(indexer);
}

static void indexer_put(struct indexer *indexer, rill_key_t key, uint64_t off)
{
    indexer->kvs[indexer->len] = (struct index_kv) { .key = key, .off = off };
    indexer->len++;

    assert(indexer->len <= indexer->cap);
}

static size_t indexer_write(struct indexer *indexer, struct index *index)
{
    index->len = indexer->len;

    uint64_t min = indexer->kvs[0].key;
    uint64_t max = indexer->kvs[indexer->len - 1].key;
    index->slope = (max - min) / indexer->len;
    if (!index->slope) index->slope = 1;

    size_t len = indexer->len * sizeof(indexer->kvs[0]);
    memcpy(index->data, indexer->kvs, len);

    return sizeof(*index) + len;
}

// One pass interpolation search. We assume that the keys are hashes and
// therefore uniformly distributed. So a single jump should get us close enough
// to our goal.
static bool index_find(
        struct index *index, rill_key_t key, size_t *key_idx, uint64_t *off)
{
    size_t i = (key - index->data[0].key) / index->slope;
    if (i >= index->len) i = index->len - 1;

    while (i && key < index->data[i].key) i--;

    for (; i < index->len; ++i) {
        struct index_kv *kv = &index->data[i];
        if (key < kv->key) break;
        if (key != kv->key) continue;

        *key_idx = i;
        *off = kv->off;
        return true;
    }

    return false;
}

static rill_key_t index_get(struct index *index, size_t i)
{
    return i < index->len ? index->data[i].key : 0;
}
