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
    uint64_t __unused; // kept for backwards compatibility
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

static size_t indexer_write(
    struct indexer *indexer, struct index *index,
    size_t cap)
{
    index->len = indexer->len;
    size_t len = indexer->len * sizeof(indexer->kvs[0]);

    assert(len <= cap);

    memcpy(index->data, indexer->kvs, len);
    return sizeof(*index) + len;
}

// RIP fancy pants interpolation search :(
static bool index_find(
        struct index *index, rill_key_t key, size_t *key_idx, uint64_t *off)
{
    size_t idx = 0;
    size_t len = index->len;
    struct index_kv *low = index->data;

    while (len > 1) {
        size_t mid = len / 2;
        if (key < low[mid].key) len = mid;
        else { low += mid; len -= mid; idx += mid;}
    }

    struct index_kv *kv = &index->data[idx];
    if (kv->key != key) return false;
    *key_idx = idx;
    *off = kv->off;
    return true;
}

static rill_key_t index_get(struct index *index, size_t i)
{
    return i < index->len ? index->data[i].key : 0;
}
