/* index.c
   Rémi Attab (remi.attab@gmail.com), 24 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/


// -----------------------------------------------------------------------------
// config
// -----------------------------------------------------------------------------

struct rill_packed index_kv
{
    uint64_t key;
    uint64_t off;
};

struct rill_packed index
{
    uint64_t len;
    uint64_t __unused; // kept for backwards compatibility
    struct index_kv data[];
};

static size_t index_cap(size_t len)
{
    return sizeof(struct index) + len * sizeof(struct index_kv);
}

static void index_put(struct index *index, rill_val_t key, uint64_t off)
{
    index->data[index->len] = (struct index_kv) { .key = key, .off = off };
    index->len++;
}

// RIP fancy pants interpolation search :(
static bool index_find(
        struct index *index, rill_val_t key, size_t *key_idx, uint64_t *off)
{
    size_t idx = 0;
    size_t len = index->len;
    struct index_kv *low = index->data;

    while (len > 1) {
        size_t mid = len / 2;
        if (key < low[mid].key) len = mid;
        else { low += mid; len -= mid; idx += mid;}
    }

    struct index_kv *row = &index->data[idx];
    if (row->key != key) return false;
    *key_idx = idx;
    *off = row->off;
    return true;
}

static rill_val_t index_get(struct index *index, size_t i)
{
    return i < index->len ? index->data[i].key : 0;
}
