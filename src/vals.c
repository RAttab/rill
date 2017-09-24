/* vals.c
   Rémi Attab (remi.attab@gmail.com), 01 Oct 2017
   FreeBSD-style copyright and disclaimer apply
*/

// -----------------------------------------------------------------------------
// vals
// -----------------------------------------------------------------------------

struct rill_packed vals
{
    uint64_t len;
    uint64_t data[];
};

typedef struct htable vals_rev_t;

static size_t vals_cap(struct vals *vals)
{
    return sizeof(*vals) + vals->len * sizeof(vals->data[0]);
}

static rill_val_t vals_itov(struct vals *vals, size_t index)
{
    assert(index <= vals->len);
    return vals->data[index - 1];
}

static size_t vals_vtoi(vals_rev_t *rev, rill_val_t val)
{
    if (!val) return 0; // \todo giant hack for coder_finish

    struct htable_ret ret = htable_get(rev, val);
    assert(ret.ok);
    return ret.value;
}

static void vals_rev_make(struct vals *vals, vals_rev_t *rev)
{
    htable_reset(rev);
    htable_reserve(rev, vals->len);

    for (size_t index = 1; index <= vals->len; ++index) {
        struct htable_ret ret = htable_put(rev, vals->data[index-1], index);
        assert(ret.ok);
    }
}

static int val_cmp(const void *l, const void *r)
{
    rill_val_t lhs = *((rill_val_t *) l);
    rill_val_t rhs = *((rill_val_t *) r);

    if (lhs < rhs) return -1;
    if (lhs > rhs) return 1;
    return 0;
}

static void vals_compact(struct vals *vals)
{
    assert(vals->len);
    qsort(vals->data, vals->len, sizeof(vals->data[0]), &val_cmp);

    size_t j = 0;
    for (size_t i = 1; i < vals->len; ++i) {
        if (vals->data[j] == vals->data[i]) continue;
        vals->data[++j] = vals->data[i];
    }

    assert(j + 1 <= vals->len);
    vals->len = j + 1;
}

static struct vals *vals_from_pairs(struct rill_pairs *pairs)
{
    struct vals *vals =
        calloc(1, sizeof(*vals) + sizeof(vals->data[0]) * pairs->len);
    if (!vals) return NULL;

    vals->len = pairs->len;
    for (size_t i = 0; i < pairs->len; ++i)
        vals->data[i] = pairs->data[i].val;

    vals_compact(vals);
    return vals;
}

static struct vals *vals_merge(struct vals *vals, struct vals *merge)
{
    if (!vals) {
        size_t len = sizeof(*vals) + sizeof(vals->data[0]) * merge->len;
        vals = calloc(1, len);
        memcpy(vals, merge, len);
        return vals;
    }

    vals = realloc(vals,
            sizeof(*vals) + sizeof(vals->data[0]) * (vals->len + merge->len));
    if (!vals) return NULL;

    memcpy( vals->data + vals->len,
            merge->data,
            sizeof(merge->data[0]) * merge->len);
    vals->len += merge->len;

    vals_compact(vals);
    return vals;
}
