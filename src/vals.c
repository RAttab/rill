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

static size_t vals_vtoi(const vals_rev_t *rev, rill_val_t val)
{
    if (!val) return 0; // \todo giant hack for coder_finish

    struct htable_ret ret = htable_get(rev, val);
    assert(ret.ok);
    return ret.value;
}

// \todo should technically return bool for htable resize errors. Need to fix
// htable interface.
static void vals_rev_make(const struct vals *vals, vals_rev_t *rev)
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
    rill_val_t lhs = *((const rill_val_t *) l);
    rill_val_t rhs = *((const rill_val_t *) r);

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

static struct vals *vals_for_col(const struct rill_rows *rows, enum rill_col col)
{
    struct vals *vals =
        calloc(1, sizeof(*vals) + sizeof(vals->data[0]) * rows->len);

    if (!vals) return NULL;

    vals->len = rows->len;
    for (size_t i = 0; i < rows->len; ++i)
        vals->data[i] = rill_row_get(&rows->data[i], col);

    vals_compact(vals);
    return vals;
}

static struct vals *vals_add_index(struct vals *vals, const struct index *index)
{
    assert(merge);

    if (!vals) {
        vals = calloc(1, sizeof(*vals) + index->len * sizeof(vals->data[0]));
        if (!vals) {
            rill_fail("unable to allocate memory for vals: %lu", index->len);
            return NULL;
        }

        for (size_t i = 0; i < index->len; ++i)
            vals->data[i] = index->data[i].key;
        vals->len = index->len;

        return vals;
    }

    size_t len = vals->len + index->len;
    vals = realloc(vals, sizeof(*vals) + len * sizeof(vals->data[0]));
    if (!vals) {
        rill_fail("unable to allocate memory for vals: %lu + %lu",
                vals->len, index->len);
        return NULL;
    }

    for (size_t i = 0; i < index->len; ++i)
        vals->data[vals->len + i] = index->data[i].key;
    vals->len += index->len;

    vals_compact(vals);
    return vals;
}
