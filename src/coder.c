/* coder.c
   Rémi Attab (remi.attab@gmail.com), 10 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

// -----------------------------------------------------------------------------
// leb128
// -----------------------------------------------------------------------------

static inline uint8_t *leb128_encode(uint8_t *it, uint64_t val)
{
    static const size_t shift = 7;
    static const uint64_t more_mask = 1UL << shift;
    static const uint64_t body_mask = (1UL << shift) - 1;

    do {
        *it = val & body_mask;
        *it |= (val >>= shift) ? more_mask : 0;
        it++;
    } while (val);

    return it;
}

static inline bool leb128_decode(uint8_t **it, uint8_t *end, uint64_t *val)
{
    static const size_t shift = 7;
    static const uint64_t more_mask = 1UL << shift;
    static const uint64_t body_mask = (1UL << shift) - 1;

    if (*it == end) return it;

    uint8_t data;
    size_t pos = 0;
    *val = 0;

    do {
        data = **it; (*it)++;
        *val |= (data & body_mask) << pos;
        pos += shift;
    } while ((data & more_mask) && *it != end);

    return !(data & more_mask);
}


// -----------------------------------------------------------------------------
// vals
// -----------------------------------------------------------------------------

struct rill_packed vals
{
    uint64_t len;
    uint64_t data[];
};

typedef struct htable vals_rev_t;

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


// -----------------------------------------------------------------------------
// coder
// -----------------------------------------------------------------------------

static const size_t coder_max_val_len = sizeof(rill_val_t) + 2 + 1;

struct coder
{
    struct vals *vals;
    vals_rev_t rev;

    rill_key_t key;
    uint8_t *it;
    uint8_t *end;

    size_t keys;
    size_t pairs;
};

// -----------------------------------------------------------------------------
// encode
// -----------------------------------------------------------------------------

static inline bool coder_write_sep(struct coder *coder)
{
    if (rill_unlikely(coder->it + 1 > coder->end)) return false;

    *coder->it = 0;
    coder->it++;

    return true;
}

static inline bool coder_write_key(struct coder *coder, rill_key_t key)
{
    if (rill_unlikely(coder->it + sizeof(key) > coder->end)) return false;

    memcpy(coder->it, &key, sizeof(key));
    coder->it += sizeof(key);

    return true;
}

static inline bool coder_write_val(struct coder *coder, rill_val_t val)
{
    val = vals_vtoi(&coder->rev, val);

    uint8_t buffer[coder_max_val_len];
    size_t len = leb128_encode(buffer, val) - buffer;

    if (rill_unlikely(coder->it + len > coder->end)) return false;

    memcpy(coder->it, buffer, len);
    coder->it += len;

    return true;
}

static bool coder_encode(struct coder *coder, const struct rill_kv *kv)
{
    if (coder->key != kv->key) {
        if (rill_likely(coder->key)) {
            if (!coder_write_sep(coder)) return false;
        }

        coder->key = kv->key;
        if (!coder_write_key(coder, kv->key)) return false;
        coder->keys++;
    }

    if (!coder_write_val(coder, kv->val)) return false;

    coder->pairs++;
    return true;
}

static bool coder_finish(struct coder *coder)
{
    if (!coder_write_sep(coder)) return false;
    if (!coder_write_key(coder, 0)) return false;

    htable_reset(&coder->rev);
    return true;
}

static struct coder make_encoder(struct vals *vals, uint8_t *it, uint8_t *end)
{
    struct coder coder = {
        .vals = vals,
        .it = it,
        .end = end,
    };

    vals_rev_make(coder.vals, &coder.rev);
    return coder;
}


// -----------------------------------------------------------------------------
// decode
// -----------------------------------------------------------------------------

static inline bool coder_read_key(struct coder *coder, rill_key_t *key)
{
    if (rill_unlikely(coder->it + sizeof(*key) > coder->end)) {
        rill_fail("unable to decode key: %p + %lu = %p > %p'\n",
                (void *) coder->it, sizeof(*key),
                (void *) (coder->it + sizeof(*key)),
                (void *) coder->end);
        return false;
    }

    memcpy(key, coder->it, sizeof(*key));
    coder->it += sizeof(*key);

    return true;
}

static inline bool coder_read_val(struct coder *coder, rill_val_t *val)
{
    if (!leb128_decode(&coder->it, coder->end, val)) {
        rill_fail("unable to decode value at '%p-%p'\n",
                (void *) coder->it, (void *) coder->end);
        return false;
    }

    if (*val) *val = vals_itov(coder->vals, *val);
    return true;
}

static bool coder_decode(struct coder *coder, struct rill_kv *kv)
{
    if (rill_likely(coder->key)) {
        kv->key = coder->key;
        if (!coder_read_val(coder, &kv->val)) return false;
        if (kv->val) return true;
    }

    if (!coder_read_key(coder, &coder->key)) return false;
    kv->key = coder->key;
    if (!kv->key) return true; // eof

    return coder_read_val(coder, &kv->val);
}

static struct coder make_decoder(struct vals *vals, uint8_t *it, uint8_t *end)
{
    return (struct coder) {
        .vals = vals,
        .it = it,
        .end = end,
    };
}
