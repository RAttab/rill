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
// encode
// -----------------------------------------------------------------------------

static const size_t coder_max_val_len = sizeof(rill_val_t) + 2 + 1;

struct encoder
{
    uint8_t *it, *start, *end;

    size_t keys;
    rill_val_t key;

    vals_rev_t rev;
    struct index *index;

    size_t rows;
};

static size_t coder_cap(size_t vals, size_t rows)
{
    size_t bytes = 1;
    while (vals >= 1UL << (bytes * 7)) bytes++;

    return (bytes + 1) // + 1 -> end-of-values terminator
        * (rows + 1); // + 1 -> end-of-rows terminator
}

static uint64_t coder_off(struct encoder *coder)
{
    return coder->it - coder->start;
}


static inline bool coder_write_sep(struct encoder *coder)
{
    if (rill_unlikely(coder->it + 1 > coder->end)) {
        rill_fail("not enough space to write sep: %p + 1 > %p\n",
                (void *) coder->it, (void *) coder->end);
        return false;
    }

    *coder->it = 0;
    coder->it++;

    return true;
}

static inline bool coder_write_val(struct encoder *coder, rill_val_t val)
{
    val = vals_vtoi(&coder->rev, val);

    uint8_t buffer[coder_max_val_len];
    size_t len = leb128_encode(buffer, val) - buffer;

    if (rill_unlikely(coder->it + len > coder->end)) {
        rill_fail("not enough space to write val: %p + %lu > %p\n",
                (void *) coder->it, len, (void *) coder->end);
        return false;
    }

    memcpy(coder->it, buffer, len);
    coder->it += len;

    return true;
}

static bool coder_encode(struct encoder *coder, const struct rill_row *row)
{
    if (coder->key != row->key) {
        if (rill_likely(coder->key)) {
            if (!coder_write_sep(coder)) return false;
        }

        index_put(coder->index, row->key, coder_off(coder));
        coder->key = row->key;
        coder->keys++;
    }

    if (!coder_write_val(coder, row->val)) return false;

    coder->rows++;
    return true;
}

static bool coder_finish(struct encoder *coder)
{
    if (!coder_write_sep(coder)) return false;
    if (!coder_write_sep(coder)) return false;
    return true;
}

static void coder_close(struct encoder *coder)
{
    htable_reset(&coder->rev);
}

static struct encoder make_encoder(
        uint8_t *start,
        uint8_t *end,
        struct vals *vals,
        struct index *index)
{
    struct encoder coder = {
        .it = start, .start = start, .end = end,
        .index = index,
    };

    vals_rev_make(vals, &coder.rev);
    return coder;
}


// -----------------------------------------------------------------------------
// decoder
// -----------------------------------------------------------------------------

struct decoder
{
    uint8_t *it, *end;

    size_t keys;
    rill_val_t key;

    struct index *lookup;
    struct index *index;

    struct vals *vals;
};

static inline bool coder_read_val(struct decoder *coder, rill_val_t *val)
{
    if (!leb128_decode(&coder->it, coder->end, val)) {
        rill_fail("unable to decode value at '%p-%p'\n",
                (void *) coder->it, (void *) coder->end);
        return false;
    }

    if (*val) *val = coder->lookup->data[*val - 1].key;
    return true;
}

static bool coder_decode(struct decoder *coder, struct rill_row *row)
{
    if (rill_likely(coder->key)) {
        row->key = coder->key;
        if (!coder_read_val(coder, &row->val)) return false;
        if (row->val) return true;
    }

    coder->key = index_get(coder->index, coder->keys);
    coder->keys++;

    row->key = coder->key;
    if (!row->key) return true; // eof

    return coder_read_val(coder, &row->val);
}

static struct decoder make_decoder_at(
        uint8_t *it, uint8_t *end,
        struct index *lookup,
        struct index *index,
        size_t key_idx)
{
    return (struct decoder) {
        .it = it, .end = end,
        .keys = key_idx,
        .lookup = lookup,
        .index = index,
    };
}
