/* rill.h
   Rémi Attab (remi.attab@gmail.com), 03 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

// -----------------------------------------------------------------------------
// types
// -----------------------------------------------------------------------------

typedef uint64_t rill_ts_t;
typedef uint64_t rill_key_t;
typedef uint64_t rill_val_t;


// -----------------------------------------------------------------------------
// kv
// -----------------------------------------------------------------------------

struct rill_kv
{
    rill_key_t key;
    rill_val_t val;
};

static inline bool rill_kv_nil(const struct rill_kv *kv)
{
    return !kv->key && !kv->val;
}

static inline int rill_kv_cmp(const struct rill_kv *lhs, const struct rill_kv *rhs)
{
    if (lhs->key < rhs->key) return -1;
    if (lhs->key > rhs->key) return +1;

    if (lhs->val < rhs->val) return -1;
    if (lhs->val > rhs->val) return +1;

    return 0;
}


// -----------------------------------------------------------------------------
// pairs
// -----------------------------------------------------------------------------

struct rill_pairs
{
    size_t len, cap;
    struct rill_kv *data;
};

void rill_pairs_free(struct rill_pairs *pairs);
bool rill_pairs_reset(struct rill_pairs *pairs, size_t cap);
bool rill_pairs_push(struct rill_pairs *pairs, rill_key_t key, rill_val_t val);
void rill_pairs_compact(struct rill_pairs *pairs);

bool rill_pairs_scan_key(
        const struct rill_pairs *pairs,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out);

bool rill_pairs_scan_val(
        const struct rill_pairs *pairs,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out);

void rill_pairs_print(const struct rill_pairs *pairs);



// -----------------------------------------------------------------------------
// store
// -----------------------------------------------------------------------------

struct rill_store;

struct rill_store *rill_store_open(const char *file);
void rill_store_close(struct rill_store *store);

bool rill_store_write(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_pairs *pairs);

bool rill_store_merge(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_store **list, size_t len);

bool rill_store_rm(struct rill_store *store);

const char * rill_store_file(struct rill_store *store);
rill_ts_t rill_store_ts(struct rill_store *store);
size_t rill_store_quant(struct rill_store *store);

bool rill_store_scan_key(
        struct rill_store *store,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out);
bool rill_store_scan_val(
        struct rill_store *store,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out);

void rill_store_print_head(struct rill_store *store);
void rill_store_print(struct rill_store *store);


// -----------------------------------------------------------------------------
// rill
// -----------------------------------------------------------------------------

struct rill;

struct rill * rill_open(const char *dir);
void rill_close(struct rill *db);

bool rill_ingest(struct rill *db, rill_key_t key, rill_val_t val);
bool rill_rotate(struct rill *db, rill_ts_t now);

void rill_query_key(struct rill *db, rill_key_t *keys, size_t len, struct rill_pairs *out);
void rill_query_val(struct rill *db, rill_val_t *vals, size_t len, struct rill_pairs *out);
