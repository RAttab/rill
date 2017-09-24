/* rill.h
   Rémi Attab (remi.attab@gmail.com), 03 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>


// -----------------------------------------------------------------------------
// error
// -----------------------------------------------------------------------------

enum { rill_err_msg_cap = 1024 };

struct rill_error
{
    const char *file;
    int line;

    int errno_; // errno can be a macro hence the underscore.
    char msg[rill_err_msg_cap];
};

extern __thread struct rill_error rill_errno;

void rill_perror(struct rill_error *err);
size_t rill_strerror(struct rill_error *err, char *dest, size_t len);


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

inline bool rill_kv_nil(const struct rill_kv *kv)
{
    return !kv->key && !kv->val;
}

inline int rill_kv_cmp(const struct rill_kv *lhs, const struct rill_kv *rhs)
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
    struct rill_kv data[];
};

struct rill_pairs *rill_pairs_new(size_t cap);
void rill_pairs_free(struct rill_pairs *pairs);
void rill_pairs_clear(struct rill_pairs *pairs);

struct rill_pairs *rill_pairs_push(
        struct rill_pairs *pairs, rill_key_t key, rill_val_t val);

void rill_pairs_compact(struct rill_pairs *pairs);

struct rill_pairs *rill_pairs_scan_key(
        const struct rill_pairs *pairs,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out);

struct rill_pairs *rill_pairs_scan_val(
        const struct rill_pairs *pairs,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out);

void rill_pairs_print(const struct rill_pairs *pairs);


// -----------------------------------------------------------------------------
// store
// -----------------------------------------------------------------------------

struct rill_store;
struct rill_store_it;

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

const char * rill_store_file(const struct rill_store *store);
rill_ts_t rill_store_ts(const struct rill_store *store);
size_t rill_store_quant(const struct rill_store *store);
size_t rill_store_vals(const struct rill_store *store);

struct rill_pairs *rill_store_query_key(
        struct rill_store *store, rill_key_t key, struct rill_pairs *out);
struct rill_pairs *rill_store_scan_keys(
        struct rill_store *store,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out);
struct rill_pairs *rill_store_scan_vals(
        struct rill_store *store,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out);

size_t rill_store_dump_vals(
        const struct rill_store *store, rill_val_t *out, size_t cap);

struct rill_store_it *rill_store_begin(struct rill_store *store);
void rill_store_it_free(struct rill_store_it *it);
bool rill_store_it_next(struct rill_store_it *it, struct rill_kv *kv);

void rill_store_print_head(struct rill_store *store);
void rill_store_print(struct rill_store *store);


// -----------------------------------------------------------------------------
// acc
// -----------------------------------------------------------------------------

struct rill_acc;

enum { rill_acc_read_only = 0 };

struct rill_acc *rill_acc_open(const char *dir, size_t cap);
void rill_acc_close(struct rill_acc *acc);

void rill_acc_ingest(struct rill_acc *acc, rill_key_t key, rill_val_t val);
bool rill_acc_write(struct rill_acc *acc, const char *file, rill_ts_t now);


// -----------------------------------------------------------------------------
// rotate
// -----------------------------------------------------------------------------

bool rill_rotate(const char *dir, rill_ts_t now);


// -----------------------------------------------------------------------------
// query
// -----------------------------------------------------------------------------

struct rill_query;

struct rill_query * rill_query_open(const char *dir);
void rill_query_close(struct rill_query *db);

struct rill_pairs *rill_query_key(
        const struct rill_query *query, rill_key_t key, struct rill_pairs *out);

struct rill_pairs *rill_query_keys(
        const struct rill_query *query,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out);

struct rill_pairs *rill_query_vals(
        const struct rill_query *query,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out);


// -----------------------------------------------------------------------------
// misc
// -----------------------------------------------------------------------------

size_t rill_scan_dir(const char *dir, struct rill_store **list, size_t cap);
