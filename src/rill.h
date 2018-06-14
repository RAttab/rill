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
typedef uint64_t rill_val_t;


// -----------------------------------------------------------------------------
// row
// -----------------------------------------------------------------------------

struct rill_row
{
    rill_val_t a, b;
};

inline bool rill_row_nil(const struct rill_row *row)
{
    return !row->a && !row->b;
}

inline int rill_row_cmp(const struct rill_row *lhs, const struct rill_row *rhs)
{
    if (lhs->a < rhs->a) return -1;
    if (lhs->a > rhs->a) return +1;

    if (lhs->b < rhs->b) return -1;
    if (lhs->b > rhs->b) return +1;

    return 0;
}


// -----------------------------------------------------------------------------
// rows
// -----------------------------------------------------------------------------

struct rill_rows
{
    size_t len, cap;
    struct rill_row *data;
};

struct rill_rows *rill_rows_new(size_t cap);
void rill_rows_free(struct rill_rows *rows);
void rill_rows_clear(struct rill_rows *rows);

struct rill_rows *rill_rows_reserve(struct rill_rows *rows, size_t cap);

struct rill_rows *rill_rows_push(
        struct rill_rows *rows, rill_val_t key, rill_val_t val);

void rill_rows_compact(struct rill_rows *rows);

void rill_rows_print(const struct rill_rows *rows);

void rill_rows_invert(struct rill_rows* rows);


// -----------------------------------------------------------------------------
// store
// -----------------------------------------------------------------------------

enum rill_col { rill_col_a = 0, rill_col_b = 1 };

struct rill_store;
struct rill_store_it;
struct rill_space;

struct rill_store *rill_store_open(const char *file);
void rill_store_close(struct rill_store *store);

bool rill_store_write(
        const char *file,
        rill_ts_t ts,
        size_t quant,
        struct rill_rows *rows);

bool rill_store_merge(
        const char *file,
        rill_ts_t ts, size_t quant,
        struct rill_store **list, size_t len);

bool rill_store_rm(struct rill_store *store);

const char * rill_store_file(const struct rill_store *store);
unsigned rill_store_version(const struct rill_store *store);
rill_ts_t rill_store_ts(const struct rill_store *store);
size_t rill_store_quant(const struct rill_store *store);
size_t rill_store_keys_count(const struct rill_store *store, enum rill_col column);
size_t rill_store_rows(const struct rill_store *store);
size_t rill_store_index_len(const struct rill_store *store, enum rill_col col);


struct rill_space* rill_store_space(struct rill_store *store);
size_t rill_store_space_header(struct rill_space *space);
size_t rill_store_space_index(struct rill_space *space, enum rill_col col);
size_t rill_store_space_rows(struct rill_space *space, enum rill_col col);
void rill_space_free(struct rill_space* space);


struct rill_rows *rill_store_query_value(
        struct rill_store *store, rill_val_t val, struct rill_rows *out);
struct rill_rows *rill_store_query_key(
        struct rill_store *store, rill_val_t key, struct rill_rows *out);


size_t rill_store_keys(
        const struct rill_store *store, rill_val_t *out, size_t cap,
        enum rill_col column);

struct rill_store_it *rill_store_begin(
        struct rill_store *store, enum rill_col column);
void rill_store_it_free(struct rill_store_it *it);
bool rill_store_it_next(struct rill_store_it *it, struct rill_row *row);


// -----------------------------------------------------------------------------
// acc
// -----------------------------------------------------------------------------

struct rill_acc;

enum { rill_acc_read_only = 0 };

struct rill_acc *rill_acc_open(const char *dir, size_t cap);
void rill_acc_close(struct rill_acc *acc);

void rill_acc_ingest(struct rill_acc *acc, rill_val_t key, rill_val_t val);
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

struct rill_rows *rill_query_key(
        const struct rill_query *query,
        rill_val_t key,
        struct rill_rows *out);

struct rill_rows *rill_query_keys(
        const struct rill_query *query,
        const rill_val_t *keys, size_t len,
        struct rill_rows *out);

struct rill_rows *rill_query_vals(
        const struct rill_query *query,
        const rill_val_t *vals, size_t len,
        struct rill_rows *out);

struct rill_rows *rill_query_all(
    const struct rill_query *query, enum rill_col col);


// -----------------------------------------------------------------------------
// misc
// -----------------------------------------------------------------------------

size_t rill_scan_dir(const char *dir, struct rill_store **list, size_t cap);
