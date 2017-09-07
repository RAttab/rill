/* store.h
   Rémi Attab (remi.attab@gmail.com), 30 Aug 2017
   FreeBSD-style copyright and disclaimer apply
*/

#pragma once

#include "rill.h"

#include <stddef.h>


// -----------------------------------------------------------------------------
// store
// -----------------------------------------------------------------------------

struct rill_pairs;
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
