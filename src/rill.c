/* rill.c
   Rémi Attab (remi.attab@gmail.com), 03 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "store.h"
#include "utils.h"

#include <assert.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>


// -----------------------------------------------------------------------------
// config
// -----------------------------------------------------------------------------

enum { hours = 24, days = 30, months = 13};
enum
{
    quant_hour = 60 * 60,
    quant_day = hours * quant_hour,
    quant_month = days * quant_day,
};


// -----------------------------------------------------------------------------
// rill
// -----------------------------------------------------------------------------

struct rill
{
    const char *dir;
    rill_ts_t ts;

    lock_t lock;
    struct rill_pairs *acc;
    struct rill_pairs *dump;

    struct rill_store *hourly[hours];
    struct rill_store *daily[days];
    struct rill_store *monthly[months];
};

static bool load_store(struct rill *db, const char *file)
{
    struct rill_store *store = rill_store_open(file);
    if (!store) goto fail_open;

    rill_ts_t ts = rill_store_ts(store);
    size_t quant = rill_store_quant(store);

    struct rill_store **bucket = NULL;
    switch (quant) {
    case quant_hour: bucket = &db->hourly[(ts / quant_hour) % hours]; break;
    case quant_day: bucket = &db->daily[(ts / quant_hour) % days]; break;
    case quant_month: bucket = &db->monthly[(ts / quant_month) % months]; break;
    default:
        fail("unknown quant '%lu' for '%s'", quant, file);
        goto fail_quant;
    }

    if (*bucket) {
        fail("file '%s' is a duplicate for quant '%lu' at timestamp %lu'",
                file, quant, ts);
        goto fail_dup;
    }

    *bucket = store;

    return true;

  fail_dup:
  fail_quant:
    rill_store_close(store);
  fail_open:
    return false;
}

struct rill * rill_open(const char *dir)
{
    struct rill *db = calloc(1, sizeof(*db));
    if (!db) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_struct;
    }

    db->dir = strndup(dir, NAME_MAX);
    if (!db->dir) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_dir;
    }

    db->acc = calloc(1, sizeof(*db->acc));
    if (!db->acc) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_acc;
    }

    db->dump = calloc(1, sizeof(*db->dump));
    if (!db->dump) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_dump;
    }

    if (!rill_pairs_reset(db->acc, 1 *1000 * 1000)) {
        fail("unable to allocate pairs for '%s'", dir);
        goto fail_pairs;
    }

    if (!rill_pairs_reset(db->dump, 1 *1000 * 1000)) {
        fail("unable to allocate pairs for '%s'", dir);
        goto fail_pairs;
    }

    if (mkdir(dir, 0775) == -1 && errno != EEXIST) {
        fail_errno("unable to open create dir '%s'", dir);
        goto fail_mkdir;
    }

    DIR *dir_handle = opendir(dir);
    if (!dir_handle) {
        fail_errno("unable to open dir '%s'", dir);
        goto fail_dir;
    }

    struct dirent it, *entry;
    while (true) {
        if (readdir_r(dir_handle, &it, &entry) == -1) {
            fail_errno("unable to read dir '%s'", dir);
            goto fail_readdir;
        }
        else if (!entry) break;
        else if (entry->d_type != DT_REG) continue;

        char file[NAME_MAX];
        snprintf(file, sizeof(file), "%s/%s", db->dir, entry->d_name);
        (void) load_store(db, file);
    }

    closedir(dir_handle);

    return db;

  fail_readdir:
    closedir(dir_handle);
  fail_dir:
  fail_mkdir:
  fail_pairs:
    rill_pairs_free(db->dump);
    free(db->dump);
  fail_alloc_dump:
    rill_pairs_free(db->acc);
    free(db->acc);
  fail_alloc_acc:
    free((char *) db->dir);
  fail_alloc_dir:
    free(db);
  fail_alloc_struct:
    return NULL;
}

void rill_close(struct rill *db)
{
    for (size_t i = 0; i < hours; ++i) {
        if (db->hourly[i]) rill_store_close(db->hourly[i]);
    }

    for (size_t i = 0; i < days; ++i) {
        if (db->daily[i]) rill_store_close(db->daily[i]);
    }

    for (size_t i = 0; i < months; ++i) {
        if (db->monthly[i]) rill_store_close(db->monthly[i]);
    }

    rill_pairs_free(db->acc);
    rill_pairs_free(db->dump);

    free((char *) db->dir);
    free(db->dump);
    free(db->acc);
    free(db);
}


// -----------------------------------------------------------------------------
// ingest
// -----------------------------------------------------------------------------

bool rill_ingest(struct rill *db, rill_ts_t now, rill_key_t key, rill_val_t val)
{
    (void) now;

    bool ret;
    {
        lock(&db->lock);

        ret = rill_pairs_push(db->acc, key, val);

        unlock(&db->lock);
    }

    return ret;
}


// -----------------------------------------------------------------------------
// rotate
// -----------------------------------------------------------------------------
// \todo since we're deleting data, should be reviewed for robustness.
//
// \todo if we have a gap in ingestion, it's possible that we don't expire some
//       data or that we hit one of the asserts. Need to improve the mechanism a
//       bit.


static bool rotate_monthly(
        struct rill *db,
        struct rill_store **store,
        rill_ts_t ts,
        struct rill_store **list, size_t len)
{
    char file[NAME_MAX];
    snprintf(file, sizeof(file), "%s/%06lu.rill", db->dir, ts / quant_month);

    if (*store) {
        (void) rill_store_rm(*store);
        *store = NULL;
    }
    if (!rill_store_merge(file, ts, quant_month, list, len)) return false;
    if (!(*store = rill_store_open(file))) return false;

    for (size_t i = 0; i < len; ++i) {
        if (!list[i]) continue;
        (void) rill_store_rm(list[i]);
        list[i] = NULL;
    }

    return true;
}

static bool rotate_daily(
        struct rill *db,
        struct rill_store **store,
        rill_ts_t ts,
        struct rill_store **list, size_t len)
{
    char file[NAME_MAX];
    snprintf(file, sizeof(file), "%s/%06lu-%02lu.rill", db->dir,
            ts / quant_month,
            (ts / quant_day) % days);

    assert(!*store);
    if (!rill_store_merge(file, ts, quant_day, list, len)) return false;
    if (!(*store = rill_store_open(file))) return false;

    for (size_t i = 0; i < len; ++i) {
        if (!list[i]) continue;
        (void) rill_store_rm(list[i]);
        list[i] = NULL;
    }

    return true;
}

static bool rotate_hourly(struct rill *db, struct rill_store **store, rill_ts_t ts)
{
    char file[NAME_MAX];
    snprintf(file, sizeof(file), "%s/%06lu-%02lu-%02lu.rill", db->dir,
            ts / quant_month,
            (ts / quant_day) % days,
            (ts / quant_hour) % hours);

    {
        lock(&db->lock);

        struct rill_pairs *tmp = db->acc;
        db->acc = db->dump;
        db->dump = tmp;

        unlock(&db->lock);
    }

    assert(!*store);
    if (!rill_store_write(file, ts, quant_hour, db->dump)) return false;
    if (!(*store = rill_store_open(file))) return false;

    rill_pairs_reset(db->dump, db->dump->cap);

    return true;
}

bool rill_rotate(struct rill *db, rill_ts_t now)
{
    if (now / quant_month != db->ts / quant_month) {
        size_t quant = db->ts / quant_month;
        if (!rotate_monthly(db, &db->monthly[quant % months], db->ts, db->daily, days)) {
            fail("unable to complete monthly rotation '%lu'", quant);
            return false;
        }
    }

    if (now / quant_day != db->ts / quant_day) {
        size_t quant = db->ts / quant_day;
        if (!rotate_daily(db, &db->daily[quant % days], db->ts, db->hourly, hours)) {
            fail("unable to complete daily rotation '%lu'", quant);
            return false;
        }
    }

    if (now / quant_hour != db->ts / quant_hour) {
        size_t quant = db->ts / quant_hour;
        if (!rotate_hourly(db, &db->hourly[(now / quant_hour) % hours], db->ts)) {
            fail("unable to complete hourly rotation '%lu'", quant);
            return false;
        }
    }

    db->ts = now;
    return true;
}


// -----------------------------------------------------------------------------
// query
// -----------------------------------------------------------------------------

void rill_query_key(struct rill *db, rill_key_t *keys, size_t len, struct rill_pairs *out)
{
    for (size_t i = 0; i < hours; ++i) {
        if (!db->hourly[i]) continue;
        rill_store_scan_key(db->hourly[i], keys, len, out);
    }

    for (size_t i = 0; i < days; ++i) {
        if (!db->daily[i]) continue;
        rill_store_scan_key(db->daily[i], keys, len, out);
    }

    for (size_t i = 0; i < months; ++i) {
        if (!db->monthly[i]) continue;
        rill_store_scan_key(db->monthly[i], keys, len, out);
    }

    rill_pairs_compact(out);
}

void rill_query_val(struct rill *db, rill_val_t *vals, size_t len, struct rill_pairs *out)
{
    for (size_t i = 0; i < hours; ++i) {
        if (!db->hourly[i]) continue;
        rill_store_scan_val(db->hourly[i], vals, len, out);
    }

    for (size_t i = 0; i < days; ++i) {
        if (!db->daily[i]) continue;
        rill_store_scan_val(db->daily[i], vals, len, out);
    }

    for (size_t i = 0; i < months; ++i) {
        if (!db->monthly[i]) continue;
        rill_store_scan_val(db->monthly[i], vals, len, out);
    }

    rill_pairs_compact(out);
}
