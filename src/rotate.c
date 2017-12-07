/* rotate.c
   Rémi Attab (remi.attab@gmail.com), 16 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <limits.h>
#include <unistd.h>


// -----------------------------------------------------------------------------
// rotate
// -----------------------------------------------------------------------------

static ssize_t expire(rill_ts_t now, struct rill_store **list, ssize_t len)
{
    if (len < 0) return len;
    if (now < expire_secs) return len; // mostly for tests.

    size_t i = 0;
    for (; i < (size_t) len; ++i) {
        if (rill_store_ts(list[i]) < (now - expire_secs)) break;
    }

    size_t end = i;
    for (; i < (size_t) len; ++i) {
        rill_store_rm(list[i]);
        list[i] = NULL;
    }

    return end;
}

static int file_exists(const char *file)
{
    struct stat s;
    if (!stat(file, &s)) return 1;
    if (errno == ENOENT) return 0;

    rill_fail_errno("unable to stat '%s'", file);
    return -1;
}


static bool file_name(
        const char *dir, rill_ts_t ts, rill_ts_t quant, char *out, size_t len)
{
    rill_ts_t month = ts / month_secs;
    rill_ts_t week = (ts / week_secs) % weeks_in_month;
    rill_ts_t day = (ts / day_secs) % days_in_week;
    rill_ts_t hour = (ts / hour_secs) % hours_in_day;

    char base[NAME_MAX];
    if (quant == hour_secs)
        snprintf(base, sizeof(base), "%s/%05lu-%02lu-%02lu-%02lu.rill",
                dir, month, week, day, hour);
    else if (quant == day_secs)
        snprintf(base, sizeof(base), "%s/%05lu-%02lu-%02lu.rill", dir, month, week, day);
    else if (quant == week_secs)
        snprintf(base, sizeof(base), "%s/%05lu-%02lu.rill", dir, month, week);
    else if (quant == month_secs)
        snprintf(base, sizeof(base), "%s/%05lu.rill", dir, month);
    else assert(false);

    strncpy(out, base, len < sizeof(base) ? len : sizeof(base));

    int ret;
    size_t i = 0;
    while ((ret = file_exists(out)) == 1)
        snprintf(out, len, "%s.%lu", base, i++);

    if (ret == -1) return false;
    return true;
}

static struct rill_store *merge(
        const char *dir,
        rill_ts_t ts, rill_ts_t quant,
        struct rill_store **list, size_t len)
{
    assert(len > 0);
    if (len == 1) {
        struct rill_store *result = list[0];
        list[0] = NULL;
        return result;
    }

    char file[PATH_MAX];
    if (!file_name(dir, ts, quant, file, sizeof(file))) return NULL;
    if (!rill_store_merge(file, ts, quant, list, len)) return NULL;

    for (size_t i = 0; i < len; ++i) {
        rill_store_rm(list[i]);
        list[i] = NULL;
    }

    return rill_store_open(file);
}

static ssize_t merge_quant(
        const char *dir,
        rill_ts_t now, rill_ts_t quant,
        struct rill_store **list, ssize_t len)
{
    if (len <= 1) return len;

    size_t out_len = 0;
    struct rill_store *out[(size_t) len];

    size_t start = 0;
    rill_ts_t current_quant = rill_store_ts(list[0]) / quant;

    for (size_t i = 0; i < (size_t) len; i++) {
        size_t end = i + 1;
        assert(i >= start);
        assert(end > start);

        size_t next_ts = i + 1 != (size_t) len ? rill_store_ts(list[i + 1]) : -1UL;
        if (next_ts / quant == current_quant) continue;

        rill_ts_t earliest_ts = rill_store_ts(list[start]);
        if (earliest_ts / quant != now / quant) {
            struct rill_store *store = merge(dir, earliest_ts, quant, list + start, end - start);
            if (!store) goto fail;
            out[out_len++] = store;
        }

        // if a file is in the quant represented by now then we don't want to
        // merge it as we're still filling in this quant. Additionally, if it's
        // in our current quant then it will also be in all bigger quants so we
        // can just forget these files for the rest of the rotation.
        else {
            for (size_t j = start; j < end; ++j) {
                rill_store_close(list[j]);
                list[j] = NULL;
            }
        }

        current_quant = next_ts / quant;
        start = i + 1;
    }

    for (size_t i = 0; i < (size_t) len; ++i) assert(!list[i]);
    memcpy(list, out, out_len * sizeof(out[0]));
    return out_len;

  fail:
    for (size_t i = 0; i < out_len; ++i)
        rill_store_close(out[i]);

    return -1;
}

static int store_cmp(const void *l, const void *r)
{
    const struct rill_store *const *lhs = l;
    const struct rill_store *const *rhs = r;

    // earliest (biggest) to oldest (smallest)
    if (rill_store_ts(*lhs) < rill_store_ts(*rhs)) return +1;
    if (rill_store_ts(*lhs) > rill_store_ts(*rhs)) return -1;
    return 0;
}

// Note that an flock is released on process termination on linux. This means
// that we don't have to worry about cleaning up in case of segfaults or signal
// termination.
static int lock(const char *dir)
{
    int fd = open(dir, O_DIRECTORY | O_RDONLY);
    if (fd == -1) {
        rill_fail_errno("unable to open: %s\n", dir);
        return -1;
    }

    if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        if (errno == EWOULDBLOCK) return 0;

        rill_fail_errno("unable acquire flock on '%s'\n", dir);
        close(fd);
        return -1;
    }

    return fd;
}

static void unlock(int fd)
{
    flock(fd, LOCK_UN);
    close(fd);
}

bool rill_rotate(const char *dir, rill_ts_t now)
{
    int fd = lock(dir);
    if (!fd) return true;
    if (fd == -1) return false;

    enum { cap = 1024 };
    struct rill_store *list[cap];
    size_t list_len = rill_scan_dir(dir, list, cap);
    qsort(list, list_len, sizeof(list[0]), store_cmp);

    ssize_t len = list_len;
    len = expire(now, list, len);
    len = merge_quant(dir, now, hour_secs, list, len);
    len = merge_quant(dir, now, day_secs, list, len);
    len = merge_quant(dir, now, week_secs, list, len);
    len = merge_quant(dir, now, month_secs, list, len);

    for (size_t i = 0; i < list_len; ++i) {
        if (list[i]) rill_store_close(list[i]);
    }

    unlock(fd);
    return len >= 0;
}
