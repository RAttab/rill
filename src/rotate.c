/* rotate.c
   Rémi Attab (remi.attab@gmail.com), 16 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <dirent.h>
#include <limits.h>

// -----------------------------------------------------------------------------
// rotate
// -----------------------------------------------------------------------------

static void rotate_acc(const char *dir, rill_ts_t now)
{
    struct rill_acc *acc = rill_acc_open(dir, rill_acc_read_only);
    if (!acc) return;

    char file[PATH_MAX];
    snprintf(file, sizeof(file), "%s/%010lu.rill", dir, now);
    printf("rotate: writing acc to '%s' with timestamp '%lu'\n", file, now);

    (void) rill_acc_write(acc, file, now);
    rill_acc_close(acc);
}

static size_t load_dir(const char *dir, struct rill_store **list, size_t cap)
{
    DIR *dir_handle = opendir(dir);
    if (!dir_handle) return 0;

    size_t len = 0;
    struct dirent *entry = NULL;
    while ((entry = readdir(dir_handle))) {
        if (entry->d_type != DT_REG) continue;
        if (!strcmp(entry->d_name, "acc")) continue;

        char file[PATH_MAX];
        snprintf(file, sizeof(file), "%s/%s", dir, entry->d_name);

        list[len] = rill_store_open(file);
        if (!list[len]) continue;

        len++;
        if (len == cap) {
            fail("rotate: too many files to rotate in '%s'", dir);
            break;
        }
    }

    closedir(dir_handle);
    return len;
}

static int store_cmp(const void *l, const void *r)
{
    const struct rill_store *const *lhs = l;
    const struct rill_store *const *rhs = r;

    if (rill_store_ts(*lhs) < rill_store_ts(*rhs)) return +1;
    if (rill_store_ts(*lhs) > rill_store_ts(*rhs)) return -1;
    return 0;
}

static size_t expire(struct rill_store **list, size_t len, rill_ts_t now)
{
    if (now < expiration) return len; // mostly for tests.

    size_t i = 0;
    for (; i < len; ++i) {
        if (rill_store_ts(list[i]) < (now - expiration)) break;
    }

    size_t end = i;
    for (; i < len; ++i) {
        printf("rotate: expiring '%s' with timestamp '%lu < %lu'\n",
                rill_store_file(list[i]), rill_store_ts(list[i]), (now - expiration));
        rill_store_rm(list[i]);
        list[i] = NULL;
    }
    return end;
}

static bool merge(
        struct rill_store **list, size_t len, const char *dir, rill_ts_t now)
{
    rill_ts_t earliest = rill_store_ts(list[0]);

    rill_ts_t quant = 0;
    if (earliest / hour != now / hour) quant = hour;
    if (earliest / day != now / day) quant = day;
    if (earliest / month != now / month) quant = month;

    printf("rotate: now=%lu, earliest=%lu, quant=%lu\n", now, earliest, quant);

    if (!quant) return true;

    rill_ts_t oldest = ((now / quant) - 1) * quant;
    size_t merge_end = 0;
    for (; merge_end < len; ++merge_end) {
        if (rill_store_ts(list[merge_end]) < oldest) break;

        printf("rotate: merging '%s' with timestamp '%lu >= %lu'\n",
                rill_store_file(list[merge_end]),
                rill_store_ts(list[merge_end]),
                oldest);
    }
    if (merge_end <= 1) return true;

    rill_ts_t ts = oldest + quant - 1;

    char file[PATH_MAX];
    if (quant == hour)
        snprintf(file, sizeof(file), "%s/%05lu-%02lu-%02lu.rill",
                dir, ts / month, (ts / day) % days, (ts / hour) % hours);
    else if (quant == day)
        snprintf(file, sizeof(file), "%s/%05lu-%02lu.rill",
                dir, ts / month, (ts / day) % days);
    else if (quant == month)
        snprintf(file, sizeof(file), "%s/%05lu.rill", dir, ts / month);


    printf("rotate: merging to '%s' with timestamp '%lu'\n", file, ts);

    if (!rill_store_merge(file, ts, quant, list, merge_end)) return false;

    for (size_t i = 0; i < merge_end; ++i) {
        printf("rotate: deleting '%s'\n", rill_store_file(list[i]));
        rill_store_rm(list[i]);
        list[i] = NULL;
    }

    return true;
}


bool rill_rotate(const char *dir, rill_ts_t now)
{
    printf("rotate: rotating '%s' at timestamp '%lu'\n", dir, now);

    enum { cap = 1024 };
    struct rill_store *list[cap];
    size_t len = load_dir(dir, list, cap);
    qsort(list, len, sizeof(list[0]), store_cmp);

    // We don't want the latest file in the merge list.
    rotate_acc(dir, now);

    len = expire(list, len, now);
    if (!len) return true;

    bool ret = merge(list, len, dir, now);
    for (size_t i = 0; i < len; ++i) {
        if (list[i]) rill_store_close(list[i]);
    }

    return ret;
}
