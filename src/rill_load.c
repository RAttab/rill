/* bench.c
   Rémi Attab (remi.attab@gmail.com), 04 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "rng.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>

void rm(const char *path)
{
    DIR *dir = opendir(path);
    if (!dir) return;

    struct dirent stream, *entry;
    while (true) {
        if (readdir_r(dir, &stream, &entry) == -1) abort();
        else if (!entry) break;
        else if (entry->d_type != DT_REG) continue;

        char file[PATH_MAX];
        snprintf(file, sizeof(file), "%s/%s", path, entry->d_name);
        unlink(file);
    }

    closedir(dir);
    rmdir(path);
}

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    rm("db");

    enum {
        keys_per_sec = 200,
        seconds = 3 * month_secs,
        rotation_rate = 10 * min_secs,

        keys_range = 20 * 1000 * 1000,
        vals_range = 100 * 1000,
        vals_per_key = 4,

        acc_cap = keys_per_sec * vals_per_key * rotation_rate,
    };

    struct rill_acc *acc = rill_acc_open("db", acc_cap);
    if (!acc) rill_abort();

    struct rng rng = rng_make(0);
    for (size_t ts = 0; ts < seconds; ++ts) {
        for (size_t i = 0; i < keys_per_sec; ++i) {
            uint64_t key = rng_gen_range(&rng, 0, keys_range) + 1;

            for (size_t j = 0; j < vals_per_key; ++j) {
                uint64_t val = rng_gen_range(&rng, 0, vals_range) + 1;
                rill_acc_ingest(acc, key, val);
            }
        }

        if (ts % rotation_rate == 0) {
            if (!rill_rotate("db", ts)) rill_abort();
        }
    }

    rill_acc_close(acc);
    if (!rill_rotate("db", seconds + 60 * 60)) rill_abort();

    return 0;
}
