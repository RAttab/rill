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

uint64_t rng_gen_val(struct rng *rng, uint64_t min, uint64_t range)
{
    uint64_t max = rng_gen_range(rng, 0, range) + 1;
    return rng_gen_range(rng, min, min + max) + 1;
}

int main(int argc, char **argv)
{
    (void) argc, (void) argv;
    rm("db");

    enum {
        keys_per_sec = 200,
        seconds = 3 * month_secs,
        rotation_rate = 10 * min_secs,

        keys_range = 1 * 1000 * 1000 * 1000,
        vals_range = 10 * 1000,
        vals_per_key = 4,

        acc_cap = keys_per_sec * vals_per_key * rotation_rate,
    };

    struct rill_acc *acc = rill_acc_open("db", acc_cap);
    if (!acc) return 1;

    struct rng rng = rng_make(0);
    for (size_t ts = 0; ts < seconds; ++ts) {
        for (size_t i = 0; i < keys_per_sec; ++i) {
            uint64_t key = rng_gen_val(&rng, 0, keys_range);

            for (size_t j = 0; j < vals_per_key; ++j) {
                uint64_t val = rng_gen_val(&rng, 0, vals_range);
                rill_acc_ingest(acc, key, val);
            }
        }

        if (ts % rotation_rate == 0) {
            if (!rill_rotate("db", ts)) return 0;
        }
    }

    rill_acc_close(acc);
    if (!rill_rotate("db", seconds + 60 * 60)) return 1;

    return 0;
}
