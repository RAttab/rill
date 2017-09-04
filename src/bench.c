/* bench.c
   Rémi Attab (remi.attab@gmail.com), 04 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"

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

        char file[NAME_MAX];
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

    struct rill *db = rill_open("db");
    if (!db) return 1;

    enum { max = 1000 * 1000 * 1000 };

    for (size_t i = 0; i < max; ++i) {
        if (!rill_ingest(db, i / 1000, i, i)) return 1;

        if (i % (30 * 60 * 1000) == 0) {
            if (!rill_rotate(db, i / 1000)) return 1;
        }
    }

    if (!rill_rotate(db, max + 60 * 60)) return 1;
    rill_close(db);

    return 0;
}
