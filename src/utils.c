/* utils.c
   Rémi Attab (remi.attab@gmail.com), 17 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <dirent.h>


// -----------------------------------------------------------------------------
// scan_dir
// -----------------------------------------------------------------------------

size_t rill_scan_dir(const char *dir, struct rill_store **list, size_t cap)
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

