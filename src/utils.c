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

bool is_rill_file(const char *name)
{
    static const char ext[] = ".rill";

    size_t len = strnlen(name, NAME_MAX);
    if (len < sizeof(ext)) return false;

    return !strcmp(name + (len - sizeof(ext) + 1), ext);
}

size_t rill_scan_dir(const char *dir, struct rill_store **list, size_t cap)
{
    DIR *dir_handle = opendir(dir);
    if (!dir_handle) {
        if (errno == ENOENT) return 0;
        fail_errno("unable to open dir '%s'", dir);
        return 0;
    }

    size_t len = 0;
    struct dirent *entry = NULL;
    while ((entry = readdir(dir_handle))) {
        // I found the one filesystem that doesn't support dirent->d_type...
        if (!is_rill_file(entry->d_name)) continue;

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
