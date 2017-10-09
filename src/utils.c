/* utils.c
   Rémi Attab (remi.attab@gmail.com), 17 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <errno.h>
#include <stdlib.h>
#include <stdarg.h>
#include <dirent.h>
#include <unistd.h>


// -----------------------------------------------------------------------------
// error
// -----------------------------------------------------------------------------

__thread struct rill_error rill_errno = { 0 };

void rill_abort()
{
    rill_perror(&rill_errno);
    abort();
}

void rill_exit(int code)
{
    rill_perror(&rill_errno);
    exit(code);
}

size_t rill_strerror(struct rill_error *err, char *dest, size_t len)
{
    if (!err->errno_) {
        return snprintf(dest, len, "%s:%d: %s\n",
                err->file, err->line, err->msg);
    }
    else {
        return snprintf(dest, len, "%s:%d: %s - %s(%d)\n",
                err->file, err->line, err->msg,
                strerror(err->errno_), err->errno_);
    }
}

void rill_perror(struct rill_error *err)
{
    char buf[128 + rill_err_msg_cap];
    size_t len = rill_strerror(err, buf, sizeof(buf));

    if (write(2, buf, len) == -1)
        fprintf(stderr, "rill_perror failed: %s", strerror(errno));
}


void rill_vfail(const char *file, int line, const char *fmt, ...)
{
    rill_errno = (struct rill_error) { .errno_ = 0, .file = file, .line = line };

    va_list args;
    va_start(args, fmt);
    (void) vsnprintf(rill_errno.msg, rill_err_msg_cap, fmt, args);
    va_end(args);
}

void rill_vfail_errno(const char *file, int line, const char *fmt, ...)
{
    rill_errno = (struct rill_error) { .errno_ = errno, .file = file, .line = line };

    va_list args;
    va_start(args, fmt);
    (void) vsnprintf(rill_errno.msg, rill_err_msg_cap, fmt, args);
    va_end(args);
}


// -----------------------------------------------------------------------------
// scan_dir
// -----------------------------------------------------------------------------

static bool is_rill_file(const char *name)
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
        rill_fail_errno("unable to open dir '%s'", dir);
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
            rill_fail("rotate: too many files to rotate in '%s'", dir);
            break;
        }
    }

    closedir(dir_handle);
    return len;
}


// -----------------------------------------------------------------------------
// tracer
// -----------------------------------------------------------------------------

static FILE *ftrace = NULL;
static const char *trace_file = "/tmp/rill.trace";

void rill_vtrace(const char *fn, int line, const char *title, const char *fmt, ...)
{
    if (!ftrace) ftrace = fopen(trace_file, "w");

    if (!ftrace) {
        rill_fail_errno("unable to open '%s'", trace_file);
        rill_abort();
    }

    char buffer[1024];

    va_list args;
    va_start(args, fmt);
    (void) vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);

    fprintf(ftrace, "%s:%d %s %s\n", fn, line, title, buffer);
}
