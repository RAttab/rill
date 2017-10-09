/* utils.h
   Rémi Attab (remi.attab@gmail.com), 04 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#pragma once


#include <errno.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>


// -----------------------------------------------------------------------------
// compiler
// -----------------------------------------------------------------------------

#define rill_packed       __attribute__((__packed__))
#define rill_noreturn     __attribute__((noreturn))
#define rill_printf(x,y)  __attribute__((format(printf, x, y)))
#define rill_likely(x)    __builtin_expect(x, 1)
#define rill_unlikely(x)  __builtin_expect(x, 0)


// -----------------------------------------------------------------------------
// misc
// -----------------------------------------------------------------------------

#define array_len(arr) (sizeof((arr)) / sizeof((arr)[0]))


// -----------------------------------------------------------------------------
// err
// -----------------------------------------------------------------------------

void rill_abort() rill_noreturn;
void rill_exit(int code) rill_noreturn;

void rill_vfail(const char *file, int line, const char *fmt, ...)
    rill_printf(3, 4);

void rill_vfail_errno(const char *file, int line, const char *fmt, ...)
    rill_printf(3, 4);

#define rill_fail(...)                                \
    rill_vfail(__FILE__, __LINE__, __VA_ARGS__)

#define rill_fail_errno(...)                          \
    rill_vfail_errno(__FILE__, __LINE__, __VA_ARGS__)


// -----------------------------------------------------------------------------
// time
// -----------------------------------------------------------------------------

enum
{
    mins_in_hour = 60,
    hours_in_day = 24,
    days_in_week = 8, // more closely approximates a month
    weeks_in_month = 4,
    months_in_expire = 13,

    sec_secs = 1,
    min_secs = 60 * sec_secs,
    hour_secs = mins_in_hour * min_secs,
    day_secs = hours_in_day * hour_secs,
    week_secs = days_in_week * day_secs,
    month_secs = weeks_in_month * week_secs,
    expire_secs = months_in_expire * month_secs,
};


// -----------------------------------------------------------------------------
// vma
// -----------------------------------------------------------------------------

enum { page_len_s = 4096 };
static const size_t page_len = page_len_s;

static inline size_t to_vma_len(size_t len)
{
    if (!(len % page_len)) return len;
    return (len & ~(page_len - 1)) + page_len;
}


// -----------------------------------------------------------------------------
// tracer
// -----------------------------------------------------------------------------

#define trace(title, ...) \
    rill_vtrace(__FUNCTION__, __LINE__, title, __VA_ARGS__)

void rill_vtrace(
        const char *fn, int line, const char *title, const char *fmt, ...)
    rill_printf(4, 5);

#define trace_calloc(num, len)                                          \
    ({                                                                  \
        void *ret = calloc(num, len);                                   \
        trace("calloc", "%lu %lu -> %p", (size_t) num, (size_t) len, ret); \
        ret;                                                            \
    })

#define trace_realloc(ptr, len)                                         \
    ({                                                                  \
        void *ret = realloc(ptr, len);                                  \
        trace("realloc", "%p %lu -> %p", (void *) ptr, (size_t) len, ret); \
        ret;                                                            \
    })

#define trace_free(ptr)                                                 \
    ({                                                                  \
        free(ptr);                                                      \
        if (ptr) trace("free", "%p", (void *) ptr);                      \
    })

#define trace_strndup(str, len)                                         \
    ({                                                                  \
        void *ret = strndup(str, len);                                  \
        trace("strndup", "%p %lu -> %p", (void *) str, (size_t) len, ret); \
        ret;                                                            \
    })

#define trace_mmap(addr, len, prot, flags, fd, off)                     \
    ({                                                                  \
        void *ret = mmap(addr, len, prot, flags, fd, off);              \
        trace("mmap", "%p, %p, %d, %d, %d, %lu -> %p",                  \
                (void *) addr, (void *) len, prot, flags, fd, (size_t) off, ret); \
        ret;                                                            \
    })

#define trace_munmap(addr, len)                                         \
    ({                                                                  \
        int ret = munmap(addr, len);                                    \
        trace("munmap", "%p, %p -> %d",(void *) addr, (void *) len, ret); \
        ret;                                                            \
    })
