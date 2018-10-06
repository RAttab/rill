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
    months_in_expire = 16,

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
// args
// -----------------------------------------------------------------------------

inline bool rill_args_col(bool a, bool b, enum rill_col *out)
{
    if ((a && b) || (!a && !b)) return false;
    *out = a ? rill_col_a : rill_col_b;
    return true;
}
