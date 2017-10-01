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
// attributes
// -----------------------------------------------------------------------------

#define rill_packed       __attribute__((__packed__))
#define rill_likely(x)    __builtin_expect(x, 1)
#define rill_unlikely(x)  __builtin_expect(x, 0)


// -----------------------------------------------------------------------------
// misc
// -----------------------------------------------------------------------------

#define array_len(arr) (sizeof((arr)) / sizeof((arr)[0]))

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


enum { page_len_s = 4096 };
static const size_t page_len = page_len_s;


// -----------------------------------------------------------------------------
// err
// -----------------------------------------------------------------------------

#define fail(fmt, ...) \
    fprintf(stderr, "[fail] "fmt"\n", __VA_ARGS__)

#define fail_errno(fmt, ...) \
    fprintf(stderr, "[fail] "fmt"(%d): %s\n", __VA_ARGS__, errno, strerror(errno))


// -----------------------------------------------------------------------------
// vma
// -----------------------------------------------------------------------------

static inline size_t to_vma_len(size_t len)
{
    if (!(len % page_len)) return len;
    return (len & ~(page_len - 1)) + page_len;
}
