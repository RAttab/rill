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

enum
{
    hours = 24,
    days = 31,
    months = 13,
};

enum
{
    min = 60,
    hour = 60 * min,
    day = hours * hour,
    month = days * day,
    expiration = months * month,
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
