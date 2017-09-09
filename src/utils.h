/* utils.h
   Rémi Attab (remi.attab@gmail.com), 04 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#pragma once


#include <errno.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <stdatomic.h>


// -----------------------------------------------------------------------------
// attributes
// -----------------------------------------------------------------------------

#define rill_packed       __attribute__((__packed__))
#define rill_likely(x)    __builtin_expect(x, 1)
#define rill_unlikely(x)  __builtin_expect(x, 0)


// -----------------------------------------------------------------------------
// misc
// -----------------------------------------------------------------------------

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
// lock
// -----------------------------------------------------------------------------

typedef atomic_size_t lock_t;

static inline void lock(atomic_size_t *l)
{
    bool ret = false;
    uint64_t old;

    do {
        old = atomic_load_explicit(l, memory_order_relaxed);
        if (old) continue;

        ret = atomic_compare_exchange_weak_explicit(l, &old, 1,
                memory_order_acquire, memory_order_relaxed);
    } while (!ret);
}


static inline void unlock(atomic_size_t *l)
{
    atomic_store_explicit(l, 0, memory_order_release);
}
