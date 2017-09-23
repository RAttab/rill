/* rill_historical_dump.c
   Rémi Attab (remi.attab@gmail.com), 22 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/


#include "rill.h"
#include "htable.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

static const uint64_t val_mask = 1UL << 63;

static void read_utf8(int fd, char *out, size_t to_read)
{
    size_t len = 0;
    for (size_t i = 0; i < to_read; ++i) {
        uint8_t c;
        assert(read(fd, &c, sizeof(c)) > 0);
        out[len++] = c;

        size_t extra = 0;
        if (c >= 0xc0) extra++;
        if (c >= 0xe0) extra++;
        for (size_t j = 0; j < extra; ++j) {
            assert(read(fd, &c, sizeof(c)) > 0);
            out[len++] = c;
        }
    }

    out[len++] = 0;
}

static void read_table(const char *file, struct htable *table)
{
    htable_reset(table);

    int fd = open(file, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "file not there %s: %s", file, strerror(errno));
        abort();
    }

    ssize_t ret;
    while (true) {
        uint64_t len = 0;
        assert(ret = read(fd, &len, sizeof(len)) >= 0);
        if (!ret || !len) break;

        char *name = calloc(len * 4 + 1, sizeof(*name));
        read_utf8(fd, name, len);

        uint64_t key = 0;
        assert(read(fd, &key, sizeof(key)) > 0);

        uint64_t type = 0;
        assert(read(fd, &type, sizeof(type)) > 0);

        assert ((key & val_mask) == 0);
        if (type == 12) key |= val_mask;
        else assert(type == 13);

        {
            uint64_t val = key & ~val_mask;
            size_t type = key & val_mask ? 1 : 0;
            static const char *type_str[] = {"set", "rov"};
            printf("%s %lu -> %s\n", type_str[type], val, name);
        }

        assert(htable_put(table, key, (uint64_t)name).ok);
    }
}

void dump_store(const char *file, struct htable *table)
{
    struct rill_store *store = rill_store_open(file);
    assert(store);

    rill_store_print_head(store);

    rill_key_t current = 0;
    struct rill_kv kv = {0};
    struct rill_store_it *it = rill_store_begin(store);
    while (rill_store_it_next(it, &kv)) {
        if (rill_kv_nil(&kv)) break;

        if (kv.key != current) {
            current = kv.key;
            printf("%p:\n", (void *) kv.key);
        }

        uint64_t val = kv.val & ~val_mask;
        size_t type = kv.val & val_mask ? 1 : 0;
        static const char *type_str[] = {"set", "rov"};
        printf("    %s %lu", type_str[type], val);

        struct htable_ret ret = htable_get(table, kv.val);
        if (ret.ok) printf(" -> %s", (char *) ret.value);
        printf("\n");
    }

    rill_store_close(store);
}

int main(int argc, char **argv)
{
    const char *store_file = NULL;
    const char *table_file = NULL;

    if (argc != 3) {
        fprintf(stderr,
                "invalid number of arguments\n"
                "    rill_historical_dump <store> <table>");
        return 1;
    }

    table_file = argv[argc - 1];
    store_file = argv[argc - 2];

    struct htable table = {0};
    read_table(table_file, &table);
    dump_store(store_file, &table);

    return 0;
}
