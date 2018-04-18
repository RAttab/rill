/* rill_query.c
   Rémi Attab (remi.attab@gmail.com), 01 Oct 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdlib.h>
#include <getopt.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

bool is_file(const char *path)
{
    struct stat st = {0};
    stat(path, &st);
    return S_ISREG(st.st_mode);
}

void usage()
{
    fprintf(stderr, "rill_query [-k <key>|-v <val>] <db>\n");
    exit(1);
}

uint64_t read_u64(char *arg)
{
    size_t n = strnlen(arg, 128);

    bool is_hex = false;
    if (n > 2 && arg[0] == '0' && arg[1] == 'x') {
        if (n > 2 + 16) {
            rill_fail("value too big '%s'\n", arg);
            rill_exit(1);
        }

        is_hex = true;
    }

    uint64_t value = 0;

    for (size_t i = 2; i < n; ++i) {
        char c = arg[i];
        value *= is_hex ? 16 : 10;

        if (c >= '0' && c <= '9') value += c - '0';
        else if (is_hex && c >= 'a' && c <= 'f') value += c - 'a' + 10;
        else if (is_hex && c >= 'A' && c <= 'F') value += c - 'A' + 10;
        else {
            rill_fail("invalid character '%c' in '%s'\n", c, arg);
            rill_exit(1);
        }
    }

    return value;
}

int main(int argc, char *argv[])
{
    rill_key_t key = 0;
    rill_val_t val = 0;

    int opt = 0;
    while ((opt = getopt(argc, argv, "k:v:")) != -1) {
        switch (opt) {
        case 'k': key = read_u64(optarg); break;
        case 'v': val = read_u64(optarg); break;
        default: usage(); exit(1);
        }
    }

    if (key && val) { usage(); }
    if (!key && !val) { usage(); }
    if (optind >= argc) { usage(); }

    const char *db = argv[optind];
    struct rill_pairs *pairs = rill_pairs_new(100);

    if (is_file(db)) {
        struct rill_store *store = rill_store_open(db);
        if (!store) rill_exit(1);

        if (key) pairs = rill_store_query_key(store, key, pairs);
        else pairs = rill_store_query_value(store, val, pairs);

        rill_store_close(store);
    }
    else {
        struct rill_query *query = rill_query_open(db);
        if (!query) rill_exit(1);

        if (key) pairs = rill_query_key(query, key, pairs);
        else pairs = rill_query_vals(query, &val, 1, pairs);

        rill_query_close(query);
    }

    if (!pairs) rill_exit(1);

    for (size_t i = 0; i < pairs->len; ++i)
        printf("0x%lx 0x%lx\n", pairs->data[i].key, pairs->data[i].val);

    rill_pairs_free(pairs);
    return 0;
}
