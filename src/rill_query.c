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


// -----------------------------------------------------------------------------
// query
// -----------------------------------------------------------------------------

static bool is_file(const char *path)
{
    struct stat st = {0};
    stat(path, &st);
    return S_ISREG(st.st_mode);
}

static void query(const char *db, enum rill_col col, rill_val_t val)
{
    struct rill_rows rows = {0};

    if (is_file(db)) {
        struct rill_store *store = rill_store_open(db);
        if (!store) rill_exit(1);

        if (!rill_store_query(store, col, val, &rows)) rill_exit(1);

        rill_store_close(store);
    }
    else {
        struct rill_query *query = rill_query_open(db);
        if (!query) rill_exit(1);

        if (!rill_query_key(query, col, val, &rows)) rill_exit(1);

        rill_query_close(query);
    }

    for (size_t i = 0; i < rows.len; ++i)
        printf("0x%lx 0x%lx\n", rows.data[i].a, rows.data[i].b);

    rill_rows_free(&rows);
}


// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

static void usage()
{
    fprintf(stderr, "rill_query -<a|b> <val> <db>\n");
    exit(1);
}

static uint64_t read_u64(char *arg)
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
    bool col_a = false;
    bool col_b = false;

    int opt = 0;
    while ((opt = getopt(argc, argv, "+ab")) != -1) {
        switch (opt) {
        case 'a': col_a = true; break;
        case 'b': col_b = true; break;
        default: usage(); exit(1);
        }
    }

    if (optind + 1 >= argc) usage();

    enum rill_col col;
    if (!rill_args_col(col_a, col_b, &col)) usage();

    rill_val_t val = read_u64(argv[optind]);
    const char *db = argv[optind + 1];

    query(db, col, val);
    
    return 0;
}
