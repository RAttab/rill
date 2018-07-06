/* rill_ingest.c
   Rémi Attab (remi.attab@gmail.com), 21 Nov 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <limits.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>

static inline uint64_t endian_btol(uint64_t x)
{
    return __builtin_bswap64(x);
}

struct rill_store *load_file(const char *file, rill_ts_t ts, rill_ts_t quant)
{
    printf("loading: %s\n", file);

    struct stat st = {0};
    if (stat(file, &st) == -1) {
        rill_fail_errno("unable to stat '%s'", file);
        rill_exit(1);
    }

    int fd = open(file, O_RDONLY);
    if (fd == -1) {
        rill_fail_errno("unable to open '%s'", file);
        rill_exit(1);
    }

    const int prot = PROT_READ | PROT_WRITE;
    size_t len = to_vma_len(st.st_size);

    void *ptr = mmap(0, len + page_len, prot, MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
    if (ptr == MAP_FAILED) {
        rill_fail_errno("unable to mmap anon '%p'", (void *) (len + page_len));
        rill_exit(1);
    }

    const int flags = MAP_PRIVATE | MAP_FIXED | MAP_POPULATE;
    void *data = mmap((uint8_t *)ptr + page_len, len, prot, flags, fd, 0);
    if (data == MAP_FAILED) {
        rill_fail_errno("unable to mmap fixed '%d'", fd);
        rill_exit(1);
    }

    struct rill_row *it = data;
    struct rill_row *end = it + (st.st_size / sizeof(*it));
    for (; it < end; ++it) {
        rill_val_t a = endian_btol(it->a);
        rill_val_t b = endian_btol(it->b);
        *it = (struct rill_row) { .a = a, .b = b };
    }

    struct rill_rows *rows = ((struct rill_rows *)data) - 1;
    if (!rows) rill_exit(1);

    rows->cap = rows->len = st.st_size / sizeof(rows->data[0]);
    rill_rows_compact(rows);

    char file_rill[PATH_MAX];
    snprintf(file_rill, sizeof(file_rill), "%s.rill", file);

    if (!rill_store_write(file_rill, ts, quant, rows)) rill_exit(1);
    munmap(ptr, page_len);
    munmap(data, len);

    struct rill_store *store = rill_store_open(file_rill);
    if (!store) rill_exit(1);
    return store;
}

void usage()
{
    fprintf(stderr, "rill_ingest -t <ts> -q <quant> -o <output> <files...>\n");
    exit(1);
}

int main(int argc, char **argv)
{
    rill_ts_t ts = 0;
    rill_ts_t quant = 0;
    char *output = NULL;

    int opt = 0;
    while ((opt = getopt(argc, argv, "+t:q:o:")) != -1) {
        switch (opt) {
        case 't': ts = atol(optarg); break;
        case 'q': quant = atol(optarg); break;
        case 'o': output = optarg; break;
        default: usage();
        }
    }

    if (!ts || !quant || !output) usage();
    if (optind >= argc) usage();

    struct rill_store *merge[64] = {0};

    for (; optind < argc; optind++) {
        struct rill_store *store = load_file(argv[optind], ts, quant);
        for (size_t i = 0; i < 64; ++i) {
            if (!merge[i]) { merge[i] = store; break; }

            printf("merging: %lu\n", i);

            char out[PATH_MAX];
            snprintf(out, sizeof(out), "%s.rill.%lu", argv[optind], i);

            struct rill_store *list[2] = { store, merge[i] };
            if (!rill_store_merge(out, ts, quant, list, 2)) rill_exit(1);

            store = rill_store_open(out);
            if (!store) rill_exit(1);

            merge[i] = NULL;
            rill_store_rm(list[0]);
            rill_store_rm(list[1]);

        }
    }

    if (!rill_store_merge(output, ts, quant, merge, 64)) rill_exit(1);
    for (size_t i = 0; i < 64; ++i) {
        if (!merge[i]) continue;
        rill_store_rm(merge[i]);
    }

    return 0;
}
