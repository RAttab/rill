#include "test.h"

static struct rill_store *make_store(
    const char *name, struct rill_rows *rows)
{
    unlink(name);
    assert(rill_store_write(name, 0, 0, rows));
    return 0;
}

bool generate()
{
    struct rng rng = rng_make(0);
    struct rill_rows *rows = make_rng_rows(&rng);
    make_store("test.store.generated", rows);
    rill_rows_free(rows);
    return true;
}

bool generate_simple()
{
    const size_t len = 20;
    struct rill_rows *rows = rill_rows_new(len);

    for (size_t i = 0; i < len; ++i) {
        rill_rows_push(rows, i + 1, 3 + i * 20);
    }

    make_store("test.store.simple", rows);
    rill_rows_free(rows);

    return true;
}

bool generate_with_multiple_values()
{
    const size_t len = 100;
    struct rill_rows *rows = rill_rows_new(len);

    for (size_t i = 0; i < 20; ++i)
        for (size_t j = 1; j < 5; ++j)
            rill_rows_push(rows, i + 1, j * 100);

    make_store("test.store.multvals", rows);
    rill_rows_free(rows);

    return true;
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    (void) argc, (void) argv;

    (void) generate();
    (void) generate_simple();
    (void) generate_with_multiple_values();

    printf("generated some rill database(s)\n");
    return 0;
}
