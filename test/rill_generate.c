#include "test.h"

static struct rill_store *make_store(
    const char *name, struct rill_pairs *pairs)
{
    unlink(name);
    assert(rill_store_write(name, 0, 0, pairs));
    return 0;
}

bool generate()
{
    struct rng rng = rng_make(0);
    struct rill_pairs *pairs = make_rng_pairs(&rng);
    make_store("test.store.generated", pairs);
    rill_pairs_free(pairs);
    return true;
}

bool generate_simple()
{
    const size_t len = 20;
    struct rill_pairs *pairs = rill_pairs_new(len);

    for (size_t i = 0; i < len; ++i) {
        rill_pairs_push(pairs, i + 1, 3 + i * 20);
    }

    make_store("test.store.simple", pairs);
    rill_pairs_free(pairs);

    return true;
}

bool generate_with_multiple_values()
{
    const size_t len = 100;
    struct rill_pairs *pairs = rill_pairs_new(len);

    for (size_t i = 0; i < 20; ++i)
        for (size_t j = 1; j < 5; ++j)
            rill_pairs_push(pairs, i + 1, j * 100);

    make_store("test.store.multvals", pairs);
    rill_pairs_free(pairs);

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
