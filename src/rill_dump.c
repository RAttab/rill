/* dump.c
   Rémi Attab (remi.attab@gmail.com), 07 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int main(int argc, char **argv)
{
    const char *file = NULL;
    bool header_only = false;

    switch (argc) {
    case 3:
        if (strcmp(argv[1], "-h") != 0) {
            printf("unknown arg '%s'", argv[1]);
            return 1;
        }
        header_only = true;
    case 2:
        file = argv[argc - 1];
        break;
    default:
        printf("you done goofed mate\n");
        return 1;
    }

    struct rill_store *store = rill_store_open(file);
    if (!store) rill_exit(1);

    rill_store_print_head(store);
    if (!header_only) rill_store_print(store);

    rill_store_close(store);

    return 0;
}
