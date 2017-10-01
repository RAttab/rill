/* rill_rotate.c
   Rémi Attab (remi.attab@gmail.com), 20 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <time.h>
#include <stdio.h>

int main(int argc, const char **argv)
{
    if (argc != 2) {
        fprintf(stderr, "./rill_rotate <path>\n");
        return 1;
    }

    struct timespec ts;
    (void) clock_gettime(CLOCK_REALTIME, &ts);

    printf("rotating '%s' at '%lu'\n", argv[1], ts.tv_sec);
    if (!rill_rotate(argv[1], ts.tv_sec)) rill_exit(1);

    return 0;
}

