#! /usr/bin/env bash

set -o errexit -o nounset -o pipefail -o xtrace

: ${PREFIX:="."}

declare -a SRC
SRC=(htable rng pairs store rill)

CFLAGS="-g -O3 -march=native -pipe -std=gnu11 -D_GNU_SOURCE"
CFLAGS="$CFLAGS -I${PREFIX}/src"

CFLAGS="$CFLAGS -Werror -Wall -Wextra"
CFLAGS="$CFLAGS -Wundef -Wcast-align -Wwrite-strings -Wunreachable-code -Wformat=2"
CFLAGS="$CFLAGS -Wswitch-enum -Wswitch-default -Winit-self -Wno-strict-aliasing"
CFLAGS="$CFLAGS -fno-strict-aliasing"

OBJ=""
for src in "${SRC[@]}"; do
    gcc -c -o "$src.o" "${PREFIX}/src/$src.c" $CFLAGS
    OBJ="$OBJ $src.o"
done
ar rcs librill.a $OBJ

gcc -o rill_load "${PREFIX}/src/load.c" librill.a $CFLAGS
gcc -o rill_query "${PREFIX}/src/query.c" librill.a $CFLAGS
gcc -o rill_dump "${PREFIX}/src/dump.c" librill.a $CFLAGS

gcc -o test_coder "${PREFIX}/test/coder_test.c" librill.a $CFLAGS && ./test_coder
gcc -o test_rill "${PREFIX}/test/rill_test.c" librill.a $CFLAGS && ./test_rill
