#! /usr/bin/env bash

set -o errexit -o nounset -o pipefail -o xtrace

: ${PREFIX:="."}

declare -a SRC
SRC=(htable rng utils rows store acc rotate query)

declare -a BIN
BIN=(load dump query rotate ingest merge count)

declare -a TEST
TEST=(index coder store)

CC=${OTHERC:-gcc}
LEAKCHECK_ENABLED=${LEAKCHECK_ENABLED:-}

CFLAGS="-ggdb -O3 -march=native -pipe -std=gnu11 -D_GNU_SOURCE"
CFLAGS="$CFLAGS -I${PREFIX}/src"

CFLAGS="$CFLAGS -Werror -Wall -Wextra"
CFLAGS="$CFLAGS -Wundef"
CFLAGS="$CFLAGS -Wcast-align"
CFLAGS="$CFLAGS -Wwrite-strings"
CFLAGS="$CFLAGS -Wunreachable-code"
CFLAGS="$CFLAGS -Wformat=2"
CFLAGS="$CFLAGS -Wswitch-enum"
CFLAGS="$CFLAGS -Wswitch-default"
CFLAGS="$CFLAGS -Winit-self"
CFLAGS="$CFLAGS -Wno-strict-aliasing"
CFLAGS="$CFLAGS -fno-strict-aliasing"
CFLAGS="$CFLAGS -Wno-implicit-fallthrough"

OBJ=""
for src in "${SRC[@]}"; do
    $CC -c -o "$src.o" "${PREFIX}/src/$src.c" $CFLAGS
    OBJ="$OBJ $src.o"
done
ar rcs librill.a $OBJ

for bin in "${BIN[@]}"; do
    $CC -o "rill_$bin" "${PREFIX}/src/rill_$bin.c" librill.a $CFLAGS
done

for test in "${TEST[@]}"; do
    $CC -o "test_$test" "${PREFIX}/test/${test}_test.c" librill.a $CFLAGS
    "./test_$test"
done

# this one takes a while so it's usually run manually
$CC -o "test_rotate" "${PREFIX}/test/rotate_test.c" librill.a $CFLAGS


if [ -n "$LEAKCHECK_ENABLED" ]; then
    for test in "{TEST[@]}"; do
        valgrind \
            --leak-check=full \
            --track-origins=yes \
            --trace-children=yes \
            --error-exitcode=1 \
            "./test_$test"
    done
fi
