#! /usr/bin/env bash

set -o errexit -o nounset -o pipefail -o xtrace

: ${PREFIX:="."}

declare -a SRC
SRC=(htable rng utils rows store acc rotate query)
CC=${OTHERC:-gcc}

LEAKCHECK_ENABLED=${LEAKCHECK_ENABLED:-}
LEAKCHECK=${OTHERMEMCHECK:-valgrind}
LEAKCHECK_ARGS="--leak-check=full --track-origins=yes --trace-children=yes --error-exitcode=1"

CFLAGS="-g -O3 -march=native -pipe -std=gnu11 -D_GNU_SOURCE"
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

$CC -o rill_load "${PREFIX}/src/rill_load.c" librill.a $CFLAGS
$CC -o rill_dump "${PREFIX}/src/rill_dump.c" librill.a $CFLAGS
$CC -o rill_query "${PREFIX}/src/rill_query.c" librill.a $CFLAGS
$CC -o rill_rotate "${PREFIX}/src/rill_rotate.c" librill.a $CFLAGS
$CC -o rill_ingest "${PREFIX}/src/rill_ingest.c" librill.a $CFLAGS
$CC -o rill_merge "${PREFIX}/src/rill_merge.c" librill.a $CFLAGS
$CC -o rill_count "${PREFIX}/src/rill_count.c" librill.a $CFLAGS

$CC -o test_indexer "${PREFIX}/test/index_test.c" librill.a $CFLAGS && ./test_index
$CC -o test_coder "${PREFIX}/test/coder_test.c" librill.a $CFLAGS && ./test_coder
$CC -o test_store "${PREFIX}/test/store_test.c" librill.a $CFLAGS && ./test_store
$CC -o test_rotate "${PREFIX}/test/rotate_test.c" librill.a $CFLAGS

if [ -n "$LEAKCHECK_ENABLED" ]
then
    echo test_indexer =======================================
    $LEAKCHECK $LEAKCHECK_ARGS ./test_indexer
    echo test_coder =========================================
    $LEAKCHECK $LEAKCHECK_ARGS ./test_coder
    echo test_store =========================================
    $LEAKCHECK $LEAKCHECK_ARGS ./test_store
fi
