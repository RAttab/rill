#! /usr/bin/env bash

set -o errexit -o nounset -o pipefail -o xtrace

: ${PREFIX:="."}

declare -a SRC
SRC=(pairs store rill)

CFLAGS="-g -O3 -march=native -pipe -std=gnu11 -D_GNU_SOURCE"
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

gcc -o bench "${PREFIX}/src/bench.c" librill.a $CFLAGS
