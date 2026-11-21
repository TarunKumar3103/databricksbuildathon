#!/bin/sh
# Profile-guided clang 18 build of src/main.cpp into $1. Run from the repo root, as the Dockerfile does.
#
# An instrumented binary runs both routes (dense, and general with SS_DENSE=0) on a 300K-event set that the
# organisers' generator (pgo/generate, linked statically) writes here, and the real binary is then compiled with
# that profile. Anything that fails on the way (a build machine without AVX-512, a missing tool) gives the
# plain clang build instead. A profile changes code layout and branch weights only, never what the program
# computes.
set -u
OUT=$1
F="-std=c++17 -O2 -fno-vectorize -fno-slp-vectorize -march=cascadelake -mtune=cascadelake -pthread"
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT
if mkdir -p "$W/o1" "$W/o2" "$W/prof" &&
  pgo/generate --seed 7 --events 300000 --clicks 75000 --output "$W/data" > /dev/null &&
  clang++-18 $F -fprofile-instr-generate -c src/main.cpp -o "$W/main.o" &&
  clang++-18 $F -c pgo/train_exit.cpp -o "$W/te.o" &&
  clang++-18 -static -pthread -fprofile-instr-generate -Wl,--wrap=_exit -o "$W/train" "$W/main.o" "$W/te.o" &&
  LLVM_PROFILE_FILE="$W/prof/a-%p.profraw" "$W/train" "$W/data" "$W/o1" > /dev/null 2>&1 &&
  LLVM_PROFILE_FILE="$W/prof/b-%p.profraw" SS_DENSE=0 "$W/train" "$W/data" "$W/o2" > /dev/null 2>&1 &&
  llvm-profdata-18 merge -o "$W/prof.profdata" "$W"/prof/*.profraw &&
  clang++-18 $F -fprofile-instr-use="$W/prof.profdata" -Wno-profile-instr-unprofiled -Wno-profile-instr-out-of-date \
    -static -o "$OUT" src/main.cpp; then
  echo "pgo: profile-guided clang build" >&2
else
  echo "pgo: training did not work here, plain clang build" >&2
  clang++-18 $F -static -o "$OUT" src/main.cpp || exit 1
fi
strip "$OUT"
