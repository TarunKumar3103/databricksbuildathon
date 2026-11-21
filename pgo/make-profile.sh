#!/bin/bash
# make-profile.sh : rebuild pgo/clang18.profdata for the current src/. Run it after every source change; a stale
# profile still builds (the Dockerfile silences the warnings) but loses the gain where the code changed.
# Needs clang 18.1.x with its profile runtime (Ubuntu 24.04: clang-18 libclang-rt-18-dev llvm-18) and the
# organisers' generator as $GENERATE. The training data is generated, never the dev or practice set.
set -eu
R=$(cd "$(dirname "$0")/.." && pwd); G=${GENERATE:-generate}; T=$(mktemp -d)
F="-std=c++17 -O2 -fno-vectorize -fno-slp-vectorize -march=cascadelake -mtune=cascadelake -static -pthread -fno-pie"
clang++ $F -fprofile-instr-generate -fprofile-update=atomic -o "$T/train" "$R/src/main.cpp"
"$G" --seed 4242 --events 3000000 --clicks 750000 --output "$T/d" > /dev/null   # dense route
"$G" --seed 4243 --events 1000000 --clicks 250000 --output "$T/g" > /dev/null   # general route (the fallback)
mkdir -p "$T/o1" "$T/o2"
SS_THREADS=8 LLVM_PROFILE_FILE="$T/d-%p.profraw" "$T/train" "$T/d" "$T/o1"
SS_THREADS=8 SS_DENSE=0 LLVM_PROFILE_FILE="$T/g-%p.profraw" "$T/train" "$T/g" "$T/o2"
llvm-profdata merge -o "$R/pgo/clang18.profdata" "$T"/*.profraw
rm -rf "$T"
echo "wrote $R/pgo/clang18.profdata"
