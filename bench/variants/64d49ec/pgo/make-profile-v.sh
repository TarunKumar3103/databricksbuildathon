#!/bin/bash
# mkprof.sh V SRCROOT : pgo/make-profile.sh with clang V (writes SRCROOT/pgo/clangV.profdata). Same training sets.
set -eu
V=$1; R=$(cd "$2" && pwd); G=${GENERATE:?}; T=$(mktemp -d /home/claude/b7/prof$V.XXXX)
F="-std=c++17 -O2 -fno-vectorize -fno-slp-vectorize -march=cascadelake -mtune=cascadelake -static -pthread"
clang++-$V $F -fprofile-instr-generate -fprofile-update=atomic -o "$T/train" "$R/src/main.cpp"
"$G" --seed 4242 --events 3000000 --clicks 750000 --output "$T/d" > /dev/null
"$G" --seed 4243 --events 1000000 --clicks 250000 --output "$T/g" > /dev/null
mkdir -p "$T/o1" "$T/o2"
SS_THREADS=8 LLVM_PROFILE_FILE="$T/d-%p.profraw" "$T/train" "$T/d" "$T/o1"
SS_THREADS=8 SS_DENSE=0 LLVM_PROFILE_FILE="$T/g-%p.profraw" "$T/train" "$T/g" "$T/o2"
llvm-profdata-$V merge -o "$R/pgo/clang$V.profdata" "$T"/*.profraw
rm -rf "$T"
echo "wrote $R/pgo/clang$V.profdata"
