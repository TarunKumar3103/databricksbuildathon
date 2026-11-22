#!/bin/bash
# pgobuild.sh TAG [extra -D flags] : clang 18 with a profile of its own for src/, as pgo/make-profile.sh does.
# Writes bin/ss_TAG. The training sets are kept in /tmp/pgo_d and /tmp/pgo_g, the work files in work/TAG.
set -eu
TAG=$1
shift
X="$*"
G=/home/claude/work/genbuild/target/release/generate
F="-std=c++17 -O2 -fno-vectorize -fno-slp-vectorize -march=cascadelake -mtune=cascadelake -static -pthread -fno-pie"
mkdir -p bin prof "work/$TAG/o1" "work/$TAG/o2"
if [ ! -d /tmp/pgo_d ]; then "$G" --seed 4242 --events 3000000 --clicks 750000 --output /tmp/pgo_d > /dev/null; fi
if [ ! -d /tmp/pgo_g ]; then "$G" --seed 4243 --events 1000000 --clicks 250000 --output /tmp/pgo_g > /dev/null; fi
W="work/$TAG"
clang++-18 $F $X -fprofile-instr-generate -fprofile-update=atomic -o "$W/train" src/main.cpp
SS_THREADS=8 LLVM_PROFILE_FILE="$W/d-%p.profraw" "$W/train" /tmp/pgo_d "$W/o1"
SS_THREADS=8 SS_DENSE=0 LLVM_PROFILE_FILE="$W/g-%p.profraw" "$W/train" /tmp/pgo_g "$W/o2"
llvm-profdata-18 merge -o "prof/$TAG.profdata" "$W"/*.profraw
clang++-18 $F $X -fprofile-instr-use="prof/$TAG.profdata" -Wno-profile-instr-out-of-date -Wno-profile-instr-unprofiled -o "bin/ss_$TAG" src/main.cpp
strip "bin/ss_$TAG"
echo "built bin/ss_$TAG $(sha256sum "bin/ss_$TAG" | cut -c1-12) $(stat -c %s "bin/ss_$TAG")"
