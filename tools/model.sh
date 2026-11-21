#!/bin/bash
# model.sh NAME=SRC_DIR ... : a repeatable cost figure for each source tree, from cachegrind on the dev set
# with one thread:   cycles ~= instructions / 3 + 18 * mispredicted branches   (a 4-wide core like Cascade Lake)
# Figures are per event line of the dev set (1.07M lines). SRC_DIR/main.cpp is built with -march=skylake,
# because cachegrind cannot run AVX-512: every AVX-512 helper in the sources has an AVX2 fallback for this.
T=$(cd "$(dirname "$0")" && pwd); R=$(dirname "$T"); D=${SS_DATA:-$R/data}
for a in "$@"; do
  name=${a%%=*}; S=${a#*=}
  g++ -std=c++17 -O2 -g -fno-tree-vectorize -march=skylake -mtune=skylake -static -pthread -o /tmp/ss_m_$name "$S/main.cpp" 2> /dev/null || { echo "$name: build failed"; continue; }
  rm -rf /tmp/bo_m; mkdir -p /tmp/bo_m
  SS_THREADS=1 valgrind --tool=cachegrind --branch-sim=yes --cache-sim=no --cachegrind-out-file=/tmp/cgm_$name.out /tmp/ss_m_$name "$D/dev1" /tmp/bo_m > /tmp/cgm_$name.log 2>&1
  ir=$(grep "I refs" /tmp/cgm_$name.log | sed 's/.*: *//; s/,//g'); bm=$(grep "Mispredicts" /tmp/cgm_$name.log | sed -E 's/.*Mispredicts: *([0-9,]+).*/\1/; s/,//g')
  python3 -c "
ir=$ir; bm=$bm; n=1.07e6
print('%-10s instr %7.1fM (%6.1f/line)  mispred %5.2fM (%4.2f/line)  model %7.1fM cycles (%5.1f/line)' % ('$name', ir/1e6, ir/n, bm/1e6, bm/n, (ir/3+18*bm)/1e6, (ir/3+18*bm)/n))"
done
