#!/bin/bash
# cgfull.sh SRC_DIR [N] : instructions and mispredicts of the whole pipeline on the dev set (1 thread), by function
# (top N). Leaves the raw profile in /tmp/cgfull.out; for one function line by line:
#   cg_annotate /tmp/cgfull.out --show=Ir,Bcm > /tmp/annot.txt
#   python3 tools/cgl.py /tmp/annot.txt SRC_DIR/dense.h 1.07e6 'first line pattern' 'last line pattern'
T=$(cd "$(dirname "$0")" && pwd); R=$(dirname "$T"); D=${SS_DATA:-$R/data}
S=$1; N=${2:-22}
g++ -std=c++17 -O2 -g -fno-tree-vectorize -march=skylake -mtune=skylake -static -pthread -o /tmp/ss_cgf "$S/main.cpp" || exit 1
rm -rf /tmp/bo_m; mkdir -p /tmp/bo_m
SS_THREADS=1 valgrind --tool=cachegrind --branch-sim=yes --cache-sim=no --cachegrind-out-file=/tmp/cgfull.out /tmp/ss_cgf "$D/dev1" /tmp/bo_m > /tmp/cgfull.log 2>&1
grep -E "I refs|Mispredicts" /tmp/cgfull.log
cg_annotate /tmp/cgfull.out --show=Ir,Bcm --sort=Ir 2> /dev/null | sed -n '/Function:file summary/,/Annotated source/p' | grep "^>" | sed -E 's/std::vector<MFile, std::allocator<MFile> >&/V/g; s/std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >/string/g' | cut -c1-200 | head -$N
