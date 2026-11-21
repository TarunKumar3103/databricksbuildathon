#!/usr/bin/env python3
"""cgl.py CG_ANNOTATE_OUT SRC N FIRST_PATTERN LAST_PATTERN : per source line Ir/N and Bcm/N for the lines of SRC between
the first line containing FIRST_PATTERN and the first later line containing LAST_PATTERN (line numbers recovered from
the '-- line N' markers). Intrinsics headers are folded into totals only."""
import re, sys
out, srcp, N, first, last = sys.argv[1], sys.argv[2], float(sys.argv[3]), sys.argv[4], sys.argv[5]
FILE = srcp.split('/')[-1]
src = open(srcp).read().split('\n')
a = next(i for i, l in enumerate(src) if first in l) + 1
b = next(i for i, l in enumerate(src) if i + 1 > a and last in l) + 1
cur = None; ln = None; rows = {}
for line in open(out):
    m = re.match(r'-- Annotated source file: (.*)', line)
    if m: cur = m.group(1).split('/')[-1]; ln = None; continue
    m = re.match(r'-- line (\d+) -+', line)
    if m: ln = int(m.group(1)); continue
    if cur != FILE or ln is None: continue
    m = re.match(r'\s*([0-9,]+)\s+\(\s*[0-9.]+%\)\s+([0-9,]+|\.)\s*(?:\(\s*[0-9.]+%\))?\s?(.*)', line)
    if m:
        ir = int(m.group(1).replace(',', '')); bm = 0 if m.group(2) == '.' else int(m.group(2).replace(',', ''))
        rows[ln] = (ir, bm)
        ln += 1
    elif re.match(r'\s+\.\s+\.\s', line) or line.strip() == '' or re.match(r'\s+\.\s', line):
        ln += 1
tot = 0; totb = 0
for l in range(a, b):
    if l in rows:
        ir, bm = rows[l]; tot += ir; totb += bm
        if ir / N >= 0.3 or bm / N >= 0.01: print(f"{l:5d} {ir/N:7.1f} {bm/N:5.2f}  {src[l-1].strip()[:110]}")
print(f"total main.cpp lines {a}..{b}: {tot/N:.1f} instr, {totb/N:.2f} mispredicts per unit")
