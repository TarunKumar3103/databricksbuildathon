#!/usr/bin/env python3
"""fb.py ROUNDS BIN... : time the whole pipeline for several binaries, interleaved in random order, and print the
median and the minimum per phase (CPU seconds summed over threads), wall time and peak memory.
env: DATA (default $SS_DATA/big20), THREADS (default 2). Local timings mislead for changes of the instruction mix:
the scorer's CPU is not this one (see the project record)."""
import os, random, re, statistics, subprocess, sys, shutil
here = os.path.dirname(os.path.abspath(__file__))
xt = os.path.join(here, 'xt')
if not os.path.exists(xt):
    subprocess.check_call(['g++', '-O2', '-o', xt, os.path.join(here, 'xt.cpp')])
R = int(sys.argv[1]); bins = sys.argv[2:]
root = os.environ.get('SS_DATA', os.path.join(os.path.dirname(here), 'data'))
D = os.environ.get('DATA', os.path.join(root, 'big20')); T = os.environ.get('THREADS', '2')
acc = {b: [] for b in bins}
env = dict(os.environ, SS_LOG='1', SS_THREADS=T, XT_SHOW='1')
random.seed(12345)
for r in range(R):
    order = bins[:]
    random.shuffle(order)
    for b in order:
        shutil.rmtree('/tmp/bo_fb', ignore_errors=True); os.makedirs('/tmp/bo_fb')
        p = subprocess.run([xt, b, D, '/tmp/bo_fb'], env=env, capture_output=True, text=True)
        m = re.search(r'user s: links ([0-9.]+) parse ([0-9.]+) dedup ([0-9.]+) person ([0-9.]+) output ([0-9.]+) \| total ([0-9.]+) +sys s: total ([0-9.]+)', p.stderr)
        w = re.search(r'wall ([0-9.]+) .*teardown ([0-9.]+) .*user ([0-9.]+) sys ([0-9.]+) maxrss (\d+)', p.stdout)
        if not m or not w:
            print('run failed', b, p.stdout[-300:], p.stderr[-300:]); sys.exit(1)
        acc[b].append([float(x) for x in m.groups()] + [float(x) for x in w.groups()])
hdr = ['links', 'parse', 'dedup', 'person', 'output', 'userT', 'sysT', 'wall', 'teardn', 'ruser', 'rsys', 'rssMB']
print(f"{'bin':16s}" + ''.join(f"{h:>8s}" for h in hdr))
for b in bins:
    cols = list(zip(*acc[b]))
    print(f"{os.path.basename(b):16s}" + ''.join(f"{statistics.median(c):8.3f}" if i < 11 else f"{statistics.median(c):8.0f}" for i, c in enumerate(cols)))
    print(f"{'  (min)':16s}" + ''.join(f"{min(c):8.3f}" if i < 11 else f"{min(c):8.0f}" for i, c in enumerate(cols)))
