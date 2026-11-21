# Test and measurement tools

Everything a change has to pass before its commit is offered for scoring, and the tools used to decide whether a
change is worth making. All scripts find the repo from their own location and take the test data from `$SS_DATA`
(default `<repo>/data`, which is git-ignored).

## One-time setup

```sh
# 1. the organisers' generator (generator.zip on the track page; Rust 1.85+)
cargo build --release            # in the unzipped generator folder
export GENERATE=/path/to/target/release/generate     # or put `generate` on PATH

# 2. the published dev answer (dev-answer.zip on the track page)
mkdir -p data && unzip dev-answer.zip -d data/       # must end up as data/dev-answer/*.csv

# 3. test data
tools/mkdata.sh small            # dev set + 21 seeds with reference outputs (a few minutes)
tools/mkdata.sh all              # + 20M and 40M sets with golden outputs (needs ~6 GB disk, ~5 GB RAM)
```

`pip install polars` for the comparison scripts. A 60M-event set cannot be generated on an 8 GB machine (the
generator alone needs more than 6 GB); 48M works.

## Before every push

```sh
make native                                   # bin/shopping-sessions
tools/verify.sh bin/shopping-sessions full    # self-test, 36 fixtures, dev answer, 21 seeds, 20M + 40M at 1/3/8 threads
tools/mut/loop.sh bin/shopping-sessions data/s7_e200000_c50000 1 3     # 36 damaged data sets, see below
tools/mut/loop.sh bin/shopping-sessions data/s2_e200000_c50000 1 3
g++ -std=c++17 -O2 -fno-tree-vectorize -march=cascadelake -mtune=cascadelake -static -pthread -o /tmp/ss src/main.cpp
tools/ctest.sh /tmp/ss ss-test                # the static binary in a container with the scorer's flags
make fuzz                                     # only when src/scan.h changed
```

`verify.sh BIN quick` skips the two big sets and takes about a minute; use it while iterating.

### The mutation suite

The dense route (`src/dense.h`) relies on the shape the generator gives the data and checks that shape at run time.
`tools/mut/mutate.py` copies a generated set and breaks one assumption at a time (users swapped between events,
timestamps moved, links added or removed, upper-cased and malformed IDs, aliases outside the linked set, prices
too large for the compact record, earlier copies of events under another user, exact repeats). `loop.sh` runs every
mode for a range of seeds and compares the binary's output with the general route (`SS_DENSE=0`, or the binary in
`$SS_ORACLE`). Any change to the dense route needs this, and any new assumption needs a new mode that breaks it.

## Measuring

| Tool | What it gives |
| --- | --- |
| `tools/model.sh a=SRC_A b=SRC_B` | Instructions and branch mispredicts per event line (cachegrind, dev set, 1 thread) and a modelled cycle count. Repeatable to the instruction, so it resolves changes of 1% |
| `tools/cgfull.sh SRC 40` | The same profile by function |
| `tools/cgl.py` | The profile of one function line by line (usage in `cgfull.sh`) |
| `tools/fb.py ROUNDS BIN...` | Wall time, CPU seconds per phase and peak memory, binaries interleaved, median and minimum |
| `SS_LOG=1 BIN DATA OUT` | Phase timings, which route ran, and the reason when the dense route gave up |
| `tools/difftest.sh BIN SEED EVENTS` | One more seed against the Python reference |

Two cautions. The model is built with `-march=skylake` (cachegrind cannot run AVX-512), so every AVX-512 helper
in the sources keeps an AVX2 fallback. And local timings mislead for changes of the instruction mix: the scorer
is a Cascade Lake Xeon, where gather instructions are slow and 512-bit code lowers the clock, while the usual
development machine is a newer CPU with neither problem.

## Switches of the binary

`SS_LOG=1` log to stderr; `SS_THREADS=n`; `SS_DENSE=0` general route only; `SS_DENSE_FAIL=parse|scan` give up the
dense route after that phase (tests the way back); `--selftest` kernels against scalar references.
