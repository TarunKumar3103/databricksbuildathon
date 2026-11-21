# The scorer bench (branch `c-bench`)

**Not an entry. Never submit a `c-bench` commit for a score.** The dev check rejects it on purpose, and the
rejection message is the report.

## What one submission measures

The practice board times one run per submission, and the same image scores up to 20% apart on different
scorer machines. The final runs on about 60M events, where fresh memory costs more per MB, so the practice
board cannot say which build is fastest there. This bench answers that inside one submission, on one scorer
machine:

1. Build `a` writes the correct dev output to `/output`, so the dev check sees every expected row.
2. A row whose `user_id` starts with `!` goes into `user_daily` at once, so the run is rejected whatever
   happens next.
3. For each size (20M events and 5M clicks, then 60M and 15M; `BENCH_SIZES`, which also sets the rounds),
   `generate` writes a data set to `/scratch` and
   it is synced to disk. Every build in `variants.txt` then runs on it in a fresh process (`fork`, `execve`,
   `wait4`), one round after another, with the order rotated each round. A build is timed from `fork` to
   its exit, so process start, page faults and teardown are included. Only the container's own start-up
   (about 90 ms) is not.
4. The report replaces the first row: one row per size and one for the machine.

No new run starts after `BENCH_TMAX` seconds (150 by default), and every run is killed after 120 s.

`generate` is the organisers' generator from `generator.zip`, unchanged, linked statically so the image needs
no Rust stage: `RUSTFLAGS="-C target-feature=+crt-static -C target-cpu=x86-64-v2" cargo build --release
--target x86_64-unknown-linux-gnu`, then `strip`. It writes the dev set byte for byte as the regular build does.
sha256 `179ace9bd7e60d7932672ddf7105b83c5a56a3fe121e1b8ffc3c63d1f4cede53`.

## Reading the rows

Row `!1 20M` and row `!2 60M`, one entry per build in its `user_id`:

`a=512,u2.91,s0.62,d` means median wall 512 ms, median user CPU 2.91 s and system CPU 0.62 s, route `d` (dense;
`g` general, `f` dense given up), every round good. A number after the route counts the good runs when some
round did not run (`BENCH_TMAX`), and `!2` counts failed runs. (Benches 1 to 5 also printed peak resident
memory, as `596M`, and the good runs, as `d3`; bench 1 the best wall time, as `512/498`, and the minor page
faults, as `f150k`. Bench 6 left memory out to make room for twelve builds.) `rows=U/C` are the `user_daily`
and `campaign_daily` row counts, the same for every run unless `DIFFER` follows.

Its `top_category` holds the phase split of each build from `SS_LOG`: `a:149+1514+805` is the user CPU in ms
of links, parse, and dedup or scan, medians over the rounds. If `BENCH_TIMELINE` names a build (or several, comma-separated), the
timeline of its run with the median wall time follows, as `k@20M:5/38/221/292/297/301`: the ms since the process started
at which the log says the input was mapped, the last links line, the parse done, the dense scan done and the
output written, then the in-process time from `ABS start` to `ABS end` (after the arenas are released). The
wall time in the first field minus the last number is process start and teardown. (Bench 1 printed `a:365=149+1514+805+6+14`: the
in-process time until the output is written, then the means of all five phases.) The `sessions`, `events` and `revenue_cents` columns repeat the median wall times of the
first three builds.

The last row has the machine (`cpus`, kernel, free space on `/scratch`, cgroup `cpu.max/memory.max`, the dev
run's wall time) and, in `top_category`, each set's generation time, generator peak memory, size on disk,
dirty pages left and the time the sync took. `sessions` there is the dev run in ms and `revenue_cents` the
whole bench in seconds.

## Changing what it compares

Put each build's sources under `bench/variants/<name>/src/`, add its compile line to the `Dockerfile`, and add
a line to `variants.txt`; `bench/mkbench.py PLAN` does all three for gcc builds of any commits. A binary built
elsewhere can sit in `bench/prebuilt/` (not `bench/bin/`, which `.gitignore` hides), which is copied as it is. A build whose binary is not in the image (an
optional build stage that could not run) is reported as `tag=missing`. Builds are compared fairly only within
one run of the bench.

Phase splits are medians over the rounds (bench 1 used means).

## Testing it locally

```sh
g++ -std=c++17 -O2 -march=cascadelake -static -pthread -o bench/bench bench/driver.cpp
DATA_DIR=data/dev1 OUTPUT_DIR=/tmp/out SCRATCH_DIR=/tmp/scr BENCH_DIR=<folder with the binaries> \
    BENCH_SIZES=2:0.5:2,6:1.5:1 bench/bench
```

`BENCH_SIZES` is `events M : clicks M : rounds` per size.
