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
of links, parse, and dedup or scan, medians over the rounds. If `BENCH_TIMELINE` names a build, the timeline of
its run with the median wall time follows, as `k@20M:5/38/221/292/297/301`: the ms since the process started
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

## Bench 9 (B, branch b-probe): where the cycles go, and the floors

Four builds: `v` (v4 cf4f788, scored 0.38), `b` (b-v4n d720cb5), `s` (d720cb5 built with `-DSTAGES` and a
profile of its own, source in `bench/variants/d720cb5-stg`) and `f` (`s` with `SS_SOL=1`). `BENCH_STAGES=s`
and `BENCH_SOL=f` add one row per size, `!s1 20M ...` and `!s2 60M ...`, from the runs with the median wall
time. `s` against `b` is what the counters cost (about 2% locally).

The `user_id` of an `!s` row is build s's `STAGES` line, in thread-ms summed over all threads (time stamp
counter, so page faults inside a stage count to it): `mhz` the counter's rate, `ev`/`ck` the lines; `e1`
scanner stage 1 of the event files (masks, bit streams, separator list; the input's page faults land here),
`e2` stage 2 (columns, slow lines) without the batches, `pe` the event batches, and their parts `fe` bytes
fetched, `id` IDs, `li` lists by timestamp form, `ts` timestamp kernels, `re` records and checks, `ma` plain
and purchase masks, `sc` the scatter, `pu` purchases and slow lines; `c1 c2 pc cfe cid cli cts cam cre csc`
the same for clicks (`cam` campaign lookup); `te`/`tc` whole event and click tasks, `dr` dropping parsed pages,
`tl` link tasks; `seg` scan segments as a whole, `pla` place(), `slo` the slot loop without closing persons,
`clo` closing persons (with `lck` loading click partitions and `srt` sorting a person's clicks), `mem` clearing
the slot table, `ahd` segment ends; `busy` time inside tasks for links/parse/scan/output, `wall` ms from the
files mapped to the links done, the parse, the scan and the output.

Its `top_category` is `PERF` (cycles, instructions, IPC, branch and cache misses per phase, user space, all
threads; `err=` errno if the container forbids perf events) then `SOL`, build f's floors in wall ms with all
threads: `touch` one byte per page (the page faults alone), `read` every byte once the pages are mapped,
`masks` the scanner's three masks per 64 bytes, `cols` the event files through the column scanner with no
field work, `retouch` the faults again after the pages are dropped.

## Bench 11 (B, branch b-bench11): the scatter

Bench 9 put the scatter at 272 thread-ms of the parse's 1,387 at 20M, most of it waiting on memory. Ten
builds: `v` (v4 8384d32 as its Dockerfile, sha256 eb129ec6..., scored 0.38), `h` (v4 cf4452a as its
Dockerfile, sha256 1f8a1564...: A's two scan commits on top of v, for the 60M row A asked for), and, each with
a profile of its own made by pgo/make-profile.sh's recipe, `p4`, `p8`, `p16` (b-pf 0e5c528 built with `-DSS_PFD=4`, `8`,
`16`: each record's staging line is prefetched for writing that many records ahead, events and clicks), `z`
(b-pf 0e5c528 with `-DSS_PREFAULT=1`: each thread faults its arena in, a write per 4 KB, when the parse first
maps it, about 90% of what the parse will put there, so that the drain's non-temporal stores do not meet the
page faults one by one), `zp` (`z` with `-DSS_PFD=8`), `s` (b-pfs 8847acc, which is b-pf 43899be plus the
stage counters, with `-DSTAGES`), `ps` (`s` with `-DSS_PFD=8`) and `f` (`s` with `SS_SOL=1`).

Rows, after `!1 20M` and `!2 60M` as before:

- `!f1 20M ...` / `!f2 60M ...`: build f's floors in wall ms with all threads. Bench 9's five (`touch read
  masks cols retouch`), then the arena's: `zf` one byte written per 4 KB of fresh memory (page faults and the
  kernel's zeroing alone), `ntw` non-temporal 64-byte lines over that memory once it is faulted in, `stw`
  ordinary stores over it, `ntf` non-temporal lines into fresh memory (faults, zeroing and stores, as the
  drain meets them), `sct` the parse's own scatter (`WCScatter<D16>` and its arena) fed records of random
  partitions, as many partitions as the parse has, `sctp` the same prefetching 8 records ahead; `MB` the
  memory each of these writes, in all threads (the records of the input, capped at 384 MB). `sct` and `sctp`
  include about 8 instructions per record for the random numbers.
- `!s1 20M s:... ps:...` / `!s2 60M ...`: builds s and ps, the values of `e1/e2/pe/sc/pu/c1/pc/csc/te/tc/seg/clo/busy/wall`
  in that order (bench 9's names, thread-ms; `busy` and `wall` have four parts each).
- `!S2 60M s ...`: build s's whole `STAGES` line at 60M (bench 9's was cut off).

The PERF lines are left out (bench 9: perf events are refused, EPERM).
