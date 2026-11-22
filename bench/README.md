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

## Bench 10 (A, branch a-bench10): why the scan gains nothing from the second hyperthread, and the scatter

Benches 8 and 9 left two leads. Four threads (one per core) scan as fast as eight, so two threads on a core
get in each other's way there; and the scatter of the records runs at about three times the cycles per
instruction of the stages around it. Fourteen builds, each a small change to v4 286f27c:

- `j` v4 8384d32 (scored 0.38), `k` v4 286f27c (j plus the segment ends as a pass of their own).
- `n` 286f27c with the knobs of `bench/variants/286f27c-knobs/knobs.py`, all at their defaults (should equal k),
  and the same binary with one knob set: `pw` (`SS_PW=1`: a record's staging line is asked for, for writing,
  when its partition is known), `lw` (`SS_LW=1`: load_clicks asks for the lines its keys go to), `p0` / `p2`
  (`SS_PF`: place() and load_clicks ask for no block ahead / for the block after the next), `h2` / `h3`
  (`SS_PFH`: the next block with prefetcht2 / prefetchnta), `s4` / `s6` (`SS_ST`: the scan's segments on 4
  threads pinned one to a core / on 6 threads).
- `e13`, `e12`: event partitions of 2^13 and 2^12 indexes (slot tables of 128 and 64 KB), segments of the same
  number of events as in k. `c32`: parse batches of 32 lines.

Every build has a profile of its own (`pgobuild.sh`). The rows are short so that all of them fit the message:

- `!0`: the machine facts first (bench 8's were cut off): `up=` ms from the container's hostname file, hosts
  file and cgroup to the start of the process, `mnt=` file systems, `thp=`, the CPUs and memory nodes; its
  `top_category` has the CPU count, kernel, cgroup limits and the dev run.
- `!1 20M`, `!2 60M`: per build `tag=wall/parse/scan`: median wall ms, then the medians of the parse's and the
  scan's user CPU in ms. A letter after it is the route if it was not the dense one, `!n` counts failed runs,
  `,n` the good runs if a round did not run. `top_category` has the timelines of `BENCH_TIMELINE`'s builds.
- `!3`: the whole bench in seconds, CPU throttling, and the generation of the sets.

## Bench 11 (A, branch a-bench11): the new head at 60M, and larger event partitions with the scan on 4 threads

Same harness and rows as bench 10. Eleven builds: `j` v4 8384d32 (0.38), `k` v4 286f27c, `m` v4 cf4452a (head:
segment ends taken as soon as their segments are through, the largest click partitions loaded once), `w` (m
with the partitions' record counts taken in parallel), `n` (cf4452a with the knobs of
`bench/variants/cf4452a-knobs/knobs.py` at their defaults) and the same binary as `nb` (no click partition
shared), `g5` / `g20` (segments of 5 / 20 partitions), `s4` (the scan on 4 pinned threads); `e15` (event
partitions of 2^15 indexes: half the staging lines, 512 KB slot tables) and `e154` (e15 with the scan on 4
pinned threads, so one slot table per core). Four rounds at both sizes.

## Bench 13 (A, branch a-bench13): partition sizes on the new head, and clicks placed by click index

Same harness as bench 11; an entry in rows `!1` and `!2` is now `tag=wall/parse CPU/scan CPU/system CPU`, all
in ms (medians; the first two CPU numbers are user time of those phases, the last the system time of the whole
process). Twelve builds, four rounds at both sizes, timelines of `x`, `k10`, `c12`, `c13`.

- `x` v4 2589f28 (head) and `m` v4 cf4452a (0.38), as their Dockerfiles build them.
- `x14`, `x16`: the head with event partitions of 2^14 and 2^16 indexes (256 KB and 1 MB slot tables; bench 11
  had 2^15 against 2^14 on cf4452a, which still prefetched blocks).
- `k9`, `k10`: the head with click partitions of 512 and 1,024 persons. At 60M there are 7,240 click
  partitions of 256 persons, so a thread's click staging lines take 463 KB, more than the events' 124 KB.
- `c11`, `c12`, `c13`: the branch `a-ckidx` (sources in `bench/variants/ckidx`), which gives up the person
  partitions for clicks: click_ids are unscrambled to click indexes, which the generator hands out person by
  person, and a click partition is 2^11, 2^12 or 2^13 consecutive indexes, placed in a slot table like the
  events. In the cost model its click path has more instructions than load_clicks, and here its scan is no
  faster; what it can win is the scatter (1,900 partitions at 60M with 2^13).
- `g3`, `g10`: the head with segments of 3 and 10 event partitions; `c12g`: c12 with segments of 10.

The knob builds are `bench/variants/2589f28-knobs/knobs.diff` on 2589f28 with `-DSS_KE=` / `-DSS_KC=`, each
with a profile of its own (`pgobuild.sh`). Every build matches the references here (66 runs over segment sizes
and thread counts, the 20M set at 8 threads).

## Bench 15 (A, branch a-bench15): the start, the malloc heaps and the staging prefetch on the head

Rows as in bench 13 (`tag=wall/parse CPU/scan CPU/system CPU`, ms, medians). Eight builds, eight rounds at 20M
and six at 60M, timelines of `c`, `th`, `thp8`.

- `c` and `c2`: v4 226ca9f (head) twice, the same binary: their difference is the noise. `t`: v4 6d5b17b (0.3641).
- `s`: c with the serial start shortened (the user key from 512 sampled lines instead of 4,096; the event key's
  sample is taken by the setup thread while the links are read): about 1 ms here.
- `tp`: s with `mallopt(M_TOP_PAD, 64 MB)` at the start of main, which is bench 12's `vt` built in (4 ms at 20M
  and 12 ms at 60M there, on 8384d32).
- `th`: tp with `madvise(MADV_HUGEPAGE)` on each worker thread's malloc heap. The heaps hold 80 to 90 MB at
  20M, touched 4 KB at a time and all still there at exit, where the kernel frees them on one CPU: the benches
  have 5 to 8 ms between the in-process end and the process being gone. Here with 8 threads at 20M: exit 4.6
  to 5.8 ms without, 3.1 to 3.8 ms with; system time 0.43 s with the top pad alone, 0.27 s with both.
- `thp4`, `thp8`: th with B's staging-line prefetch (`SS_PFD` of b-pf2, commit 6a0c613 applied on 226ca9f) 4
  and 8 records ahead: bench 12 measured it on 8384d32, which had twice the event staging lines.

`bench/variants/226ca9f-heap/start-and-heap.diff` is the change of `s`, `tp`, `th` on 226ca9f (`-DSS_TOPPAD_MB=64`,
`-DSS_HEAPHUGE`); each build has a profile of its own. Every build matches the references here (66 runs over
segment sizes and thread counts, the 20M set at 8 threads).

## Bench 18 (A, branch a-bench18): v5 data (opaque IDs), the general route

`bench/generate` is now the v5 generator (shopping-sessions-5), built static the same way; it writes the v5 dev
set byte for byte. An entry in rows `!1` and `!2` is `tag=wall/parse/dedup/person/system`, all ms (medians;
the middle three are user CPU of those phases). The timeline's fourth number is now `dedup + routing done`.
Eight builds, five rounds at 20M and four at 60M, timelines of `h`, `f`, `r16`.

- `h` 415ae10 (scored 0.5600), `f` ff33593, `f2` the same binary as f (the noise).
- `p13`: f with up to 2^13 event partitions instead of 2^12 (dedup partitions half the size at 60M).
- `pop`: f with `madvise(MADV_POPULATE_READ)` on each parse chunk before it is read.
- `r16`: **timing only, wrong output**: f with 16-byte event records (event_id hash and user hash only; no ts,
  payload or ingest_ts), to see what halving the event records' memory traffic is worth in parse and dedup
  before building it for real. Its person phase is not comparable (every event at ts 0).
- `f4`, `f16`: f on 4 and 16 threads.

## Bench 19 (A, branch a-bench18): links overlapped with the event parse, partition and chunk sizes

Rows as in bench 18. Bench 18 found: 415ae10 = ff33593 = itself within 1% (two copies of ff33593: 616 / 618 ms
at 20M, 2001 / 1986 at 60M); 2^13 event partitions +3% at 60M; MADV_POPULATE_READ nothing; 4 threads +25%,
16 threads +11%; 16-byte event records (timing only) -18% at 20M and -19% at 60M, of which parse -87 ms and
dedup -121 ms wall at 60M. At 60M the timeline was links 102 ms, parse to 999, dedup to 1529, output at 1990.

- `f` ff33593; `ov` f with the link lines parsed in the same pool run as the event lines (they do not need H;
  the click lines, which do, come after the links are finished), `ov2` the same binary (noise).
  Source: `bench/variants/ff33593-ov.diff` (also adds the knobs below, read from the environment).
- `ovP2` / `ovp2`: person partitions doubled / halved (`SS_P2ADJ`); `ovp1`: event-id partitions halved.
- `ovc4` / `ovc16`: parse chunks of 4 / 16 MB.

## Bench 20 (A): the event emit loop with mask registers, and -O3

Bench 19: every build within 1% of f at both sizes (links overlapped, partition counts, chunk sizes): none kept
except the overlap, which is neutral. `ve` (`bench/variants/ff33593-ve.diff`): f + the overlap + the validity
checks of emit_event_columns eight lines at a time (AVX-512VL masks, type bytes gathered), one 32-byte store
per record. `ve2` the same binary, `ve3` ve at -O3.

## Bench 21 (A): what removing the dedup pass could win

`nd` is **timing only, wrong output** (duplicates are counted): ve with the links finished before the event
parse, each event line routed to its person at parse (as the clicks are), and no dedup pass. It is the upper
bound of a design that removes duplicates inside the person phase.

## Bench 22 (A): the PQ route

Bench 20: ve = f within 1% (fewer instructions in the emit loop buy nothing: the parse is not bound by them).
`pq` (`bench/variants/ff33593-pq.diff`, branch a-merge): every valid event line goes to its person at parse as a
16-byte R2 record (P), and every event line to its event-id partition as a 16-byte Q record (event-id hash,
ingest_ts in whole seconds, and where its P record is: thread, person partition, index). Dedup reads only Q and
sends the P records that lose to their person partition as kill entries, which the person phase skips. Bytes
per event: 64 moved instead of 96 (R1 32 written and read, R2 16 written and read). `pqoff` is the same
binary with SS_PQ=0 (the R1 route). `nd` as in bench 21.

## Bench 23 (A): the PQ route with fewer partitions

Bench 22: pq +5% at both sizes (parse CPU +35%: the person lookup and a third scatter stream at parse; dedup
CPU -62%); nd -17%. A parse thread then fills staging lines for R2, Q, the 33-byte Q and R2C at once (about
13k lines, 850 KB at 60M), more than L2. Here the event-id and person partitions are halved and quartered
together (SS_P1ADJ, SS_P2ADJ), on PQ and on the R1 route.

## Bench 24 (A): threads per phase

Bench 23: the PQ route stays 3 to 5% slower with fewer partitions; the R1 route with fewer partitions = f.
`ss-nt` is ff33593 with `bench/variants/ff33593-phase-threads.diff`: workers whose number is at or above
SS_DEDUP_NT / SS_PERSON_NT / SS_PARSE_NT take no tasks in that phase. Bench 18 had 4 threads for the whole run
+25%; the dedup's second pass and the person phase work in L2-sized partitions, which two hyperthreads share.
