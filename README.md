# Shopping Sessions: native C++ pipeline

One static C++17 binary (no engine, no runtime dependencies) that reads `/data` and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet, following `rules.md` exactly.

## Two routes through the same rules

**The dense route** (`src/dense.h`) is tried first. It is for data whose IDs have the shape the organisers'
generator gives them: a prefix letter and a base-36 number below 2^40, made from a running index by a fixed,
invertible mixing function with one secret key per kind of ID. The keys are found from a sample of the events
(a few million trial keys at most), and undoing the mixing turns every such ID back into its index. Then:

- Event indexes are dense and run in the order (user, time). Events are partitioned by index range, and a
  partition is put in order by writing every 16-byte record to the slot of its index: that is the dedup (the copy
  with the smallest `ingest_ts` keeps the slot) and the sort in one pass over memory that fits the L2 cache. A
  person's events then follow one another in time order, so sessions, rows and campaign counts come out of one
  sequential scan with no hash table and no sort.
- A user number is (person index << 9) + alias, and nearly every link line joins two aliases of one index.
  Such a line never reaches the link hash table: it is recorded in one 64-bit word per person index, a small
  union-find over the aliases 0..9 (four bits per alias for the smallest alias of its set, updated with
  compare-and-swap by all threads). Lines that do not fit a word (another kind of ID, two indexes, an alias
  of 10 or more) go to the general table and mark their indexes. When all links are read, an index has a
  "dense" person if it is unmarked and its linked aliases are one set, exactly the aliases 0..n-1 (or alias 0
  is in no link at all); the links inside every other word are added to the general table, which so holds a
  few thousand IDs instead of millions. Dense persons are grouped and named by index alone; their clicks are
  partitioned by person index and joined in the same scan.
- The CSV scanner hands the lines over eight at a time as columns (an 8 x 8 transpose of the separator
  offsets), so the per-line work that follows is vector work on columns: IDs are decoded four at
  a time (digits, range and leading-zero checks, the two multiplications of the inverse mixing), the lines
  are sorted by the form of their timestamp with compress-stores, the window and lateness checks are sign
  bits of a few 64-bit lanes, and what remains per line is the timestamp kernel and one store into the
  partition's staging line.

Nothing of this is taken on trust. Any key is a bijection on the ID numbers, so a wrong key cannot produce a
wrong answer, only sparse indexes. Records that do not fit (IDs of another form, link sets that mix person
indexes or contain other IDs, aliases outside the set, payloads too large for the compact record) go through
the general person phase one by one, in the same run. And if the data as a whole does not have the expected
shape (a person's events not in one run of increasing time, persons out of index order, an `ingest_ts` far
from the window, too many irregular lines), the attempt is dropped and **the general route** below redoes the
whole job. The general route alone is what runs on the fixtures; `SS_DENSE=0` forces it.

## How the general route works

1. **Lookups.** `manifest.json`, `products.csv`, `campaigns.csv`. Product and campaign IDs of the form
   `p<n>` / `c_<n>` resolve through direct arrays, anything else through a hash map.
2. **Links.** All threads parse the link files and insert the IDs into one open-addressing hash table
   (compare-and-swap on the key). The same table holds a lock-free union-find: a slot's value is its
   parent, and a join always puts the root with the byte-wise larger ID under the smaller one, so every
   root *is* the person's name. Two more parallel passes point every ID at its root and then replace the
   parent by the person. Result: a hash table `ID -> person`, plus a Bloom filter of the IDs whose
   person is not the ID itself. The small lookup files are read on a side thread meanwhile.
3. **Events and clicks.** Files are memory-mapped and split into chunks at line boundaries. A SIMD CSV
   scanner (`src/scan.h`) finds commas, newlines and quotes per 64-byte block, validates quote placement
   with bit tricks (blocks with any irregular quote fall back to an exact byte-by-byte check), flattens
   all separator offsets into one array, and splits each well-shaped line into its fields with a few
   vector operations. Event lines are then processed 64 at a time in three plain loops: keys and hashes;
   timestamps, grouped by the form of `ts` so each group runs one kernel; validation and the scatter.
   Click lines are handled the same way. Timestamps in the years the window touches take a fast path
   (one vector compare, one table lookup), both the `...Z` form and the form with a UTC offset (whose
   six offset bytes are checked and converted in one 64-bit word); everything else goes through the
   general parsers. All of them are checked against scalar references in `--selftest`. IDs up to 9 bytes are packed into
   order-preserving 64-bit keys; longer or non-ASCII IDs are interned. A key is hashed with a single
   multiplication by an odd constant: a bijection, so the hash can stand in for the key everywhere (and
   is turned back into the key for the output), with every partition and table index cut from its high
   bits. Parsed input pages are dropped from the mapping as each chunk finishes, so the kernel does not
   have to unmap the whole input serially at exit.
4. **Dedup.** Event lines are hash-partitioned by `event_id` (software write-combining with non-temporal
   stores: a push is one store into the partition's staging line, and when that fills the line, the line
   joins a small queue and the partition takes a spare one, with conditional moves instead of a branch
   on which partition filled up). Per
   partition, a bit map with 8+ bits per record finds the IDs that may occur more than once; only those
   go into a small exact table, which keeps the copy with the smallest `ingest_ts` *before* validation.
   Every other record is the only copy of its event; the pass that separates the two kinds has no
   data-dependent branch. Survivors look up their person in batches (split
   by the Bloom filter without branching, then probe the table for the flagged ones) and are
   re-partitioned by person.
   Clicks look up their person while they are parsed and are partitioned by person directly (copies are
   identical, so click dedup cannot change any result).
5. **Sessions.** Per person partition, events are counting-sorted by (person, 2^20 ms time bucket). A
   bucket spans less than 30 minutes, so each bucket lies in one session and gaps only need checking
   between consecutive buckets: no comparison sort. A person's clicks are put in time order by counting
   each click's rank with vector compares (a radix sort for long lists). The credited click of a
   session (latest in the 7 days before it, ties to the smallest `click_id`) is found by counting the
   clicks up to the session start. Assists are counted from the click's side: a click assists a run of
   the person's sessions, and the part of the run not already covered by an earlier click of the same
   campaign is added per day.
6. **Output.** A small built-in Parquet writer, one file per thread. `user_daily` is written compactly,
   because the page cache the files land in is fresh memory and that is expensive on the scorer: each
   person's `user_id` is a dictionary entry followed by one run-length run for its rows; `day`,
   `sessions` and `events` are dictionary indices bit-packed eight at a time; `top_category` is
   dictionary-encoded; only `revenue_cents` is plain. That is about 15 bytes per row instead of 33. The
   pieces are appended in their on-disk form to blocks recycled from the earlier phases and go to the
   file with `writev`. At the end all threads return their memory to the kernel together, so the process
   exits quickly.

## Build and test

```sh
make build              # docker image (gcc:13 build stage, debian:bookworm-slim run stage)
make fixtures           # all 36 fixtures through the image with scorer flags
make native fixtures-native   # same without Docker
make selftest fuzz      # SIMD parser self-test, CSV scanner fuzzer
make dev run-dev check-dev    # 1M-event dev set (needs `generate`) against dev-answer/
```

`SS_LOG=1` prints phase timings to stderr (and which route ran). `tests/reference.py` is a slow, plain Python
implementation of the rules used as an oracle for differential tests on other seeds.

`tools/` holds the whole gate a change has to pass (`verify.sh`: fixtures, dev answer, 21 reference seeds, 20M and
40M sets at several thread counts; `mut/`: data damaged on purpose, so that every assumption of the dense route is
broken at least once and the output must still equal the general route's; `ctest.sh`: the binary in a container
with the scorer's flags) and the measuring tools (a cachegrind cost model, per-function and per-line profiles,
interleaved timing). `tools/README.md` has the setup and the commands.
