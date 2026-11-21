# Shopping Sessions: native C++ pipeline

One static C++17 binary (no engine, no runtime dependencies) that reads `/data` and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet, following `rules.md` exactly.

## How it works

1. **Lookups.** `manifest.json`, `products.csv`, `campaigns.csv`. Product and campaign IDs of the form
   `p<n>` / `c_<n>` resolve through direct arrays, anything else through a hash map.
2. **Links.** Parsed in parallel; distinct IDs are deduplicated per hash partition, then joined with a
   lock-free union-find that always links the root with the byte-wise larger ID under the smaller one,
   so every root *is* the person's name. Result: a hash table `ID -> person`, plus a Bloom filter of the
   IDs whose person is not the ID itself.
3. **Events and clicks.** Files are memory-mapped and split into chunks at line boundaries. A SIMD CSV
   scanner (`src/scan.h`) finds commas, newlines and quotes per 64-byte block, validates quote placement
   with bit tricks (blocks with any irregular quote fall back to an exact byte-by-byte check), flattens
   all separator offsets into one array, and splits each well-shaped line into its fields with a few
   vector operations. Event lines are then processed 64 at a time in three plain loops: keys and hashes;
   timestamps, grouped by the form of `ts` so each group runs one kernel; validation and the scatter.
   Click lines are handled the same way. F1 timestamps in the years the window touches take a fast path
   (one vector compare, one table lookup); everything else goes through the general parsers. All of
   them are checked against scalar references in `--selftest`. IDs up to 9 bytes are packed into
   order-preserving 64-bit keys; longer or non-ASCII IDs are interned. A key is hashed with a single
   multiplication by an odd constant: a bijection, so the hash can stand in for the key everywhere (and
   is turned back into the key for the output), with every partition and table index cut from its high
   bits. Parsed input pages are dropped from the mapping as each chunk finishes, so the kernel does not
   have to unmap the whole input serially at exit.
4. **Dedup.** Event lines are hash-partitioned by `event_id` (software write-combining with non-temporal
   stores; full lines go through a small queue so no branch depends on which partition filled up). Per
   partition, a bit map with 8+ bits per record finds the IDs that may occur more than once; only those
   go into a small exact table, which keeps the copy with the smallest `ingest_ts` *before* validation.
   Every other record is the only copy of its event. Survivors look up their person in batches (split
   by the Bloom filter without branching, then probe the table for the flagged ones) and are
   re-partitioned by person.
   Clicks are partitioned by person directly (copies are identical, so click dedup cannot change any
   result).
5. **Sessions.** Per person partition, events are counting-sorted by (person, 2^20 ms time bucket). A
   bucket spans less than 30 minutes, so each bucket lies in one session and gaps only need checking
   between consecutive buckets: no comparison sort. Clicks per person are sorted by time; a sliding
   7-day window gives the credited click (latest, ties to the smallest `click_id`) and the assisting
   campaigns.
6. **Output.** A small built-in Parquet writer (PLAIN pages, dictionary-encoded `top_category`, `DATE`
   for `day`), one file per thread.

## Build and test

```sh
make build              # docker image (gcc:13 build stage, debian:bookworm-slim run stage)
make fixtures           # all 36 fixtures through the image with scorer flags
make native fixtures-native   # same without Docker
make selftest fuzz      # SIMD parser self-test, CSV scanner fuzzer
make dev run-dev check-dev    # 1M-event dev set (needs `generate`) against dev-answer/
```

`SS_LOG=1` prints phase timings to stderr. `tests/reference.py` is a slow, plain Python
implementation of the rules used as an oracle for differential tests on other seeds.
