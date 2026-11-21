# Shopping Sessions: native C++ pipeline

One static C++17 binary (no engine, no runtime dependencies) that reads `/data` and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet, following `rules.md` exactly.

## How it works

1. **Lookups.** `manifest.json`, `products.csv`, `campaigns.csv`. Product and campaign IDs of the form
   `p<n>` / `c_<n>` resolve through direct arrays, anything else through a hash map.
2. **Links.** Parsed in parallel; distinct IDs are deduplicated per hash partition, then joined with a
   lock-free union-find that always links the root with the byte-wise larger ID under the smaller one,
   so every root *is* the person's name. Result: a hash table `ID -> person name`.
3. **Events and clicks.** Files are memory-mapped and split into chunks at line boundaries. A SIMD CSV
   scanner (`src/scan.h`) finds commas, newlines and quotes per 64-byte block, validates quote placement
   with bit tricks (blocks with any irregular quote fall back to an exact byte-by-byte check), flattens
   all separator offsets into one array, and splits each well-shaped line into its fields with a few
   vector operations. Timestamps (all three forms) are parsed with SSE/AVX2 and checked against scalar
   references in `--selftest`. Parsed input pages are dropped from the mapping as each chunk finishes, so
   the kernel does not have to unmap the whole input serially at exit.
   IDs up to 9 bytes are packed into order-preserving 64-bit keys; longer or non-ASCII IDs are interned.
4. **Dedup.** Event lines are hash-partitioned by `event_id` (software write-combining with non-temporal
   stores; full lines go through a small queue so no branch depends on which partition filled up). Per
   partition, a bit map with 8+ bits per record finds the IDs that may occur more than once; only those
   go into a small exact table, which keeps the copy with the smallest `ingest_ts` *before* validation.
   Every other record is the only copy of its event. Survivors look up their person in batches (hash and
   prefetch, split by a Bloom filter without branching, then probe) and are re-partitioned by person.
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
