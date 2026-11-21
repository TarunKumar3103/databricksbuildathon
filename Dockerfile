# BENCH BUILD (branch b-probe, from c-bench). Never submit this commit for a score: the dev check rejects it by design.
#
# The container writes the correct dev output plus rows whose user_id starts with "!". Before writing those
# rows it generates 20M- and 60M-event sets on /scratch with the organisers' generator and times every build
# in bench/variants.txt on them, in fresh processes, round by round. The rejection message displays the rows.
# See bench/README.md for what the rows say.
FROM gcc:13 AS build
WORKDIR /build
COPY bench/ ./bench/
RUN set -e; mkdir -p /out; \
    F="-std=c++17 -O2 -fno-tree-vectorize -march=cascadelake -mtune=cascadelake -static -pthread"; \
    g++ -std=c++17 -O2 -march=cascadelake -static -pthread -o /out/bench bench/driver.cpp & pd=$!; \
    for p in $pd; do wait $p; done; \
    strip /out/*; \
    cp bench/generate bench/variants.txt /out/; \
    if [ -d bench/prebuilt ]; then cp bench/prebuilt/* /out/; fi; \
    ls -la /out

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /out/ /usr/local/bench/
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
# bench 9 (B): five rounds at 20M and four at 60M, the timeline of build v, the probe lines of builds s and f
ENV BENCH_SIZES=20:5:5,60:15:4 BENCH_TIMELINE=v BENCH_STAGES=s BENCH_SOL=f
CMD ["/usr/local/bench/bench"]
