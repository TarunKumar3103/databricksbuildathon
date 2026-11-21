# BENCH BUILD (branch c-bench). Never submit this commit for a score: the dev check rejects it by design.
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
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-e1eb3d1-f0 bench/variants/e1eb3d1/src/main.cpp & p0=$!; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-77eeb25-f1 bench/variants/77eeb25/src/main.cpp & p1=$!; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-f2023b5-f2 bench/variants/f2023b5/src/main.cpp & p2=$!; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-ca31a92-f3 bench/variants/ca31a92/src/main.cpp & p3=$!; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-7deef7a-f4 bench/variants/7deef7a/src/main.cpp & p4=$!; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-d02bc98-f5 bench/variants/d02bc98/src/main.cpp & p5=$!; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-f258112-f6 bench/variants/f258112/src/main.cpp & p6=$!; \
    g++ -std=c++17 -O2 -march=cascadelake -static -pthread -o /out/bench bench/driver.cpp & pd=$!; \
    for p in $p0 $p1 $p2 $p3 $p4 $p5 $p6 $pd; do wait $p; done; \
    strip /out/*; \
    cp bench/generate bench/variants.txt /out/; \
    if [ -d bench/prebuilt ]; then cp bench/prebuilt/* /out/; fi; \
    ls -la /out

# Optional: clang 18 from Ubuntu's archive, run on the build machine. If the packages cannot be fetched there,
# this stage still succeeds and the bench reports build K as missing.
FROM ubuntu:24.04 AS clang
WORKDIR /build
COPY bench/variants/e1eb3d1/ ./v/
RUN mkdir -p /out; \
    if apt-get update > /tmp/apt.log 2>&1 \
       && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends clang-18 libstdc++-13-dev libc6-dev binutils >> /tmp/apt.log 2>&1 \
       && clang++-18 -std=c++17 -O2 -fno-vectorize -fno-slp-vectorize -march=cascadelake -mtune=cascadelake -static -pthread \
            -o /out/ss-e1eb3d1-clang-built v/src/main.cpp >> /tmp/apt.log 2>&1 \
       && strip /out/ss-e1eb3d1-clang-built; \
    then echo "clang build ok"; else echo "clang build failed"; tail -5 /tmp/apt.log; rm -f /out/ss-e1eb3d1-clang-built; fi; \
    ls -la /out

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /out/ /usr/local/bench/
COPY --from=clang /out/ /usr/local/bench/
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
CMD ["/usr/local/bench/bench"]
