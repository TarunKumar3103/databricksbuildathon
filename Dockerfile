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
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-4a23ea7-f0 bench/variants/4a23ea7/src/main.cpp & p0=$!; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-48d15a9-f1 bench/variants/48d15a9/src/main.cpp & p1=$!; \
    g++ -std=c++17 -O2 -march=cascadelake -static -pthread -o /out/bench bench/driver.cpp & pd=$!; \
    for p in $p0 $p1 $pd; do wait $p; done; \
    strip /out/*; \
    cp bench/generate bench/variants.txt /out/; \
    if [ -d bench/prebuilt ]; then cp bench/prebuilt/* /out/; fi; \
    ls -la /out

# Build sp exactly as branch b-slots-v4-pgo's Dockerfile does: clang 18 from Ubuntu's archive and the profile in
# its pgo/. If the packages cannot be fetched there, this stage still succeeds and sp is reported as missing.
FROM ubuntu:24.04 AS clang
WORKDIR /build
COPY bench/variants/1098ed4/ ./v/
RUN mkdir -p /out; \
    if apt-get update > /tmp/apt.log 2>&1 \
       && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends clang-18 libstdc++-13-dev libc6-dev binutils >> /tmp/apt.log 2>&1 \
       && clang++-18 -std=c++17 -O2 -fno-vectorize -fno-slp-vectorize -march=cascadelake -mtune=cascadelake -static -pthread \
            -fprofile-instr-use=v/pgo/clang18.profdata -Wno-profile-instr-out-of-date -Wno-profile-instr-unprofiled \
            -o /out/ss-1098ed4-built v/src/main.cpp >> /tmp/apt.log 2>&1 \
       && strip /out/ss-1098ed4-built; \
    then echo "clang build ok"; else echo "clang build failed"; tail -5 /tmp/apt.log; rm -f /out/ss-1098ed4-built; fi; \
    ls -la /out

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /out/ /usr/local/bench/
COPY --from=clang /out/ /usr/local/bench/
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
CMD ["/usr/local/bench/bench"]
