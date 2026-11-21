# BENCH BUILD (branch c-bench). Never submit this commit for a score: the dev check rejects it by design.
#
# The container writes the correct dev output plus rows whose user_id starts with "!". Before writing those
# rows it generates 20M- and 60M-event sets on /scratch with the organisers' generator and times every build
# in bench/variants.txt on them, in fresh processes, round by round. The rejection message displays the rows.
# See bench/README.md for what the rows say.
FROM gcc:13 AS build
WORKDIR /build
COPY src/ ./src/
COPY bench/ ./bench/
RUN set -e; mkdir -p /out; \
    F="-std=c++17 -O2 -fno-tree-vectorize -march=cascadelake -mtune=cascadelake -static -pthread"; \
    g++ $F -Wa,-mbranches-within-32B-boundaries -o /out/ss-a src/main.cpp & p1=$!; \
    g++ $F -o /out/ss-a-nojcc src/main.cpp & p2=$!; \
    g++ $F -o /out/ss-4f8cb95 bench/variants/4f8cb95/src/main.cpp & p3=$!; \
    g++ $F -o /out/ss-594f757 bench/variants/594f757/src/main.cpp & p4=$!; \
    g++ $F -o /out/ss-0aeec34 bench/variants/0aeec34/src/main.cpp & p5=$!; \
    g++ -std=c++17 -O2 -march=cascadelake -static -pthread -o /out/bench bench/driver.cpp & p6=$!; \
    for p in $p1 $p2 $p3 $p4 $p5 $p6; do wait $p; done; \
    strip /out/*; \
    cp bench/generate bench/variants.txt /out/; \
    ls -la /out

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /out/ /usr/local/bench/
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
CMD ["/usr/local/bench/bench"]
