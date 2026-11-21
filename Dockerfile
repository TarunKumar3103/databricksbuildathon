# Shopping Sessions: native C++17 pipeline, one static binary, no runtime dependencies.
#
# Build stage: the scorers are Intel Xeon (Cascade Lake or newer), so target Cascade Lake, with clang 18 from
# Ubuntu's archive. On the cost model the same source needs about 5% fewer cycles per event line than with
# gcc 13, and about 3% fewer again with the profile (fewer instructions and fewer mispredicted branches).
FROM ubuntu:24.04 AS build
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends clang-18 llvm-18 libclang-rt-18-dev \
        libstdc++-13-dev libc6-dev binutils \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY src/ ./src/
COPY pgo/ ./pgo/
# A profile-guided build (pgo/build.sh): an instrumented binary runs on a 300K-event set that the organisers'
# generator writes in this stage, and the binary is compiled with that profile; if training cannot run on the
# build machine, the plain clang build is used. -fno-vectorize -fno-slp-vectorize: the hot loops are vectorised
# by hand (as -fno-tree-vectorize with gcc).
RUN sh pgo/build.sh /shopping-sessions

# Run stage: just the binary. The scorer runs it as uid 1000 with a read-only root filesystem;
# it only reads $DATA_DIR and writes $OUTPUT_DIR.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
CMD ["/usr/local/bin/shopping-sessions"]
