# Shopping Sessions: native C++17 pipeline, one static binary, no runtime dependencies.
#
# Build stage: the scorers are Intel Xeon (Cascade Lake or newer), so target Cascade Lake.
FROM gcc:13 AS build
WORKDIR /build
COPY src/ ./src/
# -fno-tree-vectorize: the hot loops are vectorised by hand; the auto-vectoriser only gets in the way
# (it turns runs of scalar stores into slower insert/permute sequences).
# -Wa,-mbranches-within-32B-boundaries: Skylake to Cascade Lake cores keep a jump that crosses or ends on a
# 32-byte boundary out of the decoded-uop cache (the JCC erratum microcode fix); the assembler pads so that
# no jump does.
RUN g++ -std=c++17 -O2 -fno-tree-vectorize -march=cascadelake -mtune=cascadelake -static -pthread \
        -Wa,-mbranches-within-32B-boundaries \
        -o /shopping-sessions src/main.cpp \
 && strip /shopping-sessions

# Run stage: just the binary. The scorer runs it as uid 1000 with a read-only root filesystem;
# it only reads $DATA_DIR and writes $OUTPUT_DIR.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
CMD ["/usr/local/bin/shopping-sessions"]
