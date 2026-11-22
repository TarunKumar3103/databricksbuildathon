# Shopping Sessions: native C++17 pipeline, one static binary, no runtime dependencies.
#
# Build stage: the scorers are Intel Xeon (Cascade Lake or newer), so target Cascade Lake, with clang 18 from
# Ubuntu's archive. On the cost model the same source needs about 5% fewer cycles
# per event line than with gcc 13 (fewer instructions and fewer mispredicted branches).
FROM ubuntu:24.04 AS build
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends clang-18 libstdc++-13-dev libc6-dev binutils \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY src/ ./src/
COPY pgo/ ./pgo/
# -fno-vectorize -fno-slp-vectorize: the hot loops are vectorised by hand (as -fno-tree-vectorize with gcc).
# -fprofile-instr-use: branch weights and layout from a profile of this source on generated data, made with
# pgo/make-profile.sh by the same clang version. Nothing runs on the build machine.
# -fno-pie: Ubuntu's clang makes position-independent code even for a static binary; without it the tables
# have absolute addresses. On the scorer bench 0.7% faster at 20M events and 0.9% at 60M.
RUN clang++-18 -std=c++17 -O2 -fno-vectorize -fno-slp-vectorize -march=cascadelake -mtune=cascadelake -static -pthread -fno-pie \
        -fprofile-instr-use=pgo/clang18.profdata -Wno-profile-instr-out-of-date -Wno-profile-instr-unprofiled \
        -o /shopping-sessions src/main.cpp \
 && strip /shopping-sessions

# The executable is static. Include the mount points in the read-only root image.
RUN install -d /runtime-root/data /runtime-root/output /runtime-root/scratch /runtime-root/tmp \
 && chmod 1777 /runtime-root/tmp
FROM scratch
COPY --from=build /runtime-root/ /
COPY --from=build /shopping-sessions /usr/local/bin/shopping-sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
