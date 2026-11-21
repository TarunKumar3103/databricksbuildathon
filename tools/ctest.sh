#!/bin/bash
# ctest.sh BIN TAG : put a locally built static binary into an image and run it the way the scorer does
# (scripts/docker_run.sh; CPUS and MEM default to 2 and 6g): fixtures, the dev set against the published
# answer, big20 against its golden output. The image is FROM scratch, so no base image has to be pulled.
BIN=$(readlink -f "$1"); TAG=$2
T=$(cd "$(dirname "$0")" && pwd); R=$(dirname "$T"); S=$R/scripts; D=${SS_DATA:-$R/data}
B=$(mktemp -d); cp "$BIN" "$B/shopping-sessions"
printf 'FROM scratch\nCOPY shopping-sessions /usr/local/bin/shopping-sessions\nENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch\nCMD ["/usr/local/bin/shopping-sessions"]\n' > "$B/Dockerfile"
docker build -q -t "$TAG" "$B" > /dev/null || { echo "image build failed"; exit 1; }
rm -rf "$B"
export IMAGE=$TAG CPUS=${CPUS:-2} MEM=${MEM:-6g}
fail=0
out=$(python3 -B "$S/run_fixtures.py" "$R/fixtures" -- "$S/docker_run.sh" 2>&1 | tail -1); echo "fixtures: $out"; echo "$out" | grep -q "36/36" || fail=1
O=$(mktemp -d); "$S/docker_run.sh" "$D/dev1" "$O" > /dev/null 2>&1; out=$(python3 -B "$S/compare.py" "$O" "$D/dev-answer" 2>&1 | tail -1); echo "dev: $out"; [ "$out" = MATCH ] || fail=1; rm -rf "$O"
if [ -d "$D/golden_big20" ]; then
  O=$(mktemp -d); "$S/docker_run.sh" "$D/big20" "$O" > /dev/null 2>&1; out=$(python3 -B "$S/compare.py" "$O" "$D/golden_big20" 2>&1 | tail -1); echo "big20: $out"; [ "$out" = MATCH ] || fail=1; rm -rf "$O"
fi
exit $fail
