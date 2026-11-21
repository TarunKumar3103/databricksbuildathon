#!/bin/bash
# verify.sh BIN [quick|full] : every correctness check for a build. Prints one line per check and a verdict.
# Test data comes from $SS_DATA (default <repo>/data); tools/mkdata.sh builds it.
#   quick: self-test, 36 fixtures, the dev answer, the reference seeds
#   full : also the 20M and 40M sets against their golden outputs, at 1, 3 and 8 threads
BIN=$(readlink -f "$1"); MODE=${2:-full}
T=$(cd "$(dirname "$0")" && pwd); R=$(dirname "$T"); S=$R/scripts; D=${SS_DATA:-$R/data}
fail=0
out=$("$BIN" --selftest 2>&1); rc=$?; echo "selftest (exit $rc): $(echo "$out" | tail -2 | tr '\n' ' ')"; [ $rc = 0 ] || fail=1
out=$(python3 -B "$S/run_fixtures.py" "$R/fixtures" -- "$BIN" 2>&1 | tail -1); echo "fixtures: $out"; echo "$out" | grep -q "36/36" || fail=1
if [ -d "$D/dev1" ] && [ -d "$D/dev-answer" ]; then
  O=$(mktemp -d); "$BIN" "$D/dev1" "$O" 2>/dev/null; out=$(python3 -B "$S/compare.py" "$O" "$D/dev-answer" 2>&1 | tail -1); echo "dev answer: $out"; [ "$out" = MATCH ] || fail=1; rm -rf "$O"
else
  echo "dev answer: MISSING ($D/dev1 or $D/dev-answer)"; fail=1
fi
n=0
for d in "$D"/s*_e*_c*; do
  [ -d "$d" ] || continue
  name=$(basename "$d"); F=$D/ref_$name; [ -f "$F/done" ] || continue
  O=$(mktemp -d); "$BIN" "$d" "$O" 2>/dev/null; out=$(python3 -B "$S/compare.py" "$O" "$F" 2>&1 | tail -1); [ "$out" = MATCH ] || { echo "seed $name: $out"; fail=1; }; rm -rf "$O"; n=$((n + 1))
done
echo "reference seeds: checked $n"; [ $n -gt 0 ] || fail=1
if [ "$MODE" = full ]; then
  for big in big20 big40; do
    G=$D/golden_$big
    if [ ! -d "$G" ]; then echo "no golden output for $big"; fail=1; continue; fi
    O=$(mktemp -d); "$BIN" "$D/$big" "$O" 2>/dev/null; out=$(python3 -B "$S/compare.py" "$O" "$G" 2>&1 | tail -1); echo "$big vs golden: $out"; [ "$out" = MATCH ] || fail=1; rm -rf "$O"
    for t in 1 3 8; do
      O=$(mktemp -d); SS_THREADS=$t "$BIN" "$D/$big" "$O" 2>/dev/null; out=$(python3 -B "$S/compare.py" "$O" "$G" 2>&1 | tail -1); [ "$out" = MATCH ] || { echo "$big threads=$t: $out"; fail=1; }; rm -rf "$O"
      [ $big = big40 ] && break
    done
  done
fi
[ $fail = 0 ] && echo "VERDICT: ALL PASS" || echo "VERDICT: FAILURES"
exit $fail
