#!/bin/bash
# loop.sh BIN BASE FIRST LAST : every mutation of the generated set BASE for the seeds FIRST..LAST. A run passes
# when BIN's output equals the general route's on the damaged data. Prints the failures and a count.
# The general route is $SS_ORACLE if set (a binary), else BIN itself with SS_DENSE=0. MODES limits the modes.
BIN=$(readlink -f "$1"); BASE=$2
M=$(cd "$(dirname "$0")" && pwd); S=$M/../../scripts
T=$(mktemp -d); mkdir -p "$T/o0"
UK=$(SS_LOG=1 "$BIN" "$BASE" "$T/o0" 2>&1 | grep -o "ukey=[0-9a-f]*" | cut -d= -f2)
if [ -z "$UK" ] || [ "$UK" = 0 ]; then echo "no user key found: $BASE is not a generated set, or BIN has no dense route"; exit 1; fi
oracle() { if [ -n "${SS_ORACLE:-}" ]; then "$SS_ORACLE" "$1" "$2" 2> /dev/null; else SS_DENSE=0 "$BIN" "$1" "$2" 2> /dev/null; fi; }
n=0; bad=0
for s in $(seq "$3" "$4"); do
  for m in ${MODES:-swapusers timeorder crosslinks upper faringest droplinks revised bigprice newalias oddids clickusers dupties}; do
    MSEED=$s python3 "$M/mutate.py" "$BASE" "$T/d" "$m" "$UK" || { echo "$m seed $s: mutate failed"; bad=$((bad + 1)); continue; }
    rm -rf "$T/a" "$T/b"; mkdir -p "$T/a" "$T/b"
    oracle "$T/d" "$T/a"; "$BIN" "$T/d" "$T/b" 2> /dev/null
    r=$(python3 -B "$S/compare.py" "$T/b" "$T/a" 2>&1 | tail -1); n=$((n + 1))
    [ "$r" = MATCH ] || { echo "$m seed $s: $r"; bad=$((bad + 1)); }
  done
done
rm -rf "$T"
echo "mutations on $(basename "$BASE"): $n runs, $bad failures"
[ $bad = 0 ]
