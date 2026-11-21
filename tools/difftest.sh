#!/bin/bash
# difftest.sh BIN SEED EVENTS [CLICKS] : generate a set, run the Python reference (cached) and BIN, compare.
set -euo pipefail
BIN=$1; SEED=$2; EV=$3; CK=${4:-$((EV / 4))}
T=$(cd "$(dirname "$0")" && pwd); R=$(dirname "$T"); S=$R/scripts; DD=${SS_DATA:-$R/data}; G=${GENERATE:-generate}
D=$DD/s${SEED}_e${EV}_c${CK}; F=$DD/ref_s${SEED}_e${EV}_c${CK}
[ -d "$D" ] || "$G" --seed "$SEED" --events "$EV" --clicks "$CK" --output "$D" > /dev/null
if [ ! -f "$F/done" ]; then rm -rf "$F"; python3 -I "$R/tests/reference.py" "$D" "$F" 2> /dev/null; touch "$F/done"; fi
O=$(mktemp -d)
"$BIN" "$D" "$O" 2> /dev/null
if python3 -B "$S/compare.py" "$O" "$F" > "$O.cmp" 2>&1; then echo "seed=$SEED events=$EV clicks=$CK: MATCH"; else echo "seed=$SEED events=$EV clicks=$CK: DIFFER"; cat "$O.cmp"; fi
rm -rf "$O" "$O.cmp"
