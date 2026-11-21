#!/bin/bash
# mkdata.sh [small|all] : build the test data under $SS_DATA (default <repo>/data).
#   small: the dev set and 21 small seeds, each with a reference output from tests/reference.py (plain Python)
#   all  : also big20 and big40 (20M and 40M events) with golden outputs from the general route of the v3 head,
#          the last build without the dense route, which the scorer has accepted on its hidden data
# Needs `generate` on PATH or in $GENERATE (cargo build --release in the track page's generator.zip). For the
# dev check, unzip the track page's dev-answer.zip to $SS_DATA/dev-answer. 60M events do not fit an 8 GB machine:
# the generator alone needs more than 6 GB.
set -eu
MODE=${1:-small}
T=$(cd "$(dirname "$0")" && pwd); R=$(dirname "$T"); D=${SS_DATA:-$R/data}; G=${GENERATE:-generate}
mkdir -p "$D"
gen() { [ -f "$4/manifest.json" ] || { rm -rf "$4"; echo "generating $(basename "$4")"; "$G" --seed "$1" --events "$2" --clicks "$3" --output "$4" > /dev/null; }; }
gen 1 1000000 250000 "$D/dev1"
[ -d "$D/dev-answer" ] || echo "note: unzip dev-answer.zip to $D/dev-answer (verify.sh needs it)"
for spec in 2:200000:50000 3:200000:50000 4:200000:50000 5:200000:50000 6:200000:50000 7:200000:50000 29:200000:50000 \
            36:200000:50000 62:200000:50000 65:200000:50000 54:20000:5000 89:20000:5000 98:20000:5000 6:5000:1250 \
            7:5000:1250 8:5000:1250 9:5000:1250 10:5000:1250 11:5000:1250 12:1000:50 13:3000:20000; do
  IFS=: read -r s e c <<< "$spec"; name=s${s}_e${e}_c${c}
  gen "$s" "$e" "$c" "$D/$name"
  if [ ! -f "$D/ref_$name/done" ]; then
    echo "reference for $name"; rm -rf "$D/ref_$name"; python3 -I "$R/tests/reference.py" "$D/$name" "$D/ref_$name" 2> /dev/null; touch "$D/ref_$name/done"
  fi
done
if [ "$MODE" = all ]; then
  REF=$(git -C "$R" rev-parse -q --verify v3 || git -C "$R" rev-parse -q --verify origin/v3)
  V=$(mktemp -d); git -C "$R" archive "$REF" src | tar -x -C "$V"
  g++ -std=c++17 -O2 -fno-tree-vectorize -march=cascadelake -mtune=cascadelake -static -pthread -o "$V/ss_v3" "$V/src/main.cpp"
  gen 20 20000000 5000000 "$D/big20"
  gen 40 40000000 10000000 "$D/big40"
  for big in big20 big40; do
    [ -d "$D/golden_$big/user_daily" ] || { echo "golden output for $big"; rm -rf "$D/golden_$big"; mkdir -p "$D/golden_$big"; "$V/ss_v3" "$D/$big" "$D/golden_$big"; }
  done
  rm -rf "$V"
fi
echo "data ready in $D"
