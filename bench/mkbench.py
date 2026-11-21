#!/usr/bin/env python3
"""mkbench.py PLAN : turn a plan of builds into bench/variants/, bench/variants.txt and the Dockerfile.

Run it from the repo root on branch c-bench (or a branch made from it), then commit and push. The plan has one
build per line:

    tag  commit  [extra compile flags ...]  [ENV=VALUE ...]

`commit` is anything git can name (a hash, a branch such as origin/v4); its src/ is copied to
bench/variants/<short hash>/src. Builds with the same commit and flags share one binary, so a line that only
adds ENV settings costs no extra compile. The first line also writes the dev output. Example:

    a   origin/v4  -Wa,-mbranches-within-32B-boundaries
    n   origin/v4
    t7  origin/v4  -Wa,-mbranches-within-32B-boundaries  SS_THREADS=7
    p   4f8cb95
"""
import os, re, shutil, subprocess, sys

BASE = "-std=c++17 -O2 -fno-tree-vectorize -march=cascadelake -mtune=cascadelake -static -pthread"


def git(*a):
    return subprocess.check_output(["git", *a], text=True).strip()


def main():
    plan = open(sys.argv[1]).read().splitlines()
    builds = {}  # (hash, flags) -> binary name
    lines = []
    for raw in plan:
        note = raw.split("#", 1)[1].strip() if "#" in raw else ""
        line = raw.split("#", 1)[0].split()
        if not line:
            continue
        tag, rev, rest = line[0], line[1], line[2:]
        flags = [w for w in rest if not re.match(r"^[A-Z_][A-Z0-9_]*=", w)]
        env = [w for w in rest if re.match(r"^[A-Z_][A-Z0-9_]*=", w)]
        h = git("rev-parse", "--short=7", rev + "^{commit}")
        d = f"bench/variants/{h}"
        if not os.path.isdir(d + "/src"):
            os.makedirs(d, exist_ok=True)
            arch = subprocess.run(["git", "archive", h, "src"], check=True, capture_output=True).stdout
            subprocess.run(["tar", "-x", "-C", d], input=arch, check=True)
        key = (h, " ".join(flags))
        if key not in builds:
            builds[key] = f"ss-{h}" + (f"-f{len(builds)}" if flags else "")
        lines.append((tag, builds[key], env, (f"{h} {' '.join(flags)}".strip() + (f": {note}" if note else ""))))
    with open("bench/variants.txt", "w") as f:
        f.write("# Written by bench/mkbench.py. tag, binary in /usr/local/bench, then ENV=VALUE settings.\n")
        f.write("# The first build also writes the dev output that the report rows are added to.\n")
        for tag, b, env, what in lines:
            f.write(f"{tag} {b} {' '.join(env)}".rstrip() + f"   # {what}\n")
    compiles = []
    for i, ((h, flags), b) in enumerate(builds.items()):
        compiles.append(f'    g++ $F {flags + " " if flags else ""}-o /out/{b} bench/variants/{h}/src/main.cpp & p{i}=$!; \\')
    waits = " ".join(f"$p{i}" for i in range(len(builds)))
    df = open("Dockerfile").read()
    new_run = (
        "RUN set -e; mkdir -p /out; \\\n"
        f'    F="{BASE}"; \\\n'
        + "\n".join(compiles)
        + "\n    g++ -std=c++17 -O2 -march=cascadelake -static -pthread -o /out/bench bench/driver.cpp & pd=$!; \\\n"
        + f"    for p in {waits} $pd; do wait $p; done; \\\n"
        + "    strip /out/*; \\\n    cp bench/generate bench/variants.txt /out/; \\\n"
        + "    if [ -d bench/prebuilt ]; then cp bench/prebuilt/* /out/; fi; \\\n    ls -la /out\n"
    )
    df2, n = re.subn(r"RUN set -e; mkdir -p /out;.*?ls -la /out\n", lambda m: new_run, df, flags=re.S)
    assert n == 1, "Dockerfile: the bench RUN step was not found"
    open("Dockerfile", "w").write(df2)
    keep = {f"bench/variants/{h}" for h, _ in builds}
    for d in os.listdir("bench/variants"):
        if f"bench/variants/{d}" not in keep:
            shutil.rmtree(f"bench/variants/{d}")
    print(open("bench/variants.txt").read())


if __name__ == "__main__":
    main()
