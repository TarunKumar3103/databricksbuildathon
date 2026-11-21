#!/usr/bin/env python3
"""mutate.py BASE OUT MODE UKEY(hex) : a copy of a generated dataset with one kind of irregularity added."""
import os, random, shutil, sys, glob
base, out, mode, ukey = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4], 16)
import zlib
random.seed(zlib.crc32(mode.encode()) + int(os.environ.get("MSEED", "0")))
M40 = (1 << 40) - 1
C1, C2 = 0x9E3779B97F4B, 0xD6E8FEB86659
C1I, C2I = pow(C1, -1, 1 << 40), pow(C2, -1, 1 << 40)
D36 = "0123456789abcdefghijklmnopqrstuvwxyz"
def b36(v):
    s = ""
    while True:
        s = D36[v % 36] + s; v //= 36
        if v == 0: return s
def scr(v, key):
    m = ((v * C1) ^ key) & M40; m ^= m >> 17; return (m * C2) & M40
def unscr(v, key):
    m = (v * C2I) & M40; m ^= m >> 17; m ^= m >> 34; return ((m ^ key) * C1I) & M40
def unum(uid):
    try: return unscr(int(uid[1:], 36), ukey)
    except Exception: return None
shutil.rmtree(out, ignore_errors=True)
shutil.copytree(base, out)
def rewrite(folder, fn):
    for path in sorted(glob.glob(os.path.join(out, folder, "*.csv"))):
        lines = open(path, "rb").read().decode("latin-1").split("\n")
        tail = lines[-1]; body = lines[1:-1] if tail == "" else lines[1:]
        new = fn(body)
        open(path, "wb").write(("\n".join([lines[0]] + new) + "\n").encode("latin-1"))
def plain(line, nf):
    return '"' not in line and line.count(",") == nf - 1
allusers = []
for path in sorted(glob.glob(os.path.join(base, "events", "*.csv")))[:3]:
    for line in open(path, "rb").read().decode("latin-1").split("\n")[1:2000]:
        if plain(line, 7) and line.split(",")[1][:1] == "u": allusers.append(line.split(",")[1])
def ev(fn): rewrite("events", fn)
def ck(fn): rewrite("clicks", fn)
def lk(fn): rewrite("links", fn)

if mode == "swapusers":
    def f(body):
        idx = [i for i, l in enumerate(body) if plain(l, 7)]
        for _ in range(len(idx) // 100):
            a, b = random.sample(idx, 2)
            fa, fb = body[a].split(","), body[b].split(",")
            fa[1], fb[1] = fb[1], fa[1]
            body[a], body[b] = ",".join(fa), ",".join(fb)
        return body
    ev(f)
elif mode == "timeorder":
    def f(body):
        for i, l in enumerate(body):
            if plain(l, 7) and random.random() < 0.01:
                p = l.split(",")
                if len(p[2]) == 13 and p[2].isdigit():
                    p[2] = str(int(p[2]) + random.choice([-1, 1]) * random.randint(60_000, 5 * 3600_000))
                    body[i] = ",".join(p)
        return body
    ev(f)
elif mode == "crosslinks":
    first = [True]
    def f(body):
        if first[0]:
            first[0] = False
            for _ in range(300):
                body.append(random.choice(allusers) + "," + random.choice(allusers))
        return body
    lk(f)
elif mode == "upper":
    def fe(body):
        for i, l in enumerate(body):
            if plain(l, 7) and random.random() < 0.01:
                p = l.split(","); k = random.choice([0, 1]); p[k] = p[k].upper(); body[i] = ",".join(p)
        return body
    def fc(body):
        for i, l in enumerate(body):
            if plain(l, 4) and random.random() < 0.01:
                p = l.split(","); k = random.choice([0, 1]); p[k] = p[k].upper(); body[i] = ",".join(p)
        return body
    def fl(body):
        for i, l in enumerate(body):
            if plain(l, 2) and random.random() < 0.02:
                p = l.split(","); k = random.choice([0, 1]); p[k] = p[k].upper(); body[i] = ",".join(p)
        return body
    ev(fe); ck(fc); lk(fl)
elif mode == "faringest":
    n = [0]
    def f(body):
        for i, l in enumerate(body):
            if plain(l, 7) and random.random() < 0.0005 and n[0] < 5:
                p = l.split(","); p[3] = "2020-01-01T00:00:00Z"; body[i] = ",".join(p); n[0] += 1
        return body
    ev(f)
elif mode == "droplinks":
    lk(lambda body: [l for l in body if random.random() > 0.3])
elif mode == "revised":
    def f(body):
        extra = []
        for l in body:
            if plain(l, 7) and random.random() < 0.01:
                p = l.split(",")
                if len(p[3]) == 20 and p[3][11:13].isdigit() and int(p[3][11:13]) >= 1:
                    p[1] = random.choice(allusers)
                    p[3] = p[3][:11] + "%02d" % (int(p[3][11:13]) - 1) + p[3][13:]
                    extra.append(",".join(p))
        return body + extra
    ev(f)
elif mode == "bigprice":
    def f(body):
        for i, l in enumerate(body):
            if plain(l, 7):
                p = l.split(",")
                if p[4] == "purchase" and random.random() < 0.3:
                    p[6] = random.choice(["10000000", "200000", "131071", "131072", "0000131073"]); body[i] = ",".join(p)
        return body
    ev(f)
elif mode == "newalias":
    def f(body):
        for i, l in enumerate(body):
            if plain(l, 7) and random.random() < 0.01:
                p = l.split(","); n = unum(p[1])
                if n is not None and (n >> 9) < (1 << 22):
                    n2 = (n & ~511) | ((n & 511) + random.choice([1, 2, 7, 300])) % 512
                    p[1] = "u" + b36(scr(n2, ukey)); body[i] = ",".join(p)
        return body
    def g(body):
        for i, l in enumerate(body):
            if plain(l, 4) and random.random() < 0.02:
                p = l.split(","); n = unum(p[1])
                if n is not None and (n >> 9) < (1 << 22):
                    n2 = (n & ~511) | ((n & 511) + random.choice([1, 2, 7, 300])) % 512
                    p[1] = "u" + b36(scr(n2, ukey)); body[i] = ",".join(p)
        return body
    ev(f); ck(g)
elif mode == "oddids":
    def f(body):
        for i, l in enumerate(body):
            if plain(l, 7) and random.random() < 0.01:
                p = l.split(","); k = random.choice([0, 1]); c = random.randint(0, 5)
                if len(p[k]) < 2: continue
                if c == 0: p[k] = p[k][0] + "0" + p[k][1:]
                elif c == 1: p[k] = p[k][0] + "zzzzzzzz"
                elif c == 2: p[k] = "x" + p[k][1:]
                elif c == 3: p[k] = p[k] + "0123"
                elif c == 4: p[k] = p[k][0]
                else: p[k] = p[k][:-1] + " "
                body[i] = ",".join(p)
        return body
    def g(body):
        for i, l in enumerate(body):
            if plain(l, 4) and random.random() < 0.02:
                p = l.split(","); k = random.choice([0, 1]); c = random.randint(0, 5)
                if len(p[k]) < 2: continue
                if c == 0: p[k] = p[k][0] + "0" + p[k][1:]
                elif c == 1: p[k] = p[k][0] + "zzzzzzzz"
                elif c == 2: p[k] = "x" + p[k][1:]
                elif c == 3: p[k] = p[k] + "0123"
                elif c == 4: p[k] = p[k][0]
                else: p[k] = p[k][:-1] + " "
                body[i] = ",".join(p)
        return body
    ev(f); ck(g)
elif mode == "clickusers":
    def g(body):
        for i, l in enumerate(body):
            if plain(l, 4) and random.random() < 0.03:
                p = l.split(","); p[1] = random.choice(allusers); body[i] = ",".join(p)
        return body
    ck(g)
elif mode == "dupties":
    # copies with the same event_id and the same content (ties on ingest), and clicks repeated
    def f(body):
        extra = [l for l in body if plain(l, 7) and random.random() < 0.02]
        return body + extra + extra[: len(extra) // 2]
    def g(body):
        extra = [l for l in body if plain(l, 4) and random.random() < 0.05]
        return body + extra
    ev(f); ck(g)
elif mode == "aliaslinks":
    # links between other aliases of one person: sets that are not the aliases 0..n-1, several sets under one
    # index, aliases at and beyond the limit of the per-person link word; events and clicks moved onto such aliases
    AL = [0, 1, 2, 3, 4, 5, 8, 9, 10, 11]
    def realias(uid, a):
        n = unum(uid)
        if n is None or (n >> 9) >= (1 << 22): return uid
        return "u" + b36(scr((n & ~511) | a, ukey))
    first = [True]
    def fl(body):
        for i, l in enumerate(body):
            if plain(l, 2) and random.random() < 0.03:
                p = l.split(","); k = random.choice([0, 1]); p[k] = realias(p[k], random.choice(AL)); body[i] = ",".join(p)
        if first[0]:
            first[0] = False
            for _ in range(400):
                u = random.choice(allusers)
                body.append(realias(u, random.choice(AL)) + "," + realias(u, random.choice(AL)))
        return body
    def fe(body):
        for i, l in enumerate(body):
            if plain(l, 7) and random.random() < 0.02:
                p = l.split(","); p[1] = realias(p[1], random.choice(AL)); body[i] = ",".join(p)
        return body
    def fc(body):
        for i, l in enumerate(body):
            if plain(l, 4) and random.random() < 0.03:
                p = l.split(","); p[1] = realias(p[1], random.choice(AL)); body[i] = ",".join(p)
        return body
    lk(fl); ev(fe); ck(fc)
elif mode == "window":
    # another window in the manifest: longer than the 16 days the dense scan has lanes for, moved (events that
    # were just outside become valid and are out of time order), not starting at midnight, or shorter
    import json, datetime
    mp = os.path.join(out, "manifest.json")
    m = json.load(open(mp))
    def shift(t, hours):
        d = datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(hours=hours)
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    a, b = random.choice([(0, 49), (-30, 30), (-72, 72), (7, 7), (-5, -5), (0, 48), (-24, 24), (50, -50), (0, -100), (13, 0)])
    m["window_start"], m["window_end"] = shift(m["window_start"], a), shift(m["window_end"], b)
    json.dump(m, open(mp, "w"))
else:
    sys.exit("unknown mode")
