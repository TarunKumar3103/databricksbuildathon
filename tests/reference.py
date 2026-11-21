#!/usr/bin/env python3
"""Plain, slow reference implementation of rules.md. Used only as a test oracle.

Usage: reference.py DATA_DIR OUTPUT_DIR   (writes OUTPUT_DIR/{user_daily,campaign_daily}/part-0.parquet)
"""
import glob
import json
import os
import sys

DAY = 86_400_000
MIN30 = 1_800_000
WEEK = 7 * DAY
DIGITS = set(b"0123456789")


def days_from_civil(y, m, d):
    y = y - 1 if m <= 2 else y
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def dim(y, m):
    if m == 2:
        return 29 if is_leap(y) else 28
    return 30 if m in (4, 6, 9, 11) else 31


def all_digits(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def parse_civil(s):
    """s: 19 bytes YYYY-MM-DDTHH:MM:SS -> ms or None"""
    if len(s) != 19:
        return None
    if s[4:5] != b"-" or s[7:8] != b"-" or s[10:11] != b"T" or s[13:14] != b":" or s[16:17] != b":":
        return None
    parts = [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19]]
    if not all(all_digits(p) for p in parts):
        return None
    y, mo, d, h, mi, se = (int(p) for p in parts)
    if not (1 <= mo <= 12):
        return None
    if not (1 <= d <= dim(y, mo)):
        return None
    if h > 23 or mi > 59 or se > 59:
        return None
    return ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000


def parse_ts(s):
    if len(s) == 0:
        return None
    if all_digits(s):
        return int(s)
    if len(s) == 20 and s[19:20] == b"Z":
        return parse_civil(s[:19])
    if len(s) == 25 and s[19:20] in (b"+", b"-") and s[22:23] == b":":
        base = parse_civil(s[:19])
        if base is None:
            return None
        oh, om = s[20:22], s[23:25]
        if not (all_digits(oh) and all_digits(om)):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if s[19:20] == b"+" else base + off
    return None


def parse_line(line, nfields):
    if line == b"":
        return None
    parts = line.split(b",")
    if len(parts) != nfields:
        return None
    out = []
    for p in parts:
        if b'"' in p:
            if len(p) >= 2 and p[:1] == b'"' and p[-1:] == b'"' and b'"' not in p[1:-1]:
                out.append(p[1:-1])
            else:
                return None
        else:
            out.append(p)
    return out


def read_rows(folder, nfields):
    rows = []
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        with open(path, "rb") as f:
            data = f.read()
        lines = data.split(b"\n")
        if data.endswith(b"\n"):
            lines.pop()
        for line in lines[1:]:
            r = parse_line(line, nfields)
            if r is not None:
                rows.append(r)
    return rows


def read_lookup(path):
    out = {}
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()
    for line in lines[1:]:
        r = parse_line(line, 2)
        if r is None:
            continue
        out[r[0]] = r[1]
    return out


def main():
    data_dir, out_dir = sys.argv[1], sys.argv[2]
    with open(os.path.join(data_dir, "manifest.json")) as f:
        man = json.load(f)
    ws = parse_ts(man["window_start"].encode())
    we = parse_ts(man["window_end"].encode())
    products = read_lookup(os.path.join(data_dir, "products.csv"))
    campaigns = read_lookup(os.path.join(data_dir, "campaigns.csv"))

    # Links
    parent = {}

    def find(x):
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in read_rows(os.path.join(data_dir, "links"), 2):
        if a == b"" or b == b"":
            continue
        parent.setdefault(a, a)
        parent.setdefault(b, b)
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    name = {}
    for x in parent:
        r = find(x)
        if r not in name or x < name[r]:
            name[r] = x

    def person(uid):
        if uid in parent:
            return name[find(uid)]
        return uid

    # Events
    kept = {}
    for f in read_rows(os.path.join(data_dir, "events"), 7):
        eid = f[0]
        if eid == b"":
            continue
        ing = parse_ts(f[3])
        assert ing is not None, f
        if eid not in kept or ing < kept[eid][0]:
            kept[eid] = (ing, f)
    by_person = {}
    for eid, (ing, f) in kept.items():
        uid, ts, et, pid, price = f[1], parse_ts(f[2]), f[4], f[5], f[6]
        if uid == b"" or ts is None or not (ws <= ts < we):
            continue
        if et not in (b"view", b"cart", b"purchase"):
            continue
        if pid == b"":
            continue
        pv = 0
        if et == b"purchase":
            if not all_digits(price):
                continue
            pv = int(price)
            if not (1 <= pv <= 10_000_000):
                continue
        if ing - ts > DAY:
            continue
        cat = products.get(pid, b"unknown")
        by_person.setdefault(person(uid), []).append((ts, et == b"purchase", pv, cat))

    # Clicks
    seen = set()
    clicks_by_person = {}
    for f in read_rows(os.path.join(data_dir, "clicks"), 4):
        cid, uid, camp, ts = f
        if cid == b"" or cid in seen:
            continue
        seen.add(cid)
        if uid == b"":
            continue
        t = parse_ts(ts)
        if t is None or not (ws - WEEK <= t < we):
            continue
        if camp not in campaigns:
            continue
        clicks_by_person.setdefault(person(uid), []).append((t, cid, camp))

    user_daily = {}
    camp_daily = {}

    def camp_row(c, day):
        k = (c, day)
        if k not in camp_daily:
            camp_daily[k] = [0, 0, 0, 0]
        return camp_daily[k]

    for p, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > MIN30:
                sessions.append([e])
            else:
                sessions[-1].append(e)
            prev = e[0]
        clicks = clicks_by_person.get(p, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            rev = sum(e[2] for e in s)
            cands = [c for c in clicks if start - WEEK <= c[0] <= start]
            if cands:
                best = min(cands, key=lambda c: (-c[0], c[1]))
                credit = best[2]
            else:
                credit = b"organic"
            r = camp_row(credit, day)
            r[0] += 1
            r[1] += 1 if rev > 0 else 0
            r[2] += rev
            for c in set(c[2] for c in cands):
                camp_row(c, day)[3] += 1
            u = user_daily.setdefault((p, day), [0, 0, 0, {}])
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for e in s:
                if e[1]:
                    u[3][e[3]] = u[3].get(e[3], 0) + e[2]

    import datetime

    import polars as pl

    def to_date(d):
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=d)

    def dec(b):
        return b.decode("utf-8", "surrogateescape")

    rows = []
    for (p, day), (ns, ne, rev, cats) in user_daily.items():
        top = None
        if cats:
            top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0]
        rows.append((dec(p), to_date(day), ns, ne, rev, None if top is None else dec(top)))
    ud = pl.DataFrame(rows, schema={"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64,
                                    "events": pl.Int64, "revenue_cents": pl.Int64, "top_category": pl.String},
                      orient="row")
    crow = []
    for (c, day), (ns, npur, rev, ass) in camp_daily.items():
        ch = b"organic" if c == b"organic" else campaigns[c]
        crow.append((dec(c), dec(ch), to_date(day), ns, npur, rev, ass))
    cd = pl.DataFrame(crow, schema={"campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
                                    "sessions": pl.Int64, "purchasing_sessions": pl.Int64,
                                    "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64}, orient="row")
    os.makedirs(os.path.join(out_dir, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "campaign_daily"), exist_ok=True)
    ud.write_parquet(os.path.join(out_dir, "user_daily", "part-0.parquet"))
    cd.write_parquet(os.path.join(out_dir, "campaign_daily", "part-0.parquet"))
    print(f"user_daily {ud.height} rows, campaign_daily {cd.height} rows", file=sys.stderr)


if __name__ == "__main__":
    main()
