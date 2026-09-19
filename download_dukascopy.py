"""
Téléchargeur Dukascopy — bougies M1 BID + ASK (XAUUSD) et M1 BID (actifs corrélés pour SMT).

100 % bibliothèque standard Python (aucun pip install nécessaire).
Reprend là où il s'est arrêté si on le relance (cache mensuel).

Usage (Windows, dans le dossier du projet) :
    python download_dukascopy.py                 # 2015-01-01 -> hier, tous les actifs
    python download_dukascopy.py --start 2018-01-01 --only XAUUSD

Sortie : data/raw/<SYMBOLE>/<SYMBOLE>_<ANNEE>.csv.gz
Colonnes : time_utc (début de minute, ISO), bid_o,bid_h,bid_l,bid_c,bid_v, [ask_o,ask_h,ask_l,ask_c,ask_v]
"""
import argparse
import csv
import datetime as dt
import gzip
import io
import lzma
import os
import struct
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://datafeed.dukascopy.com/datafeed/{sym}/{y:04d}/{m:02d}/{d:02d}/{side}_candles_min_1.bi5"

# symbole Dukascopy -> (côtés à télécharger, plage de prix plausible pour détecter le diviseur)
INSTRUMENTS = {
    "XAUUSD":       (("BID", "ASK"), (250.0, 10000.0)),
    "XAGUSD":       (("BID",), (3.0, 200.0)),
    "DOLLARIDXUSD": (("BID",), (60.0, 140.0)),    # proxy DXY
    "USA500IDXUSD": (("BID",), (1000.0, 12000.0)),  # proxy S&P 500
}
DIVISORS = (1, 10, 100, 1000, 10000, 100000)
REC = struct.Struct(">IIIIIf")  # time(s depuis minuit UTC), open, close, low, high, volume


class FetchError(RuntimeError):
    pass


def fetch(url, retries=8):
    """Dukascopy limite le débit : on réessaie avec une attente croissante (2, 4, 8 ... 60 s)."""
    for k in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=45) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return b""
        except Exception:
            pass
        time.sleep(min(2 ** (k + 1), 60))
    raise FetchError(url)


def decode(raw):
    if not raw:
        return []
    data = lzma.decompress(raw)
    return [REC.unpack_from(data, i) for i in range(0, len(data) - len(data) % REC.size, REC.size)]


def pick_divisor(rows, rng):
    closes = sorted(r[2] for r in rows if r[5] > 0)
    if not closes:
        return None
    med = closes[len(closes) // 2]
    for d in DIVISORS:
        if rng[0] <= med / d <= rng[1]:
            return d
    return None


def day_rows(sym, sides, day, divisor_box, rng):
    per_side = {}
    for side in sides:
        url = BASE.format(sym=sym, y=day.year, m=day.month - 1, d=day.day, side=side)
        per_side[side] = decode(fetch(url))
    if not any(per_side.values()):
        return []
    if divisor_box[0] is None:
        divisor_box[0] = pick_divisor(per_side[sides[0]], rng)
        if divisor_box[0] is None:
            return []
    div = divisor_box[0]
    midnight = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
    maps = {}
    for side, rows in per_side.items():
        m = {}
        for t, o, c, lo, hi, v in rows:
            if v <= 0:
                continue  # minute sans tick (marché fermé / pas de cotation)
            sec = t if t < 86400 * 2 else t // 1000
            m[sec] = (o / div, hi / div, lo / div, c / div, round(v, 4))
        maps[side] = m
    keys = sorted(set(maps[sides[0]]).intersection(*[set(maps[s]) for s in sides[1:]]) if len(sides) > 1 else maps[sides[0]])
    out = []
    for sec in keys:
        ts = (midnight + dt.timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%SZ")
        row = [ts]
        for s in sides:
            row.extend(maps[s][sec])
        out.append(row)
    return out


def month_days(y, m, start, end):
    d = dt.date(y, m, 1)
    while d.month == m:
        if start <= d <= end:
            yield d
        d += dt.timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default=(dt.date.today() - dt.timedelta(days=1)).isoformat())
    ap.add_argument("--only", nargs="*", default=None, help="ex: XAUUSD XAGUSD")
    ap.add_argument("--threads", type=int, default=3)
    ap.add_argument("--out", default=os.path.join("data", "raw"))
    a = ap.parse_args()
    start, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    syms = a.only or list(INSTRUMENTS)
    failed_months = []

    for sym in syms:
        sides, rng = INSTRUMENTS[sym]
        header = ["time_utc"] + [f"{s.lower()}_{k}" for s in sides for k in "ohlcv"]
        cache = os.path.join(a.out, sym, "_mois")
        os.makedirs(cache, exist_ok=True)
        divisor_box = [None]
        months = sorted({(d.year, d.month) for d in (start + dt.timedelta(n) for n in range((end - start).days + 1))})
        for (y, m) in months:
            path = os.path.join(cache, f"{y:04d}-{m:02d}.csv.gz")
            last_month = (y, m) == (end.year, end.month)
            if os.path.exists(path) and not last_month:
                continue
            days = list(month_days(y, m, start, end))

            def safe(d):
                try:
                    return day_rows(sym, sides, d, divisor_box, rng)
                except FetchError:
                    return None

            with ThreadPoolExecutor(a.threads) as ex:
                results = list(ex.map(safe, days))
            # jours en échec : nouvelle tentative, un par un, après une pause
            for i, d in enumerate(days):
                if results[i] is None:
                    time.sleep(10)
                    results[i] = safe(d)
            if any(r is None for r in results):
                bad = [d.isoformat() for d, r in zip(days, results) if r is None]
                print(f"{sym} {y}-{m:02d} : ÉCHEC sur {bad} — mois ignoré, relance le script plus tard "
                      f"pour le compléter", flush=True)
                failed_months.append(f"{sym} {y}-{m:02d}")
                continue
            n = 0
            with gzip.open(path, "wt", newline="") as f:
                w = csv.writer(f)
                w.writerow(header)
                for rows in results:
                    w.writerows(rows)
                    n += len(rows)
            print(f"{sym} {y}-{m:02d} : {n:>6} minutes  (diviseur prix = {divisor_box[0]})", flush=True)

        # consolidation annuelle
        years = sorted({y for y, _ in months})
        for y in years:
            files = sorted(f for f in os.listdir(cache) if f.startswith(f"{y:04d}-"))
            if not files:
                continue
            out = os.path.join(a.out, sym, f"{sym}_{y}.csv.gz")
            with gzip.open(out, "wt", newline="") as fo:
                fo.write(",".join(header) + "\n")
                for fn in files:
                    with gzip.open(os.path.join(cache, fn), "rt") as fi:
                        next(fi, None)
                        for line in fi:
                            fo.write(line)
            print(f"==> {out}", flush=True)
    if failed_months:
        print(f"\nINCOMPLET : {len(failed_months)} mois en échec {failed_months}.")
        print("Relance simplement : python download_dukascopy.py  (seuls les mois manquants seront téléchargés)")
    else:
        print("Terminé.")


if __name__ == "__main__":
    sys.exit(main())
