"""
Gold'n'Eye Scanner — historique long des 16 actifs du scanner (H1 et M15), à partir des bougies M1 Dukascopy.

100 % bibliothèque standard Python. Conçu pour tourner dans GitHub Actions (le bac à sable de Claude
n'a pas accès à Internet, mais il peut cloner le dépôt et lire les fichiers produits).

Usage :
    python historique_scanner.py --only XAUUSD           # un actif (nom OANDA/TradingView sans préfixe)
    python historique_scanner.py                          # les 16 actifs
    python historique_scanner.py --start 2024-01-01       # date de départ si aucun fichier n'existe encore

Version 2.2 (15/10/2026) : 8 paires de devises en plus, option --suffix (historique ancien dans des fichiers séparés).\nVersion 2.1 (15/10/2026) : diviseur BTC corrigé (prix ×10) avec remise à l'échelle automatique des fichiers existants.
Version 2 (15/10/2026) : téléchargement par tranches de 30 jours, enregistrement après chaque tranche,
journal de progression, samedis ignorés (sauf BTC), arrêt propre avec --max-minutes.

Sortie (incrémentale : reprend après la dernière bougie déjà enregistrée) :
    historique/<EXCHANGE>_<TICKER>_1h.csv.gz   colonnes t,o,h,l,c  (t = début de bougie, unix UTC)
    historique/<EXCHANGE>_<TICKER>_15m.csv.gz

Prix BID Dukascopy : ils diffèrent un peu d'OANDA (quelques dixièmes de pip à quelques points),
mais la structure (liquidités, zones, sweeps) est la même. À garder en tête pour comparer au réel.
"""
import argparse
import csv
import datetime as dt
import gzip
import lzma
import os
import struct
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://datafeed.dukascopy.com/datafeed/{sym}/{y:04d}/{m:02d}/{d:02d}/BID_candles_min_1.bi5"
REC = struct.Struct(">IIIIIf")  # secondes depuis minuit UTC, open, close, low, high, volume
DIVISORS = (1, 10, 100, 1000, 10000, 100000, 1000000)

# symbole du scanner -> (symbole Dukascopy, plage de prix plausible pour trouver le diviseur)
INSTRUMENTS = {
    "OANDA:XAUUSD":    ("XAUUSD",        (250.0, 10000.0)),
    "OANDA:XAGUSD":    ("XAGUSD",        (3.0, 200.0)),
    "OANDA:WTICOUSD":  ("LIGHTCMDUSD",   (10.0, 300.0)),
    "OANDA:EURUSD":    ("EURUSD",        (0.6, 2.0)),
    "OANDA:GBPUSD":    ("GBPUSD",        (0.8, 2.5)),
    "OANDA:USDJPY":    ("USDJPY",        (60.0, 250.0)),
    "OANDA:AUDUSD":    ("AUDUSD",        (0.4, 1.2)),
    "OANDA:USDCAD":    ("USDCAD",        (0.9, 1.9)),
    "OANDA:USDCHF":    ("USDCHF",        (0.5, 1.6)),
    "OANDA:GBPJPY":    ("GBPJPY",        (100.0, 300.0)),
    "OANDA:EURJPY":    ("EURJPY",        (90.0, 260.0)),
    "OANDA:NAS100USD": ("USATECHIDXUSD", (3000.0, 60000.0)),
    "OANDA:SPX500USD": ("USA500IDXUSD",  (1000.0, 15000.0)),
    "OANDA:US30USD":   ("USA30IDXUSD",   (10000.0, 90000.0)),
    "OANDA:DE30EUR":   ("DEUIDXEUR",     (5000.0, 50000.0)),
    "COINBASE:BTCUSD": ("BTCUSD",        (5000.0, 500000.0)),   # v2.1 : (1000, 1e6) prenait le mauvais diviseur (prix ×10)
    # v2.2 : paires de devises supplémentaires (test H4 sur des données neuves)
    "OANDA:NZDUSD":    ("NZDUSD",        (0.4, 1.0)),
    "OANDA:EURGBP":    ("EURGBP",        (0.6, 1.1)),
    "OANDA:AUDJPY":    ("AUDJPY",        (50.0, 130.0)),
    "OANDA:CADJPY":    ("CADJPY",        (60.0, 140.0)),
    "OANDA:EURAUD":    ("EURAUD",        (1.2, 2.2)),
    "OANDA:GBPCHF":    ("GBPCHF",        (0.9, 1.6)),
    "OANDA:EURCHF":    ("EURCHF",        (0.8, 1.3)),
    "OANDA:AUDCAD":    ("AUDCAD",        (0.7, 1.1)),
}
FRAMES = {"1h": 3600, "15m": 900}


class FetchError(RuntimeError):
    pass


ERRORS = {"n": 0}


def fetch(url, retries=8):
    """Dukascopy limite le débit : nouvelles tentatives avec attente croissante (2, 4, 8 … 60 s).
    Les premières erreurs sont affichées dans le log (code HTTP ou message) pour diagnostiquer un blocage."""
    for k in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=45) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return b""
            err = f"HTTP {e.code}"
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        ERRORS["n"] += 1
        if ERRORS["n"] <= 20 or ERRORS["n"] % 100 == 0:
            print(f"   ⚠️ erreur n°{ERRORS['n']} ({err}) sur {url.split('/datafeed/')[-1]} — nouvel essai", flush=True)
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


def day_minutes(dsym, day, rng, div_box):
    """Bougies M1 d'une journée : liste de (t unix, o, h, l, c). Minutes sans cotation ignorées."""
    rows = decode(fetch(BASE.format(sym=dsym, y=day.year, m=day.month - 1, d=day.day)))  # mois 0-indexé
    if not rows:
        return []
    if div_box[0] is None:
        div_box[0] = pick_divisor(rows, rng)
        if div_box[0] is None:
            return []
    div = div_box[0]
    midnight = int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc).timestamp())
    out = []
    for sec, o, c, lo, hi, v in rows:
        if v <= 0:
            continue
        sec = sec if sec < 86400 * 2 else sec // 1000
        out.append((midnight + sec, o / div, hi / div, lo / div, c / div))
    return out


def resample(minutes, step):
    """Agrège des bougies M1 triées en bougies de `step` secondes (alignées sur l'heure UTC, comme TradingView)."""
    bars, cur = [], None
    for t, o, h, l, c in minutes:
        k = t - t % step
        if cur is None or cur[0] != k:
            if cur is not None:
                bars.append(cur)
            cur = [k, o, h, l, c]
        else:
            cur[2] = max(cur[2], h)
            cur[3] = min(cur[3], l)
            cur[4] = c
    if cur is not None:
        bars.append(cur)
    return bars


def load_csv(path):
    if not os.path.exists(path):
        return []
    with gzip.open(path, "rt") as f:
        r = csv.reader(f)
        next(r, None)
        return [[int(x[0])] + [float(v) for v in x[1:5]] for x in r if x]


def save_csv(path, bars):
    tmp = path + ".tmp"
    with gzip.open(tmp, "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t", "o", "h", "l", "c"])
        for b in bars:
            w.writerow([b[0]] + [f"{v:.6g}" if abs(v) < 1 else f"{v:.10g}" for v in b[1:]])
    os.replace(tmp, path)


def update_symbol(sym, start, end, out_dir, threads, deadline, chunk_days=30, suffix=""):
    """Télécharge par tranches de `chunk_days` jours et ENREGISTRE après chaque tranche :
    si le temps manque ou si Dukascopy bloque, tout ce qui est déjà téléchargé est gardé
    et le passage suivant reprend là où on s'est arrêté."""
    dsym, rng = INSTRUMENTS[sym]
    crypto = sym.startswith("COINBASE:")
    base = sym.replace(":", "_") + suffix
    paths = {tf: os.path.join(out_dir, f"{base}_{tf}.csv.gz") for tf in FRAMES}
    existing = {tf: load_csv(p) for tf, p in paths.items()}
    # reprise : on recommence au début du dernier jour déjà présent (la dernière journée peut être incomplète)
    last = min((existing[tf][-1][0] for tf in FRAMES if existing[tf]), default=None)
    first_day = dt.datetime.fromtimestamp(last, dt.timezone.utc).date() if last else start
    days = [first_day + dt.timedelta(n) for n in range((end - first_day).days + 1)]
    if not crypto:
        days = [d for d in days if d.weekday() != 5]   # samedi : marché fermé (sauf crypto)
    if not days:
        print(f"{sym} : déjà à jour", flush=True)
        return True
    print(f"{sym} : {len(days)} jours à télécharger, du {days[0]} au {days[-1]}"
          f" (déjà enregistré : {len(existing['1h'])} bougies H1)", flush=True)
    div_box = [None]
    # diviseur : déterminé sur un jour de semaine récent, avant le téléchargement parallèle
    t_test = time.time()
    probe = None
    for d in [x for x in reversed(days) if x.weekday() < 5][:5]:   # 5 jours de semaine récents au plus
        try:
            probe = day_minutes(dsym, d, rng, div_box)
            if probe and div_box[0]:
                break
        except FetchError:
            pass
    if div_box[0] is None:
        print(f"{sym} : impossible de lire les données Dukascopy (5 jours testés, {time.time() - t_test:.0f} s)"
              " — Dukascopy bloque ou ne répond pas, actif ignoré", flush=True)
        return False
    print(f"{sym} : connexion OK, diviseur {div_box[0]} ({time.time() - t_test:.1f} s pour le test)", flush=True)
    # fichiers enregistrés avec un mauvais diviseur (ex. BTC ×10 avant la v2.1) : remise à l'échelle automatique
    if existing["1h"] and probe:
        ratio = existing["1h"][-1][4] / probe[-1][4]
        for k in (1, 2, 3, 4):
            for f in (10 ** k, 10 ** -k):
                if abs(ratio / f - 1) < 0.25:
                    print(f"{sym} : prix déjà enregistrés {f:g} fois trop grands — remise à l'échelle", flush=True)
                    for tf in FRAMES:
                        existing[tf] = [[b[0]] + [v / f for v in b[1:]] for b in existing[tf]]
                        save_csv(paths[tf], existing[tf])

    def safe(d):
        if time.time() > deadline:
            return None
        try:
            return day_minutes(dsym, d, rng, div_box)
        except FetchError:
            return None

    t_start = time.time()
    done = 0
    with ThreadPoolExecutor(threads) as ex:
        for c0 in range(0, len(days), chunk_days):
            if time.time() > deadline:
                print(f"{sym} : temps écoulé — arrêt propre au {days[c0]} ({done}/{len(days)} jours)."
                      " Le prochain passage reprendra ici.", flush=True)
                return False
            chunk = days[c0:c0 + chunk_days]
            results = list(ex.map(safe, chunk))
            n_fail = sum(r is None for r in results)
            for i, d in enumerate(chunk):          # quelques jours en échec : nouvel essai, un par un
                if results[i] is None and n_fail <= 5 and time.time() < deadline:
                    time.sleep(10)
                    results[i] = safe(d)
            failed = [d for d, r in zip(chunk, results) if r is None]
            if failed:
                # on n'enregistre que jusqu'au premier jour manquant, pour ne jamais laisser de trou au milieu
                results = [r for d, r in zip(chunk, results) if d < failed[0]]
            minutes = sorted(m for r in results for m in r)
            if minutes:
                t0 = minutes[0][0]
                for tf, step in FRAMES.items():
                    new = resample(minutes, step)
                    existing[tf] = [b for b in existing[tf] if b[0] < t0 - t0 % step] + new
                    save_csv(paths[tf], existing[tf])
            done += len(chunk) if not failed else chunk.index(failed[0])
            el = (time.time() - t_start) / 60
            print(f"{sym} : {chunk[-1] if not failed else failed[0]} — {done}/{len(days)} jours"
                  f" • {len(existing['1h'])} bougies H1 • {el:.0f} min", flush=True)
            if failed:
                print(f"{sym} : {len(failed)} jour(s) en échec à partir du {failed[0]} — "
                      "enregistré jusqu'à la veille, relancer plus tard", flush=True)
                return False
    print(f"{sym} : terminé ({len(existing['1h'])} bougies H1, {len(existing['15m'])} bougies M15,"
          f" diviseur {div_box[0]})", flush=True)
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default=dt.date.today().isoformat())
    ap.add_argument("--only", nargs="*", default=None, help="ex : XAUUSD EURUSD (sans préfixe)")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--out", default="historique")
    ap.add_argument("--suffix", default="", help="ajouté au nom des fichiers, ex : _2019-2023")
    ap.add_argument("--max-minutes", type=float, default=0,
                    help="arrêt propre (avec sauvegarde) après N minutes ; 0 = pas de limite")
    a = ap.parse_args()
    deadline = time.time() + a.max_minutes * 60 if a.max_minutes else float("inf")
    os.makedirs(a.out, exist_ok=True)
    syms = list(INSTRUMENTS)
    if a.only:
        wanted = {x.upper() for x in a.only}
        syms = [s for s in syms if s.split(":")[1] in wanted]
    ok = True
    for s in syms:
        ok &= update_symbol(s, dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end), a.out, a.threads, deadline, suffix=a.suffix)
    print("Terminé." if ok else "INCOMPLET : relancer pour compléter.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
