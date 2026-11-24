"""Graficzny raport dzienny i tygodniowy (PNG) dla Jakuba.

Prośba Jakuba z 20.11.2026: „Możesz robić te podsumowania dzienne i tygodniowe
w krótszej formie graficznej?” - raporty wysyłają zaplanowane zadania Claude'a
(codziennie ~19:40, w niedzielę wersja tygodniowa). Zadanie klonuje repo i
uruchamia:

    python live/report_card.py --mode daily  --out /tmp/hydra_dzien.png
    python live/report_card.py --mode weekly --out /tmp/hydra_tydzien.png

Skrypt TYLKO CZYTA pliki z `data/` (i `git log` - commity automatu), rysuje
jedną kartę w formacie telefonu i wypisuje na stdout 2-4 linijki tekstu
(alarmy + sygnał) do powiadomienia. Nie jest częścią pipeline'u: matplotlib nie
jest w requirements.txt i jest importowany dopiero przy rysowaniu.

Część obliczeniowa (`collect`) jest czysta i testowana bez matplotlib
(`tests/test_report_card.py`).
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hydra_signals import strategies as strat  # noqa: E402
from live import market_analysis as ma  # noqa: E402

try:
    from zoneinfo import ZoneInfo

    TZ = ZoneInfo("Europe/Warsaw")
except Exception:  # noqa: BLE001
    TZ = dt.timezone(dt.timedelta(hours=1))

HOUR = 3600
DAY = 86400

# progi alarmów (w sekundach)
MAX_COMMIT_AGE = 2 * HOUR
MAX_CANDLE_AGE = 2 * HOUR
MAX_HOURLY_SOURCE_AGE = 3 * HOUR
MAX_STABLES_DAY_AGE = 3 * DAY
KNOWN_WARNINGS = ("base: przeskok zaleglosci",)
# ostrzeżenie źródła danych jest alarmem tylko wtedy, gdy to źródło jest teraz
# nieświeże; inaczej to przejściowy błąd (np. Deribit 503), który sam minął
WARNING_SOURCE = {"options:": "options", "derivs:": "derivs", "market:": "eth", "stables:": "stables",
                  "perp_flow:": "taker", "wallet_timing:": "wallets"}

NAMES = {
    "vote": "GŁOSOWANIE (główny)",
    "eth": "ETH kup i trzymaj",
    "trend14": "trend 14 dni",
    "hydra": "Hydra (portfele)",
    "btc14": "trend BTC",
    "ethbtc14": "ETH i BTC zgodne",
    "hybrid": "hybryda",
    "pullback": "trend + portfele",
    "skilled7": "portfele z wyczuciem",
    "antiweak7": "przeciw słabym portfelom",
    "taker24": "agresorzy 24 h",
    "trendoi": "trend + OI",
    "stables7": "stablecoiny",
    "trendstables": "trend + stablecoiny",
    "trendcalm": "trend w spokojnym rynku",
    "optskew": "skew opcji",
}

SOURCES = (
    ("eth", "Ceny ETH/BTC"),
    ("derivs", "Funding/OI"),
    ("options", "Opcje"),
    ("stables", "Stablecoiny"),
    ("taker", "Agresorzy"),
    ("wallets", "Portfele"),
)


# ---------------------------------------------------------------------------
# Formatowanie po polsku
# ---------------------------------------------------------------------------


def fmt_num(v: float, nd: int = 0) -> str:
    s = f"{abs(v):,.{nd}f}".replace(",", " ").replace(".", ",")
    return ("−" if v < 0 else "") + s


def fmt_pct(v: float | None, nd: int = 1, plus: bool = True) -> str:
    if v is None:
        return "–"
    sign = "+" if v > 0 and plus else ""
    return sign + fmt_num(v, nd) + "%"


def fmt_usd(v: float | None, nd: int = 0) -> str:
    return "–" if v is None else fmt_num(v, nd) + " $"


def local(ts: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts, TZ)


def fmt_time(ts: float | None) -> str:
    return "–" if ts is None else local(ts).strftime("%d.%m %H:%M")


# ---------------------------------------------------------------------------
# Wczytanie danych
# ---------------------------------------------------------------------------


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _manifest_ts(name: str) -> float | None:
    # 2026-11-14_095913748956_43583047754.json (UTC)
    try:
        d, t = os.path.basename(name).split("_")[:2]
        return dt.datetime.strptime(d + t[:6], "%Y-%m-%d%H%M%S").replace(tzinfo=dt.timezone.utc).timestamp()
    except (ValueError, IndexError):
        return None


def _auto_commits(repo: Path, days: int = 8) -> list[float]:
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), "log", f"--since={days} days ago", "--format=%ct|%s"],
            capture_output=True, text=True, timeout=30, check=False,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    return sorted(float(line.split("|", 1)[0]) for line in out.splitlines()
                  if "|" in line and "Automatyczna aktualizacja danych" in line)


def load(repo: Path) -> dict:
    data = repo / "data"
    manifests = []
    for p in sorted(glob.glob(str(data / "manifests" / "*.json")))[-400:]:
        ts = _manifest_ts(p)
        if ts is not None:
            manifests.append((ts, _load_json(Path(p), {})))
    return {
        "candles": _load_json(data / "candles_history.json", []),
        "race": _load_json(data / "race_state.json", {}),
        "market": _load_json(data / "market_history.json", {}),
        "derivs": _load_json(data / "derivs_history.json", {}),
        "options": _load_json(data / "options_history.json", {}),
        "stables": _load_json(data / "stables_history.json", {}),
        "manifests": manifests,
        "commits": _auto_commits(repo),
    }


# ---------------------------------------------------------------------------
# Obliczenia (czyste, testowalne)
# ---------------------------------------------------------------------------


def _curve_at(curve: list, ts: float) -> float:
    """Wynik krzywej kapitału (%) w chwili ts - ostatni punkt nie późniejszy;
    przed pierwszym punktem 0 (strategia jeszcze nie grała)."""
    val = 0.0
    for p in curve:
        if p[0] is None:
            continue
        if p[0] > ts:
            break
        val = p[2]
    return val


def _candle_at(candles: list[dict], ts: float) -> dict | None:
    best = None
    for c in candles:
        if c.get("ts") is not None and c["ts"] <= ts:
            best = c
    return best


def health(raw: dict, now: float) -> tuple[list[str], dict[str, bool], dict]:
    alarms: list[str] = []
    ok: dict[str, bool] = {}
    commits = raw.get("commits") or []
    if commits and now - commits[-1] > MAX_COMMIT_AGE:
        alarms.append(f"Ostatnia aktualizacja danych {fmt_time(commits[-1])} — ponad 2 h temu")
    elif not commits:
        alarms.append("Brak commitów automatu w historii repo (sprawdź GitHub Actions)")
    candles = [c for c in raw.get("candles") or [] if c.get("ts") is not None]
    last = candles[-1] if candles else {}
    if not last or now - last["ts"] > MAX_CANDLE_AGE:
        alarms.append("Brak nowej świecy od ponad 2 h")
    eth = ((raw.get("market") or {}).get("coins") or {}).get("ETH") or []
    ok["eth"] = bool(eth) and now - eth[-1][0] / 1000 <= MAX_HOURLY_SOURCE_AGE
    snaps = (raw.get("derivs") or {}).get("snapshots") or []
    ok["derivs"] = bool(snaps) and now - snaps[-1]["t"] / 1000 <= MAX_HOURLY_SOURCE_AGE
    o = raw.get("options") or {}
    dv, os_ = o.get("dvol") or [], o.get("snapshots") or []
    ok["options"] = (bool(dv) and now - dv[-1][0] / 1000 <= MAX_HOURLY_SOURCE_AGE
                     and bool(os_) and now - os_[-1]["t"] / 1000 <= MAX_HOURLY_SOURCE_AGE)
    st = (raw.get("stables") or {}).get("total") or []
    ok["stables"] = bool(st) and now - st[-1][0] <= MAX_STABLES_DAY_AGE
    ok["taker"] = last.get("takerImbalance24h") is not None
    ok["wallets"] = last.get("walletSkilledNet7d") is not None
    for key, label in SOURCES:
        if not ok[key]:
            alarms.append(f"Źródło „{label}” nie dostarcza świeżych danych")
    # manifesty z ostatniej doby: błędy w sekcjach źródeł i nieznane ostrzeżenia
    recent = [m for ts, m in raw.get("manifests") or [] if now - ts <= DAY]
    known = 0
    transient: dict[str, int] = {}
    other: dict[str, int] = {}
    for m in recent:
        for w in m.get("warnings") or []:
            if any(w.startswith(k) for k in KNOWN_WARNINGS):
                known += 1
                continue
            src = next((v for k, v in WARNING_SOURCE.items() if w.startswith(k)), None)
            if src is not None and ok.get(src):
                label = dict(SOURCES)[src]
                transient[label] = transient.get(label, 0) + 1
            else:
                other[w[:90]] = other.get(w[:90], 0) + 1
    for w, n in sorted(other.items(), key=lambda x: -x[1])[:3]:
        alarms.append(f"Ostrzeżenie ×{n}: {w}")
    stats = {
        "runs24h": len(recent),
        "maxDuration": max((m.get("duration_s") or 0 for m in recent), default=None),
        "knownWarnings": known,
        "transient": transient,
        "commits24h": sum(1 for t in commits if now - t <= DAY),
        "lastCommit": commits[-1] if commits else None,
    }
    return alarms, ok, stats


def collect(raw: dict, now: float, mode: str = "daily") -> dict:
    window = 7 * DAY if mode == "weekly" else DAY
    candles = [c for c in raw.get("candles") or [] if c.get("ts") is not None and c.get("price")]
    alarms, ok, stats = health(raw, now)
    out: dict = {"mode": mode, "now": now, "alarms": alarms, "sources": ok, "stats": stats}
    if not candles:
        return out
    last = candles[-1]
    sig = last.get("signal", "HOLD")
    # początek obecnej serii sygnału
    i = len(candles) - 1
    while i > 0 and candles[i - 1].get("signal") == sig:
        i -= 1
    entry = candles[i]
    d = {"LONG": 1, "SHORT": -1}.get(sig, 0)
    out["signal"] = {
        "value": sig,
        "since": entry["ts"],
        "entryPrice": entry["price"],
        "pnlPct": d * (last["price"] / entry["price"] - 1) * 100 if d else None,
    }
    prev = _candle_at(candles, now - DAY)
    prev_w = _candle_at(candles, now - 7 * DAY)
    out["price"] = {
        "value": last["price"],
        "ts": last["ts"],
        "chg24h": (last["price"] / prev["price"] - 1) * 100 if prev else None,
        "chg7d": (last["price"] / prev_w["price"] - 1) * 100 if prev_w else None,
        "sma": last.get("trendSma"),
        "distPct": last.get("trendDistPct"),
        "mode": last.get("marketMode"),
    }

    def counts(c: dict | None) -> dict | None:
        if not c or c.get("voteShort") is None:
            return None
        return {"SHORT": c["voteShort"], "HOLD": c["voteHold"], "LONG": c["voteLong"]}

    votes = last.get("votes") or {}
    out["votes"] = {
        "now": counts(last),
        "before": counts(_candle_at(candles, now - window)),
        "who": {s: [NAMES.get(k, k) for k, v in votes.items() if v == s] for s in ("SHORT", "HOLD", "LONG")},
    }
    # zmiany głównego sygnału w oknie
    changes = []
    for a, b in zip(candles, candles[1:]):
        if b["ts"] >= now - window and b.get("signal") != a.get("signal"):
            changes.append({"ts": b["ts"], "signal": b.get("signal"), "price": b["price"]})
    out["changes"] = changes
    # wykres ceny (okno: dzienny 7 dni, tygodniowy 30 dni)
    span = 30 * DAY if mode == "weekly" else 7 * DAY
    # bledne ceny z puli (np. 29.10.2026: 2986 $) - te same zasady co wyscig
    flags = strat.price_flags(candles)
    ch = [c for c, ok in zip(candles, flags) if ok and c["ts"] >= now - span]
    out["chart"] = {
        "t": [c["ts"] for c in ch],
        "price": [c["price"] for c in ch],
        "sma": [c.get("trendSma") for c in ch],
        "signal": [c.get("signal", "HOLD") for c in ch],
    }
    # wyścig na żywo
    race = raw.get("race") or {}
    rows = []
    for k, book in (race.get("strategies") or {}).items():
        s = strat.summarize_book(book)
        curve = book.get("curve") or []
        rows.append({
            "key": k,
            "name": NAMES.get(k, k),
            "result": s["resultPct"],
            "change": s["resultPct"] - _curve_at(curve, now - window),
            "position": s["position"],
            "trades": s["trades"],
        })
    rows.sort(key=lambda r: -r["result"])
    out["race"] = {"start": (race.get("start") or {}).get("ts"), "rows": rows,
                   "curves": {k: [[p[0], p[2]] for p in (b.get("curve") or []) if p[0] is not None]
                              for k, b in (race.get("strategies") or {}).items() if k in ("vote", "eth", "pullback", "trend14")}}
    if mode == "weekly":
        out["market"] = market_week(raw, last, now)
    return out


def market_week(raw: dict, last: dict, now: float) -> dict:
    m: dict = {}
    fund = [r for r in (raw.get("derivs") or {}).get("funding") or [] if r["t"] / 1000 >= now - 7 * DAY]
    if fund:
        m["fundingApr"] = sum(r["rate"] for r in fund) / len(fund) * 24 * 365 * 100
    snaps = [s for s in (raw.get("derivs") or {}).get("snapshots") or [] if s.get("oi")]
    wk = [s for s in snaps if s["t"] / 1000 >= now - 7 * DAY]
    if len(wk) >= 2:
        m["oiChg7d"] = (wk[-1]["oi"] / wk[0]["oi"] - 1) * 100
    st = (raw.get("stables") or {}).get("total") or []
    if len(st) >= 9:
        # przedostatni punkt = ostatni pełny dzień (tak jak na stronie)
        m["stables"] = st[-2][1]
        m["stablesChg7d"] = (st[-2][1] / st[-9][1] - 1) * 100
    if last.get("dvol") is not None:
        m["dvol"] = last["dvol"]
        m["dvolSma"] = last.get("dvolSma14")
    os_ = (raw.get("options") or {}).get("snapshots") or []
    if os_:
        m["skew"] = os_[-1].get("skew")
        m["pcOi"] = os_[-1].get("pcOi")
    m["skilled"] = last.get("walletSkilledNet7d")
    return m


def text_summary(s: dict) -> str:
    lines = []
    if s["alarms"]:
        lines.append("⚠ ALARMY: " + "; ".join(s["alarms"]))
    else:
        lines.append("✓ Automatyzacja i wszystkie źródła danych działają.")
    if "signal" in s:
        sg, p = s["signal"], s["price"]
        v = (s.get("votes") or {}).get("now")
        vt = f" (głosy {v['SHORT']} SHORT / {v['LONG']} LONG / {v['HOLD']} neutr.)" if v else ""
        lines.append(f"Sygnał {sg['value']}{vt} od {fmt_time(sg['since'])}"
                     + (f", {fmt_pct(sg['pnlPct'])} od wejścia" if sg["pnlPct"] is not None else "")
                     + f". ETH {fmt_usd(p['value'])} ({fmt_pct(p['chg24h'])} w 24 h).")
        vote = next((r for r in s["race"]["rows"] if r["key"] == "vote"), None)
        eth = next((r for r in s["race"]["rows"] if r["key"] == "eth"), None)
        if vote and eth:
            lines.append(f"Wyścig na żywo: głosowanie {fmt_pct(vote['result'])}, ETH kup i trzymaj {fmt_pct(eth['result'])}.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Rysowanie (matplotlib)
# ---------------------------------------------------------------------------

C = {
    "bg": "#0C0C10", "panel": "#131318", "border": "#2A2A33",
    "text": "#F3F1EC", "text2": "#A8A6B0", "muted": "#6E6C78",
    "good": "#3DFF9B", "goodd": "#1FA65F", "bad": "#FF3B5C", "badd": "#B8253F",
    "accent": "#FFC94A", "neutral": "#6E6C78",
}
SIG_COLOR = {"LONG": C["good"], "SHORT": C["bad"], "HOLD": C["text2"]}
SIG_LABEL = {"LONG": "LONG", "SHORT": "SHORT", "HOLD": "NEUTRALNY"}


def _analysis_lines(an: dict | None) -> list[str]:
    import textwrap

    if not an or not an.get("text"):
        return []
    lines: list[str] = []
    for para in str(an["text"]).split("\n"):
        lines += textwrap.wrap(para.strip(), width=92) or [""]
    return lines[:9]


def _font():
    from matplotlib import font_manager

    names = {f.name for f in font_manager.fontManager.ttflist}
    for n in ("IBM Plex Sans", "Inter", "DejaVu Sans"):
        if n in names:
            return n
    return "sans-serif"


def _panel(fig, rect, title=None):
    from matplotlib.patches import FancyBboxPatch

    x, y, w, h = rect
    fig.patches.append(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=0.012",
                                      transform=fig.transFigure, facecolor=C["panel"],
                                      edgecolor=C["border"], linewidth=1, zorder=-1))
    if title:
        fig.text(x + 0.03, y + h - 0.012, title.upper(), color=C["text2"], fontsize=10,
                 va="top", ha="left", fontweight="medium")


def render(s: dict, out: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    weekly = s["mode"] == "weekly"
    n_rows = len((s.get("race") or {}).get("rows") or [])
    an_lines = _analysis_lines(s.get("analysis"))
    an_px = (135 + 22 * len(an_lines)) if s.get("analysis") else 0
    px = 158 + 26 * min(len(s["alarms"]), 4) + 205 + an_px + 165 + (315 if weekly else 305) + 85 + 27 * n_rows + 80
    if weekly:
        px += 275 + 215
    H = px / 100
    fig = plt.figure(figsize=(10.8, H), dpi=100, facecolor=C["bg"])
    plt.rcParams["font.family"] = [_font(), "DejaVu Sans"]  # DejaVu: znaki ✓ ✕ ●

    def ytop(px_from_top):  # pozycja od góry w px -> ułamek figury
        return 1 - px_from_top / (H * 100)

    # --- nagłówek ---
    now = local(s["now"])
    fig.text(0.05, ytop(55), "HYDRA", color=C["accent"], fontsize=30, fontweight="bold", va="center")
    fig.text(0.255, ytop(55), "raport tygodniowy" if weekly else "raport dzienny", color=C["text2"],
             fontsize=15, va="center")
    fig.text(0.95, ytop(55), now.strftime("%d.%m.%Y, %H:%M"), color=C["text2"], fontsize=13, va="center", ha="right")
    # status
    n_al = len(s["alarms"])
    st_col = C["bad"] if n_al else C["good"]
    st_txt = (f"✕  {n_al} {'alarm' if n_al == 1 else 'alarmy' if n_al < 5 else 'alarmów'}" if n_al
              else "✓  wszystko działa")
    fig.text(0.05, ytop(110), st_txt, color=st_col, fontsize=14, fontweight="bold", va="center",
             bbox=dict(boxstyle="round,pad=0.45", facecolor=C["panel"], edgecolor=st_col, linewidth=1.2))
    stx = s["stats"]
    line = f"{stx['runs24h']} przebiegów w 24 h · ostatnia aktualizacja {fmt_time(stx['lastCommit'])}"
    fig.text(0.33, ytop(100), line, color=C["text2"], fontsize=11.5, va="center")
    if stx.get("transient"):
        tr = ", ".join(f"{k} ×{n}" for k, n in stx["transient"].items())
        fig.text(0.33, ytop(122), f"przejściowe błędy (dane już świeże): {tr}", color=C["muted"], fontsize=10.5,
                 va="center")
    y = 150
    for a in s["alarms"][:4]:
        fig.text(0.05, ytop(y), "• " + a, color=C["bad"], fontsize=11.5, va="center")
        y += 26
    y += 8

    if "signal" not in s:
        fig.text(0.05, ytop(y + 40), "Brak świec w historii.", color=C["text"], fontsize=16)
        fig.savefig(out, facecolor=C["bg"])
        plt.close(fig)
        return

    # --- sygnał + cena ---
    sg, p = s["signal"], s["price"]
    _panel(fig, (0.04, ytop(y + 190), 0.92, 190 / (H * 100)))
    fig.text(0.075, ytop(y + 22), "GŁÓWNY SYGNAŁ · GŁOSOWANIE STRATEGII", color=C["text2"], fontsize=10, va="top")
    fig.text(0.075, ytop(y + 85), SIG_LABEL[sg["value"]], color=SIG_COLOR[sg["value"]], fontsize=44,
             fontweight="bold", va="center")
    sub = f"od {fmt_time(sg['since'])} · wejście {fmt_usd(sg['entryPrice'])}"
    fig.text(0.075, ytop(y + 135), sub, color=C["text2"], fontsize=12.5, va="center")
    if sg["pnlPct"] is not None:
        fig.text(0.075, ytop(y + 165), f"{fmt_pct(sg['pnlPct'])} od wejścia", va="center", fontsize=14,
                 fontweight="bold", color=C["good"] if sg["pnlPct"] >= 0 else C["bad"])
    fig.text(0.925, ytop(y + 22), "ETH", color=C["text2"], fontsize=10, va="top", ha="right")
    fig.text(0.925, ytop(y + 80), fmt_usd(p["value"]), color=C["text"], fontsize=34, va="center", ha="right",
             fontweight="bold")
    chg = p["chg7d"] if weekly else p["chg24h"]
    fig.text(0.925, ytop(y + 125), f"{fmt_pct(chg)} {'w 7 dni' if weekly else 'w 24 h'}", ha="right", va="center",
             fontsize=14, color=C["text2"] if chg is None else (C["good"] if chg >= 0 else C["bad"]))
    if p["distPct"] is not None:
        fig.text(0.925, ytop(y + 160), f"{fmt_pct(p['distPct'], plus=False).lstrip('−')} {'nad' if p['distPct'] >= 0 else 'pod'} średnią 14 dni",
                 ha="right", va="center", fontsize=12, color=C["text2"])
    y += 205

    # --- ocena rynku (pisana przez Claude'a w zaplanowanym zadaniu) ---
    an = s.get("analysis")
    if an:
        _panel(fig, (0.04, ytop(y + an_px - 15), 0.92, (an_px - 15) / (H * 100)), "Ocena rynku")
        if an.get("stage"):
            fig.text(0.925, ytop(y + 12), an["stage"], color=C["accent"], fontsize=11, va="top", ha="right",
                     fontweight="bold")
        arrows = {"wzrost": ("↑", C["good"]), "spadek": ("↓", C["bad"]), "bok": ("→", C["text2"])}
        cx = 0.075
        for lab, key in (("24 h", "f24"), ("7 dni", "f7")):
            val = an.get(key)
            if val in arrows:
                ar, col = arrows[val]
                t = fig.text(cx, ytop(y + 68), f"{lab}: {ar} {val}", color=col, fontsize=13, fontweight="bold",
                             va="center", bbox=dict(boxstyle="round,pad=0.35", facecolor=C["bg"], edgecolor=col,
                                                    linewidth=1))
                cx += 0.2
        if an.get("conf"):
            fig.text(cx, ytop(y + 68), f"pewność: {an['conf']}", color=C["text2"], fontsize=11.5, va="center")
        yy = y + 105
        for ln in an_lines:
            fig.text(0.075, ytop(yy), ln, color=C["text"], fontsize=12, va="center")
            yy += 22
        if an.get("track"):
            fig.text(0.075, ytop(yy + 4), an["track"], color=C["muted"], fontsize=10.5, va="center")
        y += an_px

    # --- głosy ---
    v = s["votes"]["now"]
    _panel(fig, (0.04, ytop(y + 150), 0.92, 150 / (H * 100)), "Kto jak głosuje")
    if v:
        ax = fig.add_axes((0.075, ytop(y + 85), 0.85, 32 / (H * 100)))
        ax.set_axis_off()
        total = sum(v.values()) or 1
        left = 0
        for key, col in (("SHORT", C["badd"]), ("HOLD", "#3A3A45"), ("LONG", C["goodd"])):
            n = v[key]
            if n:
                ax.barh(0, n, left=left, color=col, height=1, edgecolor=C["panel"], linewidth=2)
                lab = {"SHORT": "SHORT", "HOLD": "neutr.", "LONG": "LONG"}[key]
                ax.text(left + n / 2, 0, f"{n} {lab}", ha="center", va="center", color=C["text"], fontsize=12,
                        fontweight="bold")
            left += n
        ax.set_xlim(0, total)
        ax.set_ylim(-0.5, 0.5)
        b = s["votes"]["before"]
        if b:
            delta = "bez zmian" if b == v else f"{b['SHORT']} SHORT / {b['HOLD']} neutr. / {b['LONG']} LONG"
        else:
            delta = "głosowania jeszcze nie było"
        fig.text(0.075, ytop(y + 112), f"{'Tydzień' if weekly else 'Dobę'} temu: {delta}",
                 color=C["text2"], fontsize=11, va="center")
        neutral = s["votes"]["who"]["HOLD"]
        if neutral:
            fig.text(0.075, ytop(y + 134), "neutralnie: " + ", ".join(neutral), color=C["muted"], fontsize=10.5,
                     va="center")
    y += 165

    # --- wykres ceny ---
    chh = 300 if weekly else 290
    _panel(fig, (0.04, ytop(y + chh), 0.92, chh / (H * 100)),
           "Cena ETH i sygnał — 30 dni" if weekly else "Cena ETH i sygnał — 7 dni")
    chart = s["chart"]
    ax = fig.add_axes((0.11, ytop(y + chh - 40), 0.82, (chh - 85) / (H * 100)))
    ax.set_facecolor(C["panel"])
    t = [local(x).replace(tzinfo=None) for x in chart["t"]]
    if len(t) >= 2:
        # tło według sygnału
        start = 0
        for k in range(1, len(t) + 1):
            if k == len(t) or chart["signal"][k] != chart["signal"][start]:
                sgn = chart["signal"][start]
                if sgn in ("LONG", "SHORT"):
                    ax.axvspan(t[start], t[min(k, len(t) - 1)], color=C["goodd"] if sgn == "LONG" else C["badd"],
                               alpha=0.22, linewidth=0)
                start = k
        sma = [(a, b) for a, b in zip(t, chart["sma"]) if b is not None]
        if sma:
            ax.plot([a for a, _ in sma], [b for _, b in sma], color=C["text2"], linewidth=1.3, linestyle=(0, (4, 3)))
        ax.plot(t, chart["price"], color=C["text"], linewidth=1.8)
        ax.scatter([t[-1]], [chart["price"][-1]], color=C["accent"], s=28, zorder=5)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=6))
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda val, _: fmt_num(val)))
        ax.grid(axis="y", color=C["border"], linewidth=0.8)
        ax.margins(x=0)
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.tick_params(colors=C["text2"], labelsize=10, length=0)
    fig.text(0.925, ytop(y + 20), "— cena   - - średnia 14 dni   tło: sygnał", color=C["muted"], fontsize=9.5,
             va="center", ha="right")
    y += chh + 15

    # --- wyścig ---
    rows = s["race"]["rows"]
    rh = 27
    rk = 70 + rh * len(rows)
    title = "Wyścig na żywo — wynik od startu 14.11"
    _panel(fig, (0.04, ytop(y + rk), 0.92, rk / (H * 100)), title)
    if rows:
        ax = fig.add_axes((0.36, ytop(y + rk - 25), 0.42 if weekly else 0.5, (rk - 70) / (H * 100)))
        if weekly:
            fig.text(0.875, ytop(y + 20), "tydzień", color=C["muted"], fontsize=9.5, va="center", ha="right")
        ax.set_facecolor(C["panel"])
        vals = [r["result"] for r in rows]
        lo, hi = min(0, min(vals)), max(0, max(vals))
        pad = (hi - lo) * 0.18 or 1
        for i, r in enumerate(rows):
            col = C["goodd"] if r["result"] >= 0 else C["badd"]
            if r["key"] == "vote":
                col = C["accent"]
            elif r["key"] == "eth":
                col = C["neutral"]
            ax.barh(i, r["result"], color=col, height=0.62)
            if weekly:
                fig.text(0.875, ytop(y + 45 + rh * i + rh / 2), fmt_pct(r["change"]), color=C["text2"], fontsize=10.5,
                         va="center", ha="right")
            ax.text(r["result"] + (pad * 0.08 if r["result"] >= 0 else -pad * 0.08), i, fmt_pct(r["result"]),
                    va="center", ha="left" if r["result"] >= 0 else "right", color=C["text"], fontsize=10.5)
            name_col = C["accent"] if r["key"] == "vote" else C["text"] if r["key"] != "eth" else C["text2"]
            fig.text(0.075, ytop(y + 45 + rh * i + rh / 2), r["name"], color=name_col, fontsize=11,
                     va="center", fontweight="bold" if r["key"] in ("vote", "eth") else "normal")
            pos = r["position"]
            fig.text(0.925, ytop(y + 45 + rh * i + rh / 2), {"LONG": "L", "SHORT": "S", "HOLD": "–"}[pos],
                     color=SIG_COLOR[pos], fontsize=11, va="center", ha="right", fontweight="bold")
        ax.axvline(0, color=C["text2"], linewidth=0.8)
        ax.set_xlim(lo - pad, hi + pad)
        ax.set_ylim(len(rows) - 0.5, -0.5)
        ax.set_axis_off()
    y += rk + 15

    # --- tygodniowe: krzywe kapitału i rynek ---
    if weekly:
        ch2 = 260
        _panel(fig, (0.04, ytop(y + ch2), 0.92, ch2 / (H * 100)), "Kapitał na żywo — główny sygnał vs reszta")
        ax = fig.add_axes((0.11, ytop(y + ch2 - 40), 0.82, (ch2 - 85) / (H * 100)))
        ax.set_facecolor(C["panel"])
        styles = {"vote": (C["accent"], 2.2, "-", "głosowanie"), "pullback": ("#FF8A65", 1.4, "-", "trend + portfele"),
                  "eth": (C["text2"], 1.4, "--", "ETH kup i trzymaj")}
        for k, (col, lw, ls, lab) in styles.items():
            cv = s["race"]["curves"].get(k) or []
            if len(cv) >= 2:
                ax.plot([local(a).replace(tzinfo=None) for a, _ in cv], [b for _, b in cv], color=col, linewidth=lw,
                        linestyle=ls, label=lab)
        ax.axhline(0, color=C["border"], linewidth=0.9)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda val, _: fmt_pct(val, 0)))
        ax.grid(axis="y", color=C["border"], linewidth=0.8)
        for sp in ax.spines.values():
            sp.set_visible(False)
        ax.tick_params(colors=C["text2"], labelsize=10, length=0)
        lx = 0.925
        for k in ("eth", "pullback", "vote"):
            col, _lw, ls, lab = styles[k]
            t_ = fig.text(lx, ytop(y + 20), ("- - " if ls == "--" else "— ") + lab, color=col, fontsize=10,
                          va="center", ha="right")
            lx -= 0.012 + 0.0095 * len(lab) + 0.02
        y += ch2 + 15

        m = s.get("market") or {}
        mh = 200
        _panel(fig, (0.04, ytop(y + mh), 0.92, mh / (H * 100)), "Rynek w tygodniu")
        tiles = [
            ("Funding (śr. rocznie)", fmt_pct(m.get("fundingApr")), None),
            ("Open interest 7 dni", fmt_pct(m.get("oiChg7d")), m.get("oiChg7d")),
            ("Stablecoiny 7 dni", fmt_pct(m.get("stablesChg7d"), 2), m.get("stablesChg7d")),
            ("DVOL / średnia 14 d",
             f"{fmt_num(m['dvol'], 1)} / {fmt_num(m['dvolSma'], 1)}" if m.get("dvol") and m.get("dvolSma") else "–", None),
            ("Skew opcji · put/call", f"{fmt_num(m['skew'], 1)} · {fmt_num(m['pcOi'], 2)}" if m.get("skew") is not None and m.get("pcOi") is not None else "–", None),
            ("Portfele z wyczuciem 7 d", (fmt_num(m["skilled"] / 1000, 1) + " tys. ETH") if m.get("skilled") is not None else "–", m.get("skilled")),
        ]
        for i, (lab, val, sgn) in enumerate(tiles):
            cx = 0.075 + (i % 3) * 0.29
            cy = y + 55 + (i // 3) * 75
            fig.text(cx, ytop(cy), lab, color=C["text2"], fontsize=10.5, va="center")
            col = C["text"] if sgn is None else (C["good"] if sgn > 0 else C["bad"] if sgn < 0 else C["text"])
            fig.text(cx, ytop(cy + 32), val, color=col, fontsize=17, fontweight="bold", va="center")
        y += mh + 15

    # --- źródła danych ---
    xs = 0.05
    for key, label in SOURCES:
        good = s["sources"].get(key)
        fig.text(xs, ytop(y + 18), ("● " if good else "✕ ") + label, color=C["good"] if good else C["bad"],
                 fontsize=11, va="center")
        xs += 0.155
    fig.text(0.05, ytop(y + 50), "Papierowy system Hydra — to nie jest porada inwestycyjna.", color=C["muted"],
             fontsize=9.5, va="center")
    fig.savefig(out, facecolor=C["bg"])
    plt.close(fig)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", default=str(ROOT))
    ap.add_argument("--mode", choices=("daily", "weekly"), default="daily")
    ap.add_argument("--out", default="hydra_raport.png")
    ap.add_argument("--now", type=float, default=None, help="unix s (domyślnie teraz)")
    ap.add_argument("--stats-json", default=None,
                    help="zapisz statystyki rynku z wielu okresów (+ rozliczenie prognoz) do JSON - podstawa analizy")
    ap.add_argument("--forecast-log", default=None,
                    help="plik z dziennikiem prognoz (tabela z claude/hydrav2-prognozy.md) do rozliczenia")
    ap.add_argument("--analysis", default=None,
                    help='JSON z oceną rynku: {"stage", "text", "f24", "f7", "conf", "track"} - rysowany na karcie')
    ap.add_argument("--no-render", action="store_true", help="tylko statystyki, bez obrazka")
    ap.add_argument("--from-archive", action="store_true",
                    help="ocena rynku z dzisiejszego wpisu w data/reports.json (napisanego przez Claude'a w "
                         "GitHub Actions); kod wyjścia 3, gdy takiego wpisu nie ma (brak klucza API albo błąd)")
    args = ap.parse_args(argv)
    raw = load(Path(args.repo))
    now = args.now or dt.datetime.now(dt.timezone.utc).timestamp()
    s = collect(raw, now, args.mode)
    if args.stats_json:
        stats = ma.analysis_stats(raw, now)
        if args.forecast_log:
            try:
                rows = ma.parse_forecasts(Path(args.forecast_log).read_text(encoding="utf-8"))
            except OSError:
                rows = []
            coins = (raw.get("market") or {}).get("coins") or {}
            stats["forecast_score"] = ma.score_forecasts(rows, ma.Series(coins.get("ETH")), now)
        stats["race_live"] = [{k: r[k] for k in ("key", "name", "result", "change", "position", "trades")}
                              for r in (s.get("race") or {}).get("rows", [])]
        Path(args.stats_json).write_text(json.dumps(stats, ensure_ascii=False, indent=1), encoding="utf-8")
    if args.from_archive:
        reports = _load_json(Path(args.repo) / "data" / "reports.json", [])
        entry = next((e for e in reversed(reports if isinstance(reports, list) else [])
                      if e.get("type") == args.mode and now - e.get("ts", 0) <= 4 * HOUR), None)
        if not entry or not str(entry.get("source", "")).startswith("Claude"):
            print("BRAK_ANALIZY_Z_ARCHIWUM: " + ("tylko ocena z reguł" if entry else "brak dzisiejszego wpisu"),
                  file=sys.stderr)
            return 3
        s["analysis"] = {k: entry.get(k) for k in ("stage", "text", "f24", "f7", "conf", "track")}
    if args.analysis:
        try:
            s["analysis"] = json.loads(Path(args.analysis).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(f"(nie udało się wczytać oceny rynku: {exc})", file=sys.stderr)
    if not args.no_render:
        render(s, args.out)
    print(text_summary(s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
