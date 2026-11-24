"""Statystyki rynku z wielu przedziałów czasu + rozliczanie prognoz - dla
analizy w raportach dziennych i tygodniowych (prośba Jakuba z 24.11.2026:
„analizował wyniki, patrzył na statystyki z różnych przedziałów czasowych i do
raportów dodawał parę zdań: na jakim etapie rynku jesteśmy, co się dzieje i w
którą stronę może pójść”).

Ten moduł tylko LICZY (czyste funkcje, bez sieci). Słowną ocenę i prognozę
pisze zaplanowane zadanie Claude'a na podstawie `analysis_stats()`; prognozy
trafiają do dziennika w projekcie (`claude/hydrav2-prognozy.md`), a
`score_forecasts()` rozlicza je z rzeczywistą ceną ETH, obok prostego punktu
odniesienia („prognoza = kierunek głównego sygnału”), żeby było widać, czy
słowne prognozy w ogóle cokolwiek dodają.

Ceny ETH/BTC biorę z `data/market_history.json` (świece godzinowe Hyperliquid,
~200 dni) - bez błędnych odczytów z puli i z dłuższą historią niż świece Hydry.
"""

from __future__ import annotations

import bisect
import math
import re
from typing import Iterable

HOUR = 3600
DAY = 86400

WINDOWS = (("24h", 1), ("3d", 3), ("7d", 7), ("14d", 14), ("30d", 30), ("90d", 90))
# pasmo „bez zmian” przy rozliczaniu prognoz (w %)
FLAT_BAND = {"24h": 1.0, "7d": 3.0}
HORIZON = {"24h": DAY, "7d": 7 * DAY}
UP, DOWN, FLAT = "wzrost", "spadek", "bok"


class Series:
    """Godzinowa seria [close_ms, close] z wyszukiwaniem „ostatnia cena znana w chwili ts”."""

    def __init__(self, rows: Iterable | None):
        rows = sorted(rows or [], key=lambda r: r[0])
        self.t = [r[0] / 1000.0 for r in rows]
        self.v = [float(r[1]) for r in rows]

    def at(self, ts: float, max_age: float = 3 * HOUR) -> float | None:
        i = bisect.bisect_right(self.t, ts) - 1
        if i < 0 or ts - self.t[i] > max_age:
            return None
        return self.v[i]

    def window(self, start: float, end: float) -> list[float]:
        lo = bisect.bisect_left(self.t, start)
        hi = bisect.bisect_right(self.t, end)
        return self.v[lo:hi]

    def daily_closes(self, end: float) -> list[float]:
        """Ostatnie zamknięcie każdego dnia UTC do `end` (bez dnia bieżącego)."""
        out: dict[int, float] = {}
        cut = (int(end) // DAY) * DAY
        for t, v in zip(self.t, self.v):
            if t <= cut:
                out[int((t - 1) // DAY)] = v
        return [out[d] for d in sorted(out)]


def pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or a == 0:
        return None
    return (b / a - 1.0) * 100.0


def _r(v, nd=2):
    return None if v is None else round(v, nd)


def rsi(closes: list[float], n: int = 14) -> float | None:
    """RSI Wildera z zamknięć dziennych."""
    if len(closes) < n + 1:
        return None
    gains = losses = 0.0
    for a, b in zip(closes[:n], closes[1 : n + 1]):
        d = b - a
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    ag, al = gains / n, losses / n
    for a, b in zip(closes[n:], closes[n + 1 :]):
        d = b - a
        ag = (ag * (n - 1) + max(d, 0.0)) / n
        al = (al * (n - 1) + max(-d, 0.0)) / n
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def realized_vol(values: list[float]) -> float | None:
    """Zmienność zrealizowana z cen godzinowych, w % w skali roku."""
    if len(values) < 3:
        return None
    rets = [math.log(b / a) for a, b in zip(values, values[1:]) if a > 0 and b > 0]
    if len(rets) < 2:
        return None
    m = sum(rets) / len(rets)
    var = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(24 * 365) * 100.0


def _sma(values: list[float], n: int) -> float | None:
    return sum(values[-n:]) / n if len(values) >= n else None


def _candle_at(candles: list[dict], ts: float) -> dict | None:
    best = None
    for c in candles:
        if c.get("ts") is None:
            continue
        if c["ts"] > ts:
            break
        best = c
    return best


def price_block(series: Series, now: float, first_ts: float | None) -> dict:
    p = series.at(now)
    out: dict = {"now": _r(p, 2)}
    if p is None:
        return out
    ch = {k: _r(pct(series.at(now - d * DAY), p)) for k, d in WINDOWS}
    if first_ts is not None:
        ch["od_poczatku_hydry"] = _r(pct(series.at(first_ts, max_age=6 * HOUR), p))
    out["change_pct"] = ch
    daily = series.daily_closes(now)
    for n in (14, 50, 100):
        s = _sma(daily, n)
        out[f"dist_sma{n}d_pct"] = _r(pct(s, p))
    s14_now, s14_wk = _sma(daily, 14), _sma(daily[:-7], 14) if len(daily) > 21 else None
    out["sma14d_slope_7d_pct"] = _r(pct(s14_wk, s14_now))
    out["rsi14d"] = _r(rsi(daily), 1)
    w30 = series.window(now - 30 * DAY, now)
    if w30:
        lo, hi = min(w30), max(w30)
        out["range30d"] = {"low": _r(lo), "high": _r(hi),
                           "position_0_1": _r((p - lo) / (hi - lo), 2) if hi > lo else None,
                           "from_high_pct": _r(pct(hi, p)), "from_low_pct": _r(pct(lo, p))}
    out["realized_vol_pct"] = {k: _r(realized_vol(series.window(now - d * DAY, now)), 1)
                               for k, d in (("24h", 1), ("7d", 7), ("30d", 30))}
    last7 = daily[-8:]
    out["down_days_of_last_7"] = sum(1 for a, b in zip(last7, last7[1:]) if b < a) if len(last7) == 8 else None
    return out


def analysis_stats(raw: dict, now: float) -> dict:
    """Wszystkie liczby, na których opiera się słowna analiza (słownik JSON)."""
    coins = (raw.get("market") or {}).get("coins") or {}
    eth, btc = Series(coins.get("ETH")), Series(coins.get("BTC"))
    # tylko to, co było znane w chwili `now` (pozwala też liczyć wstecz)
    candles = [c for c in raw.get("candles") or [] if c.get("ts") is not None and c.get("price") and c["ts"] <= now]
    first_ts = candles[0]["ts"] if candles else None
    last = candles[-1] if candles else {}
    st: dict = {"generated_ts": int(now), "eth": price_block(eth, now, first_ts), "btc": price_block(btc, now, first_ts)}
    # ETH względem BTC: różnica zmian w punktach procentowych (+ = ETH silniejszy)
    st["eth_vs_btc_change_pct"] = {
        k: _r((st["eth"].get("change_pct") or {}).get(k) - (st["btc"].get("change_pct") or {}).get(k))
        if (st["eth"].get("change_pct") or {}).get(k) is not None and (st["btc"].get("change_pct") or {}).get(k) is not None else None
        for k, _ in WINDOWS
    }

    # sygnał i głosowanie w czasie
    def votes(c):
        if not c or c.get("voteShort") is None:
            return None
        return {"SHORT": c["voteShort"], "LONG": c["voteLong"], "HOLD": c["voteHold"]}

    sig = last.get("signal")
    i = len(candles) - 1
    while i > 0 and candles[i - 1].get("signal") == sig:
        i -= 1
    changes = [(b["ts"], b.get("signal")) for a, b in zip(candles, candles[1:]) if b.get("signal") != a.get("signal")]
    st["signal"] = {
        "now": sig,
        "since_ts": candles[i]["ts"] if candles else None,
        "hours_in_signal": _r((now - candles[i]["ts"]) / HOUR, 1) if candles else None,
        "entry_price": candles[i]["price"] if candles else None,
        "votes": {k: votes(_candle_at(candles, now - d * DAY)) for k, d in (("teraz", 0), ("24h_temu", 1), ("3d_temu", 3), ("7d_temu", 7))},
        "votes_detail": last.get("votes"),
        "changes_7d": sum(1 for t, _ in changes if t >= now - 7 * DAY),
        "changes_30d": sum(1 for t, _ in changes if t >= now - 30 * DAY),
        "trend14_dist_pct": last.get("trendDistPct"),
        "market_mode": last.get("marketMode"),
        "efficiency_ratio_14d": last.get("trendEr14"),
    }

    # rynek kontraktów (Hyperliquid)
    d = raw.get("derivs") or {}
    fund = [r for r in d.get("funding") or [] if r["t"] / 1000 <= now]

    def fund_avg(days):
        rows = [r["rate"] for r in fund if r["t"] / 1000 >= now - days * DAY]
        return _r(sum(rows) / len(rows) * 24 * 365 * 100, 1) if rows else None

    snaps = [s for s in d.get("snapshots") or [] if s.get("oi") and s["t"] / 1000 <= now]
    oi = Series([[s["t"], s["oi"]] for s in snaps])
    oi_now = oi.at(now, 6 * HOUR)
    wk = [c for c in candles if c["ts"] >= now - 7 * DAY]
    forced = [c.get("forcedClose") for c in wk if c.get("forcedClose")]
    st["derivs"] = {
        "funding_apr_pct": {"teraz": _r(fund[-1]["rate"] * 24 * 365 * 100, 1) if fund else None,
                            "sr_24h": fund_avg(1), "sr_7d": fund_avg(7), "sr_30d": fund_avg(30)},
        "oi_eth_now": _r(oi_now, 0),
        "oi_change_pct": {"24h": _r(pct(oi.at(now - DAY, 6 * HOUR), oi_now)),
                          "7d": _r(pct(oi.at(now - 7 * DAY, 12 * HOUR), oi_now))},
        "oi_price_state_now": last.get("oiPriceState"),
        "forced_closes_7d": {"LONGS": forced.count("LONGS"), "SHORTS": forced.count("SHORTS")},
        "taker_imbalance_24h_now": last.get("takerImbalance24h"),
        "taker_imbalance_avg_7d": _r(sum(c["takerImbalance24h"] for c in wk if c.get("takerImbalance24h") is not None)
                                     / max(1, sum(1 for c in wk if c.get("takerImbalance24h") is not None)), 3)
        if any(c.get("takerImbalance24h") is not None for c in wk) else None,
    }

    # stablecoiny (ostatni pełny dzień = przedostatni punkt)
    # tylko pełne dni znane w chwili `now` (dzień D jest pełny od D + 24 h)
    tot = sorted((r for r in (raw.get("stables") or {}).get("total") or [] if r[0] + DAY <= now), key=lambda r: r[0])
    by_day = {int(r[0]): r[1] for r in tot}
    sb: dict = {}
    if tot:
        dl, vl = int(tot[-1][0]), tot[-1][1]
        sb = {"supply_usd_bn": _r(vl / 1e9, 1)}
        for k, n in (("7d", 7), ("30d", 30), ("90d", 90)):
            sb[f"change_{k}_pct"] = _r(pct(by_day.get(dl - n * DAY), vl), 2)
    st["stables"] = sb

    # opcje (Deribit)
    o = raw.get("options") or {}
    dv = Series(o.get("dvol"))
    dv_now = dv.at(now)
    dv200 = dv.window(now - 200 * DAY, now)
    osn = sorted((s for s in o.get("snapshots") or [] if s["t"] / 1000 <= now), key=lambda s: s["t"])
    skew = Series([[s["t"], s["skew"]] for s in osn if s.get("skew") is not None])
    st["options"] = {
        "dvol_now": _r(dv_now, 1),
        "dvol_sma14d": _r(sum(dv.window(now - 14 * DAY, now)) / len(dv.window(now - 14 * DAY, now)), 1)
        if dv.window(now - 14 * DAY, now) else None,
        "dvol_change_7d_pct": _r(pct(dv.at(now - 7 * DAY), dv_now)),
        "dvol_percentile_200d": _r(sum(1 for v in dv200 if v <= dv_now) / len(dv200) * 100, 0) if dv200 and dv_now else None,
        "skew_now": _r(skew.at(now, 6 * HOUR), 1),
        "skew_24h_ago": _r(skew.at(now - DAY, 6 * HOUR), 1),
        "skew_7d_ago": _r(skew.at(now - 7 * DAY, 6 * HOUR), 1),
        "put_call_oi_now": osn[-1].get("pcOi") if osn and now - osn[-1]["t"] / 1000 <= 6 * HOUR else None,
    }

    # portfele z wyczuciem (akumulacja 7 dni, ETH)
    wago = _candle_at(candles, now - 7 * DAY) or {}
    st["wallets"] = {
        "skilled_net7d_eth": last.get("walletSkilledNet7d"),
        "skilled_net7d_eth_week_ago": wago.get("walletSkilledNet7d"),
        "unskilled_net7d_eth": last.get("walletUnskilledNet7d"),
        "hydra_wallet_signal": last.get("signalHydra"),
        "composite": last.get("composite"),
    }
    st["hints"] = stage_hints(st)
    return st


def stage_hints(st: dict) -> list[str]:
    """Proste, deterministyczne etykiety jako punkt wyjścia do słownej oceny."""
    h: list[str] = []
    e = st.get("eth") or {}
    d14, slope, rsi_ = e.get("dist_sma14d_pct"), e.get("sma14d_slope_7d_pct"), e.get("rsi14d")
    if d14 is not None and slope is not None:
        if d14 < 0 and slope < 0:
            h.append("trend krótkoterminowy spadkowy (cena pod opadającą średnią 14 dni)")
        elif d14 > 0 and slope > 0:
            h.append("trend krótkoterminowy wzrostowy (cena nad rosnącą średnią 14 dni)")
        else:
            h.append("trend krótkoterminowy niejednoznaczny")
    d50, d100 = e.get("dist_sma50d_pct"), e.get("dist_sma100d_pct")
    if d50 is not None and d100 is not None:
        if d50 < 0 and d100 < 0:
            h.append("szerszy obraz: cena pod średnimi 50 i 100 dni (rynek niedźwiedzia w średnim terminie)")
        elif d50 > 0 and d100 > 0:
            h.append("szerszy obraz: cena nad średnimi 50 i 100 dni")
    if rsi_ is not None:
        if rsi_ < 30:
            h.append(f"RSI 14 dni {rsi_:.0f}: rynek wyprzedany (częste odbicia, ale w silnym trendzie potrafi długo zostać nisko)")
        elif rsi_ > 70:
            h.append(f"RSI 14 dni {rsi_:.0f}: rynek wykupiony")
    if d14 is not None and abs(d14) >= 15:
        h.append(f"cena {abs(d14):.0f}% od średniej 14 dni: ruch mocno rozciągnięty")
    der = st.get("derivs") or {}
    f24 = (der.get("funding_apr_pct") or {}).get("sr_24h")
    if f24 is not None and f24 < 0:
        h.append("funding ujemny: na kontraktach przeważają shorty (paliwo na gwałtowne odbicie, jeśli cena skoczy)")
    elif f24 is not None and f24 > 30:
        h.append("funding wysoki: longi przepłacają (ryzyko wymuszonych zamknięć longów)")
    op = st.get("options") or {}
    if op.get("dvol_now") and op.get("dvol_sma14d") and op["dvol_now"] > op["dvol_sma14d"] * 1.1:
        h.append("oczekiwana zmienność (DVOL) wyraźnie nad średnią: nerwowy rynek")
    if op.get("skew_now") is not None and op["skew_now"] > 5:
        h.append("skew opcji wysoki: ochrona przed spadkiem droga, rynek boi się dalszych spadków")
    sb = st.get("stables") or {}
    if sb.get("change_7d_pct") is not None:
        v = sb["change_7d_pct"]
        h.append("stablecoiny " + ("odpływają" if v < 0 else "napływają")
                 + f" ({'+' if v > 0 else '−' if v < 0 else ''}{abs(v):.2f}% w 7 dni)".replace(".", ","))
    return h


# ---------------------------------------------------------------------------
# Dziennik prognoz (claude/hydrav2-prognozy.md)
# ---------------------------------------------------------------------------

ROW_RE = re.compile(
    r"^\|\s*(\d{9,11})\s*\|[^|]*\|\s*([\d.,]+)\s*\|\s*(wzrost|spadek|bok)\s*\|\s*(wzrost|spadek|bok)\s*\|\s*([^|]*)\|\s*(LONG|SHORT|HOLD|NEUTRALNY|-)?\s*\|?"
)


def parse_forecasts(text: str) -> list[dict]:
    """Wiersze tabeli: | unix_ts | czas | cena | prognoza 24 h | prognoza 7 d | pewność | sygnał |"""
    out = []
    for line in (text or "").splitlines():
        m = ROW_RE.match(line.strip())
        if m:
            out.append({
                "ts": int(m.group(1)),
                "price": float(m.group(2).replace(",", ".")),
                "24h": m.group(3),
                "7d": m.group(4),
                "conf": m.group(5).strip(),
                "signal": (m.group(6) or "-").replace("NEUTRALNY", "HOLD"),
            })
    return out


def realized_direction(change_pct: float, horizon: str) -> str:
    band = FLAT_BAND[horizon]
    return UP if change_pct > band else DOWN if change_pct < -band else FLAT


def score_forecasts(rows: list[dict], series: Series, now: float) -> dict:
    """Trafność prognoz słownych i punktu odniesienia „kierunek głównego
    sygnału” (LONG = wzrost, SHORT = spadek, neutralny = bok) na tych samych
    chwilach. Prognoza jest rozliczana dopiero po upływie swojego horyzontu."""
    base = {"LONG": UP, "SHORT": DOWN, "HOLD": FLAT}
    out: dict = {}
    for hz in ("24h", "7d"):
        n = hits = base_hits = 0
        recent = []
        for r in rows:
            end = r["ts"] + HORIZON[hz]
            if end > now:
                continue
            p0, p1 = series.at(r["ts"]), series.at(end)
            if p0 is None or p1 is None:
                continue
            real = realized_direction(pct(p0, p1), hz)
            n += 1
            hit = r[hz] == real
            hits += hit
            if r["signal"] in base:
                base_hits += base[r["signal"]] == real
            recent.append(hit)
        out[hz] = {
            "rozliczone": n,
            "trafione": hits,
            "trafnosc_pct": _r(hits / n * 100, 0) if n else None,
            "ostatnie_10": f"{sum(recent[-10:])}/{len(recent[-10:])}" if recent else None,
            "punkt_odniesienia_sygnal_trafnosc_pct": _r(base_hits / n * 100, 0) if n else None,
        }
    out["oczekujace"] = sum(1 for r in rows if r["ts"] + HORIZON["24h"] > now)
    return out


def rows_from_reports(entries: Iterable[dict]) -> list[dict]:
    """Wpisy archiwum `data/reports.json` -> wiersze dla `score_forecasts`."""
    out = []
    for e in entries:
        if e.get("f24") in (UP, DOWN, FLAT) and e.get("f7") in (UP, DOWN, FLAT) and e.get("ts"):
            out.append({"ts": int(e["ts"]), "price": e.get("price"), "24h": e["f24"], "7d": e["f7"],
                        "conf": e.get("conf", ""), "signal": e.get("signal") or "-"})
    return out


def forecast_outcome(entry: dict, series: Series, now: float) -> dict:
    """Rozliczenie jednej prognozy: dla 24 h i 7 dni "trafiona" / "chybiona" /
    "czeka" + rzeczywista zmiana ceny (%)."""
    out = {}
    for hz, key in (("24h", "f24"), ("7d", "f7")):
        end = entry["ts"] + HORIZON[hz]
        p0, p1 = series.at(entry["ts"]), series.at(end)
        if end > now or p0 is None or p1 is None:
            out[hz] = {"status": "czeka", "change_pct": None}
            continue
        ch = pct(p0, p1)
        out[hz] = {"status": "trafiona" if entry.get(key) == realized_direction(ch, hz) else "chybiona",
                   "change_pct": _r(ch)}
    return out
