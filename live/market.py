"""BTC and ETH hourly prices from Hyperliquid: history + per-candle features
(Etap 2 planu "trend", 14.11.2026).

`data/market_history.json` keeps closed hourly candles as `[close_ms, close]`
per coin. The first run backfills `backfill_days` (Hyperliquid serves the last
5000 hourly candles, ~208 days); later runs append only what is new.

`build_lookups()` turns the history into three trend series - BTC, ETH (the
perp price, used as a reference to catch bad pool prices) and the ETH/BTC
ratio - each with the same 14-day rule as the headline signal
(`hydra_signals.strategies.TrendWindow`). `features_at()` then answers, for
one candle time, the fields stored on the candle. Everything degrades to None,
never to an error: no market data means "no opinion", and strategies that
depend on it simply keep their current position.
"""

from __future__ import annotations

import bisect
from typing import Callable

from hydra_signals import strategies as strat

HISTORY_VERSION = 1
COINS = ("BTC", "ETH")
DAY_MS = 86_400_000
HOUR_MS = 3_600_000
RETENTION_DAYS = 220
# Świeca godzinowa może być najwyżej tyle starsza od świecy Hydry, żeby jej użyć.
MAX_AGE_S = 3 * 3600

FEATURE_KEYS = (
    "btcPrice",
    "btcSma",
    "btcTrendDistPct",
    "btcSignal",
    "ethRefPrice",
    "ethBtc",
    "ethBtcSma",
    "ethBtcDistPct",
    "ethBtcSignal",
)


def empty_history() -> dict:
    return {"version": HISTORY_VERSION, "coins": {c: [] for c in COINS}}


def update_history(
    history: dict,
    now_ms: int,
    *,
    fetch: Callable[[str, int, int], list[list]],
    backfill_days: int = 200,
) -> dict:
    """Append new closed hourly candles for every coin (in place). Never raises."""
    coins = history.setdefault("coins", {})
    counters: dict = {"errors": []}
    cut = now_ms - RETENTION_DAYS * DAY_MS
    for coin in COINS:
        rows = coins.setdefault(coin, [])
        start = rows[-1][0] + 1 if rows else now_ms - int(backfill_days) * DAY_MS
        new = 0
        try:
            known = {r[0] for r in rows}
            for t, c in fetch(coin, start, now_ms):
                if t not in known:
                    rows.append([t, c])
                    known.add(t)
                    new += 1
            rows.sort(key=lambda r: r[0])
        except Exception as exc:  # noqa: BLE001 - dodatkowe źródło, nigdy nie przerywa przebiegu
            counters["errors"].append(f"{coin}: {type(exc).__name__}: {exc}")
        coins[coin] = [r for r in rows if r[0] >= cut]
        counters[coin] = {"new": new, "total": len(coins[coin])}
    return counters


class SeriesTrend:
    """Trend (14-day rule) evaluated at every row of a `[close_ms, value]` series,
    plus "what was known at time ts" lookups."""

    def __init__(self, rows: list[list]):
        self.t = [r[0] / 1000.0 for r in rows]
        self.v = [r[1] for r in rows]
        win = strat.TrendWindow()
        self.sig: list[str] = []
        self.sma: list[float | None] = []
        self.dist: list[float | None] = []
        for ts, val in zip(self.t, self.v):
            win.add(int(ts), val)
            s, m, d = win.signal_at(int(ts), val)
            self.sig.append(s)
            self.sma.append(m)
            self.dist.append(d)

    def index_at(self, ts: float) -> int | None:
        i = bisect.bisect_right(self.t, ts) - 1
        if i < 0 or ts - self.t[i] > MAX_AGE_S:
            return None
        return i


def build_lookups(history: dict | None) -> dict:
    coins = (history or {}).get("coins") or {}
    btc = coins.get("BTC") or []
    eth = coins.get("ETH") or []
    btc_map = {t: c for t, c in btc}
    ratio = [[t, c / btc_map[t]] for t, c in eth if t in btc_map and btc_map[t] > 0]
    return {"BTC": SeriesTrend(btc), "ETH": SeriesTrend(eth), "ETHBTC": SeriesTrend(ratio)}


def _r(v, nd):
    return round(v, nd) if v is not None else None


def features_at(lookups: dict | None, ts: float | None) -> dict:
    out = {k: None for k in FEATURE_KEYS}
    if not lookups or ts is None:
        return out
    b = lookups["BTC"]
    i = b.index_at(ts)
    if i is not None:
        out["btcPrice"] = _r(b.v[i], 2)
        out["btcSma"] = _r(b.sma[i], 2)
        out["btcTrendDistPct"] = _r(b.dist[i], 2)
        out["btcSignal"] = b.sig[i] if b.sma[i] is not None else None
    e = lookups["ETH"]
    j = e.index_at(ts)
    if j is not None:
        out["ethRefPrice"] = _r(e.v[j], 2)
    r = lookups["ETHBTC"]
    k = r.index_at(ts)
    if k is not None:
        out["ethBtc"] = _r(r.v[k], 6)
        out["ethBtcSma"] = _r(r.sma[k], 6)
        out["ethBtcDistPct"] = _r(r.dist[k], 2)
        out["ethBtcSignal"] = r.sig[k] if r.sma[k] is not None else None
    return out


def combined_signal(eth_signal: str | None, btc_signal: str | None) -> str | None:
    """Strategia "Trend ETH i BTC zgodne": LONG/SHORT tylko gdy oba trendy się
    zgadzają, inaczej poza rynkiem. None (brak danych o BTC) = bez zmiany pozycji."""
    if btc_signal is None:
        return None
    if eth_signal == btc_signal and eth_signal in (strat.LONG, strat.SHORT):
        return eth_signal
    return strat.HOLD
