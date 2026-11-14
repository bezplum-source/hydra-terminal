"""Etap 7 planu "trend" (pkt 10): rynek opcji ETH z Deribit.

`data/options_history.json`:
- `dvol`: godzinowe zamknięcia indeksu DVOL `[close_ms, wartość]` - oczekiwana
  przez rynek opcji zmienność ETH na 30 dni (w % rocznie). Historia jest
  dostępna, więc przy pierwszym uruchomieniu dociągamy ją wstecz (widok
  „od początku” ma te dane od pierwszej świecy Hydry).
- `snapshots`: jedna migawka na uruchomienie (put/call, skew) - tylko od
  wdrożenia do przodu, Deribit nie udostępnia historii tych liczb.

Strategie do wyścigu (progi ustalone z góry, 14.11.2026):
- "Trend w spokojnym rynku" (`trendcalm`): kierunek z trendu 14 dni, ale tylko
  wtedy, gdy DVOL jest NIE wyżej niż jego średnia z 14 dni. Gdy oczekiwana
  zmienność skacze (nerwowy rynek, gwałtowne odbicia) - poza rynkiem.
- "Skew opcji" (`optskew`): za ustawieniem rynku opcji - SHORT, gdy ochrona
  przed spadkiem (puty) jest droższa od zakładów na wzrost (calle) o więcej niż
  SKEW_BAND punktu zmienności, LONG przy odwrotnej przewadze, w środku poza
  rynkiem.
"""

from __future__ import annotations

import bisect
from typing import Callable

from hydra_signals.data_sources import deribit
from live import market

DAY_MS = 86_400_000
RETENTION_DAYS = 220
SNAPSHOT_MIN_GAP_MS = 20 * 60 * 1000
SNAPSHOT_MAX_AGE_S = 3 * 3600
SKEW_BAND = 1.0

FEATURE_KEYS = (
    "dvol",
    "dvolSma14",
    "dvolDistPct",
    "signalTrendCalm",
    "optPutCallOi",
    "optSkew",
    "signalOptSkew",
)


def empty_history() -> dict:
    return {"version": 1, "dvol": [], "snapshots": []}


def empty_features() -> dict:
    return {k: None for k in FEATURE_KEYS}


def update_history(
    history: dict,
    now_ms: int,
    *,
    fetch_dvol: Callable[[int, int], list[list]],
    fetch_summary: Callable[[], list[dict]],
    backfill_days: int = 200,
) -> dict:
    """Dopisuje nowe świece DVOL i migawkę opcji (w miejscu). Nigdy nie rzuca."""
    counters: dict = {"errors": [], "dvol_new": 0, "snapshot_added": False}
    rows = history.setdefault("dvol", [])
    snaps = history.setdefault("snapshots", [])
    try:
        start = rows[-1][0] + 1 if rows else now_ms - int(backfill_days) * DAY_MS
        known = {r[0] for r in rows}
        for t, v in fetch_dvol(start, now_ms):
            if t not in known:
                rows.append([t, v])
                known.add(t)
                counters["dvol_new"] += 1
        rows.sort(key=lambda r: r[0])
    except Exception as exc:  # noqa: BLE001
        counters["errors"].append(f"dvol: {type(exc).__name__}: {exc}")
    try:
        if not snaps or now_ms - snaps[-1]["t"] >= SNAPSHOT_MIN_GAP_MS:
            snap = deribit.summarize_options(fetch_summary(), now_ms)
            if snap is not None:
                snaps.append({"t": int(now_ms), **snap})
                counters["snapshot_added"] = True
    except Exception as exc:  # noqa: BLE001
        counters["errors"].append(f"opcje: {type(exc).__name__}: {exc}")
    cut = now_ms - RETENTION_DAYS * DAY_MS
    history["dvol"] = [r for r in rows if r[0] >= cut]
    history["snapshots"] = [s for s in snaps if s["t"] >= cut]
    counters["dvol_total"] = len(history["dvol"])
    counters["snapshot_total"] = len(history["snapshots"])
    return counters


class OptionsLookup:
    def __init__(self, history: dict | None):
        history = history or {}
        self.dvol = market.SeriesTrend(sorted(history.get("dvol") or [], key=lambda r: r[0]))
        self.snaps = sorted(history.get("snapshots") or [], key=lambda s: s["t"])
        self.snap_t = [s["t"] / 1000.0 for s in self.snaps]

    def snapshot_at(self, ts: float) -> dict | None:
        i = bisect.bisect_right(self.snap_t, ts) - 1
        if i < 0 or ts - self.snap_t[i] > SNAPSHOT_MAX_AGE_S:
            return None
        return self.snaps[i]


def trend_calm_signal(trend: str | None, dvol: float | None, sma: float | None) -> str | None:
    if dvol is None or sma is None:
        return None
    if trend in ("LONG", "SHORT") and dvol <= sma:
        return trend
    return "HOLD"


def skew_signal(skew: float | None) -> str | None:
    if skew is None:
        return None
    if skew > SKEW_BAND:
        return "SHORT"
    if skew < -SKEW_BAND:
        return "LONG"
    return "HOLD"


def features_at(lookup: OptionsLookup | None, ts: float | None, trend: str | None) -> dict:
    out = empty_features()
    if lookup is None or ts is None:
        return out
    d = lookup.dvol
    i = d.index_at(ts) if d.t else None
    if i is not None:
        out["dvol"] = round(d.v[i], 2)
        if d.sma[i] is not None:
            out["dvolSma14"] = round(d.sma[i], 2)
            out["dvolDistPct"] = round(d.dist[i], 2)
        out["signalTrendCalm"] = trend_calm_signal(trend, d.v[i], d.sma[i])
    snap = lookup.snapshot_at(ts)
    if snap is not None:
        out["optPutCallOi"] = snap.get("pcOi")
        out["optSkew"] = snap.get("skew")
        out["signalOptSkew"] = skew_signal(snap.get("skew"))
    return out
