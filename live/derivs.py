"""Funding rate + open interest history for the ETH perpetual (Hyperliquid).

Pure logic on top of `hydra_signals.data_sources.hyperliquid_derivs` (which owns
the network I/O) and `live.state` (which owns the file). Two jobs:

1. `update_history()` - keep `data/derivs_history.json` current. Funding is
   BACKFILLED from the exchange's `fundingHistory` (hourly rows, so the first
   run fills the whole look-back window and every later run only appends what
   is new). Open interest has no history endpoint, so it is collected FORWARD
   as one snapshot per run - a series that only gets long as the pipeline runs.
2. `features_at()` - for one candle timestamp, return the derived fields stored
   on the candle (funding rate / APR / z-score, open interest and its 24h
   change, premium). Every field is None when the underlying data is missing,
   never an error: a candle without derivs data is still a perfectly good
   candle, exactly like a candle whose Base window is not mature yet.

Nothing here changes the signal. Phase 1 only RECORDS the data; whether and how
it feeds the composite is decided from the recorded series (see the project doc
`hydrav2-derivs-funding-oi.md`).
"""

from __future__ import annotations

import bisect
import statistics
from typing import Any, Callable

HISTORY_VERSION = 1
DAY_MS = 86_400_000
HOUR_MS = 3_600_000

FUNDING_RETENTION_DAYS = 200
SNAPSHOT_RETENTION_DAYS = 120
# Two runs closer together than this share one snapshot (re-runs, retries).
MIN_SNAPSHOT_GAP_MS = 5 * 60 * 1000
# A candle only borrows a data point that is at most this far from its own time.
FEATURE_TOLERANCE_MS = 3 * HOUR_MS
# z-score of the funding rate against its own trailing window.
Z_WINDOW_DAYS = 30
Z_MIN_ROWS = 72

FEATURE_KEYS = (
    "fundingRate",
    "fundingApr",
    "fundingZ",
    "openInterest",
    "openInterestUsd",
    "oiChange24h",
    "perpPremium",
)


def empty_history() -> dict:
    return {"version": HISTORY_VERSION, "funding": [], "snapshots": []}


def normalize_history(raw: Any) -> dict:
    """Tolerate a missing/odd file: always return a usable history dict."""
    if not isinstance(raw, dict):
        return empty_history()
    funding = raw.get("funding")
    snapshots = raw.get("snapshots")
    return {
        "version": HISTORY_VERSION,
        "funding": funding if isinstance(funding, list) else [],
        "snapshots": snapshots if isinstance(snapshots, list) else [],
    }


def update_history(
    history: dict,
    now_ms: int,
    *,
    fetch_ctx: Callable[[], dict | None],
    fetch_funding: Callable[[int], list[dict]],
    backfill_days: int = 120,
) -> dict:
    """Append new funding rows + one open-interest snapshot to `history` (in place).

    Never raises: each source failing is recorded in the returned counters and
    the other source still runs. Returns
    `{"funding_new", "funding_total", "snapshot_added", "snapshot_total", "errors"}`.
    """
    errors: list[str] = []
    funding: list[dict] = history.setdefault("funding", [])
    snapshots: list[dict] = history.setdefault("snapshots", [])

    # --- funding (backfillable) ---
    funding_new = 0
    try:
        last_t = funding[-1]["t"] if funding else None
        start_ms = (last_t + 1) if last_t is not None else now_ms - int(backfill_days) * DAY_MS
        rows = fetch_funding(start_ms)
        known = {r["t"] for r in funding}
        for r in rows:
            if r["t"] not in known:
                funding.append({"t": r["t"], "rate": r["rate"], "premium": r.get("premium")})
                known.add(r["t"])
                funding_new += 1
        funding.sort(key=lambda r: r["t"])
    except Exception as exc:  # noqa: BLE001 - side data source, never fatal
        errors.append(f"funding: {type(exc).__name__}: {exc}")

    # --- open interest snapshot (forward only) ---
    snapshot_added = False
    try:
        if not snapshots or now_ms - snapshots[-1]["t"] >= MIN_SNAPSHOT_GAP_MS:
            ctx = fetch_ctx()
            if ctx is None:
                errors.append("asset_ctx: no data")
            else:
                snapshots.append(
                    {
                        "t": int(now_ms),
                        "funding": ctx.get("funding"),
                        "oi": ctx.get("openInterest"),
                        "mark": ctx.get("markPx"),
                        "oracle": ctx.get("oraclePx"),
                        "premium": ctx.get("premium"),
                        "dayVlm": ctx.get("dayNtlVlm"),
                    }
                )
                snapshot_added = True
    except Exception as exc:  # noqa: BLE001
        errors.append(f"asset_ctx: {type(exc).__name__}: {exc}")

    # --- retention ---
    funding_cut = now_ms - FUNDING_RETENTION_DAYS * DAY_MS
    snap_cut = now_ms - SNAPSHOT_RETENTION_DAYS * DAY_MS
    history["funding"] = [r for r in funding if r["t"] >= funding_cut]
    history["snapshots"] = [s for s in snapshots if s["t"] >= snap_cut]

    return {
        "funding_new": funding_new,
        "funding_total": len(history["funding"]),
        "snapshot_added": snapshot_added,
        "snapshot_total": len(history["snapshots"]),
        "errors": errors,
    }


def _empty_features() -> dict:
    return {k: None for k in FEATURE_KEYS}


def _at_or_before(rows: list[dict], t_ms: int, tolerance_ms: int) -> dict | None:
    times = [r["t"] for r in rows]
    i = bisect.bisect_right(times, t_ms) - 1
    if i < 0:
        return None
    return rows[i] if t_ms - rows[i]["t"] <= tolerance_ms else None


def _nearest(rows: list[dict], t_ms: int, tolerance_ms: int) -> dict | None:
    if not rows:
        return None
    times = [r["t"] for r in rows]
    i = bisect.bisect_left(times, t_ms)
    cands = [rows[j] for j in (i - 1, i) if 0 <= j < len(rows)]
    best = min(cands, key=lambda r: abs(r["t"] - t_ms))
    return best if abs(best["t"] - t_ms) <= tolerance_ms else None


def _funding_interval_hours(funding: list[dict], idx: int) -> float:
    """Median spacing (hours) of the funding rows ending at `idx`; 1h by default."""
    lo = max(0, idx - 24)
    ts = [r["t"] for r in funding[lo : idx + 1]]
    gaps = [b - a for a, b in zip(ts, ts[1:]) if b > a]
    if not gaps:
        return 1.0
    return max(statistics.median(gaps) / HOUR_MS, 1e-9)


def features_at(history: dict, ts_s: int | float | None) -> dict:
    """Derivs fields for a candle at unix time `ts_s` (seconds); None where unknown."""
    out = _empty_features()
    if ts_s is None:
        return out
    t_ms = int(ts_s * 1000)
    funding = history.get("funding") or []
    snapshots = history.get("snapshots") or []

    premium = None
    if funding:
        times = [r["t"] for r in funding]
        i = bisect.bisect_right(times, t_ms) - 1
        if i >= 0 and t_ms - funding[i]["t"] <= FEATURE_TOLERANCE_MS:
            row = funding[i]
            rate = row["rate"]
            out["fundingRate"] = round(rate, 8)
            interval_h = _funding_interval_hours(funding, i)
            out["fundingApr"] = round(rate * (8760.0 / interval_h) * 100.0, 2)
            premium = row.get("premium")
            window_start = t_ms - Z_WINDOW_DAYS * DAY_MS
            j = bisect.bisect_left(times, window_start)
            window = [r["rate"] for r in funding[j : i + 1]]
            if len(window) >= Z_MIN_ROWS:
                sd = statistics.pstdev(window)
                if sd > 0:
                    out["fundingZ"] = round((rate - statistics.fmean(window)) / sd, 2)

    snap = _at_or_before(snapshots, t_ms, FEATURE_TOLERANCE_MS)
    if snap is not None:
        oi = snap.get("oi")
        mark = snap.get("mark")
        if oi is not None:
            out["openInterest"] = round(oi, 2)
            if mark is not None:
                out["openInterestUsd"] = round(oi * mark, 0)
            prev = _nearest(snapshots, t_ms - DAY_MS, FEATURE_TOLERANCE_MS)
            if prev is not None and prev is not snap and prev.get("oi"):
                out["oiChange24h"] = round((oi / prev["oi"] - 1.0) * 100.0, 2)
        if snap.get("premium") is not None:
            premium = snap["premium"]
    if premium is not None:
        out["perpPremium"] = round(premium, 8)
    return out
