"""ETH options market data from Deribit (etap 7 planu "trend", pkt 10).

Two public, key-less JSON-RPC-over-GET calls on `https://www.deribit.com/api/v2/public/`:

* `get_volatility_index_data?currency=ETH&start_timestamp=&end_timestamp=&resolution=3600`
  - DVOL, the 30-day implied volatility index of ETH options (annualised, in
  percent). Result: `{"data": [[t_ms, open, high, low, close], ...],
  "continuation": t_ms | null}`; `continuation` is the `end_timestamp` of the
  next (older) page. History is available, so it can be backfilled.
* `get_book_summary_by_currency?currency=ETH&kind=option` - one row per listed
  option: `instrument_name` ("ETH-27DEC26-2000-P"), `open_interest` (ETH,
  reported with a delay of up to 8 h), `volume` (24 h, ETH), `mark_iv`
  (percent), `underlying_price`. Only a SNAPSHOT - collected forward in time.

`summarize_options()` turns the snapshot into three numbers:
- put/call ratio of open interest and of 24 h volume,
- 25-delta-like skew WITHOUT greeks: implied volatility of the put struck
  nearest 90% of the underlying minus that of the call nearest 110%, in the
  expiry closest to 30 days (7-60 days out). Positive = downside protection is
  more expensive than upside bets (fear), negative = the opposite (greed).
"""

from __future__ import annotations

import datetime as _dt
import statistics
from typing import Any

from .http_json import GetFn, get_json, with_query

API = "https://www.deribit.com/api/v2/public/"
CURRENCY = "ETH"
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
MAX_DVOL_PAGES = 40
SKEW_TARGET_DAYS = 30
SKEW_MIN_DAYS = 7
SKEW_MAX_DAYS = 60
SKEW_PUT_MONEYNESS = 0.9
SKEW_CALL_MONEYNESS = 1.1
SKEW_MONEYNESS_TOL = 0.05


def _result(raw: Any) -> Any:
    if isinstance(raw, dict) and "result" in raw:
        return raw["result"]
    raise ValueError(f"unexpected Deribit response: {str(raw)[:200]}")


def fetch_dvol(start_ms: int, end_ms: int, *, get: GetFn = get_json, currency: str = CURRENCY) -> list[list]:
    """Hourly DVOL closes `[[close_ms, value], ...]` between start and end
    (only candles already closed at `end_ms`), oldest first."""
    out: dict[int, float] = {}
    end = int(end_ms)
    for _ in range(MAX_DVOL_PAGES):
        res = _result(
            get(
                with_query(
                    API + "get_volatility_index_data",
                    {
                        "currency": currency,
                        "start_timestamp": int(start_ms),
                        "end_timestamp": end,
                        "resolution": "3600",
                    },
                )
            )
        )
        data = (res or {}).get("data") or []
        for row in data:
            try:
                close_ms = int(row[0]) + HOUR_MS
                close = float(row[4])
            except (TypeError, ValueError, IndexError):
                continue
            if close_ms <= end_ms and close > 0:
                out[close_ms] = close
        cont = (res or {}).get("continuation")
        if not data or not cont or int(cont) <= start_ms or int(cont) >= end:
            break
        end = int(cont)
    return [[t, out[t]] for t in sorted(out)]


def fetch_option_summary(*, get: GetFn = get_json, currency: str = CURRENCY) -> list[dict]:
    res = _result(get(with_query(API + "get_book_summary_by_currency", {"currency": currency, "kind": "option"})))
    return res if isinstance(res, list) else []


def parse_instrument(name: str) -> tuple[int, float, str] | None:
    """"ETH-27DEC26-2000-P" -> (expiry ms at 08:00 UTC, strike, "P"); None if not an option."""
    try:
        _coin, exp, strike, kind = name.split("-")
        day = _dt.datetime.strptime(exp, "%d%b%y").replace(hour=8, tzinfo=_dt.timezone.utc)
        if kind not in ("P", "C"):
            return None
        return int(day.timestamp() * 1000), float(strike), kind
    except (ValueError, AttributeError):
        return None


def _f(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def summarize_options(items: list[dict], now_ms: int) -> dict | None:
    rows = []
    for it in items or []:
        p = parse_instrument(str(it.get("instrument_name", "")))
        if p is None or p[0] <= now_ms:
            continue
        rows.append(
            {
                "exp": p[0],
                "strike": p[1],
                "kind": p[2],
                "oi": _f(it.get("open_interest")) or 0.0,
                "vol": _f(it.get("volume")) or 0.0,
                "iv": _f(it.get("mark_iv")),
                "und": _f(it.get("underlying_price")),
                "expName": str(it.get("instrument_name")).split("-")[1],
            }
        )
    if not rows:
        return None
    put_oi = sum(r["oi"] for r in rows if r["kind"] == "P")
    call_oi = sum(r["oi"] for r in rows if r["kind"] == "C")
    put_vol = sum(r["vol"] for r in rows if r["kind"] == "P")
    call_vol = sum(r["vol"] for r in rows if r["kind"] == "C")
    out: dict = {
        "pcOi": round(put_oi / call_oi, 3) if call_oi > 0 else None,
        "pcVol": round(put_vol / call_vol, 3) if call_vol > 0 else None,
        "putOiEth": round(put_oi, 1),
        "callOiEth": round(call_oi, 1),
        "skew": None,
        "atmIv": None,
        "skewExpiry": None,
        "underlying": None,
    }
    exps = sorted({r["exp"] for r in rows if SKEW_MIN_DAYS * DAY_MS <= r["exp"] - now_ms <= SKEW_MAX_DAYS * DAY_MS})
    if not exps:
        return out
    target = now_ms + SKEW_TARGET_DAYS * DAY_MS
    exp = min(exps, key=lambda e: abs(e - target))
    chain = [r for r in rows if r["exp"] == exp and r["iv"] and r["iv"] > 0]
    unds = [r["und"] for r in chain if r["und"]]
    if not chain or not unds:
        return out
    s = statistics.median(unds)

    def nearest(kind: str, m: float) -> dict | None:
        cands = [r for r in chain if r["kind"] == kind]
        if not cands:
            return None
        best = min(cands, key=lambda r: abs(r["strike"] / s - m))
        return best if abs(best["strike"] / s - m) <= SKEW_MONEYNESS_TOL else None

    put, call = nearest("P", SKEW_PUT_MONEYNESS), nearest("C", SKEW_CALL_MONEYNESS)
    atm_strike = min(chain, key=lambda r: abs(r["strike"] - s))["strike"]
    atm = [r["iv"] for r in chain if r["strike"] == atm_strike]
    out["underlying"] = round(s, 2)
    out["skewExpiry"] = next(r["expName"] for r in chain)
    out["atmIv"] = round(sum(atm) / len(atm), 2) if atm else None
    if put and call:
        out["skew"] = round(put["iv"] - call["iv"], 2)
    return out
