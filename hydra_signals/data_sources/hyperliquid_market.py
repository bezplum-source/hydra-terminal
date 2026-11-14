"""Hourly BTC / ETH price candles from Hyperliquid (Etap 2 planu "trend").

One public, key-less REST call on the same host the rest of the Hyperliquid
integration uses (`https://api.hyperliquid.xyz/info`):

    {"type": "candleSnapshot",
     "req": {"coin": "BTC", "interval": "1h", "startTime": ms, "endTime": ms}}

The response is a list of candles `{"t": open ms, "T": close ms, "s", "i",
"o", "h", "l", "c", "v", "n"}` (prices as strings). Only the most recent 5000
candles of an interval are available (1h -> about 208 days), which is enough
to backfill the whole look-back the dashboard uses.

The most recent candle can still be open; only candles whose close time `T` is
already in the past are kept, so a value is never "repainted" later.
"""

from __future__ import annotations

from typing import Any, Callable

from .hyperliquid_derivs import _http_post

PostFn = Callable[[dict], Any]
INTERVAL = "1h"


def parse_candles(raw: Any, now_ms: int) -> list[list]:
    """`[[close_ms, close_price], ...]` for closed candles, oldest first,
    de-duplicated by close time. Malformed rows are skipped."""
    if not isinstance(raw, list):
        return []
    out: dict[int, float] = {}
    for r in raw:
        try:
            close_ms = int(r["T"])
            close = float(r["c"])
        except (KeyError, TypeError, ValueError):
            continue
        if close_ms > now_ms or close <= 0:
            continue  # świeca jeszcze otwarta albo bzdurna cena
        out[close_ms] = close
    return [[t, out[t]] for t in sorted(out)]


def fetch_closes(coin: str, start_ms: int, now_ms: int, *, post: PostFn = _http_post) -> list[list]:
    """Closed hourly candles of `coin` from `start_ms` to `now_ms` (raises on network error)."""
    raw = post(
        {
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": INTERVAL, "startTime": int(start_ms), "endTime": int(now_ms)},
        }
    )
    return parse_candles(raw, now_ms)
