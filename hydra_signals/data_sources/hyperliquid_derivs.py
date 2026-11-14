"""Hyperliquid derivatives market data (ETH-PERP): funding rate + open interest.

Two public, key-less REST calls on the same host the WebSocket listener
already uses (`https://api.hyperliquid.xyz/info`):

* `metaAndAssetCtxs` - a SNAPSHOT of the current state of every perp market.
  For ETH we keep: `funding` (the rate for the current funding interval),
  `openInterest` (in ETH, base units), `markPx`, `oraclePx`, `premium`,
  `dayNtlVlm`. Hyperliquid exposes NO history for open interest over REST, so
  open interest can only be collected forward in time (one snapshot per run).
* `fundingHistory` - the funding rate for every past funding interval, from
  the launch of the market, at most 500 rows per call (paginate by advancing
  `startTime`). This one CAN be backfilled.

Everything here is a thin I/O wrapper. The network function is injectable
(`post=`) so the unit tests never touch the network, and every public
function degrades to "no data" instead of raising where the caller can
reasonably continue - the pipeline must never fail because a side data source
is down.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Callable

INFO_URL = "https://api.hyperliquid.xyz/info"
DEFAULT_COIN = "ETH"
FUNDING_PAGE_LIMIT = 500  # documented maximum number of rows per `fundingHistory` response
MAX_FUNDING_PAGES = 40  # safety stop: 40 * 500 hourly rows is far more than we ever backfill

PostFn = Callable[[dict], Any]


def _http_post(body: dict, *, timeout: float = 20.0, retries: int = 3, base_delay: float = 1.5) -> Any:
    """POST a JSON body to the Hyperliquid info endpoint with a few retries."""
    data = json.dumps(body).encode("utf-8")
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(INFO_URL, data=data, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(base_delay * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_asset_ctx(meta_and_ctxs: Any, coin: str = DEFAULT_COIN) -> dict | None:
    """Pick one coin out of a raw `metaAndAssetCtxs` response.

    The response is `[meta, [ctx, ctx, ...]]` where `meta["universe"][i]["name"]`
    names the coin and `ctx[i]` is its context at the same index. Returns None
    when the shape is unexpected or the coin is missing.
    """
    try:
        universe = meta_and_ctxs[0]["universe"]
        ctxs = meta_and_ctxs[1]
        idx = next(i for i, u in enumerate(universe) if u.get("name") == coin)
        ctx = ctxs[idx]
    except (KeyError, IndexError, TypeError, StopIteration):
        return None
    funding = _to_float(ctx.get("funding"))
    oi = _to_float(ctx.get("openInterest"))
    mark = _to_float(ctx.get("markPx"))
    if funding is None or oi is None or mark is None:
        return None
    return {
        "funding": funding,
        "openInterest": oi,
        "markPx": mark,
        "oraclePx": _to_float(ctx.get("oraclePx")),
        "premium": _to_float(ctx.get("premium")),
        "dayNtlVlm": _to_float(ctx.get("dayNtlVlm")),
    }


def fetch_asset_ctx(coin: str = DEFAULT_COIN, *, post: PostFn = _http_post) -> dict | None:
    """Current funding / open interest snapshot for `coin`, or None on failure."""
    try:
        return parse_asset_ctx(post({"type": "metaAndAssetCtxs"}), coin)
    except Exception:  # noqa: BLE001 - side data source, never fatal
        return None


def fetch_funding_history(
    start_ms: int,
    end_ms: int | None = None,
    coin: str = DEFAULT_COIN,
    *,
    post: PostFn = _http_post,
) -> list[dict]:
    """Funding rate history for `coin` from `start_ms` (inclusive) on.

    Returns rows `{"t": ms, "rate": float, "premium": float | None}`, oldest
    first, de-duplicated by time. Pages by advancing `startTime` past the last
    row of each page. Raises on a network failure of the FIRST page; later
    page failures return what was collected so far (a partial backfill is
    fine, the next run resumes from the last stored row).
    """
    out: dict[int, dict] = {}
    cursor = int(start_ms)
    for page in range(MAX_FUNDING_PAGES):
        body: dict = {"type": "fundingHistory", "coin": coin, "startTime": cursor}
        if end_ms is not None:
            body["endTime"] = int(end_ms)
        try:
            rows = post(body)
        except Exception:  # noqa: BLE001
            if page == 0:
                raise
            break
        if not isinstance(rows, list) or not rows:
            break
        last_t = None
        for r in rows:
            try:
                t = int(r["time"])
                rate = float(r["fundingRate"])
            except (KeyError, TypeError, ValueError):
                continue
            out[t] = {"t": t, "rate": rate, "premium": _to_float(r.get("premium"))}
            last_t = t if last_t is None else max(last_t, t)
        if last_t is None or len(rows) < FUNDING_PAGE_LIMIT:
            break
        cursor = last_t + 1
    return [out[t] for t in sorted(out)]
