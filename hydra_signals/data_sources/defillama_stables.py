"""Total stablecoin supply (all stablecoins, all chains) from DefiLlama
(etap 6 planu "trend", pkt 8).

One public, key-less call:

    GET https://stablecoins.llama.fi/stablecoincharts/all

returns one point per day since 2017:
`{"date": "1731542400", "totalCirculatingUSD": {"peggedUSD": 1.7e11, "peggedEUR": ...}, ...}`
(`date` = unix seconds of the day, as a STRING in the live API - parsed either
way). The supply in USD is the sum of the `totalCirculatingUSD` values (the
non-USD pegs are already converted to USD there).

Why DefiLlama and not mint/burn logs from Alchemy (the original plan): the
daily series reaches back years, so the "od początku" view covers the whole
life of Hydra, it costs no RPC budget (Alchemy's compute units are shared with
the main pipeline), and it covers every chain, not only Ethereum mainnet. The
net daily change of the total supply IS the net mint minus burn.
"""

from __future__ import annotations

from typing import Any

from .http_json import GetFn, get_json

CHART_URL = "https://stablecoins.llama.fi/stablecoincharts/all"


def _usd(value: Any) -> float | None:
    if isinstance(value, dict):
        total = 0.0
        for v in value.values():
            try:
                total += float(v)
            except (TypeError, ValueError):
                continue
        return total
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_chart(raw: Any) -> list[list]:
    """`[[day_s, supply_usd], ...]`, oldest first, one row per day. Malformed
    rows are skipped; a later row for the same day wins."""
    if not isinstance(raw, list):
        return []
    out: dict[int, float] = {}
    for r in raw:
        if not isinstance(r, dict):
            continue
        try:
            day = int(float(r["date"]))
        except (KeyError, TypeError, ValueError):
            continue
        usd = _usd(r.get("totalCirculatingUSD"))
        if usd is None or usd <= 0:
            continue
        out[day] = usd
    return [[d, out[d]] for d in sorted(out)]


def fetch_total(*, get: GetFn = get_json) -> list[list]:
    """Daily total stablecoin supply in USD (raises on network error)."""
    return parse_chart(get(CHART_URL))
