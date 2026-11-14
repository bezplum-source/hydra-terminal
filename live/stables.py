"""Etap 6 planu "trend" (pkt 8): stablecoiny - czy do krypto wpływają nowe
dolary, czy z niego wypływają.

`data/stables_history.json` trzyma dzienną łączną podaż wszystkich
stablecoinów ze wszystkich sieci (DefiLlama, patrz
`hydra_signals/data_sources/defillama_stables.py`). Zmiana podaży = ile
stablecoinów wybito (mint) minus ile spalono (burn): rosnąca podaż to nowe
pieniądze gotowe do kupowania, malejąca - pieniądze wychodzące z rynku.

Bez zaglądania w przyszłość: punkt z dnia D jest używany dopiero od końca tego
dnia (D + 24 h), bo ostatni punkt w API bywa jeszcze niepełnym dniem.

Strategie do wyścigu (progi ustalone z góry, 14.11.2026):
- "Napływ stablecoinów" (`stables7`): LONG, gdy podaż przez 7 dni wzrosła,
  SHORT, gdy spadła.
- "Trend potwierdzony przez stablecoiny" (`trendstables`): kierunek z trendu
  14 dni tylko wtedy, gdy stablecoiny płyną w tę samą stronę (trend wzrostowy
  i napływ albo trend spadkowy i odpływ); inaczej poza rynkiem.
"""

from __future__ import annotations

import bisect
from typing import Callable

DAY_S = 86_400
CHANGE_DAYS = 7
RETENTION_DAYS = 400
# Dane starsze niż tyle od świecy uznajemy za nieaktualne (API stanęło).
MAX_STALE_S = 3 * DAY_S

FEATURE_KEYS = (
    "stablesSupplyUsd",
    "stablesChange7dUsd",
    "stablesChange7dPct",
    "signalStables",
    "signalTrendStables",
)


def empty_history() -> dict:
    return {"version": 1, "total": []}


def empty_features() -> dict:
    return {k: None for k in FEATURE_KEYS}


def update_history(history: dict, now_ms: int, *, fetch: Callable[[], list[list]]) -> dict:
    """Dopisuje / nadpisuje dzienne punkty (w miejscu). Nigdy nie rzuca."""
    rows = history.setdefault("total", [])
    counters: dict = {"errors": [], "new": 0}
    try:
        fetched = fetch()
        known = {int(r[0]): r[1] for r in rows}
        for day, usd in fetched:
            if int(day) not in known:
                counters["new"] += 1
            known[int(day)] = usd  # ostatni (niepełny) dzień bywa poprawiany
        rows = [[d, known[d]] for d in sorted(known)]
    except Exception as exc:  # noqa: BLE001 - dodatkowe źródło, nigdy nie przerywa przebiegu
        counters["errors"].append(f"{type(exc).__name__}: {exc}")
    cut = now_ms / 1000.0 - RETENTION_DAYS * DAY_S
    history["total"] = [r for r in rows if r[0] >= cut]
    counters["total"] = len(history["total"])
    return counters


class StablesSeries:
    def __init__(self, rows: list[list] | None):
        rows = sorted(rows or [], key=lambda r: r[0])
        self.days = [int(r[0]) for r in rows]
        self.vals = [float(r[1]) for r in rows]
        self.by_day = dict(zip(self.days, self.vals))

    def index_at(self, ts: float) -> int | None:
        """Ostatni PEŁNY dzień znany w chwili `ts`."""
        i = bisect.bisect_right(self.days, ts - DAY_S) - 1
        if i < 0 or ts - (self.days[i] + DAY_S) > MAX_STALE_S:
            return None
        return i

    def change(self, i: int, days: int = CHANGE_DAYS) -> tuple[float, float] | None:
        ref = self.by_day.get(self.days[i] - days * DAY_S)
        if not ref:
            return None
        diff = self.vals[i] - ref
        return diff, diff / ref * 100.0


def direction(change: float | None) -> str | None:
    if change is None:
        return None
    return "LONG" if change > 0 else "SHORT" if change < 0 else "HOLD"


def trend_stables_signal(trend: str | None, change: float | None) -> str | None:
    """Kierunek trendu tylko, gdy stablecoiny płyną w tę samą stronę. Brak
    danych o stablecoinach = None (strategia trzyma pozycję)."""
    if change is None:
        return None
    if trend == "LONG" and change > 0:
        return "LONG"
    if trend == "SHORT" and change < 0:
        return "SHORT"
    return "HOLD"


def features_at(series: StablesSeries | None, ts: float | None, trend: str | None) -> dict:
    out = empty_features()
    if series is None or ts is None or not series.days:
        return out
    i = series.index_at(ts)
    if i is None:
        return out
    out["stablesSupplyUsd"] = round(series.vals[i], 0)
    ch = series.change(i)
    if ch is None:
        return out
    diff, pct = ch
    out["stablesChange7dUsd"] = round(diff, 0)
    out["stablesChange7dPct"] = round(pct, 3)
    out["signalStables"] = direction(diff)
    out["signalTrendStables"] = trend_stables_signal(trend, diff)
    return out
