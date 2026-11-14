"""Trend signal + paper-trading "strategy race" (Etap 1 planu
`hydrav2-trend-roadmap.md`, decyzja Jakuba z 14.11.2026).

Two pure, I/O-free pieces:

1. `trend_signal()` / `trend_series()` - the headline signal since this phase:
   LONG when the price is above its simple average over the last
   `TREND_DAYS` days, SHORT when below. No hysteresis, no thresholds - exactly
   the rule that was evaluated on real candles before the switch (12.09-14.11:
   +14.8% vs ETH -24.5%, but carried by one large down-move; see the project
   doc `hydrav2-contrarian-diagnosis-and-roadmap.md`). Needs at least
   `TREND_MIN_COVERAGE_DAYS` of price history behind the candle, otherwise the
   answer is HOLD ("not enough history"), never a guess.

2. `RaceBook` helpers (`new_race`, `race_step`, `replay_race`) - several
   strategies "trade" the same candles on paper with the same rules: one unit
   of capital, fully in or out, LONG or SHORT, a fee of `FEE_PER_SIDE` on every
   entry and every exit. The live race starts on the first run after deploy
   and only moves forward; `replay_race` re-plays the same rules over stored
   history for the orientational "looking back" view on the dashboard.
"""

from __future__ import annotations

import bisect
from typing import Iterable

TREND_DAYS = 14
TREND_MIN_COVERAGE_DAYS = 13.0
FEE_PER_SIDE = 0.0005  # 0.05% za wejście i za wyjście
DAY_S = 86_400

LONG, SHORT, HOLD = "LONG", "SHORT", "HOLD"
_DIR = {LONG: 1, SHORT: -1, HOLD: 0}


# ---------------------------------------------------------------------------
# Ochrona przed blednymi cenami
# ---------------------------------------------------------------------------

# Swieca, ktorej cena odbiega o wiecej niz PRICE_JUMP_MAX od ostatniej
# zaakceptowanej ceny, jest "podejrzana" (np. 29.10.2026 22:10: 2986,88 miedzy
# 2316 a 2239 - blad odczytu z puli) i nie wchodzi ani do sredniej trendu, ani
# do wyniku wyscigu. Prawdziwy, trwaly ruch o tyle zostaje przyjety, gdy druga
# swieca z rzedu potwierdzi nowy poziom (w granicach PRICE_CONFIRM_TOL) - wtedy
# tracimy najwyzej jedna swiece opoznienia. Zapisana cena swiecy NIE jest
# zmieniana - to tylko decyzja, czy jej uzyc.
PRICE_JUMP_MAX = 0.15
PRICE_CONFIRM_TOL = 0.03


class PriceGuard:
    def __init__(self) -> None:
        self.last: float | None = None
        self.pending: float | None = None

    def check(self, price: float | None) -> bool:
        """True = cena wiarygodna (i staje sie nowym punktem odniesienia)."""
        if not price:
            return False
        if self.last is None or abs(price / self.last - 1.0) <= PRICE_JUMP_MAX:
            self.last, self.pending = price, None
            return True
        if self.pending is not None and abs(price / self.pending - 1.0) <= PRICE_CONFIRM_TOL:
            self.last, self.pending = price, None
            return True
        self.pending = price
        return False


def price_flags(candles: Iterable[dict]) -> list[bool]:
    """Dla kazdej swiecy: czy jej cena jest wiarygodna (patrz `PriceGuard`)."""
    guard = PriceGuard()
    return [guard.check(c.get("price")) for c in candles]


# ---------------------------------------------------------------------------
# Trend
# ---------------------------------------------------------------------------


class TrendWindow:
    """Rolling price window keyed by unix time (seconds), for the trend rule.

    Fed in time order (`add`), answers "average of the prices in the last
    `days` days, including this one" in O(log n). Candles without a timestamp
    are simply never added (they cannot be placed in time).
    """

    def __init__(self, days: float = TREND_DAYS, min_coverage_days: float = TREND_MIN_COVERAGE_DAYS):
        self.days = days
        self.min_coverage_days = min_coverage_days
        self.ts: list[int] = []
        self.cum: list[float] = [0.0]  # prefix sums of prices

    def add(self, ts: int, price: float) -> None:
        if self.ts and ts < self.ts[-1]:
            # Out-of-order timestamp (should not happen) - ignore rather than
            # corrupt the prefix sums.
            return
        self.ts.append(int(ts))
        self.cum.append(self.cum[-1] + float(price))

    def signal_at(self, ts: int, price: float) -> tuple[str, float | None, float | None]:
        """Signal for a candle at `ts` with `price` that has ALREADY been added.

        Returns (signal, sma, distance_pct). HOLD + None when the window does
        not reach back `min_coverage_days`.
        """
        if not self.ts:
            return HOLD, None, None
        start = ts - self.days * DAY_S
        lo = bisect.bisect_right(self.ts, start)
        hi = bisect.bisect_right(self.ts, ts)
        if hi - lo <= 0:
            return HOLD, None, None
        # coverage: the oldest price we have must be old enough
        if ts - self.ts[0] < self.min_coverage_days * DAY_S:
            return HOLD, None, None
        sma = (self.cum[hi] - self.cum[lo]) / (hi - lo)
        dist = (price / sma - 1.0) * 100.0
        if price > sma:
            return LONG, sma, dist
        if price < sma:
            return SHORT, sma, dist
        return HOLD, sma, dist


def trend_series(candles: Iterable[dict], days: float = TREND_DAYS) -> list[str]:
    """Trend signal for every candle (HOLD where it cannot be computed).

    A candle without `ts` inherits the previous candle's signal - it has a
    price but no place in time, so it cannot move the average. The same goes
    for a candle whose price fails `PriceGuard` (a glitch).
    """
    win = TrendWindow(days)
    guard = PriceGuard()
    out: list[str] = []
    last = HOLD
    for c in candles:
        ts, price = c.get("ts"), c.get("price")
        ok = guard.check(price)
        if ts is None or not ok:
            out.append(last)
            continue
        win.add(ts, price)
        last = win.signal_at(ts, price)[0]
        out.append(last)
    return out


# ---------------------------------------------------------------------------
# Paper-trading race
# ---------------------------------------------------------------------------


def new_strategy_book() -> dict:
    return {
        "position": HOLD,
        "entryPrice": None,
        "entryTime": None,
        "entryBlock": None,
        "equity": 1.0,
        "lastPrice": None,
        "peak": 1.0,
        "maxDrawdownPct": 0.0,
        "trades": [],
        "curve": [],
    }


def new_race(names: Iterable[str], *, start_block: int, start_time: str, start_ts: int | None, start_price: float) -> dict:
    return {
        "version": 1,
        "start": {"block": start_block, "time": start_time, "ts": start_ts, "price": start_price},
        "lastBlock": None,
        "strategies": {n: new_strategy_book() for n in names},
    }


def _close(book: dict, price: float, time: str, block: int, fee: float) -> None:
    pos = book["position"]
    entry = book["entryPrice"]
    gross = _DIR[pos] * (price / entry - 1.0)
    net = (1.0 + gross) * (1.0 - fee) * (1.0 - fee) - 1.0  # wejście + wyjście
    book["equity"] *= 1.0 - fee  # opłata za wyjście (wejście pobrane przy otwarciu)
    book["trades"].append(
        {
            "side": pos,
            "entryTime": book["entryTime"],
            "entryBlock": book["entryBlock"],
            "entryPrice": round(entry, 2),
            "exitTime": time,
            "exitBlock": block,
            "exitPrice": round(price, 2),
            "pct": round(net * 100.0, 2),
        }
    )
    book["position"] = HOLD
    book["entryPrice"] = book["entryTime"] = book["entryBlock"] = None


def strategy_step(book: dict, *, signal: str, price: float, time: str, block: int, ts: int | None, fee: float = FEE_PER_SIDE) -> None:
    """Advance one strategy by one candle: mark the open position to `price`,
    then switch position if `signal` differs (exit + entry fees)."""
    if signal not in _DIR:
        signal = HOLD
    if book["lastPrice"] and book["position"] != HOLD:
        book["equity"] *= 1.0 + _DIR[book["position"]] * (price / book["lastPrice"] - 1.0)
    if signal != book["position"]:
        if book["position"] != HOLD:
            _close(book, price, time, block, fee)
        if signal != HOLD:
            book["equity"] *= 1.0 - fee
            book["position"] = signal
            book["entryPrice"] = price
            book["entryTime"] = time
            book["entryBlock"] = block
    book["lastPrice"] = price
    book["peak"] = max(book["peak"], book["equity"])
    dd = (book["equity"] / book["peak"] - 1.0) * 100.0
    book["maxDrawdownPct"] = round(min(book["maxDrawdownPct"], dd), 2)
    book["curve"].append([ts, block, round((book["equity"] - 1.0) * 100.0, 3)])


def race_step(race: dict, candle: dict, signals: dict[str, str], fee: float = FEE_PER_SIDE) -> None:
    """Advance every strategy in `race` by one candle (skips candles already seen)."""
    block = candle["block"]
    if race.get("lastBlock") is not None and block <= race["lastBlock"]:
        return
    for name, book in race["strategies"].items():
        strategy_step(
            book,
            signal=signals.get(name, HOLD),
            price=candle["price"],
            time=candle.get("time", ""),
            block=block,
            ts=candle.get("ts"),
            fee=fee,
        )
    race["lastBlock"] = block


def replay_race(candles: list[dict], signal_lists: dict[str, list[str]], fee: float = FEE_PER_SIDE) -> dict:
    """Replay the race rules over a candle list (for the "looking back" view)."""
    if not candles:
        return new_race(signal_lists.keys(), start_block=0, start_time="", start_ts=None, start_price=0.0)
    first = candles[0]
    race = new_race(
        signal_lists.keys(),
        start_block=first["block"],
        start_time=first.get("time", ""),
        start_ts=first.get("ts"),
        start_price=first["price"],
    )
    flags = price_flags(candles)
    for i, c in enumerate(candles):
        if not flags[i]:
            continue  # bledna cena - nie wyceniamy po niej i nie handlujemy
        race_step(race, c, {n: sigs[i] for n, sigs in signal_lists.items()}, fee=fee)
    return race


def summarize_book(book: dict) -> dict:
    trades = book["trades"]
    wins = sum(1 for t in trades if t["pct"] > 0)
    open_pct = None
    if book["position"] != HOLD and book["entryPrice"]:
        open_pct = round(_DIR[book["position"]] * (book["lastPrice"] / book["entryPrice"] - 1.0) * 100.0, 2)
    return {
        "position": book["position"],
        "resultPct": round((book["equity"] - 1.0) * 100.0, 2),
        "trades": len(trades),
        "wins": wins,
        "maxDrawdownPct": book["maxDrawdownPct"],
        "openSince": book["entryTime"],
        "openEntryPrice": round(book["entryPrice"], 2) if book["entryPrice"] else None,
        "openPct": open_pct,
    }


def downsample_curve(curve: list[list], max_points: int = 400) -> list[list]:
    """Keep at most `max_points` evenly spaced points, always including the last."""
    n = len(curve)
    if n <= max_points:
        return list(curve)
    step = n / max_points
    idx = sorted({int(i * step) for i in range(max_points)} | {n - 1})
    return [curve[i] for i in idx]
