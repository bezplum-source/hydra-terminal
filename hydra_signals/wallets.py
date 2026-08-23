"""Liczenie skuteczności portfeli (rolling PnL, win-rate) i ich klasyfikacja
na kohorty GOOD / BAD / NEUTRAL.

Metoda PnL: average-cost (analogiczna do standardowego "cost basis" używanego
w księgowaniu pozycji), obsługuje zarówno pozycje long, jak i short (transakcja
SELL bez wcześniejszego BUY otwiera krótką pozycję) - na DEX-ach spotowych
"short" w sensie dosłownym nie istnieje, ale portfele mogą sprzedawać więcej
niż kupiły w oknie obserwacji (np. wcześniej nabyty token), więc traktujemy to
symetrycznie, żeby nie tracić informacji o skuteczności.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from .models import Cohort, Side, Trade, WalletStats


@dataclass
class _PositionState:
    position: float = 0.0  # dodatnie = long, ujemne = short
    avg_price: float = 0.0
    realized_pnl_usd: float = 0.0
    closing_trades: int = 0
    winning_trades: int = 0
    n_trades: int = 0
    total_volume_eth: float = 0.0


def _apply_trade(state: _PositionState, side: Side, price: float, size: float) -> None:
    delta = size if side is Side.BUY else -size
    state.n_trades += 1
    state.total_volume_eth += size

    same_direction = state.position == 0 or (state.position > 0) == (delta > 0)

    if same_direction:
        new_abs = abs(state.position) + size
        if new_abs > 0:
            state.avg_price = (
                abs(state.position) * state.avg_price + size * price
            ) / new_abs
        state.position += delta
        return

    # Zamykamy (częściowo lub całkowicie) istniejącą pozycję.
    closing_size = min(size, abs(state.position))
    if delta > 0:
        # Kupujemy, mając pozycję krótką -> zysk gdy cena spadła poniżej avg_price.
        trade_pnl = closing_size * (state.avg_price - price)
    else:
        # Sprzedajemy, mając pozycję długą -> zysk gdy cena wzrosła powyżej avg_price.
        trade_pnl = closing_size * (price - state.avg_price)

    state.realized_pnl_usd += trade_pnl
    state.closing_trades += 1
    if trade_pnl > 0:
        state.winning_trades += 1

    remaining = size - closing_size
    state.position += delta
    if remaining > 0:
        # Pozycja "przebiła" zero i otwiera się w drugą stronę.
        state.avg_price = price
        state.position = remaining if delta > 0 else -remaining


def compute_wallet_stats(
    trades: Iterable[Trade],
    *,
    min_trades: int = 5,
) -> dict[str, WalletStats]:
    """Liczy statystyki per portfel na podstawie listy transakcji.

    Zakłada, że `trades` jest już przefiltrowane do właściwego okna czasowego
    (np. ostatnie N bloków) - ta funkcja nie zna pojęcia "teraz", tylko
    agreguje to, co dostanie.
    """

    states: dict[str, _PositionState] = defaultdict(_PositionState)

    for t in sorted(trades, key=lambda t: t.block):
        _apply_trade(states[t.wallet], t.side, t.price_usd, t.size_eth)

    out: dict[str, WalletStats] = {}
    for wallet, s in states.items():
        if s.n_trades < min_trades:
            continue
        win_rate = s.winning_trades / s.closing_trades if s.closing_trades > 0 else 0.0
        pnl_per_eth = (
            s.realized_pnl_usd / s.total_volume_eth if s.total_volume_eth > 0 else 0.0
        )
        out[wallet] = WalletStats(
            wallet=wallet,
            n_trades=s.n_trades,
            total_volume_eth=s.total_volume_eth,
            realized_pnl_usd=s.realized_pnl_usd,
            win_rate=win_rate,
            pnl_per_eth=pnl_per_eth,
        )
    return out


def classify_wallets(
    stats: dict[str, WalletStats],
    *,
    good_pct: float = 0.15,
    bad_pct: float = 0.15,
    min_closing_trades_pct_rank: bool = True,
) -> dict[str, WalletStats]:
    """Rankuje portfele i przypisuje kohorty GOOD/BAD/NEUTRAL.

    Ranking to średnia z percentylowej pozycji w `pnl_per_eth` i `win_rate` -
    celowo rank-based (nie z-score), żeby pojedyncze ekstremalne transakcje
    (typowe w crypto) nie zdominowały klasyfikacji.
    """

    if not stats:
        return stats

    df = pd.DataFrame(
        {
            "wallet": w,
            "pnl_per_eth": s.pnl_per_eth,
            "win_rate": s.win_rate,
        }
        for w, s in stats.items()
    )

    df["rank_pnl"] = df["pnl_per_eth"].rank(pct=True)
    df["rank_win"] = df["win_rate"].rank(pct=True)
    df["skill_score"] = 0.5 * df["rank_pnl"] + 0.5 * df["rank_win"]

    good_cut = df["skill_score"].quantile(1 - good_pct)
    bad_cut = df["skill_score"].quantile(bad_pct)

    for row in df.itertuples():
        s = stats[row.wallet]
        s.skill_score = row.skill_score
        if row.skill_score >= good_cut:
            s.cohort = Cohort.GOOD
        elif row.skill_score <= bad_cut:
            s.cohort = Cohort.BAD
        else:
            s.cohort = Cohort.NEUTRAL

    return stats
