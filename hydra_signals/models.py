"""Podstawowe typy danych używane w całym silniku."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Side(str, Enum):
    """Kierunek pojedynczej transakcji na DEX-ie."""

    BUY = "BUY"
    SELL = "SELL"


class Cohort(str, Enum):
    """Klasyfikacja portfela na podstawie jego historycznej skuteczności."""

    GOOD = "GOOD"
    BAD = "BAD"
    NEUTRAL = "NEUTRAL"
    # Portfel widziany zbyt rzadko / zbyt krótko, żeby cokolwiek o nim wnioskować.
    UNRATED = "UNRATED"


class Signal(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    HOLD = "HOLD"


@dataclass(frozen=True)
class Trade:
    """Pojedyncza transakcja swap na DEX-ie, przypisana do portfela.

    `size_eth` jest zawsze dodatnie — kierunek niesie pole `side`.
    `block` to numer bloku Ethereum, w którym transakcja się wydarzyła
    (używamy bloków, nie czasu, żeby okna 250-blokowe pokrywały się
    dokładnie z tym, co widać w oryginalnym cadence hydra.trading).
    """

    wallet: str
    block: int
    side: Side
    price_usd: float
    size_eth: float

    @property
    def notional_usd(self) -> float:
        return self.price_usd * self.size_eth


@dataclass
class WalletStats:
    """Zagregowana charakterystyka portfela liczona w oknie kroczącym."""

    wallet: str
    n_trades: int
    total_volume_eth: float
    realized_pnl_usd: float
    win_rate: float
    # PnL znormalizowany po wolumenie - pozwala porównywać "grubą rybę"
    # z małym portfelem na tej samej skali.
    pnl_per_eth: float
    skill_score: float = 0.0
    cohort: Cohort = Cohort.UNRATED


@dataclass
class WindowScore:
    """Wynik dla jednej "świecy" (okna N bloków) - odpowiednik linii
    "weight" / "Candle" z wycieku na hydra.trading."""

    window_end_block: int
    price_usd: float
    total_wallets_tracked: int
    active_wallets: int
    pool_size: int  # liczba portfeli faktycznie użytych w scoringu tego okna

    good_buyers: int
    good_sellers: int
    bad_buyers: int
    bad_sellers: int

    good_buy_ratio_raw: float
    bad_buy_ratio_raw: float

    ind_good_short: float
    ind_good_long: float
    ind_bad_short: float
    ind_bad_long: float

    composite_score: float
    signal: Signal

    def as_debug_line(self) -> str:
        """Renderuje wynik w formacie zbliżonym do wycieku z hydra.trading -
        głównie do wizualnej weryfikacji "czy to wygląda znajomo"."""
        return (
            f"weight  1.00000   {self.total_wallets_tracked:>7d}  "
            f"{self.active_wallets:>6d}   {self.pool_size:>6d}   "
            f"{self.pool_size:>6d}   {float(self.pool_size):.1f}\n"
            f"Candle {self.window_end_block} {self.price_usd:.8f}      "
            f"{self.ind_good_short:+.8f}      {self.ind_good_long:+.8f}      "
            f"{self.ind_bad_short:+.8f}      {self.ind_bad_long:+.8f}      "
            f"{self.pool_size:>3d}   {self.good_buyers:>3d}   {self.good_sellers:>3d}   "
            f"{self.bad_buyers:>3d}   {self.bad_sellers:>3d}"
        )
