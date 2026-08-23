"""
hydra_signals
=============

Reimplementacja (od zera, na podstawie wywnioskowanej architektury) silnika
"smart money vs dumb money" w stylu hydra.trading.

WAŻNE ZAŁOŻENIE: to NIE jest odzyskany kod źródłowy oryginalnej strony.
To autorska reimplementacja tej samej klasy strategii, zaprojektowana tak,
żeby format wyjścia (weight/candle + 4 wskaźniki) przypominał to, co dało się
zaobserwować w wyciekniętym debug-dumpie na hydra.trading. Konkretne progi,
wagi i definicje "dobrego"/"złego" tradera są punktem wyjścia do dalszego
strojenia na prawdziwych danych on-chain (Etap 1 roadmapy).
"""

from .models import Side, Trade, WalletStats, Cohort, WindowScore, Signal
from .wallets import compute_wallet_stats, classify_wallets
from .scoring import ScoringEngine, ScoringConfig
from .synthetic import generate_synthetic_market

__all__ = [
    "Side",
    "Trade",
    "WalletStats",
    "Cohort",
    "WindowScore",
    "Signal",
    "compute_wallet_stats",
    "classify_wallets",
    "ScoringEngine",
    "ScoringConfig",
    "generate_synthetic_market",
]
