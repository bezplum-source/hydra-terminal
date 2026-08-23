"""Generator syntetycznego rynku do testowania pipeline'u bez dostępu do
prawdziwych danych on-chain (Etap 1 roadmapy).

Symulujemy cenę ETH oraz populację portfeli, z których część ma prawdziwą,
choć zaszumioną, przewagę informacyjną (koreluje z przyszłym zwrotem ceny -
proxy na "informed flow"), część jest systematycznie stratna, a reszta handluje
losowo. To pozwala sprawdzić, czy nasza klasyfikacja (Etap `wallets.py`) i
scoring (Etap `scoring.py`) w ogóle są w stanie odróżnić te grupy - warunek
konieczny, zanim podłączymy prawdziwe dane.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List

import numpy as np

from .models import Side, Trade


@dataclass
class SyntheticMarket:
    trades: List[Trade]
    price_at_block: Callable[[int], float]
    wallet_true_skill: dict[str, float]
    n_blocks: int


def generate_synthetic_market(
    *,
    n_blocks: int = 6000,
    n_wallets: int = 300,
    good_frac: float = 0.2,
    bad_frac: float = 0.2,
    start_price: float = 2000.0,
    drift: float = 0.0001,
    vol: float = 0.01,
    skill_strength: float = 0.35,
    lookahead_blocks: int = 20,
    trades_per_block_lambda: float = 3.0,
    seed: int = 42,
) -> SyntheticMarket:
    rng = np.random.default_rng(seed)

    # --- ścieżka ceny (geometric random walk z lekkim driftem) ---
    step_returns = rng.normal(drift, vol, size=n_blocks)
    log_price = np.cumsum(step_returns)
    price_path = start_price * np.exp(log_price)

    def price_at_block(block: int) -> float:
        b = min(max(block, 0), n_blocks - 1)
        return float(price_path[b])

    # --- populacja portfeli z ukrytym "prawdziwym skillem" ---
    wallets = [f"0x{i:040x}" for i in range(n_wallets)]
    skills = np.zeros(n_wallets)
    order = rng.permutation(n_wallets)
    n_good = int(n_wallets * good_frac)
    n_bad = int(n_wallets * bad_frac)
    good_idx = order[:n_good]
    bad_idx = order[n_good : n_good + n_bad]
    skills[good_idx] = rng.uniform(0.4, 1.0, size=n_good)
    skills[bad_idx] = -rng.uniform(0.4, 1.0, size=n_bad)
    wallet_true_skill = dict(zip(wallets, skills.tolist()))

    trades: List[Trade] = []
    usable_blocks = n_blocks - lookahead_blocks

    for block in range(usable_blocks):
        n_trades_this_block = rng.poisson(trades_per_block_lambda)
        if n_trades_this_block == 0:
            continue

        future_ret = log_price[block + lookahead_blocks] - log_price[block]
        future_sign = np.sign(future_ret) if future_ret != 0 else 0.0
        price_now = price_path[block]

        wallet_ids = rng.integers(0, n_wallets, size=n_trades_this_block)
        for wi in wallet_ids:
            skill = skills[wi]
            # "Dobry" portfel częściej kupuje, gdy przyszły zwrot będzie dodatni
            # (i odwrotnie). To jest zaszumiony proxy na informed trading, NIE
            # perfekcyjna wróżba - stąd dodatkowy szum losowy p_buy.
            edge = skill_strength * skill * future_sign
            p_buy = float(np.clip(0.5 + 0.4 * edge, 0.05, 0.95))
            side = Side.BUY if rng.random() < p_buy else Side.SELL

            base_size = float(rng.lognormal(mean=-1.0, sigma=0.8))
            # Druga, niezależna od kierunku poszlaka "smart money": skillowane
            # portfele (dodatnie LUB ujemne) stawiają wyraźnie większe pozycje,
            # gdy akurat mają silną przewagę (|skill| wysoki) - klasyczny
            # sygnał używany w realnych narzędziach on-chain (position sizing
            # jako proxy konwiktcji), niezależny od samej trafności kierunku.
            conviction_multiplier = 1.0 + 2.0 * abs(skill) * abs(future_sign)
            size_eth = base_size * conviction_multiplier

            trades.append(
                Trade(
                    wallet=wallets[wi],
                    block=block,
                    side=side,
                    price_usd=price_now,
                    size_eth=size_eth,
                )
            )

    return SyntheticMarket(
        trades=trades,
        price_at_block=price_at_block,
        wallet_true_skill=wallet_true_skill,
        n_blocks=n_blocks,
    )
