"""End-to-end uruchomienie pipeline'u na syntetycznym rynku + prosty backtest
strategii + walidacja, że klasyfikacja portfeli w ogóle łapie ukryty "skill".

To NIE jest backtest na prawdziwej historii hydra.trading (do tego potrzebne
są realne dane on-chain z Etapu 1) - to test poprawności mechanizmu: czy przy
świecie, w którym różnica między "dobrymi" a "złymi" portfelami naprawdę
istnieje, nasz pipeline jest w stanie ją wykryć i zamienić w sensowny sygnał.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import numpy as np

from .models import Signal, WindowScore
from .scoring import ScoringConfig, ScoringEngine
from .synthetic import SyntheticMarket
from .wallets import classify_wallets, compute_wallet_stats


@dataclass
class BacktestReport:
    scores: List[WindowScore]
    equity_strategy: List[float]
    equity_buy_hold: List[float]
    classification_correlation: float
    n_signal_flips: int
    strategy_total_return: float
    buy_hold_total_return: float
    hit_rate: float  # % okien, w których kierunek sygnału zgadzał się z następnym zwrotem


def _classification_quality(market: SyntheticMarket, cfg: ScoringConfig) -> float:
    """Korelacja Spearmana (przybliżona przez rangi) między naszym skill_score
    a ukrytym prawdziwym skillem portfela - sanity check klasyfikacji."""

    stats = compute_wallet_stats(
        market.trades, min_trades=cfg.min_trades_for_classification
    )
    classify_wallets(stats, good_pct=cfg.good_pct, bad_pct=cfg.bad_pct)

    common = [w for w in stats if w in market.wallet_true_skill]
    if len(common) < 5:
        return float("nan")

    our_scores = np.array([stats[w].skill_score for w in common])
    true_skill = np.array([market.wallet_true_skill[w] for w in common])

    our_rank = our_scores.argsort().argsort().astype(float)
    true_rank = true_skill.argsort().argsort().astype(float)
    if our_rank.std() == 0 or true_rank.std() == 0:
        return float("nan")
    return float(np.corrcoef(our_rank, true_rank)[0, 1])


def run_backtest(market: SyntheticMarket, cfg: ScoringConfig | None = None) -> BacktestReport:
    cfg = cfg or ScoringConfig()
    engine = ScoringEngine(cfg)
    scores = engine.run(market.trades, market.price_at_block)

    if len(scores) < 2:
        raise ValueError("Za mało okien do backtestu - zwiększ n_blocks w danych syntetycznych.")

    equity_strategy = [1.0]
    equity_bh = [1.0]
    correct_direction = 0
    n_flips = 0

    for i in range(1, len(scores)):
        prev, cur = scores[i - 1], scores[i]
        ret = (cur.price_usd / prev.price_usd) - 1.0

        position = 1.0 if prev.signal is Signal.LONG else (-1.0 if prev.signal is Signal.SHORT else 0.0)
        equity_strategy.append(equity_strategy[-1] * (1 + position * ret))
        equity_bh.append(equity_bh[-1] * (1 + ret))

        if position != 0 and np.sign(ret) == np.sign(position):
            correct_direction += 1
        if cur.signal != prev.signal:
            n_flips += 1

    hit_rate = correct_direction / (len(scores) - 1)
    corr = _classification_quality(market, cfg)

    return BacktestReport(
        scores=scores,
        equity_strategy=equity_strategy,
        equity_buy_hold=equity_bh,
        classification_correlation=corr,
        n_signal_flips=n_flips,
        strategy_total_return=equity_strategy[-1] - 1.0,
        buy_hold_total_return=equity_bh[-1] - 1.0,
        hit_rate=hit_rate,
    )


def print_report(report: BacktestReport, n_debug_lines: int = 8) -> None:
    print("=== Przykladowe linie w formacie zblizonym do wycieku hydra.trading ===")
    for score in report.scores[:n_debug_lines]:
        print(score.as_debug_line())

    print()
    print("=== Wyniki backtestu (dane SYNTETYCZNE, nie realny rynek) ===")
    print(f"Liczba okien (swiec):              {len(report.scores)}")
    print(f"Liczba zmian sygnalu:               {report.n_signal_flips}")
    print(f"Trafnosc kierunku (hit rate):        {report.hit_rate:.1%}")
    print(f"Zwrot strategii:                    {report.strategy_total_return:+.2%}")
    print(f"Zwrot buy&hold:                      {report.buy_hold_total_return:+.2%}")
    print(
        "Korelacja klasyfikacji z prawdziwym skillem (synth):  "
        f"{report.classification_correlation:.3f}  (1.0 = perfekcyjna, 0 = losowa)"
    )
