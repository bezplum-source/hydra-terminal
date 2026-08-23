"""Uruchomienie demo: `python -m hydra_signals`.

Generuje syntetyczny rynek, odpala pełny pipeline (klasyfikacja -> scoring ->
sygnał), drukuje raport i zapisuje pełną tabelę wyników do CSV.
"""

from __future__ import annotations

import csv
from pathlib import Path

from .backtest import print_report, run_backtest
from .scoring import ScoringConfig
from .synthetic import generate_synthetic_market


def main() -> None:
    print("Generuje syntetyczny rynek (300 portfeli, 12000 blokow)...")
    market = generate_synthetic_market(
        n_blocks=12000,
        n_wallets=300,
        trades_per_block_lambda=4.0,
        skill_strength=0.8,
        seed=42,
    )

    cfg = ScoringConfig()
    print(f"Uruchamiam pipeline (okno={cfg.window_blocks} blokow)...\n")
    report = run_backtest(market, cfg)
    print_report(report)

    out_path = Path(__file__).resolve().parent.parent / "backtest_output.csv"
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "block",
                "price",
                "total_tracked",
                "active",
                "pool_size",
                "good_buyers",
                "good_sellers",
                "bad_buyers",
                "bad_sellers",
                "ind_good_short",
                "ind_good_long",
                "ind_bad_short",
                "ind_bad_long",
                "composite_score",
                "signal",
            ]
        )
        for s in report.scores:
            writer.writerow(
                [
                    s.window_end_block,
                    f"{s.price_usd:.4f}",
                    s.total_wallets_tracked,
                    s.active_wallets,
                    s.pool_size,
                    s.good_buyers,
                    s.good_sellers,
                    s.bad_buyers,
                    s.bad_sellers,
                    f"{s.ind_good_short:.6f}",
                    f"{s.ind_good_long:.6f}",
                    f"{s.ind_bad_short:.6f}",
                    f"{s.ind_bad_long:.6f}",
                    f"{s.composite_score:.6f}",
                    s.signal.value,
                ]
            )
    print(f"\nPelna tabela wynikow zapisana do: {out_path}")


if __name__ == "__main__":
    main()
