#!/usr/bin/env python3
"""Uruchamia caly pipeline (pobranie transakcji z on-chain -> klasyfikacja ->
scoring) na PRAWDZIWYCH danych z Uniswap V3.

WAZNE: to trzeba uruchomic na maszynie z normalnym dostepem do internetu
(Twoim komputerze albo docelowym serwerze) - srodowisko, w ktorym ten kod
zostal napisany, ma zablokowany ogolny ruch wychodzacy i nie moglo wykonac
zadnego live requestu do RPC. Kod jest przetestowany jednostkowo (patrz
tests/test_onchain_rpc.py) z podstawionym transportem, ale ZANIM zaufasz
wynikom na prawdziwych pieniadzach, zweryfikuj adres puli
(hydra_signals/data_sources/pools.py) na info.uniswap.org lub Etherscanie.

Przyklad uzycia (male, bezpieczne pierwsze uruchomienie - ok. 2000 blokow,
czyli kilka godzin handlu):

    python3 run_live_pipeline.py \
        --rpc-url https://ethereum.publicnode.com \
        --from-block latest-2000 \
        --to-block latest

Publiczne, bezplatne RPC (nie wymagaja zadnego konta/klucza API) do wyboru:
    - https://ethereum.publicnode.com
    - https://eth.llamarpc.com
    - https://cloudflare-eth.com
Kazdy z nich ma wlasne, nieoficjalne limity zapytan - przy wiekszych
zakresach blokow spodziewaj sie bledow/throttlingu i zmniejsz --chunk-size.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from hydra_signals.backtest import print_report, run_backtest  # noqa: E402
from hydra_signals.data_sources.onchain_rpc import JsonRpcClient, fetch_trades_from_chain  # noqa: E402
from hydra_signals.data_sources.pools import UNISWAP_V3_USDC_WETH_005  # noqa: E402
from hydra_signals.scoring import ScoringConfig  # noqa: E402


def _resolve_block(rpc: JsonRpcClient, spec: str) -> int:
    """Obsluguje '12345', 'latest' oraz 'latest-N'."""
    if spec == "latest":
        return int(rpc.call("eth_blockNumber", []), 16)
    if spec.startswith("latest-"):
        offset = int(spec.split("-", 1)[1])
        return int(rpc.call("eth_blockNumber", []), 16) - offset
    return int(spec)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rpc-url", required=True, help="np. https://ethereum.publicnode.com")
    parser.add_argument("--from-block", required=True, help="numer bloku, 'latest' lub 'latest-N'")
    parser.add_argument("--to-block", default="latest", help="numer bloku, 'latest' lub 'latest-N'")
    parser.add_argument("--chunk-size", type=int, default=2000, help="blokow na jedno zapytanie eth_getLogs")
    parser.add_argument("--out-csv", default="live_backtest_output.csv")
    args = parser.parse_args()

    rpc = JsonRpcClient(args.rpc_url)

    print(f"Rozwiazuje zakres blokow wzgledem {args.rpc_url} ...")
    from_block = _resolve_block(rpc, args.from_block)
    to_block = _resolve_block(rpc, args.to_block)
    print(f"Zakres blokow: {from_block} -> {to_block} ({to_block - from_block} blokow)")

    print("Pobieram i dekoduje transakcje Swap z puli "
          f"{UNISWAP_V3_USDC_WETH_005.token0_symbol}/{UNISWAP_V3_USDC_WETH_005.token1_symbol}...")
    trades = fetch_trades_from_chain(
        rpc, UNISWAP_V3_USDC_WETH_005, from_block, to_block, chunk_size=args.chunk_size
    )
    print(f"Pobrano {len(trades)} transakcji od {len({t.wallet for t in trades})} unikalnych portfeli.")

    if not trades:
        print("Brak transakcji w tym zakresie - sprobuj wiekszego zakresu blokow.")
        return

    prices_by_block = {}
    for t in sorted(trades, key=lambda t: t.block):
        prices_by_block[t.block] = t.price_usd

    def price_at_block(block: int) -> float:
        # najblizsza znana cena <= block, w skrajnym przypadku pierwsza znana
        candidates = [b for b in prices_by_block if b <= block]
        ref_block = max(candidates) if candidates else min(prices_by_block)
        return prices_by_block[ref_block]

    from hydra_signals.scoring import ScoringEngine

    engine = ScoringEngine(ScoringConfig())
    scores = engine.run(trades, price_at_block)

    if len(scores) < 2:
        print(
            f"Tylko {len(scores)} okno(a) w tym zakresie - potrzeba wiekszego zakresu blokow "
            "(> 2x ScoringConfig.window_blocks), zeby cokolwiek zbacktestowac."
        )
        return

    class _Market:
        pass

    market = _Market()
    market.trades = trades
    market.wallet_true_skill = {}  # brak "prawdy" na realnych danych - pomijamy korelacje

    from hydra_signals.backtest import BacktestReport, _classification_quality
    import numpy as np
    from hydra_signals.models import Signal

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

    report = BacktestReport(
        scores=scores,
        equity_strategy=equity_strategy,
        equity_buy_hold=equity_bh,
        classification_correlation=float("nan"),
        n_signal_flips=n_flips,
        strategy_total_return=equity_strategy[-1] - 1.0,
        buy_hold_total_return=equity_bh[-1] - 1.0,
        hit_rate=correct_direction / (len(scores) - 1),
    )
    print_report(report)

    out_path = Path(args.out_csv)
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["block", "price", "signal", "composite_score", "good_buyers", "good_sellers", "bad_buyers", "bad_sellers"])
        for s in scores:
            writer.writerow([s.window_end_block, f"{s.price_usd:.4f}", s.signal.value, f"{s.composite_score:.6f}", s.good_buyers, s.good_sellers, s.bad_buyers, s.bad_sellers])
    print(f"\nZapisano: {out_path}")


if __name__ == "__main__":
    main()
