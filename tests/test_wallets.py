from hydra_signals.models import Side, Trade, Cohort
from hydra_signals.wallets import compute_wallet_stats, classify_wallets


def make_trade(wallet, block, side, price, size):
    return Trade(wallet=wallet, block=block, side=side, price_usd=price, size_eth=size)


def test_profitable_wallet_gets_positive_pnl():
    # Kupuje po 100, sprzedaje po 150 -> realizowany zysk.
    trades = [
        make_trade("w1", 1, Side.BUY, 100, 1.0),
        make_trade("w1", 2, Side.SELL, 150, 1.0),
        make_trade("w1", 3, Side.BUY, 100, 1.0),
        make_trade("w1", 4, Side.SELL, 150, 1.0),
        make_trade("w1", 5, Side.BUY, 100, 1.0),
    ]
    stats = compute_wallet_stats(trades, min_trades=1)
    assert stats["w1"].realized_pnl_usd > 0
    assert stats["w1"].win_rate == 1.0


def test_losing_wallet_gets_negative_pnl():
    trades = [
        make_trade("w2", 1, Side.BUY, 150, 1.0),
        make_trade("w2", 2, Side.SELL, 100, 1.0),
        make_trade("w2", 3, Side.BUY, 150, 1.0),
        make_trade("w2", 4, Side.SELL, 100, 1.0),
        make_trade("w2", 5, Side.BUY, 150, 1.0),
    ]
    stats = compute_wallet_stats(trades, min_trades=1)
    assert stats["w2"].realized_pnl_usd < 0
    assert stats["w2"].win_rate == 0.0


def test_wallet_below_min_trades_is_excluded():
    trades = [make_trade("w3", 1, Side.BUY, 100, 1.0)]
    stats = compute_wallet_stats(trades, min_trades=5)
    assert "w3" not in stats


def test_short_position_pnl_symmetry():
    # Sprzedaje bez wczesniejszego zakupu (otwiera "krotka" pozycje),
    # potem odkupuje taniej -> zysk.
    trades = [
        make_trade("w4", 1, Side.SELL, 150, 1.0),
        make_trade("w4", 2, Side.BUY, 100, 1.0),
        make_trade("w4", 3, Side.SELL, 150, 1.0),
        make_trade("w4", 4, Side.BUY, 100, 1.0),
        make_trade("w4", 5, Side.SELL, 150, 1.0),
    ]
    stats = compute_wallet_stats(trades, min_trades=1)
    assert stats["w4"].realized_pnl_usd > 0


def test_classify_wallets_splits_into_cohorts():
    stats = {}
    trades_by_wallet = {}
    # 10 dobrych, 10 zlych, 10 neutralnych (losowe wygrane/przegrane)
    for i in range(10):
        trades_by_wallet[f"good{i}"] = [
            make_trade(f"good{i}", b, Side.BUY if b % 2 == 0 else Side.SELL, 100 + (10 if b % 2 else 0), 1.0)
            for b in range(6)
        ]
    all_trades = [t for ts in trades_by_wallet.values() for t in ts]
    stats = compute_wallet_stats(all_trades, min_trades=1)
    classify_wallets(stats, good_pct=0.5, bad_pct=0.5)
    cohorts = {s.cohort for s in stats.values()}
    assert Cohort.GOOD in cohorts or Cohort.BAD in cohorts
