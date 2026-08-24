"""Testy Fazy H1 (klasyfikacja portfeli Hyperliquid) - mirror stylu
tests/test_wallets.py, ale wejściem są `HyperliquidTrade` (dwie strony
na jedno zdarzenie), nie pojedyncze `Trade` z Uniswap."""

from __future__ import annotations

from hydra_signals.data_sources.hyperliquid_ws import AggressorSide, HyperliquidTrade
from hydra_signals.hyperliquid_wallets import (
    classify_hyperliquid_wallets,
    hyperliquid_trade_to_wallet_trades,
    hyperliquid_trades_to_wallet_trades,
)
from hydra_signals.models import Cohort, Side


def make_hl_trade(buyer, seller, price, size, ts_ms, tid=0, aggressor=AggressorSide.BUY):
    return HyperliquidTrade(
        coin="ETH",
        aggressor_side=aggressor,
        price_usd=price,
        size_eth=size,
        buyer=buyer,
        seller=seller,
        ts_ms=ts_ms,
        tid=tid,
        tx_hash="0xabc",
    )


# ---------- hyperliquid_trade_to_wallet_trades ----------


def test_single_hl_trade_becomes_buy_and_sell_wallet_trades():
    hl_trade = make_hl_trade("0xBUYER", "0xSELLER", 2500.0, 1.5, ts_ms=1_000)
    wallet_trades = hyperliquid_trade_to_wallet_trades(hl_trade)

    assert len(wallet_trades) == 2
    buy_trade = next(t for t in wallet_trades if t.side is Side.BUY)
    sell_trade = next(t for t in wallet_trades if t.side is Side.SELL)

    assert buy_trade.wallet == "0xBUYER"
    assert sell_trade.wallet == "0xSELLER"
    # Obie strony po tej samej cenie/wielkosci.
    assert buy_trade.price_usd == sell_trade.price_usd == 2500.0
    assert buy_trade.size_eth == sell_trade.size_eth == 1.5
    # ts_ms podstawiony pod block, do sortowania chronologicznego.
    assert buy_trade.block == sell_trade.block == 1_000


def test_multiple_hl_trades_flatten_to_double_the_wallet_trades():
    trades = [
        make_hl_trade("a", "b", 100.0, 1.0, ts_ms=1),
        make_hl_trade("c", "d", 200.0, 2.0, ts_ms=2),
        make_hl_trade("e", "f", 300.0, 3.0, ts_ms=3),
    ]
    wallet_trades = hyperliquid_trades_to_wallet_trades(trades)
    assert len(wallet_trades) == 6
    wallets = {t.wallet for t in wallet_trades}
    assert wallets == {"a", "b", "c", "d", "e", "f"}


# ---------- classify_hyperliquid_wallets: PnL end-to-end przez adapter ----------


def test_profitable_perp_wallet_gets_positive_pnl_as_buyer_then_seller():
    # "w1" kupuje tanio (buyer), potem odsprzedaje drogo (seller w kolejnej
    # transakcji) - realizowany zysk, tak samo jak w tests/test_wallets.py.
    trades = [
        make_hl_trade("w1", "counterparty0", 100.0, 1.0, ts_ms=1),
        make_hl_trade("counterparty1", "w1", 150.0, 1.0, ts_ms=2),
        make_hl_trade("w1", "counterparty2", 100.0, 1.0, ts_ms=3),
        make_hl_trade("counterparty3", "w1", 150.0, 1.0, ts_ms=4),
        make_hl_trade("w1", "counterparty4", 100.0, 1.0, ts_ms=5),
    ]
    stats = classify_hyperliquid_wallets(trades, min_trades=1, good_pct=0.5, bad_pct=0.5)
    assert stats["w1"].realized_pnl_usd > 0
    assert stats["w1"].win_rate == 1.0


def test_losing_perp_wallet_gets_negative_pnl():
    trades = [
        make_hl_trade("w2", "counterparty0", 150.0, 1.0, ts_ms=1),
        make_hl_trade("counterparty1", "w2", 100.0, 1.0, ts_ms=2),
        make_hl_trade("w2", "counterparty2", 150.0, 1.0, ts_ms=3),
        make_hl_trade("counterparty3", "w2", 100.0, 1.0, ts_ms=4),
        make_hl_trade("w2", "counterparty4", 150.0, 1.0, ts_ms=5),
    ]
    stats = classify_hyperliquid_wallets(trades, min_trades=1, good_pct=0.5, bad_pct=0.5)
    assert stats["w2"].realized_pnl_usd < 0
    assert stats["w2"].win_rate == 0.0


def test_true_short_position_pnl_symmetry():
    # "w3" jest SELLEREM bez wczesniejszego kupna (otwiera prawdziwa
    # krotka pozycje na perpach), potem odkupuje taniej -> zysk.
    trades = [
        make_hl_trade("counterparty0", "w3", 150.0, 1.0, ts_ms=1),
        make_hl_trade("w3", "counterparty1", 100.0, 1.0, ts_ms=2),
        make_hl_trade("counterparty2", "w3", 150.0, 1.0, ts_ms=3),
        make_hl_trade("w3", "counterparty3", 100.0, 1.0, ts_ms=4),
        make_hl_trade("counterparty4", "w3", 150.0, 1.0, ts_ms=5),
    ]
    stats = classify_hyperliquid_wallets(trades, min_trades=1, good_pct=0.5, bad_pct=0.5)
    assert stats["w3"].realized_pnl_usd > 0


def test_wallet_below_min_trades_is_excluded():
    trades = [make_hl_trade("w4", "counterparty", 100.0, 1.0, ts_ms=1)]
    stats = classify_hyperliquid_wallets(trades, min_trades=5)
    # Kazdy z wallet ma tylko 1 transakcje (< min_trades=5) -> wykluczeni.
    assert "w4" not in stats
    assert "counterparty" not in stats


def test_classify_splits_good_and_bad_cohorts():
    trades = []
    ts = 0
    # 10 portfeli konsekwentnie zyskownych (kupuja tanio jako buyer, potem
    # sprzedaja drogo jako seller w kolejnej transakcji).
    for i in range(10):
        wallet = f"good{i}"
        for _ in range(3):
            trades.append(make_hl_trade(wallet, f"cp{ts}", 100.0, 1.0, ts_ms=ts))
            ts += 1
            trades.append(make_hl_trade(f"cp{ts}", wallet, 150.0, 1.0, ts_ms=ts))
            ts += 1
    # 10 portfeli konsekwentnie stratnych (odwrotnie).
    for i in range(10):
        wallet = f"bad{i}"
        for _ in range(3):
            trades.append(make_hl_trade(wallet, f"cp{ts}", 150.0, 1.0, ts_ms=ts))
            ts += 1
            trades.append(make_hl_trade(f"cp{ts}", wallet, 100.0, 1.0, ts_ms=ts))
            ts += 1

    stats = classify_hyperliquid_wallets(trades, min_trades=1, good_pct=0.3, bad_pct=0.3)

    for i in range(10):
        assert stats[f"good{i}"].cohort in (Cohort.GOOD, Cohort.NEUTRAL)
        assert stats[f"bad{i}"].cohort in (Cohort.BAD, Cohort.NEUTRAL)
    # Przynajmniej niektorzy faktycznie wyladowali w skrajnych kohortach,
    # nie wszyscy jako NEUTRAL (sanity check, ze klasyfikacja cos robi).
    cohorts = {s.cohort for s in stats.values()}
    assert Cohort.GOOD in cohorts
    assert Cohort.BAD in cohorts


def test_classify_hyperliquid_wallets_never_mixes_up_buyer_and_seller_roles():
    # Regresja: buyer/seller nie moga sie zamienic miejscami w adapterze -
    # gdyby tak sie stalo, ponizszy "dobry" portfel wygladalby jak "zly".
    trades = [
        make_hl_trade("skilled", "loser0", 100.0, 1.0, ts_ms=1),  # skilled kupuje tanio
        make_hl_trade("loser1", "skilled", 150.0, 1.0, ts_ms=2),  # skilled sprzedaje drogo
    ]
    stats = classify_hyperliquid_wallets(trades, min_trades=1)
    assert stats["skilled"].realized_pnl_usd > 0
