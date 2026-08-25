"""Testy filtru "dust" (zgloszenie uzytkownika 2026-08-25: "Proponuje, zeby
brac pod uwage portfele z min. 1000 dolarow. Czyli zeby odsiac dust.",
doprecyzowane przez AskUserQuestion: prog per-POJEDYNCZA-TRANSAKCJA, na
OBU torach - Uniswap i Hyperliquid).

Sam prog zyje w `ScoringConfig.min_trade_notional_usd`/
`HyperliquidScoringConfig.min_trade_notional_usd` (patrz komentarze tam po
pelne uzasadnienie) i jest stosowany WYLACZNIE wewnatrz
`ScoringEngine.run()`/`HyperliquidScoringEngine.run()` (oraz
`classify_hyperliquid_wallets()` dla symetrii) - `compute_wallet_stats`/
`classify_wallets` w `wallets.py` swiadomie NIE filtruja nic same z siebie
(zeby pozostac uzywalne tez poza kontekstem "live" bez zadnego progu
dolarowego, gdyby ktos chcial to zrobic recznie).
"""

from __future__ import annotations

from hydra_signals.data_sources.hyperliquid_ws import AggressorSide, HyperliquidTrade
from hydra_signals.hyperliquid_wallets import (
    HyperliquidScoringConfig,
    HyperliquidScoringEngine,
    classify_hyperliquid_wallets,
)
from hydra_signals.models import Side, Trade
from hydra_signals.scoring import ScoringConfig, ScoringEngine


def make_trade(wallet, block, side, price, size):
    return Trade(wallet=wallet, block=block, side=side, price_usd=price, size_eth=size)


def make_hl_trade(buyer, seller, price, size, ts_ms):
    return HyperliquidTrade(
        coin="ETH",
        aggressor_side=AggressorSide.BUY,
        price_usd=price,
        size_eth=size,
        buyer=buyer,
        seller=seller,
        ts_ms=ts_ms,
        tid=ts_ms,
        tx_hash="0xabc",
    )


# =====================================================================
# Uniswap (ScoringEngine) - patrz ScoringConfig.min_trade_notional_usd
# =====================================================================


def test_trade_exactly_at_threshold_is_not_filtered():
    # Prog jest ">=", nie ">" - transakcja o notional DOKLADNIE rownym
    # progowi (100.0 * 10.0 = 1000.0) MUSI zostac policzona, nie odsiana.
    cfg = ScoringConfig()
    assert cfg.min_trade_notional_usd == 1000.0
    engine = ScoringEngine(cfg)
    trades = [make_trade("w1", 1, Side.BUY, 100.0, 10.0)]
    scores = engine.run(trades, lambda b: 100.0)
    assert len(scores) == 1
    assert scores[0].total_wallets_tracked == 1
    assert scores[0].active_wallets == 1


def test_all_sub_threshold_trades_yield_no_windows_and_no_tracked_wallets():
    # Same transakcje ponizej progu (50.0 * 1.0 = 50.0 notional) - powinny
    # zostac odsiane W CALOSCI, jeszcze zanim cokolwiek zbuduje okna/swiece
    # (engine.run zwraca pusta liste, dokladnie jak przy calkowitym braku
    # transakcji na wejsciu).
    engine = ScoringEngine(ScoringConfig())
    trades = [make_trade("dust1", b, Side.BUY, 50.0, 1.0) for b in range(0, 750, 250)]
    assert engine.run(trades, lambda b: 50.0) == []
    assert engine.total_tracked == set()


def test_dust_trade_ignored_in_window_activity_while_qualifying_trade_counts():
    # w1 buduje sobie GOOD kohorte w historii przez zyskowny round-trip
    # KWALIFIKUJACYMI sie transakcjami (notional 1500/2000, obie >= progu).
    history = [
        make_trade("w1", 1, Side.BUY, 15.0, 100.0),  # notional 1500
        make_trade("w1", 2, Side.SELL, 20.0, 100.0),  # notional 2000, zysk -> GOOD
    ]
    cfg = ScoringConfig(
        window_blocks=100,
        classification_lookback_blocks=1000,
        min_trades_for_classification=1,
        good_pct=0.99,
        bad_pct=0.0,
    )
    engine = ScoringEngine(cfg)

    # W oknie testowym w1 ma DWIE transakcje: jedna kwalifikujaca sie BUY
    # (notional 2000) i jedna DUST SELL (notional 50). Gdyby dust NIE byl
    # odsiany, net_direction wygladalby na net-SELL (100 SELL vs 1 BUY) i
    # w1 zostalby policzony jako good_seller, nie good_buyer.
    window_trades = [
        make_trade("w1", 150, Side.BUY, 2000.0, 1.0),  # notional 2000
        make_trade("w1", 150, Side.SELL, 5.0, 10.0),  # notional 50 - DUST
    ]
    scores = engine.run(window_trades, lambda b: 2000.0, history_trades=history)

    assert len(scores) == 1
    s = scores[0]
    assert s.good_buyers == 1
    assert s.good_sellers == 0


# =====================================================================
# Hyperliquid ETH-PERP (HyperliquidScoringEngine) - odpowiednik powyzszego,
# ta sama nazwa/wartosc progu (HyperliquidScoringConfig.min_trade_notional_usd)
# =====================================================================


def test_hl_trade_exactly_at_threshold_is_not_filtered():
    cfg = HyperliquidScoringConfig()
    assert cfg.min_trade_notional_usd == 1000.0
    engine = HyperliquidScoringEngine(cfg)
    trades = [make_hl_trade("buyer1", "seller1", 100.0, 10.0, ts_ms=1)]  # notional 1000.0
    score = engine.run(trades, window_end_ts_ms=1)
    assert score is not None
    assert score.total_wallets_tracked == 2  # buyer i seller oboje policzeni
    assert score.active_wallets == 2


def test_hl_all_dust_new_trades_behaves_like_empty_input():
    # "CELOWO nie aktualizujemy wtedy EMA" (patrz docstring
    # HyperliquidScoringEngine.run) - dust-only `new_trades` musi dac
    # DOKLADNIE ten sam wynik (None, stan nietkniety) co pusty `new_trades`.
    engine = HyperliquidScoringEngine(HyperliquidScoringConfig())
    trades = [make_hl_trade("a", "b", 50.0, 1.0, ts_ms=1)]  # notional 50 - dust
    assert engine.run(trades, window_end_ts_ms=1) is None
    assert engine.total_tracked == set()
    assert engine.export_state() == {
        "good_short": None,
        "good_long": None,
        "bad_short": None,
        "bad_long": None,
    }


def test_hl_dust_trade_ignored_in_window_activity_while_qualifying_trade_counts():
    history = [
        make_hl_trade("w1", "cp1", 15.0, 100.0, ts_ms=1),  # notional 1500, w1=buyer
        make_hl_trade("cp2", "w1", 20.0, 100.0, ts_ms=2),  # notional 2000, w1=seller -> zysk -> GOOD
    ]
    # min_trades_for_classification=2 wyklucza z klasyfikacji jednorazowych
    # kontrahentow (cp1/cp2/cpw - kazdy pojawia sie tylko raz), zeby liczba
    # good_sellers nizej mierzyla WYLACZNIE zachowanie w1 (jedynego portfela
    # z >=2 transakcjami), a nie przypadkowa klasyfikacje kontrahenta.
    cfg = HyperliquidScoringConfig(min_trades_for_classification=2, good_pct=0.99, bad_pct=0.0)
    engine = HyperliquidScoringEngine(cfg)

    new_trades = [
        make_hl_trade("w1", "cpw", 2000.0, 1.0, ts_ms=100),  # notional 2000, w1=buyer -> BUY
        make_hl_trade("cpw2", "w1", 5.0, 10.0, ts_ms=101),  # notional 50 - DUST, w1=seller
    ]
    score = engine.run(new_trades, history_trades=history, window_end_ts_ms=101)

    assert score is not None
    assert score.good_buyers == 1
    assert score.good_sellers == 0


def test_classify_hyperliquid_wallets_also_respects_dust_threshold():
    # `classify_hyperliquid_wallets` (funkcja H1, nieuzywana bezposrednio w
    # live-pipelinie od Fazy H2, ale publiczna) filtruje dust identycznie -
    # dla spojnosci obu wejsc do tego samego silnika PnL.
    trades = [
        make_hl_trade("w1", "cp1", 15.0, 100.0, ts_ms=1),  # notional 1500
        make_hl_trade("cp2", "w1", 20.0, 100.0, ts_ms=2),  # notional 2000, zysk
        make_hl_trade("w1", "cp3", 1.0, 1.0, ts_ms=3),  # notional 1 - dust, ignorowana
    ]
    stats = classify_hyperliquid_wallets(trades, min_trades=1, good_pct=0.99, bad_pct=0.0)
    assert stats["w1"].n_trades == 2  # NIE 3 - dust nie wchodzi do liczenia
