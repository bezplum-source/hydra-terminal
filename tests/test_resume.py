"""Testy wznawialności ScoringEngine - kluczowe dla pipeline'u cyklicznego
(GitHub Actions uruchamiany co godzinę jako osobny proces, bez pamięci
między uruchomieniami poza tym, co jawnie zapiszemy na dysk).

Sprawdzamy, że podzielenie tej samej historii transakcji na dwie części i
przepuszczenie ich przez DWA OSOBNE `ScoringEngine` (drugi zainicjalizowany
stanem wyeksportowanym z pierwszego) daje IDENTYCZNY wynik, co jeden
ciągły przebieg na całości - to jest dokładnie to, co dzieje się w
`live/run_incremental.py` między kolejnymi uruchomieniami joba.
"""

from __future__ import annotations

from hydra_signals.models import Side, Signal, Trade
from hydra_signals.scoring import ScoringConfig, ScoringEngine


def make_trade(wallet, block, side, price, size):
    return Trade(wallet=wallet, block=block, side=side, price_usd=price, size_eth=size)


def _build_trades():
    trades = []
    price_map = {}
    block = 0
    price = 100.0
    for cycle in range(30):
        for wallet_id in range(6):
            trades.append(make_trade(f"good{wallet_id}", block, Side.BUY, price, 1.0))
        for wallet_id in range(3):
            trades.append(make_trade(f"bad{wallet_id}", block, Side.SELL, price, 1.0))
        price_map[block] = price
        block += 1
        price *= 1.02
        price_map[block] = price
        for wallet_id in range(6):
            trades.append(make_trade(f"good{wallet_id}", block, Side.SELL, price, 1.0))
        for wallet_id in range(3):
            trades.append(make_trade(f"bad{wallet_id}", block, Side.BUY, price, 1.0))
        block += 25
    return trades, price_map


def _cfg():
    return ScoringConfig(
        window_blocks=25,
        classification_lookback_blocks=1000,
        min_trades_for_classification=5,
        good_pct=0.4,
        bad_pct=0.4,
        ema_short_span=2,
        ema_long_span=4,
    )


def test_split_run_matches_continuous_run():
    trades, price_map = _build_trades()

    def price_at_block(b):
        return price_map.get(b, price_map[max(k for k in price_map if k <= b)])

    engine_full = ScoringEngine(_cfg())
    scores_full = engine_full.run(trades, price_at_block)
    assert len(scores_full) >= 4  # sanity check na sam test

    # Podziel dokladnie w polowie bloków (nie transakcji), zeby obie polowy
    # mialy pelne okna po obu stronach ciecia.
    max_block = max(t.block for t in trades)
    cut = max_block // 2

    first_half = [t for t in trades if t.block <= cut]
    second_half = [t for t in trades if t.block > cut]
    assert first_half and second_half

    engine1 = ScoringEngine(_cfg())
    scores1 = engine1.run(first_half, price_at_block)
    state = engine1.export_state()

    engine2 = ScoringEngine(
        _cfg(),
        initial_ema=state,
        initial_prev_signal=Signal(state["prev_signal"]),
        initial_total_tracked=engine1.total_tracked,
    )
    # history_trades = to, co juz bylo widziane w polowie 1 (potrzebne do
    # poprawnego lookback klasyfikacji tuz po "wznowieniu procesu").
    scores2 = engine2.run(second_half, price_at_block, history_trades=first_half)

    combined = scores1 + scores2
    assert len(combined) == len(scores_full)

    for a, b in zip(combined, scores_full):
        assert a.window_end_block == b.window_end_block
        assert a.signal == b.signal
        assert abs(a.composite_score - b.composite_score) < 1e-9
        assert a.total_wallets_tracked == b.total_wallets_tracked
        assert a.good_buyers == b.good_buyers
        assert a.good_sellers == b.good_sellers


def test_export_state_roundtrip_preserves_ema_values():
    trades, price_map = _build_trades()

    def price_at_block(b):
        return price_map.get(b, price_map[max(k for k in price_map if k <= b)])

    engine = ScoringEngine(_cfg())
    engine.run(trades, price_at_block)
    state = engine.export_state()

    assert state["good_short"] is not None
    assert state["prev_signal"] in {"LONG", "SHORT", "HOLD"}

    # Nowy silnik zainicjalizowany tym stanem powinien zaczac EMA dokladnie
    # tam, gdzie poprzedni skonczyl (nie "na zimno" od None).
    resumed = ScoringEngine(
        _cfg(), initial_ema=state, initial_prev_signal=Signal(state["prev_signal"])
    )
    assert resumed._ema_good_short == state["good_short"]
    assert resumed._ema_bad_long == state["bad_long"]
