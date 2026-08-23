from hydra_signals.models import Side, Trade, Signal
from hydra_signals.scoring import ScoringConfig, ScoringEngine


def make_trade(wallet, block, side, price, size):
    return Trade(wallet=wallet, block=block, side=side, price_usd=price, size_eth=size)


def test_no_trades_returns_empty():
    engine = ScoringEngine(ScoringConfig())
    assert engine.run([], lambda b: 2000.0) == []


def test_all_neutral_wallets_no_crash_and_hold_signal():
    # Za malo transakcji per portfel, zeby cokolwiek sklasyfikowac ->
    # wszyscy zostaja UNRATED, pool powinien byc pusty, silnik nie powinien
    # sie wywalic, sygnal domyslnie HOLD (brak wczesniejszego sygnalu).
    trades = [
        make_trade(f"w{i}", block, Side.BUY, 2000.0, 1.0)
        for i in range(3)
        for block in [0, 250, 500]
    ]
    cfg = ScoringConfig(min_trades_for_classification=100)  # celowo niemozliwy prog
    engine = ScoringEngine(cfg)
    scores = engine.run(trades, lambda b: 2000.0)
    assert len(scores) > 0
    for s in scores:
        assert s.pool_size == 0
        assert s.signal == Signal.HOLD


def test_only_buys_pushes_toward_long_when_good_cohort_exists():
    trades = []
    # Zbuduj historie, w ktorej "dobrzy" (zyskowni) systematycznie kupuja
    # tuz przed wzrostem ceny, zeby zostali sklasyfikowani jako GOOD,
    # a potem w oknie testowym wszyscy kupuja -> oczekujemy sygnalu LONG.
    price_map = {}
    block = 0
    price = 100.0
    for cycle in range(20):
        for wallet_id in range(5):
            trades.append(make_trade(f"good{wallet_id}", block, Side.BUY, price, 1.0))
        price_map[block] = price
        block += 1
        price *= 1.05  # cena rosnie po kazdym zakupie -> "dobrzy" maja racje
        price_map[block] = price
        for wallet_id in range(5):
            trades.append(make_trade(f"good{wallet_id}", block, Side.SELL, price, 1.0))
        block += 5

    def price_at_block(b):
        return price_map.get(b, price)

    cfg = ScoringConfig(
        window_blocks=25,
        classification_lookback_blocks=10_000,
        min_trades_for_classification=5,
        good_pct=0.5,
        bad_pct=0.0,
        ema_short_span=2,
        ema_long_span=4,
    )
    engine = ScoringEngine(cfg)
    scores = engine.run(trades, price_at_block)
    assert len(scores) > 0
    # W co najmniej czesci okien powinnismy zobaczyc wyrazna przewage
    # kupujacych w kohortcie "dobrych" (ind_good_short > 0.5), a niektore
    # z tych okien powinny skutkowac sygnalem LONG - bo skoro nie ma kohorty
    # "zlych", composite score jest wtedy jednoznacznie dodatni.
    assert any(s.ind_good_short > 0.5 for s in scores)
    assert any(s.signal == Signal.LONG for s in scores)
