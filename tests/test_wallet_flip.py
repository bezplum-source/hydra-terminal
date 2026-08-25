"""Testy Wallet Flip (Faza 3, brief regime-detection sekcja 9) -
`ScoringEngine._wallet_flip_last_side`/`_wallet_flip_streak`, liczone
wewnątrz `ScoringEngine.run()` obok reszty metryk Fazy 0.

Wzorzec cohortowy (historia -> GOOD/BAD przez zyskowny/stratny round-trip)
skopiowany z `test_scoring.py::test_good_bad_pressure_divergence_breadth_...`
- ta sama konfiguracja (`min_trades_for_classification=2`,
`good_pct=bad_pct=0.5`, 2 portfele na kohortę) daje deterministyczny,
łatwy do ręcznego zweryfikowania podział GOOD/BAD.
"""

from __future__ import annotations

from hydra_signals.models import Side, Trade
from hydra_signals.scoring import ScoringConfig, ScoringEngine


def make_trade(wallet, block, side, price, size=20.0):
    # size domyslny podniesiony z 1.0 do 20.0 (Faza "dust filter",
    # ScoringConfig.min_trade_notional_usd=1000.0) - przy najnizszej cenie
    # uzywanej w tym pliku (70.0) daje notional 1400.0, bezpiecznie powyzej
    # progu, zeby transakcje historii/testowe nie zostaly odsiane jako dust.
    return Trade(wallet=wallet, block=block, side=side, price_usd=price, size_eth=size)


def _classification_history():
    """g1/g2 -> GOOD (round-trip z zyskiem), b1/b2 -> BAD (round-trip ze
    stratą) - identycznie jak w `test_scoring.py`."""
    return [
        make_trade("g1", 1, Side.BUY, 100.0),
        make_trade("g1", 2, Side.SELL, 200.0),  # zysk -> GOOD
        make_trade("g2", 1, Side.BUY, 100.0),
        make_trade("g2", 2, Side.SELL, 190.0),  # zysk -> GOOD
        make_trade("b1", 1, Side.BUY, 100.0),
        make_trade("b1", 2, Side.SELL, 75.0),  # strata -> BAD
        make_trade("b2", 1, Side.BUY, 100.0),
        make_trade("b2", 2, Side.SELL, 70.0),  # strata -> BAD
    ]


def _cfg(**overrides):
    defaults = dict(
        window_blocks=1000,
        classification_lookback_blocks=100_000,
        min_trades_for_classification=2,
        good_pct=0.5,
        bad_pct=0.5,
    )
    defaults.update(overrides)
    return ScoringConfig(**defaults)


def test_bullish_flip_detected_after_sufficiently_long_sell_streak():
    # g1 (GOOD): SELL SELL SELL (streak 3, >= domyslny prog 3) -> BUY.
    # Ostatnia transakcja (BUY) powinna zostac policzona jako jeden
    # potwierdzony "bullish flip" w kohorcie GOOD.
    window_trades = [
        make_trade("g1", 150, Side.SELL, 150.0),
        make_trade("g1", 151, Side.SELL, 151.0),
        make_trade("g1", 152, Side.SELL, 152.0),
        make_trade("g1", 153, Side.BUY, 153.0),
    ]
    engine = ScoringEngine(_cfg())
    scores = engine.run(window_trades, lambda b: 150.0, history_trades=_classification_history())

    assert len(scores) == 1
    s = scores[0]
    assert s.good_trader_bullish_flips == 1
    assert s.good_trader_bearish_flips == 0
    assert s.bad_trader_bullish_flips == 0
    assert s.bad_trader_bearish_flips == 0


def test_streak_shorter_than_threshold_is_not_a_confirmed_flip():
    # Tylko 2 SELL z rzedu (< domyslny prog 3) przed zmiana kierunku -
    # NIE powinno zostac policzone jako flip, mimo ze kierunek sie zmienil.
    window_trades = [
        make_trade("g1", 150, Side.SELL, 150.0),
        make_trade("g1", 151, Side.SELL, 151.0),
        make_trade("g1", 152, Side.BUY, 152.0),
    ]
    engine = ScoringEngine(_cfg())
    scores = engine.run(window_trades, lambda b: 150.0, history_trades=_classification_history())

    assert scores[0].good_trader_bullish_flips == 0


def test_bearish_flip_is_symmetric():
    # g1 (GOOD): BUY BUY BUY (streak 3) -> SELL => "bearish flip".
    window_trades = [
        make_trade("g1", 150, Side.BUY, 150.0),
        make_trade("g1", 151, Side.BUY, 151.0),
        make_trade("g1", 152, Side.BUY, 152.0),
        make_trade("g1", 153, Side.SELL, 153.0),
    ]
    engine = ScoringEngine(_cfg())
    scores = engine.run(window_trades, lambda b: 150.0, history_trades=_classification_history())

    s = scores[0]
    assert s.good_trader_bearish_flips == 1
    assert s.good_trader_bullish_flips == 0


def test_bad_trader_flips_are_counted_separately_from_good():
    # b1 (BAD): SELL SELL SELL -> BUY => bullish flip, ale w KOHORCIE BAD,
    # nie GOOD - te dwie liczby nie moga sie mieszac.
    window_trades = [
        make_trade("b1", 150, Side.SELL, 150.0),
        make_trade("b1", 151, Side.SELL, 151.0),
        make_trade("b1", 152, Side.SELL, 152.0),
        make_trade("b1", 153, Side.BUY, 153.0),
    ]
    engine = ScoringEngine(_cfg())
    scores = engine.run(window_trades, lambda b: 150.0, history_trades=_classification_history())

    s = scores[0]
    assert s.bad_trader_bullish_flips == 1
    assert s.good_trader_bullish_flips == 0
    assert s.good_trader_bearish_flips == 0
    assert s.bad_trader_bearish_flips == 0


def test_wallets_with_no_cohort_never_count_as_a_flip():
    # Prog klasyfikacji ustawiony celowo niemozliwy do spelnienia (jak w
    # `test_scoring.py::test_all_neutral_wallets_no_crash_and_hold_signal`)
    # - NIKT (ani "n1", ani portfele z historii) nie zostaje sklasyfikowany
    # jako GOOD/BAD, wiec identyczny wzorzec SELL x3 -> BUY, ktory w innych
    # testach jest potwierdzonym flipem, tutaj NIE powinien zostac
    # zaliczony do zadnej z czterech liczb - flip liczy sie WYLACZNIE dla
    # portfeli nalezacych w danym oknie do kohorty GOOD lub BAD.
    window_trades = [
        make_trade("n1", 150, Side.SELL, 150.0),
        make_trade("n1", 151, Side.SELL, 151.0),
        make_trade("n1", 152, Side.SELL, 152.0),
        make_trade("n1", 153, Side.BUY, 153.0),
    ]
    cfg = _cfg(min_trades_for_classification=100)  # celowo niemozliwy prog
    engine = ScoringEngine(cfg)
    scores = engine.run(window_trades, lambda b: 150.0, history_trades=_classification_history())

    s = scores[0]
    assert s.good_trader_bullish_flips == 0
    assert s.bad_trader_bullish_flips == 0
    assert s.good_trader_bearish_flips == 0
    assert s.bad_trader_bearish_flips == 0


def test_a_wallets_very_first_trade_ever_seen_never_counts_as_a_flip():
    # Silnik startuje "na zimno" (bez initial_wallet_flip_state) - nawet
    # jesli portfel ma juz historie transakcji w `history_trades` (uzywana
    # WYLACZNIE do klasyfikacji GOOD/BAD, patrz docstring `run()`), jego
    # PIERWSZA transakcja widziana przez `run()` nie ma z czym porownac
    # kierunku, wiec nigdy nie moze byc flipem - nawet gdy nastepuje "po"
    # transakcjach z historii klasyfikacyjnej.
    window_trades = [make_trade("g1", 150, Side.BUY, 150.0)]
    engine = ScoringEngine(_cfg())
    scores = engine.run(window_trades, lambda b: 150.0, history_trades=_classification_history())

    s = scores[0]
    assert s.good_trader_bullish_flips == 0
    assert s.good_trader_bearish_flips == 0


def test_wallet_flip_state_round_trips_across_two_separate_engine_runs():
    # Ten sam wzorzec testowy co `test_resume.py` dla ScoringEngine: streak
    # zbudowany w PIERWSZYM uruchomieniu procesu (3x SELL) musi "przezyc"
    # do DRUGIEGO, oddzielnego uruchomienia (nowy ScoringEngine, wznowiony
    # przez `initial_wallet_flip_state=...export_wallet_flip_state()`), zeby
    # pojedyncza transakcja BUY w drugim uruchomieniu zostala poprawnie
    # rozpoznana jako potwierdzony bullish flip - DOKLADNIE tak samo, jakby
    # wszystkie 4 transakcje przeszly przez JEDEN ciagly przebieg.
    history = _classification_history()
    part1 = [
        make_trade("g1", 150, Side.SELL, 150.0),
        make_trade("g1", 151, Side.SELL, 151.0),
        make_trade("g1", 152, Side.SELL, 152.0),
    ]
    part2 = [make_trade("g1", 1150, Side.BUY, 153.0)]

    engine_full = ScoringEngine(_cfg())
    scores_full = engine_full.run(part1 + part2, lambda b: 150.0, history_trades=history)

    engine1 = ScoringEngine(_cfg())
    scores1 = engine1.run(part1, lambda b: 150.0, history_trades=history)
    flip_state = engine1.export_wallet_flip_state()
    assert flip_state["g1"] == {"side": "SELL", "streak": 3}

    engine2 = ScoringEngine(_cfg(), initial_wallet_flip_state=flip_state)
    scores2 = engine2.run(part2, lambda b: 150.0, history_trades=history + part1)

    # Bez wznowienia stanu (silnik "na zimno") ta sama transakcja NIE
    # zostalaby policzona jako flip - test sam w sobie jest wiec sensowny
    # tylko jesli faktycznie wymusza uzycie wznowionego stanu.
    cold_engine = ScoringEngine(_cfg())
    cold_scores = cold_engine.run(part2, lambda b: 150.0, history_trades=history + part1)
    assert cold_scores[-1].good_trader_bullish_flips == 0

    assert scores2[-1].good_trader_bullish_flips == 1
    assert scores2[-1].good_trader_bullish_flips == scores_full[-1].good_trader_bullish_flips


def test_export_wallet_flip_state_shape():
    window_trades = [
        make_trade("g1", 150, Side.SELL, 150.0),
        make_trade("g1", 151, Side.SELL, 151.0),
    ]
    engine = ScoringEngine(_cfg())
    engine.run(window_trades, lambda b: 150.0, history_trades=_classification_history())
    state = engine.export_wallet_flip_state()
    assert state["g1"] == {"side": "SELL", "streak": 2}
