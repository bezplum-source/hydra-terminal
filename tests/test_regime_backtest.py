"""Testy offline backtestu regime-detection (Faza 5, brief regime-detection
sekcje 20-22) — `hydra_signals/regime_backtest.py`. Świadomie OSOBNY plik od
`tests/test_regime.py` (ten testuje `regime.py`, ten plik testuje warstwę
"replay + event study" zbudowaną NA WIERZCHU `regime.py`).
"""

from __future__ import annotations

from hydra_signals import regime_backtest as rb
from hydra_signals.regime import RegimeConfig


def make_candle(block, price, good=0.0, bad=0.0, div=0.0, breadth=0.5, **extra):
    candle = {
        "block": block,
        "price": price,
        "goodPressure": good,
        "badPressure": bad,
        "divergence": div,
        "breadth": breadth,
    }
    candle.update(extra)
    return candle


def _bull_candle(block, price):
    # Ekstremalne wejscie bull (jak w test_regime.py) - z 3 takich pod rzad
    # (min_confirmation_periods domyslnie=3) RegimeEngine wejdzie w BULL.
    return make_candle(block, price, good=1.0, bad=-1.0, div=2.0, breadth=1.0)


def _neutral_candle(block, price):
    return make_candle(block, price, good=0.0, bad=0.0, div=0.0, breadth=0.5)


# ---------------------------------------------------------------------------
# replay_candles - odpornosc na look-ahead bias (sekcja 21 briefu)
# ---------------------------------------------------------------------------


def test_replay_never_uses_future_candles():
    # Dwie listy swiec dzielace ten sam PREFIKS (5 pierwszych swiec
    # identyczne), ale druga ma dodatkowe, EKSTREMALNIE inne swiece na
    # koncu. Jesli replay poprawnie nie zaglada w przyszlosc, wyniki dla
    # pierwszych 5 pozycji MUSZA byc identyczne w obu przebiegach.
    shared_prefix = [_neutral_candle(i * 250, 100.0 + i) for i in range(5)]
    short_run = list(shared_prefix)
    long_run = list(shared_prefix) + [_bull_candle((5 + i) * 250, 500.0 + i * 50) for i in range(10)]

    replayed_short = rb.replay_candles(short_run)
    replayed_long = rb.replay_candles(long_run)

    for i in range(5):
        assert replayed_short[i].regime_fields == replayed_long[i].regime_fields, (
            f"Swieca #{i}: pole regime zmienilo sie po dopisaniu PRZYSZLYCH swiec - look-ahead bias!"
        )


def test_replay_creates_a_fresh_engine_each_call():
    # Dwa niezalezne wywolania replay_candles na tych samych danych musza
    # dac identyczny wynik - stan silnika NIE moze "przeciekac" miedzy
    # wywolaniami (np. przez przypadkowo dzielony obiekt konfiguracji).
    candles = [_bull_candle(i * 250, 100.0 + i * 10) for i in range(5)]
    first = rb.replay_candles(candles)
    second = rb.replay_candles(candles)
    assert [r.regime_fields for r in first] == [r.regime_fields for r in second]


# ---------------------------------------------------------------------------
# split_chronologically
# ---------------------------------------------------------------------------


def test_split_chronologically_covers_everything_without_overlap():
    candles = [make_candle(i * 250, 100.0 + i) for i in range(100)]
    split = rb.split_chronologically(candles, train_frac=0.6, validation_frac=0.2)

    assert len(split.train) == 60
    assert len(split.validation) == 20
    assert len(split.out_of_sample) == 20
    assert split.train + split.validation + split.out_of_sample == candles


def test_split_chronologically_keeps_time_order_between_splits():
    candles = [make_candle(i * 250, 100.0 + i) for i in range(30)]
    split = rb.split_chronologically(candles, train_frac=0.5, validation_frac=0.3)

    assert split.train[-1]["block"] < split.validation[0]["block"]
    assert split.validation[-1]["block"] < split.out_of_sample[0]["block"]


def test_split_chronologically_rejects_invalid_fractions():
    candles = [make_candle(i * 250, 100.0) for i in range(10)]
    try:
        rb.split_chronologically(candles, train_frac=0.8, validation_frac=0.3)
        assert False, "powinien podniesc ValueError (suma frakcji >= 1)"
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# evaluate_events / _evaluate_thresholds - pomiar progow zwrotu
# ---------------------------------------------------------------------------


def test_upward_move_after_bull_event_is_detected_as_up_hit():
    # 3 swiece bull (wejscie w BULL na 3-ciej -> event na indeksie 2), potem
    # cena rosnie o +12% wzgledem ceny w momencie eventu - powinno trafic
    # progi 5% i 10%, ale NIE 20%/30%.
    candles = [_bull_candle(i * 250, 100.0) for i in range(3)]
    candles.append(_neutral_candle(3 * 250, 112.0))
    replayed = rb.replay_candles(candles)

    event_idx = 2
    assert replayed[event_idx].regime_fields["regimeEvent"] == "START_BULL_MARKET"

    outcomes = rb._evaluate_thresholds(
        replayed, event_idx, thresholds=(0.05, 0.10, 0.20, 0.30), max_lookforward_windows=None
    )
    by_threshold = {o.threshold: o for o in outcomes}
    assert by_threshold[0.05].hit_direction == "up"
    assert by_threshold[0.10].hit_direction == "up"
    assert by_threshold[0.20].hit_direction is None
    assert by_threshold[0.30].hit_direction is None


def test_downward_move_is_detected_as_down_hit():
    candles = [_bull_candle(i * 250, 100.0) for i in range(3)]
    candles.append(_neutral_candle(3 * 250, 92.0))  # -8%
    replayed = rb.replay_candles(candles)

    outcomes = rb._evaluate_thresholds(replayed, 2, thresholds=(0.05, 0.10), max_lookforward_windows=None)
    by_threshold = {o.threshold: o for o in outcomes}
    assert by_threshold[0.05].hit_direction == "down"
    assert by_threshold[0.10].hit_direction is None


def test_max_lookforward_windows_limits_the_search_horizon():
    candles = [_bull_candle(i * 250, 100.0) for i in range(3)]
    # +10% dopiero na 3. swiecy PO evencie - poza horyzontem max_lookforward_windows=2
    candles.append(_neutral_candle(3 * 250, 100.0))
    candles.append(_neutral_candle(4 * 250, 100.0))
    candles.append(_neutral_candle(5 * 250, 110.0))
    replayed = rb.replay_candles(candles)

    outcomes_limited = rb._evaluate_thresholds(
        replayed, 2, thresholds=(0.10,), max_lookforward_windows=2
    )
    assert outcomes_limited[0].hit_direction is None

    outcomes_unlimited = rb._evaluate_thresholds(
        replayed, 2, thresholds=(0.10,), max_lookforward_windows=None
    )
    assert outcomes_unlimited[0].hit_direction == "up"


def test_events_too_close_to_end_are_skipped_not_counted_as_no_hit():
    # Event na PRZEDOSTATNIEJ swiecy (tylko 1 swieca "do przodu" dostepna) -
    # przy min_lookforward_windows_required=5 powinien zostac POMINIETY
    # calkowicie, a nie policzony jako "brak trafienia".
    candles = [_bull_candle(i * 250, 100.0) for i in range(3)]
    candles.append(_neutral_candle(3 * 250, 100.0))  # tylko 1 swieca po evencie
    replayed = rb.replay_candles(candles)

    outcomes = rb.evaluate_events(
        replayed, thresholds=(0.10,), min_lookforward_windows_required=5
    )
    assert outcomes == []


def test_regime_event_and_special_event_on_the_same_candle_count_as_two_events():
    # Skonstruowana swieca, ktora jednoczesnie konczy warunki START_BULL_MARKET
    # (po 3 potwierdzeniach) ORAZ spelnia prog CAPITULATION.
    candles = [_bull_candle(i * 250, 100.0) for i in range(3)]
    replayed = rb.replay_candles(candles)

    last = replayed[-1].regime_fields
    assert last["regimeEvent"] == "START_BULL_MARKET"
    assert last["specialEvent"] == "CAPITULATION"  # good=1.0/bad=-1.0 przekracza domyslne progi 0.4/-0.4

    outcomes = rb.evaluate_events(replayed, thresholds=(0.10,))
    events_on_last_candle = [o for o in outcomes if o.block == replayed[-1].block]
    assert {o.event for o in events_on_last_candle} == {"START_BULL_MARKET", "CAPITULATION"}


# ---------------------------------------------------------------------------
# summarize_outcomes / run_regime_backtest
# ---------------------------------------------------------------------------


def test_summarize_outcomes_aggregates_rates_correctly():
    outcomes = [
        rb.EventOutcome(
            event="START_BULL_MARKET", block=1, price_at_event=100.0,
            threshold_outcomes=[rb.ThresholdOutcome(threshold=0.10, hit_direction="up", windows_to_hit=1)],
        ),
        rb.EventOutcome(
            event="START_BULL_MARKET", block=2, price_at_event=100.0,
            threshold_outcomes=[rb.ThresholdOutcome(threshold=0.10, hit_direction="down", windows_to_hit=2)],
        ),
        rb.EventOutcome(
            event="START_BULL_MARKET", block=3, price_at_event=100.0,
            threshold_outcomes=[rb.ThresholdOutcome(threshold=0.10, hit_direction=None, windows_to_hit=None)],
        ),
    ]
    summaries = rb.summarize_outcomes(outcomes, thresholds=(0.10,))
    assert len(summaries) == 1
    s = summaries[0]
    assert s.event == "START_BULL_MARKET"
    assert s.n_occurrences == 3
    t = s.by_threshold[0]
    assert (t.n_up, t.n_down, t.n_no_hit) == (1, 1, 1)
    assert t.up_rate == 1 / 3
    assert t.down_rate == 1 / 3


def test_run_regime_backtest_end_to_end_no_events_gives_empty_summary():
    candles = [_neutral_candle(i * 250, 100.0) for i in range(10)]
    report = rb.run_regime_backtest(candles, split_name="test")
    assert report.n_candles == 10
    assert report.event_summaries == []


def test_run_regime_backtest_end_to_end_with_bull_event():
    candles = [_bull_candle(i * 250, 100.0 + i) for i in range(5)]
    report = rb.run_regime_backtest(candles, split_name="test")
    event_names = {s.event for s in report.event_summaries}
    assert "START_BULL_MARKET" in event_names
    assert "CAPITULATION" in event_names


# ---------------------------------------------------------------------------
# grid_search
# ---------------------------------------------------------------------------


def test_grid_search_picks_config_with_highest_up_rate_on_train_only():
    # cfg_strict wymaga wiecej potwierdzen (min_confirmation_periods=10) -
    # przy tylko 5 swiecach NIGDY nie wejdzie w BULL, wiec 0 eventow.
    # cfg_loose (domyslny, min_confirmation_periods=3) wejdzie w BULL i
    # zaobserwuje ruch w gore - powinien zostac wybrany.
    candles = [_bull_candle(i * 250, 100.0 + i * 5) for i in range(6)]
    cfg_strict = RegimeConfig(min_confirmation_periods=10)
    cfg_loose = RegimeConfig(min_confirmation_periods=3)

    best_cfg, best_report = rb.grid_search(
        candles, [cfg_strict, cfg_loose], score_event="START_BULL_MARKET", score_threshold=0.05
    )
    assert best_cfg is cfg_loose
    assert best_report.split_name == "train"


def test_grid_search_raises_when_no_config_produces_the_scored_event():
    candles = [_neutral_candle(i * 250, 100.0) for i in range(5)]
    try:
        rb.grid_search(candles, [RegimeConfig()], score_event="START_BULL_MARKET")
        assert False, "powinien podniesc ValueError - zaden config nie generuje eventu"
    except ValueError:
        pass


def test_grid_search_rejects_empty_config_list():
    try:
        rb.grid_search([make_candle(0, 100.0)], [])
        assert False, "powinien podniesc ValueError dla pustej listy configow"
    except ValueError:
        pass
