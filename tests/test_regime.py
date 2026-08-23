from hydra_signals import regime


def make_candle(block, good=0.0, bad=0.0, div=0.0):
    return {"block": block, "goodPressure": good, "badPressure": bad, "divergence": div}


def test_returns_none_when_history_is_empty():
    result = regime.compute_momentum(
        [], current=make_candle(249, good=0.5), window_blocks=250
    )
    assert result["1h"].good_pressure_momentum is None
    assert result["30d"].divergence_momentum is None


def test_returns_none_only_for_horizons_longer_than_available_history():
    # 3 swiece dostepne (249, 499, 749) -> "1h" (1 okno wstecz) policzalne,
    # "4h" (4 okna wstecz) juz nie.
    history = [
        make_candle(249, good=0.10),
        make_candle(499, good=0.20),
        make_candle(749, good=0.30),
    ]
    current = make_candle(999, good=0.55)
    result = regime.compute_momentum(history, current=current, window_blocks=250)

    assert result["1h"].good_pressure_momentum == round(0.55 - 0.30, 4)
    assert result["4h"].good_pressure_momentum is None
    assert result["30d"].good_pressure_momentum is None


def test_all_three_metrics_computed_together():
    history = [make_candle(249, good=0.10, bad=-0.20, div=0.30)]
    current = make_candle(499, good=0.40, bad=-0.50, div=0.90)
    result = regime.compute_momentum(
        history, current=current, window_blocks=250, horizons={"1h": 1}
    )
    snap = result["1h"]
    assert snap.good_pressure_momentum == round(0.40 - 0.10, 4)
    assert snap.bad_pressure_momentum == round(-0.50 - (-0.20), 4)
    assert snap.divergence_momentum == round(0.90 - 0.30, 4)


def test_lookup_is_by_block_not_list_position_so_gaps_dont_shift_the_reference():
    # Brakuje swiecy dla okna @499 (np. okno bez zadnej transakcji - rzadkie
    # dla plynnej puli, ale mozliwe). Cel "1h wstecz" od bloku 999 to blok
    # 749, ktory istnieje wprost - lookup PO BLOKU musi trafic dokladnie w
    # niego, a nie w cokolwiek wynikajace z pozycji na liscie.
    history = [
        make_candle(249, good=0.10),
        # brak swiecy @499
        make_candle(749, good=0.30),
    ]
    current = make_candle(999, good=0.55)
    result = regime.compute_momentum(history, current=current, window_blocks=250)
    assert result["1h"].good_pressure_momentum == round(0.55 - 0.30, 4)

    # Cel "2h wstecz" (blok 499) trafia w luke - powinien wziac NAJBLIZSZA
    # WCZESNIEJSZA swiece (@249), a nie przeskoczyc do @749 ani zwrocic None.
    result_2h = regime.compute_momentum(
        history, current=current, window_blocks=250, horizons={"2h": 2}
    )
    assert result_2h["2h"].good_pressure_momentum == round(0.55 - 0.10, 4)


def test_momentum_to_json_flattens_with_expected_key_names():
    history = [make_candle(249, good=0.10, bad=0.05, div=0.05)]
    current = make_candle(499, good=0.30, bad=-0.10, div=0.40)
    snapshots = regime.compute_momentum(
        history, current=current, window_blocks=250, horizons={"1h": 1, "4h": 4}
    )
    flat = regime.momentum_to_json(snapshots)
    assert flat["momentum_1h_good"] == round(0.30 - 0.10, 4)
    assert flat["momentum_1h_bad"] == round(-0.10 - 0.05, 4)
    assert flat["momentum_1h_divergence"] == round(0.40 - 0.05, 4)
    assert flat["momentum_4h_good"] is None
    assert flat["momentum_4h_bad"] is None
    assert flat["momentum_4h_divergence"] is None


# ---------------------------------------------------------------------------
# Faza 2: BULL_SCORE / BEAR_SCORE + maszyna stanow
# ---------------------------------------------------------------------------


def test_extreme_bull_inputs_max_out_bull_score_and_zero_bear_score():
    candle = {
        "goodPressure": 1.0,
        "badPressure": -1.0,
        "divergence": 2.0,
        "breadth": 1.0,
        "momentum_1h_good": 1.0,
    }
    score = regime.compute_regime_score(candle)
    assert score.bull_score == 90.0
    assert score.bear_score == 0.0
    assert score.momentum_horizon_used == "1h"


def test_extreme_bear_inputs_max_out_bear_score_and_zero_bull_score():
    candle = {
        "goodPressure": -1.0,
        "badPressure": 1.0,
        "divergence": -2.0,
        "breadth": 0.0,
        "momentum_1h_good": -1.0,
    }
    score = regime.compute_regime_score(candle)
    assert score.bear_score == 90.0
    assert score.bull_score == 0.0


def test_fully_neutral_inputs_split_score_evenly_between_bull_and_bear():
    # Wszystko dokladnie w punkcie neutralnym (0 pressure/divergence, 0.5
    # breadth), brak momentum w ogole (candle bez zadnego pola momentum_*)
    # -> traktowane jako 0.0 (neutralnie), NIE karane ani premiowane.
    candle = {"goodPressure": 0.0, "badPressure": 0.0, "divergence": 0.0, "breadth": 0.5}
    score = regime.compute_regime_score(candle)
    assert score.bull_score == 45.0
    assert score.bear_score == 45.0
    assert score.momentum_horizon_used is None


def test_regime_engine_requires_confirmation_periods_before_entering_bull():
    strong_bull_candle = {
        "goodPressure": 1.0, "badPressure": -1.0, "divergence": 2.0,
        "breadth": 1.0, "momentum_1h_good": 1.0,
    }
    engine = regime.RegimeEngine(regime.RegimeConfig(min_confirmation_periods=3))

    result1 = engine.process_candle(strong_bull_candle)
    assert result1["regime"] == "NEUTRAL"
    assert result1["regimeEvent"] is None

    result2 = engine.process_candle(strong_bull_candle)
    assert result2["regime"] == "NEUTRAL"
    assert result2["regimeEvent"] is None

    # 3. z rzedu spelniajaca warunek -> dopiero teraz przejscie + event
    result3 = engine.process_candle(strong_bull_candle)
    assert result3["regime"] == "BULL"
    assert result3["regimeEvent"] == "START_BULL_MARKET"


def test_a_single_weak_candle_resets_the_confirmation_streak():
    strong_bull_candle = {
        "goodPressure": 1.0, "badPressure": -1.0, "divergence": 2.0,
        "breadth": 1.0, "momentum_1h_good": 1.0,
    }
    neutral_candle = {"goodPressure": 0.0, "badPressure": 0.0, "divergence": 0.0, "breadth": 0.5}
    engine = regime.RegimeEngine(regime.RegimeConfig(min_confirmation_periods=3))

    engine.process_candle(strong_bull_candle)
    engine.process_candle(strong_bull_candle)
    engine.process_candle(neutral_candle)  # przerywa streak
    result = engine.process_candle(strong_bull_candle)
    assert result["regime"] == "NEUTRAL"  # tylko 1 z rzedu po przerwaniu, nie 3


def test_regime_exits_immediately_below_exit_threshold_no_confirmation_needed():
    strong_bull_candle = {
        "goodPressure": 1.0, "badPressure": -1.0, "divergence": 2.0,
        "breadth": 1.0, "momentum_1h_good": 1.0,
    }
    neutral_candle = {"goodPressure": 0.0, "badPressure": 0.0, "divergence": 0.0, "breadth": 0.5}
    cfg = regime.RegimeConfig(min_confirmation_periods=3)
    engine = regime.RegimeEngine(cfg)

    for _ in range(3):
        engine.process_candle(strong_bull_candle)
    assert engine.regime == "BULL"

    # Jeden slaby okres wystarczy, zeby WYJSC z BULL (bez wymogu persystencji)
    result = engine.process_candle(neutral_candle)
    assert result["regime"] == "NEUTRAL"
    assert result["regimeEvent"] == "END_BULL_MARKET"


def test_bear_side_is_symmetric():
    strong_bear_candle = {
        "goodPressure": -1.0, "badPressure": 1.0, "divergence": -2.0,
        "breadth": 0.0, "momentum_1h_good": -1.0,
    }
    cfg = regime.RegimeConfig(min_confirmation_periods=2)
    engine = regime.RegimeEngine(cfg)

    engine.process_candle(strong_bear_candle)
    result = engine.process_candle(strong_bear_candle)
    assert result["regime"] == "BEAR"
    assert result["regimeEvent"] == "START_BEAR_MARKET"


def test_regime_engine_state_round_trips_via_export_state():
    # Ten sam test co dla ScoringEngine w tests/test_resume.py, tylko dla
    # RegimeEngine: podzielenie tej samej sekwencji swiec na dwa oddzielne
    # "uruchomienia procesu" (drugie wznowione ze stanu pierwszego) musi
    # dac IDENTYCZNY wynik co jeden ciagly przebieg.
    candles = [
        {"goodPressure": 1.0, "badPressure": -1.0, "divergence": 2.0, "breadth": 1.0, "momentum_1h_good": 1.0}
        for _ in range(5)
    ]
    cfg = regime.RegimeConfig(min_confirmation_periods=3)

    continuous = regime.RegimeEngine(cfg)
    continuous_results = [continuous.process_candle(c) for c in candles]

    part1_engine = regime.RegimeEngine(cfg)
    part1_results = [part1_engine.process_candle(c) for c in candles[:2]]
    resumed_state = part1_engine.export_state()

    part2_engine = regime.RegimeEngine(cfg, initial_state=resumed_state)
    part2_results = [part2_engine.process_candle(c) for c in candles[2:]]

    assert part1_results + part2_results == continuous_results


# ---------------------------------------------------------------------------
# Faza 4: CAPITULATION / DISTRIBUTION (sekcje 18-19 briefu regime-detection)
# ---------------------------------------------------------------------------


def test_no_special_event_when_pressures_are_mild():
    candle = {"goodPressure": 0.1, "badPressure": -0.1}
    event = regime.detect_special_event(candle)
    assert event.event is None
    assert event.confidence == 0.0


def test_capitulation_detected_when_good_buys_while_bad_sells_hard():
    # good_pressure >= 0.4 (dobrzy kupuja) ORAZ bad_pressure <= -0.4 (zli
    # sprzedaja) jednoczesnie - definicja CAPITULATION z sekcji 18 briefu
    # ("słabe ręce sprzedają w panice, dobrzy absorbują podaż").
    candle = {"goodPressure": 0.6, "badPressure": -0.7}
    event = regime.detect_special_event(candle)
    assert event.event == "CAPITULATION"
    assert event.confidence > 0.0


def test_distribution_is_symmetric_mirror_of_capitulation():
    # good_pressure <= -0.4 (dobrzy sprzedaja) ORAZ bad_pressure >= 0.4 (zli
    # kupuja) - dokladnie odwrotny wzorzec, sekcja 19 briefu.
    candle = {"goodPressure": -0.6, "badPressure": 0.7}
    event = regime.detect_special_event(candle)
    assert event.event == "DISTRIBUTION"
    assert event.confidence > 0.0


def test_only_good_pressure_extreme_without_bad_pressure_is_not_an_event():
    # Warunek CAPITULATION/DISTRIBUTION wymaga OBU stron jednoczesnie -
    # sama skrajna good_pressure, bez odpowiadajacej jej bad_pressure, nie
    # powinna wywolac zadnego eventu.
    candle = {"goodPressure": 0.9, "badPressure": 0.0}
    event = regime.detect_special_event(candle)
    assert event.event is None


def test_extreme_capitulation_inputs_reach_full_confidence_without_flip_bonus():
    candle = {"goodPressure": 1.0, "badPressure": -1.0}
    event = regime.detect_special_event(candle)
    assert event.event == "CAPITULATION"
    assert event.confidence == 100.0


def test_wallet_flip_confirmation_adds_confidence_bonus_to_capitulation():
    # Ten sam graniczny przypadek CAPITULATION, raz bez potwierdzenia przez
    # Wallet Flip (Faza 3), raz z nim (goodBullishFlips > 0) - drugi
    # powinien miec WYZSZA pewnosc, zgodnie z sekcja 19 briefu ("szczegolnie
    # gdy wystepuje zwiekszona aktywnosc dobrych traderow").
    candle_no_flip = {"goodPressure": 0.45, "badPressure": -0.45, "goodBullishFlips": 0}
    candle_with_flip = {"goodPressure": 0.45, "badPressure": -0.45, "goodBullishFlips": 2}

    event_no_flip = regime.detect_special_event(candle_no_flip)
    event_with_flip = regime.detect_special_event(candle_with_flip)

    assert event_no_flip.event == "CAPITULATION"
    assert event_with_flip.event == "CAPITULATION"
    assert event_with_flip.confidence == event_no_flip.confidence + 10.0


def test_wallet_flip_confirmation_uses_bearish_flips_for_distribution_not_bullish():
    # Bonus dla DISTRIBUTION musi patrzec na goodBearishFlips, NIE
    # goodBullishFlips - to inny kierunek zdarzenia, wiec musi byc inne pole.
    candle = {
        "goodPressure": -0.45,
        "badPressure": 0.45,
        "goodBullishFlips": 5,  # zla strona - nie powinna dac bonusu
        "goodBearishFlips": 0,
    }
    event = regime.detect_special_event(candle)
    assert event.event == "DISTRIBUTION"

    candle_with_bearish = dict(candle, goodBearishFlips=1)
    event_with_bonus = regime.detect_special_event(candle_with_bearish)
    assert event_with_bonus.confidence == event.confidence + 10.0


def test_confidence_never_exceeds_100_even_with_flip_bonus_at_the_ceiling():
    candle = {"goodPressure": 1.0, "badPressure": -1.0, "goodBullishFlips": 3}
    event = regime.detect_special_event(candle)
    assert event.event == "CAPITULATION"
    assert event.confidence == 100.0


def test_custom_thresholds_are_respected():
    cfg = regime.RegimeConfig(
        capitulation_good_pressure_threshold=0.8,
        capitulation_bad_pressure_threshold=-0.8,
    )
    mild_candle = {"goodPressure": 0.5, "badPressure": -0.5}
    # Z domyslnym configiem (prog 0.4) to JEST CAPITULATION (0.5 >= 0.4) -
    # ale z tym zaostrzonym configiem (prog 0.8) juz nie, mimo ze wejscie
    # sie nie zmienilo. To wlasnie ma pokazac ten test: sam parametr cfg
    # zmienia wynik, przy identycznej swiecy.
    assert regime.detect_special_event(mild_candle).event == "CAPITULATION"
    assert regime.detect_special_event(mild_candle, cfg).event is None

    strong_candle = {"goodPressure": 0.85, "badPressure": -0.85}
    assert regime.detect_special_event(strong_candle, cfg).event == "CAPITULATION"


def test_regime_engine_process_candle_includes_special_event_fields():
    # `RegimeEngine.process_candle` musi doklejac oba nowe pola do zwracanego
    # slownika, niezaleznie od aktualnego stanu maszyny BULL/BEAR/NEUTRAL.
    engine = regime.RegimeEngine()
    result = engine.process_candle({"goodPressure": 0.6, "badPressure": -0.7})
    assert result["specialEvent"] == "CAPITULATION"
    assert result["specialEventConfidence"] > 0.0


def test_special_event_does_not_affect_bull_bear_state_machine():
    # Brief wprost zabrania automatycznego wiazania CAPITULATION/DISTRIBUTION
    # z maszyna stanow BULL/BEAR (sekcja 19: "Nie traktuj tego automatycznie
    # jako START_BULL"). Skrajny CAPITULATION (good=1.0) NIE ma tu ustawionego
    # momentum/divergence/breadth na wartosci wymagane do wejscia w BULL, wiec
    # regime powinien pozostac NEUTRAL mimo wykrytego special eventu.
    engine = regime.RegimeEngine(regime.RegimeConfig(min_confirmation_periods=3))
    candle = {"goodPressure": 1.0, "badPressure": -1.0}
    for _ in range(5):
        result = engine.process_candle(candle)
    assert result["specialEvent"] == "CAPITULATION"
    assert result["regime"] == "NEUTRAL"
    assert result["regimeEvent"] is None
