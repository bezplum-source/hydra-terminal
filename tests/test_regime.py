

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
