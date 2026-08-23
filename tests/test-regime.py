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
