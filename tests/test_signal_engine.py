"""Testy jednostkowe `hydra_signals.scoring.SignalEngine`/`SignalConfig`
(Faza "sygnał z histerezą" - zgłoszenie użytkownika 2026-08-31: "zmienia
sygnał co każdy blok... hydra.trading trzyma LONG od 2 tygodni"). Architektura
1:1 skopiowana z `regime.RegimeEngine` - testy tu celowo mirroruja strukture
`tests/test_regime.py` (confirmation periods, reset streaku, natychmiastowe
wyjscie, symetria SHORT, round-trip stanu), zeby latwo bylo porownac
zachowanie obu maszyn stanow."""

from __future__ import annotations

from hydra_signals.models import Signal
from hydra_signals.scoring import SignalConfig, SignalEngine


def test_starts_in_hold_by_default():
    engine = SignalEngine()
    assert engine.signal is Signal.HOLD


def test_requires_confirmation_periods_before_entering_long():
    cfg = SignalConfig(enter_threshold=0.35, exit_threshold=0.1, min_confirmation_periods=3)
    engine = SignalEngine(cfg)

    assert engine.process(0.5) is Signal.HOLD
    assert engine.process(0.5) is Signal.HOLD
    # 3. z rzedu przekraczajaca enter_threshold -> dopiero teraz LONG
    assert engine.process(0.5) is Signal.LONG


def test_a_single_weak_candle_resets_the_confirmation_streak():
    cfg = SignalConfig(enter_threshold=0.35, exit_threshold=0.1, min_confirmation_periods=3)
    engine = SignalEngine(cfg)

    engine.process(0.5)
    engine.process(0.5)
    engine.process(0.0)  # przerywa streak (nie spelnia enter_threshold)
    result = engine.process(0.5)
    assert result is Signal.HOLD  # tylko 1 z rzedu po przerwaniu, nie 3


def test_exits_to_hold_immediately_below_exit_threshold_no_confirmation_needed():
    cfg = SignalConfig(enter_threshold=0.35, exit_threshold=0.1, min_confirmation_periods=3)
    engine = SignalEngine(cfg)

    for _ in range(3):
        engine.process(0.5)
    assert engine.signal is Signal.LONG

    # Jeden odczyt ponizej exit_threshold wystarczy, zeby wyjsc z LONG - bez
    # wymogu kilku swiec z rzedu (asymetria jak w RegimeEngine).
    result = engine.process(0.05)
    assert result is Signal.HOLD


def test_composite_between_exit_and_enter_threshold_holds_position_without_flipping():
    """To jest wlasnie histereza: raz wszedlszy w LONG, silnik NIE wychodzi
    na kazdym drobnym cofnieciu ponizej enter_threshold - dopiero ponizej
    (znacznie nizszego) exit_threshold. Dokladnie to zachowanie mialo
    rozwiazac zgloszenie uzytkownika (migotanie tuz przy granicy)."""
    cfg = SignalConfig(enter_threshold=0.35, exit_threshold=0.1, min_confirmation_periods=3)
    engine = SignalEngine(cfg)
    for _ in range(3):
        engine.process(0.5)
    assert engine.signal is Signal.LONG

    # Wartosc ponizej enter_threshold (0.35), ale WCIAZ powyzej exit_threshold
    # (0.1) - stary, bezstanowy prog (symetryczny 0.2-0.35) migotalby tu do
    # HOLD/SHORT; nowa maszyna stanow zostaje w LONG.
    for composite in (0.2, 0.15, 0.11, 0.3, 0.18):
        assert engine.process(composite) is Signal.LONG


def test_short_side_is_symmetric():
    cfg = SignalConfig(enter_threshold=0.35, exit_threshold=0.1, min_confirmation_periods=2)
    engine = SignalEngine(cfg)

    engine.process(-0.5)
    result = engine.process(-0.5)
    assert result is Signal.SHORT

    # Histereza dziala symetrycznie po stronie SHORT.
    assert engine.process(-0.2) is Signal.SHORT
    assert engine.process(0.11) is Signal.HOLD  # ponad -exit_threshold -> wyjscie


def test_cannot_flip_directly_from_long_to_short_must_pass_through_hold():
    """Architektonicznie niemozliwe (jak BULL<->BEAR w RegimeEngine): nawet
    gdy composite gwaltownie przeskakuje z mocno dodatniego na mocno ujemny w
    JEDNEJ swiecy, silnik najpierw wraca do HOLD - wejscie w SHORT wymaga
    wlasnego, swiezego streaku potwierdzajacego."""
    cfg = SignalConfig(enter_threshold=0.35, exit_threshold=0.1, min_confirmation_periods=2)
    engine = SignalEngine(cfg)
    engine.process(0.5)
    engine.process(0.5)
    assert engine.signal is Signal.LONG

    result = engine.process(-0.9)  # silny odczyt SHORT, ale to dopiero 1. swieca po LONG
    assert result is Signal.HOLD  # nie SHORT wprost


def test_signal_engine_state_round_trips_via_export_state():
    # Ten sam test co dla RegimeEngine w tests/test_regime.py, tylko dla
    # SignalEngine: podzielenie tej samej sekwencji swiec na dwa oddzielne
    # "uruchomienia procesu" (drugie wznowione ze stanu pierwszego) musi dac
    # IDENTYCZNY wynik co jeden ciagly przebieg.
    cfg = SignalConfig(enter_threshold=0.35, exit_threshold=0.1, min_confirmation_periods=3)
    composites = [0.5, 0.5, 0.5, 0.2, 0.05, -0.5, -0.5, -0.5, -0.1]

    continuous = SignalEngine(cfg)
    continuous_results = [continuous.process(c) for c in composites]

    split_point = 4
    run1 = SignalEngine(cfg)
    run1_results = [run1.process(c) for c in composites[:split_point]]
    state = run1.export_state()

    run2 = SignalEngine(cfg, initial_state=state)
    run2_results = [run2.process(c) for c in composites[split_point:]]

    assert run1_results + run2_results == continuous_results


def test_default_config_values_match_documented_empirical_choice():
    # Wartosci startowe (patrz docstring SignalConfig) - wybrane empirycznie
    # na zywej historii 225 swiec, nie wynik backtestu. Ten test istnieje
    # wylacznie po to, zeby przypadkowa zmiana defaultow nie przeszla niezauwazona.
    cfg = SignalConfig()
    assert cfg.enter_threshold == 0.35
    assert cfg.exit_threshold == 0.1
    assert cfg.min_confirmation_periods == 3
