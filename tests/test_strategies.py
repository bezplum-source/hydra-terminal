"""Testy Etapu 1 planu "trend": sygnał trendu 14 dni i "wyścig strategii"
(gra na papierze) - `hydra_signals/strategies.py`."""

from __future__ import annotations

import pytest

from hydra_signals import strategies as s

DAY = s.DAY_S
H = 3600
T0 = 1_790_000_000


# ---------- trend ----------


def test_trend_is_hold_until_enough_history():
    w = s.TrendWindow()
    for i in range(24 * 12):  # 12 dni < 13 dni wymaganego pokrycia
        w.add(T0 + i * H, 2000.0)
    assert w.signal_at(T0 + (24 * 12 - 1) * H, 2000.0) == ("HOLD", None, None)


def test_trend_long_short_and_distance():
    w = s.TrendWindow()
    n = 24 * 20
    for i in range(n - 1):
        w.add(T0 + i * H, 2000.0)
    last_ts = T0 + (n - 1) * H
    w.add(last_ts, 2200.0)
    sig, sma, dist = w.signal_at(last_ts, 2200.0)
    # średnia z ostatnich 14 dni (336 godzin + bieżąca świeca), w tym jedna cena 2200
    k = 24 * 14  # świece z ts > last_ts - 14 dni: 336 (wliczając bieżącą)
    expected_sma = (2000.0 * (k - 1) + 2200.0) / k
    assert sig == "LONG"
    assert sma == pytest.approx(expected_sma)
    assert dist == pytest.approx((2200.0 / expected_sma - 1) * 100)

    w2 = s.TrendWindow()
    for i in range(n - 1):
        w2.add(T0 + i * H, 2000.0)
    w2.add(last_ts, 1800.0)
    assert w2.signal_at(last_ts, 1800.0)[0] == "SHORT"


def test_trend_window_ignores_prices_older_than_14_days():
    w = s.TrendWindow()
    # 10 dni bardzo wysokiej ceny, potem 14 dni niskiej - stara cena nie może wpływać
    for i in range(24 * 10):
        w.add(T0 + i * H, 9000.0)
    base = T0 + 24 * 10 * H
    for i in range(24 * 14 + 1):
        w.add(base + i * H, 2000.0)
    last = base + 24 * 14 * H
    sig, sma, _ = w.signal_at(last, 2000.0)
    assert sma == pytest.approx(2000.0)
    assert sig == "HOLD"  # cena dokładnie równa średniej


def test_trend_window_ignores_out_of_order_timestamps():
    w = s.TrendWindow()
    w.add(T0, 1.0)
    w.add(T0 - 10, 999.0)
    assert w.ts == [T0] and w.cum == [0.0, 1.0]


def test_trend_series_inherits_signal_for_candles_without_ts():
    candles = [{"ts": T0 + i * H, "price": 2000.0} for i in range(24 * 14)]
    candles.append({"ts": T0 + 24 * 14 * H, "price": 2200.0})
    candles.append({"ts": None, "price": 1900.0})  # brak czasu - nie przesuwa średniej
    out = s.trend_series(candles)
    assert out[0] == "HOLD"
    assert out[-2] == "LONG" and out[-1] == "LONG"


# ---------- gra na papierze ----------


def _step(book, sig, price, block):
    s.strategy_step(book, signal=sig, price=price, time=f"t{block}", block=block, ts=T0 + block)


def test_strategy_step_equity_math_with_fees_and_trade_log():
    f = s.FEE_PER_SIDE
    b = s.new_strategy_book()
    _step(b, "LONG", 100.0, 1)
    assert b["equity"] == pytest.approx(1 - f) and b["position"] == "LONG"
    _step(b, "LONG", 110.0, 2)
    assert b["equity"] == pytest.approx((1 - f) * 1.1)
    _step(b, "SHORT", 110.0, 3)  # zamknięcie LONG i otwarcie SHORT
    assert b["trades"][0]["side"] == "LONG"
    assert b["trades"][0]["pct"] == pytest.approx(round((1.1 * (1 - f) ** 2 - 1) * 100, 2))
    assert b["equity"] == pytest.approx((1 - f) * 1.1 * (1 - f) * (1 - f))
    _step(b, "SHORT", 99.0, 4)  # SHORT zarabia na spadku o 10%
    assert b["equity"] == pytest.approx((1 - f) ** 3 * 1.1 * 1.1)
    _step(b, "HOLD", 99.0, 5)
    assert b["position"] == "HOLD" and len(b["trades"]) == 2 and b["trades"][1]["pct"] > 0
    eq = b["equity"]
    _step(b, "HOLD", 50.0, 6)  # bez pozycji cena nie zmienia kapitału
    assert b["equity"] == eq


def test_drawdown_and_summary():
    b = s.new_strategy_book()
    _step(b, "LONG", 100.0, 1)
    _step(b, "LONG", 120.0, 2)
    _step(b, "LONG", 90.0, 3)
    summ = s.summarize_book(b)
    assert summ["position"] == "LONG" and summ["trades"] == 0
    assert summ["openPct"] == pytest.approx(-10.0)
    assert summ["maxDrawdownPct"] == pytest.approx(-25.0)
    assert summ["resultPct"] == pytest.approx(round(((1 - s.FEE_PER_SIDE) * 0.9 - 1) * 100, 2))


def test_race_step_skips_candles_already_seen_and_replay_matches_steps():
    race = s.new_race(["a", "b"], start_block=1, start_time="t", start_ts=None, start_price=100.0)
    c1 = {"block": 1, "price": 100.0, "time": "t1", "ts": T0}
    c2 = {"block": 2, "price": 105.0, "time": "t2", "ts": T0 + 1}
    s.race_step(race, c1, {"a": "LONG", "b": "SHORT"})
    s.race_step(race, c2, {"a": "LONG", "b": "SHORT"})
    eq = race["strategies"]["a"]["equity"]
    s.race_step(race, c2, {"a": "SHORT", "b": "SHORT"})  # ten sam blok drugi raz - ignorowany
    assert race["strategies"]["a"]["equity"] == eq and race["strategies"]["a"]["position"] == "LONG"
    rep = s.replay_race([c1, c2], {"a": ["LONG", "LONG"], "b": ["SHORT", "SHORT"]})
    assert rep["strategies"]["a"]["equity"] == pytest.approx(eq)
    assert rep["strategies"]["b"]["equity"] < 1.0


def test_unknown_signal_is_treated_as_hold():
    b = s.new_strategy_book()
    _step(b, "???", 100.0, 1)
    assert b["position"] == "HOLD" and b["equity"] == 1.0


def test_downsample_keeps_last_point_and_limit():
    curve = [[i, i, float(i)] for i in range(1000)]
    out = s.downsample_curve(curve, 100)
    assert len(out) <= 101 and out[-1] == curve[-1] and out[0] == curve[0]
    assert s.downsample_curve(curve[:5], 100) == curve[:5]


# ---------- ochrona przed błędnymi cenami ----------


def test_price_guard_rejects_single_spike_and_accepts_confirmed_new_level():
    g = s.PriceGuard()
    assert g.check(2300.0)
    assert not g.check(2986.88)  # pojedynczy skok o ~30% (jak 29.10.2026)
    assert g.check(2239.0)  # powrót do normalnego poziomu
    assert not g.check(3000.0)  # duży skok...
    assert g.check(3010.0)  # ...potwierdzony drugą świecą -> nowy poziom przyjęty
    assert g.check(3005.0)
    assert not g.check(None) and not g.check(0)


def test_price_flags_and_replay_ignore_glitch_candle():
    prices = [2300.0, 2310.0, 2986.88, 2290.0, 2280.0]
    candles = [{"block": i, "price": p, "time": f"t{i}", "ts": T0 + i * H} for i, p in enumerate(prices)]
    assert s.price_flags(candles) == [True, True, False, True, True]
    rep = s.replay_race(candles, {"short": ["SHORT"] * 5})
    book = rep["strategies"]["short"]
    # bez ochrony SHORT „straciłby” ~29% na błędnej świecy; z ochroną wynik to zwykły ruch 2300 -> 2280
    assert book["maxDrawdownPct"] > -1.0
    expected = (1 - s.FEE_PER_SIDE)
    for a, b in ((2300.0, 2310.0), (2310.0, 2290.0), (2290.0, 2280.0)):  # świeca 2986.88 pominięta
        expected *= 1 - (b / a - 1)
    assert book["equity"] == pytest.approx(expected)


def test_trend_series_ignores_glitch_price():
    candles = [{"ts": T0 + i * H, "price": 2000.0 + i * 0.01} for i in range(24 * 14)]
    candles.append({"ts": T0 + 24 * 14 * H, "price": 2600.0})  # błąd odczytu
    candles.append({"ts": T0 + (24 * 14 + 1) * H, "price": 1990.0})
    out = s.trend_series(candles)
    assert out[-2] == out[-3]  # błędna świeca dziedziczy poprzedni sygnał zamiast skakać na LONG
    assert out[-1] == "SHORT"
