"""Testy etapu 4 planu "trend": agresorzy na Hyperliquid, open interest razem
z ceną, przybliżone wymuszone zamknięcia (`live/perp_flow.py`)."""

from __future__ import annotations

import pytest

from hydra_signals.data_sources import hyperliquid_ws as hl_ws
from live import perp_flow as pf

H = 3600
DAY = 86_400
T0 = 1_790_000_000


def _t(ts, side, usd=10_000.0, price=2000.0):
    return hl_ws.HyperliquidTrade(coin="ETH", aggressor_side=side, price_usd=price, size_eth=usd / price,
                                  buyer="b", seller="s", ts_ms=int(ts * 1000), tid=int(ts), tx_hash="0x")


def test_taker_flow_window_imbalance_and_no_look_ahead():
    trades = [_t(T0 + i * H, "B" if i % 4 else "A") for i in range(48)]  # 3/4 agresywnych kupujących
    trades.append(_t(T0 + 47 * H + 1, "A", usd=10_000_000.0))  # transakcja PO świecy - nie może się liczyć
    flow = pf.TakerFlow(trades)
    f = flow.at(T0 + 47 * H)
    assert f["takerBuyUsd24h"] == pytest.approx(18 * 10_000, rel=1e-6)
    assert f["takerSellUsd24h"] == pytest.approx(6 * 10_000, rel=1e-6)
    assert f["takerImbalance24h"] == pytest.approx(0.5) and f["signalTaker"] == "LONG"
    assert flow.at(T0 + 10 * H)["takerImbalance24h"] is None  # okno wychodzi przed początek bufora
    assert flow.at(None)["signalTaker"] is None
    assert pf.TakerFlow([]).at(T0)["signalTaker"] is None


def test_oi_price_state_and_trend_oi_signal():
    s = pf.oi_price_state
    assert s(2.0, 1.0) == "NEW_LONGS" and s(2.0, -1.0) == "SHORT_COVER"
    assert s(-2.0, 1.0) == "NEW_SHORTS" and s(-2.0, -1.0) == "LONG_EXIT"
    assert s(None, 1.0) is None and s(1.0, None) is None
    t = pf.trend_oi_signal
    assert t("SHORT", 0.5) == "SHORT" and t("LONG", 0.5) == "LONG"
    assert t("SHORT", -0.5) == "HOLD"  # spadek bez nowych pieniędzy - poza rynkiem
    assert t("HOLD", 0.5) == "HOLD" and t("LONG", None) is None


def _closes(fn, hours=60):
    return pf.EthCloses([[(T0 + h * H) * 1000, fn(h)] for h in range(hours)])


def test_price_change_and_staleness():
    p = _closes(lambda h: 2000.0 + 10 * h)
    assert p.change_pct(T0 + 30 * H, DAY) == pytest.approx((2300 / 2060 - 1) * 100)
    assert p.at(T0 + 70 * H) is None  # ostatnia cena sprzed > 3 h
    assert p.change_pct(T0 + 5 * H, DAY) is None


def test_forced_close_detects_oi_drop_with_price_move():
    prices = _closes(lambda h: 2000.0 if h < 10 else 1950.0)  # -2,5% w godzinie 10
    snaps = [{"t": (T0 + 9 * H) * 1000, "oi": 100_000.0, "mark": 2000.0},
             {"t": (T0 + 10 * H) * 1000, "oi": 96_000.0, "mark": 1950.0}]  # OI -4%
    assert pf.forced_close(snaps, prices, T0 + 10 * H + 600) == "LONGS"
    snaps_small = [snaps[0], {**snaps[1], "oi": 99_000.0}]  # OI tylko -1%
    assert pf.forced_close(snaps_small, prices, T0 + 10 * H + 600) is None
    assert pf.forced_close(snaps, prices, T0 + 13 * H) is None  # migawka za stara względem świecy
    up = _closes(lambda h: 2000.0 if h < 10 else 2050.0)
    assert pf.forced_close(snaps, up, T0 + 10 * H + 600) == "SHORTS"
    assert pf.forced_close(snaps[:1], prices, T0 + 10 * H) is None


def test_features_at_combines_everything_and_degrades_to_none():
    trades = [_t(T0 + i * H, "A") for i in range(30)]
    f = pf.features_at(pf.TakerFlow(trades), _closes(lambda h: 2000.0 - h), [], T0 + 29 * H, "SHORT", 2.0)
    assert f["signalTaker"] == "SHORT" and f["oiPriceState"] == "NEW_SHORTS" and f["signalTrendOi"] == "SHORT"
    assert f["priceChange24h"] < 0 and f["forcedClose"] is None
    assert pf.features_at(None, None, [], None, None, None) == pf.empty_features()
