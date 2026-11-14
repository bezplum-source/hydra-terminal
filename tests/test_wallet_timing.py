"""Testy etapu 5 planu "trend": portfele z wyczuciem / bez wyczucia i akumulacja
(`live/wallet_timing.py`). Dane w pełni syntetyczne, bez sieci."""

from __future__ import annotations

import numpy as np
import pytest

from hydra_signals.models import Side, Trade
from live import wallet_timing as wt

DAY = 86_400
T0 = 1_790_000_000
B0 = 26_000_000
SPB = 12  # sekund na blok


def _block(ts):
    return B0 + int((ts - T0) / SPB)


def _anchors(days=40):
    return [(_block(T0 + i * 3600), T0 + i * 3600) for i in range(0, days * 24, 6)]


def _closes(days=40, fn=None):
    fn = fn or (lambda h: 2000.0 + 5 * h)  # cena rośnie przez cały okres
    return [[(T0 + h * 3600) * 1000, fn(h)] for h in range(days * 24)]


def _trades(days=20, n_noise=40):
    """`goodN` zawsze kupuje (cena rośnie -> trafione), `badN` zawsze sprzedaje
    (nietrafione), `noiseN` na przemian."""
    out = []
    for d in range(days):
        for k in range(3):
            ts = T0 + d * DAY + k * 3 * 3600
            for i in range(10):
                out.append(Trade(f"good{i}", _block(ts), Side.BUY, 2000.0, 1.0))
                out.append(Trade(f"bad{i}", _block(ts), Side.SELL, 2000.0, 1.0))
            for i in range(n_noise):
                side = Side.BUY if (d + k + i) % 2 else Side.SELL
                out.append(Trade(f"noise{i}", _block(ts), side, 2000.0, 1.0))
    return out


def test_classifies_wallets_by_what_price_did_after_their_trades():
    w = wt.WalletTiming(_trades(), _anchors(), _closes())
    cls = w.classify(T0 + 15 * DAY)
    assert cls is not None
    skilled, unskilled, n = cls
    names = lambda codes: {w.wallet_names[c] for c in codes}  # noqa: E731
    assert {f"good{i}" for i in range(10)} <= names(skilled)
    assert {f"bad{i}" for i in range(10)} <= names(unskilled)
    assert not any(x.startswith("bad") for x in names(skilled))
    assert n == 60


def test_no_classification_before_enough_history_and_no_look_ahead():
    w = wt.WalletTiming(_trades(), _anchors(), _closes())
    assert w.classify(T0 + 5 * DAY) is None  # < 7 dni ocenionych transakcji
    # klasyfikacja na moment X używa tylko transakcji z wynikiem znanym przed X
    cutoff = T0 + 10 * DAY
    used = (w.ts < cutoff - wt.HORIZON_S) & ~np.isnan(w.edge)
    assert used.sum() > 0 and (w.ts[used] < cutoff - wt.HORIZON_S).all()


def test_features_accumulation_signals():
    w = wt.WalletTiming(_trades(), _anchors(), _closes())
    f = w.features_at(T0 + 15 * DAY + 3600)
    # portfele z wyczuciem (good) kupują netto -> LONG; bez wyczucia (bad) sprzedają -> przeciw nim LONG
    assert f["walletSkilledNet7d"] > 0 and f["signalSkilled"] == "LONG"
    assert f["walletUnskilledNet7d"] < 0 and f["signalVsUnskilled"] == "LONG"
    assert f["walletSkilledCount"] >= 10


def test_features_none_when_window_or_data_missing():
    w = wt.WalletTiming(_trades(), _anchors(), _closes())
    assert w.features_at(T0 + 3 * DAY) == wt.empty_features()  # okno 7 dni wychodzi przed bufor
    assert w.features_at(None) == wt.empty_features()
    assert wt.WalletTiming([], _anchors(), _closes()).features_at(T0 + 15 * DAY) == wt.empty_features()
    assert wt.WalletTiming(_trades(), [], _closes()).features_at(T0 + 15 * DAY) == wt.empty_features()
    assert wt.WalletTiming(_trades(), _anchors(), []).features_at(T0 + 15 * DAY) == wt.empty_features()


def test_dust_trades_are_ignored_and_stale_prices_give_no_edge():
    dust = [Trade("dust", _block(T0 + i * 3600), Side.BUY, 2000.0, 0.1) for i in range(100)]  # $200
    w = wt.WalletTiming(_trades() + dust, _anchors(), _closes())
    assert "dust" not in set(w.wallet_names)
    # ceny tylko przez pierwsze 3 dni -> wynik późniejszych transakcji nieznany (NaN)
    w2 = wt.WalletTiming(_trades(), _anchors(), _closes(days=3))
    assert np.isnan(w2.edge[w2.ts > T0 + 4 * DAY]).all()


def test_trade_time_keeps_increasing_after_last_anchor():
    trades = [Trade("x", _block(T0 + 30 * DAY) + k * 100, Side.BUY, 2000.0, 1.0) for k in range(3)]
    w = wt.WalletTiming(trades, _anchors(days=20), _closes())
    assert w.ts[1] - w.ts[0] == pytest.approx(100 * wt.SECONDS_PER_BLOCK)
