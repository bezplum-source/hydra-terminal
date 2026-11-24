"""Statystyki rynku z wielu okresów i rozliczanie prognoz (live/market_analysis.py)."""

import json

import pytest

from live import market_analysis as ma
from live import report_card as rc

H = 3600
D = 86400
NOW = 1_795_000_000.0 // D * D + 12 * H


def _hourly(start_price, step, days=120):
    n = days * 24
    return [[(NOW - (n - i) * H) * 1000, start_price + step * i] for i in range(n + 1)]


def test_rsi_extremes_and_known_value():
    assert ma.rsi(list(range(1, 30))) == 100.0
    assert ma.rsi(list(range(30, 1, -1))) == pytest.approx(0.0)
    assert ma.rsi([1.0] * 10) is None
    # naprzemiennie +1 / -1 -> równowaga
    alt = [100 + (i % 2) for i in range(40)]
    assert 40 < ma.rsi(alt) < 60


def test_realized_vol_zero_for_constant_and_positive_for_moves():
    assert ma.realized_vol([100.0] * 50) == 0.0
    assert ma.realized_vol([100.0, 101.0, 99.0, 102.0, 98.0]) > 50
    assert ma.realized_vol([1.0]) is None


def test_price_block_windows_and_smas_on_falling_market():
    s = ma.Series(_hourly(3000.0, -0.25))  # stały spadek
    b = ma.price_block(s, NOW, NOW - 60 * D)
    ch = b["change_pct"]
    assert ch["24h"] < 0 and ch["30d"] < ch["7d"] < ch["24h"] < 0
    assert ch["od_poczatku_hydry"] < 0
    assert b["dist_sma14d_pct"] < 0 and b["dist_sma50d_pct"] < b["dist_sma14d_pct"]
    assert b["sma14d_slope_7d_pct"] < 0 and b["rsi14d"] < 5
    assert b["range30d"]["position_0_1"] == 0.0 and b["down_days_of_last_7"] == 7


def _raw():
    eth = _hourly(3000.0, -0.25)
    btc = _hourly(60000.0, -2.0)
    candles = [{"ts": NOW - (200 - i) * H, "price": 2000.0, "signal": "SHORT", "voteShort": 10, "voteLong": 1,
                "voteHold": 3, "trendDistPct": -7.0, "marketMode": "TREND", "takerImbalance24h": -0.1,
                "walletSkilledNet7d": -500.0, "forcedClose": "LONGS" if i == 199 else None,
                "oiPriceState": "NEW_SHORTS"} for i in range(200)]
    return {
        "market": {"coins": {"ETH": eth, "BTC": btc}},
        "candles": candles,
        "derivs": {"funding": [{"t": (NOW - k * H) * 1000, "rate": -0.00001} for k in range(48)],
                   "snapshots": [{"t": (NOW - k * H) * 1000, "oi": 1000.0 - k} for k in range(24 * 8)]},
        "stables": {"total": [[int(NOW // D * D) - k * D, 1e11 + (40 - k) * 1e8] for k in range(40, -1, -1)]},
        "options": {"dvol": [[(NOW - k * H) * 1000, 60.0 + (5 if k < 24 else 0)] for k in range(24 * 30)],
                    "snapshots": [{"t": (NOW - k * H) * 1000, "skew": 6.0, "pcOi": 0.5} for k in range(10)]},
    }


def test_analysis_stats_sections_and_hints():
    st = ma.analysis_stats(_raw(), NOW)
    assert set(st) >= {"eth", "btc", "eth_vs_btc_change_pct", "signal", "derivs", "stables", "options", "wallets", "hints"}
    assert st["signal"]["votes"]["teraz"] == {"SHORT": 10, "LONG": 1, "HOLD": 3}
    assert st["derivs"]["funding_apr_pct"]["sr_24h"] < 0
    assert st["derivs"]["oi_change_pct"]["24h"] > 0  # OI rósł (starsze migawki mniejsze)
    assert st["derivs"]["forced_closes_7d"]["LONGS"] == 1
    assert st["stables"]["change_7d_pct"] > 0
    assert st["options"]["dvol_now"] == 65.0 and st["options"]["skew_now"] == 6.0
    hints = " ".join(st["hints"])
    assert "spadkowy" in hints and "funding ujemny" in hints and "skew" in hints
    json.dumps(st)  # musi dać się zapisać jako JSON


LOG = """# Dziennik prognoz
| ts | czas | cena | 24 h | 7 d | pewność | sygnał |
|---|---|---|---|---|---|---|
| {t1} | 01.11 19:41 | 2000,00 | spadek | spadek | umiarkowana | SHORT |
| {t2} | 02.11 19:41 | 1990.00 | wzrost | bok | niska | SHORT |
| {t3} | dziś | 1700 | bok | spadek | niska | SHORT |
śmieci | nie wiersz
"""


def test_parse_and_score_forecasts_against_price_and_signal_baseline():
    s = ma.Series(_hourly(5000.0, -1.0))  # ~ -24 $ dziennie (~1%), ~ -7% tygodniowo
    t1, t2, t3 = int(NOW - 10 * D), int(NOW - 9 * D), int(NOW - H)
    rows = ma.parse_forecasts(LOG.format(t1=t1, t2=t2, t3=t3))
    assert [r["24h"] for r in rows] == ["spadek", "wzrost", "bok"] and rows[0]["price"] == 2000.0
    sc = ma.score_forecasts(rows, s, NOW)
    real24 = ma.realized_direction(ma.pct(s.at(t1), s.at(t1 + D)), "24h")
    assert sc["24h"]["rozliczone"] == 2 and sc["oczekujace"] == 1
    exp_hits = (real24 == "spadek") + (ma.realized_direction(ma.pct(s.at(t2), s.at(t2 + D)), "24h") == "wzrost")
    assert sc["24h"]["trafione"] == exp_hits
    assert sc["7d"]["rozliczone"] == 2
    # punkt odniesienia: SHORT = spadek; w stałym spadku 7 dni zawsze trafia
    assert sc["7d"]["punkt_odniesienia_sygnal_trafnosc_pct"] == 100


def test_report_card_cli_writes_stats_and_scores_log(tmp_path, monkeypatch):
    raw = _raw()
    monkeypatch.setattr(rc, "load", lambda repo: {**raw, "race": {}, "manifests": [], "commits": [NOW - 600]})
    log = tmp_path / "log.md"
    log.write_text(LOG.format(t1=int(NOW - 10 * D), t2=int(NOW - 9 * D), t3=int(NOW - H)), encoding="utf-8")
    out = tmp_path / "stats.json"
    assert rc.main(["--now", str(NOW), "--stats-json", str(out), "--forecast-log", str(log), "--no-render"]) == 0
    st = json.loads(out.read_text(encoding="utf-8"))
    assert st["forecast_score"]["24h"]["rozliczone"] == 2 and "race_live" in st


def test_render_with_analysis_panel(tmp_path, monkeypatch):
    pytest.importorskip("matplotlib")
    raw = _raw()
    monkeypatch.setattr(rc, "load", lambda repo: {**raw, "race": {}, "manifests": [], "commits": [NOW - 600]})
    an = tmp_path / "an.json"
    an.write_text(json.dumps({"stage": "bessa", "text": "Zdanie pierwsze. " * 20, "f24": "bok", "f7": "spadek",
                              "conf": "niska", "track": "Trafność: 1/2"}), encoding="utf-8")
    png = tmp_path / "c.png"
    assert rc.main(["--now", str(NOW), "--analysis", str(an), "--out", str(png)]) == 0
    assert png.stat().st_size > 10_000
