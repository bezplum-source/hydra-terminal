"""Ocena rynku z reguł (live/report_rules.py): czynniki, bilans, prognozy, tekst."""

from live import market_analysis as ma
from live import report_rules as rules


def _stats(**over):
    st = {
        "eth": {"now": 1700.0, "change_pct": {"24h": -1.0, "3d": -2.0, "7d": -6.0, "30d": -25.0, "od_poczatku_hydry": -30.0},
                "dist_sma14d_pct": -8.0, "sma14d_slope_7d_pct": -10.0, "dist_sma50d_pct": -20.0,
                "dist_sma100d_pct": -28.0, "rsi14d": 40.0},
        "signal": {"now": "SHORT", "votes": {"teraz": {"SHORT": 11, "LONG": 1, "HOLD": 2}}, "market_mode": "TREND",
                   "changes_7d": 0},
        "derivs": {"funding_apr_pct": {"sr_24h": 5.0}, "taker_imbalance_24h_now": -0.1},
        "stables": {"change_7d_pct": -1.5},
        "wallets": {"skilled_net7d_eth": -3000.0},
        "options": {"skew_now": 7.0, "dvol_now": 60.0, "dvol_sma14d": 58.0},
    }
    for k, v in over.items():
        st[k] = {**st.get(k, {}), **v}
    return st


def test_clear_downtrend_gives_down_calls_with_confidence():
    a = rules.analyze(_stats())
    assert a["f7"] == "spadek" and a["f24"] == "spadek" and a["score7"] < -0.75
    assert a["conf"] == "wysoka" and a["stage"] == "bessa · trend spadkowy"
    assert "Za spadkiem" in a["text"] and "Obraz zmieniłby powrót nad średnią 14 dni" in a["text"]
    assert "tys. ETH" in a["text"] or any("tys. ETH" in f["note"] for f in a["factors"])


def test_oversold_with_negative_funding_softens_short_term_call():
    a = rules.analyze(_stats(eth={"rsi14d": 22.0, "dist_sma14d_pct": -16.0},
                             derivs={"funding_apr_pct": {"sr_24h": -25.0}}))
    assert a["stage"] == "bessa · wyprzedanie"
    assert a["score24"] > rules.analyze(_stats())["score24"]  # rozciągnięcie i funding ciągną 24 h w górę
    assert a["f7"] == "spadek"  # trend dalej przeważa w 7 dniach


def test_dvol_spike_lowers_confidence_and_mixed_picture_is_flat():
    a = rules.analyze(_stats(options={"dvol_now": 80.0, "dvol_sma14d": 60.0}))
    assert a["conf"] == "umiarkowana"
    mixed = _stats(eth={"dist_sma14d_pct": 2.0, "sma14d_slope_7d_pct": -1.0, "change_pct": {"30d": 1.0},
                        "dist_sma50d_pct": 3.0, "dist_sma100d_pct": -2.0},
                   signal={"votes": {"teraz": {"SHORT": 5, "LONG": 5, "HOLD": 4}}},
                   stables={"change_7d_pct": 0.1}, wallets={"skilled_net7d_eth": 100.0}, options={"skew_now": 1.0})
    assert rules.analyze(mixed)["f7"] == "bok"


def test_missing_data_is_skipped_and_previous_report_is_compared():
    st = {"eth": {"now": 1700.0, "change_pct": {"24h": 0.5}}}
    a = rules.analyze(st)
    assert a["f24"] == "bok" and a["score7"] is None
    prev = {"price": 1800.0, "signal": "LONG", "votes": {"SHORT": 3, "LONG": 9}, "stage": "hossa · korekta"}
    b = rules.analyze(_stats(), "daily", prev)
    assert "Od poprzedniego raportu" in b["text"] and "z LONG na SHORT" in b["text"]


def test_backtest_uses_only_past_data_and_reports_baselines():
    H, D = 3600, 86400
    end = 1_795_000_000.0
    n = 120 * 24
    eth = [[(end - (n - i) * H) * 1000, 3000.0 - i * 0.4] for i in range(n + 1)]
    candles = [{"ts": end - (40 - k) * D, "price": 2000.0, "signal": "SHORT"} for k in range(40)]
    bt = rules.backtest({"market": {"coins": {"ETH": eth, "BTC": eth}}, "candles": candles}, end - 30 * D, end)
    assert bt["days"] == 30 and bt["24h"]["rozliczone"] == 30
    assert bt["7d"]["punkt_odniesienia_sygnal_trafnosc_pct"] is not None and "24h" in bt["flat_pct"]
    assert rules.backtest({"market": {"coins": {}}, "candles": []}, end - D * 3, end) is None
    assert ma.FLAT == "bok"
