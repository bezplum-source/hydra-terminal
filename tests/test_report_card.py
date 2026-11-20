"""Graficzny raport dzienny / tygodniowy (live/report_card.py) - część obliczeniowa
bez sieci i bez matplotlib; rysowanie tylko gdy matplotlib jest zainstalowany."""

import pytest

from hydra_signals import strategies as strat
from live import report_card as rc

NOW = 1_795_000_000.0
H = 3600
D = 86400


def _raw(*, stale=False, warnings=()):
    candles = []
    for i in range(24 * 8):
        ts = NOW - (24 * 8 - i) * H + 600
        sig = "LONG" if i < 24 * 5 else "SHORT"
        c = {"ts": ts, "price": 2000.0 - i, "signal": sig, "time": "t", "block": i,
             "trendSma": 2100.0, "trendDistPct": -5.0, "marketMode": "TREND",
             "takerImbalance24h": 0.1, "walletSkilledNet7d": -100.0, "dvol": 60.0, "dvolSma14": 55.0}
        if i >= 24 * 7:
            c.update(voteShort=10, voteHold=3, voteLong=1, votes={"trend14": "SHORT", "hydra": "HOLD", "btc14": "LONG"})
        candles.append(c)
    race = strat.new_race(["vote", "eth"], start_block=0, start_time="t", start_ts=int(NOW - 3 * D), start_price=1900.0)
    for k, c in enumerate(candles[-72:]):
        strat.race_step(race, c, {"vote": "SHORT", "eth": "LONG"})
    age = 5 * H if stale else 600
    ms = (NOW - age) * 1000
    return {
        "candles": candles,
        "race": race,
        "market": {"coins": {"ETH": [[ms, 1800.0]]}},
        "derivs": {"snapshots": [{"t": ms - 7 * D * 1000 + 3600_000, "oi": 100.0}, {"t": ms, "oi": 90.0}],
                   "funding": [{"t": ms, "rate": 0.00001}]},
        "options": {"dvol": [[ms, 60.0]], "snapshots": [{"t": ms, "skew": 4.0, "pcOi": 0.5}]},
        "stables": {"total": [[int(NOW // D * D) - k * D, 1e11 - k * 1e8] for k in range(10, -1, -1)]},
        "manifests": [(NOW - H, {"warnings": list(warnings), "duration_s": 80.0})],
        "commits": [NOW - 7 * D, NOW - age],
    }


def test_collect_daily_signal_votes_and_race():
    s = rc.collect(_raw(), NOW, "daily")
    assert s["alarms"] == [] and all(s["sources"].values())
    sg = s["signal"]
    assert sg["value"] == "SHORT" and sg["entryPrice"] == 2000.0 - 24 * 5
    assert sg["pnlPct"] > 0  # cena spada, short zarabia
    assert s["votes"]["now"] == {"SHORT": 10, "HOLD": 3, "LONG": 1}
    assert s["votes"]["who"]["LONG"] == ["trend BTC"]
    assert s["price"]["chg24h"] < 0 and s["price"]["distPct"] == -5.0
    keys = [r["key"] for r in s["race"]["rows"]]
    assert keys[0] == "vote" and "eth" in keys  # posortowane po wyniku
    assert "market" not in s
    assert len(s["chart"]["t"]) <= 24 * 7 + 1


def test_collect_weekly_has_market_and_signal_changes():
    s = rc.collect(_raw(), NOW, "weekly")
    assert any(c["signal"] == "SHORT" for c in s["changes"])
    m = s["market"]
    assert m["oiChg7d"] == pytest.approx(-10.0) and m["stablesChg7d"] > 0 and m["skew"] == 4.0
    assert s["votes"]["before"] is None  # tydzien temu glosowania nie bylo


def test_health_alarms_for_stale_sources_and_unknown_warnings():
    s = rc.collect(_raw(stale=True, warnings=["base: przeskok zaleglosci - x", "options: HTTPError 403"]), NOW)
    text = " ".join(s["alarms"])
    assert "ponad 2 h" in text and "Funding/OI" in text and "Opcje" in text
    assert "options: HTTPError 403" in text and "przeskok" not in text
    assert s["stats"]["knownWarnings"] == 1
    assert rc.text_summary(s).startswith("⚠ ALARMY")
    assert rc.text_summary(rc.collect(_raw(), NOW)).startswith("✓")


def test_polish_number_formatting():
    assert rc.fmt_num(1735.6) == "1 736" and rc.fmt_num(-2.5, 1) == "−2,5"
    assert rc.fmt_pct(9.04) == "+9,0%" and rc.fmt_pct(None) == "–"


def test_render_png_daily_and_weekly(tmp_path):
    pytest.importorskip("matplotlib")
    for mode in ("daily", "weekly"):
        out = tmp_path / f"{mode}.png"
        rc.render(rc.collect(_raw(), NOW, mode), str(out))
        assert out.stat().st_size > 10_000
