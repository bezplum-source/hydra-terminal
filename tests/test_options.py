"""Etap 7 planu "trend": opcje ETH z Deribit - DVOL, put/call, skew, cechy na świecy."""

import datetime as dt
import urllib.parse

from hydra_signals.data_sources import deribit as db
from live import options as op

H = 3_600_000
DAY_MS = 86_400_000
NOW_MS = 1_789_000_000_000


def _exp_name(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%d%b%y").upper().lstrip("0")


def test_parse_instrument():
    exp, strike, kind = db.parse_instrument("ETH-27DEC26-2000-P")
    assert dt.datetime.fromtimestamp(exp / 1000, dt.timezone.utc) == dt.datetime(2026, 12, 27, 8, tzinfo=dt.timezone.utc)
    assert strike == 2000.0 and kind == "P"
    assert db.parse_instrument("ETH-5DEC26-1800-C")[2] == "C"
    assert db.parse_instrument("ETH-PERPETUAL") is None
    assert db.parse_instrument("ETH-27DEC26") is None


def test_fetch_dvol_pages_backwards_with_continuation_and_drops_open_candle():
    calls = []
    start, end = NOW_MS - 10 * H, NOW_MS

    def get(url):
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        calls.append(q)
        e = int(q["end_timestamp"])
        if e == end:  # strona 1: 5 najnowszych świec (ostatnia jeszcze otwarta)
            rows = [[end - k * H, 0, 0, 0, 60.0 + k] for k in range(5)]
            return {"result": {"data": rows, "continuation": end - 5 * H}}
        rows = [[e - k * H, 0, 0, 0, 70.0 + k] for k in range(1, 6)]
        return {"result": {"data": rows, "continuation": None}}

    out = db.fetch_dvol(start, end, get=get)
    assert calls[0]["currency"] == "ETH" and calls[0]["resolution"] == "3600"
    assert len(calls) == 2
    ts = [r[0] for r in out]
    assert ts == sorted(ts) and max(ts) <= end  # świeca otwarta o `end` odrzucona
    assert out[-1] == [end, 61.0]


def _chain():
    """Opcje: wygasanie za 30 dni (do skew) i za 3 dni (za krótko). S = 2000."""
    e30, e3 = NOW_MS + 30 * DAY_MS, NOW_MS + 3 * DAY_MS
    n30, n3 = _exp_name(e30), _exp_name(e3)
    items = []
    for name, strike, kind, iv, oi in [
        (n30, 1800, "P", 75.0, 300.0),
        (n30, 2000, "P", 65.0, 100.0),
        (n30, 2000, "C", 63.0, 100.0),
        (n30, 2200, "C", 60.0, 200.0),
        (n3, 2000, "P", 90.0, 100.0),
        (n3, 2000, "C", 50.0, 100.0),
    ]:
        items.append({"instrument_name": f"ETH-{name}-{strike}-{kind}", "open_interest": oi, "volume": oi / 10,
                      "mark_iv": iv, "underlying_price": 2000.0})
    items.append({"instrument_name": "ETH-PERPETUAL", "open_interest": 1e9})
    return items


def test_summarize_options_put_call_and_skew_from_the_30_day_expiry():
    s = db.summarize_options(_chain(), NOW_MS)
    assert s["putOiEth"] == 500.0 and s["callOiEth"] == 400.0 and s["pcOi"] == 1.25
    assert s["pcVol"] == 1.25
    assert s["skew"] == 15.0  # put 1800 (75) - call 2200 (60)
    assert s["atmIv"] == 64.0 and s["underlying"] == 2000.0
    assert db.summarize_options([], NOW_MS) is None


def test_update_history_appends_dvol_and_snapshot_and_survives_errors():
    h = op.empty_history()
    c = op.update_history(
        h, NOW_MS,
        fetch_dvol=lambda s, e: [[NOW_MS - 2 * H, 60.0], [NOW_MS - H, 61.0]],
        fetch_summary=_chain,
    )
    assert c["dvol_new"] == 2 and c["snapshot_added"] and h["snapshots"][0]["skew"] == 15.0
    # migawka nie częściej niż co 20 min; błąd jednego źródła nie blokuje drugiego
    c = op.update_history(
        h, NOW_MS + 5 * 60_000,
        fetch_dvol=lambda s, e: (_ for _ in ()).throw(OSError("deribit down")),
        fetch_summary=_chain,
    )
    assert c["errors"] and not c["snapshot_added"] and len(h["dvol"]) == 2


def _history(dvol_values, snap_skew=None):
    rows = [[NOW_MS - (len(dvol_values) - 1 - i) * H, v] for i, v in enumerate(dvol_values)]
    snaps = [] if snap_skew is None else [{"t": NOW_MS, "pcOi": 1.1, "skew": snap_skew}]
    return {"dvol": rows, "snapshots": snaps}


def test_trend_calm_follows_trend_only_when_dvol_not_above_its_average():
    hours = 24 * 15
    calm = op.OptionsLookup(_history([60.0] * (hours - 1) + [55.0]))
    f = op.features_at(calm, NOW_MS / 1000, "SHORT")
    assert f["dvol"] == 55.0 and f["dvolSma14"] > 55.0 and f["signalTrendCalm"] == "SHORT"
    nervous = op.OptionsLookup(_history([60.0] * (hours - 1) + [80.0]))
    assert op.features_at(nervous, NOW_MS / 1000, "SHORT")["signalTrendCalm"] == "HOLD"
    # za mało historii na średnią 14 dni -> brak sygnału (None = trzyma pozycję)
    young = op.OptionsLookup(_history([60.0] * 48))
    f = op.features_at(young, NOW_MS / 1000, "LONG")
    assert f["dvol"] == 60.0 and f["dvolSma14"] is None and f["signalTrendCalm"] is None


def test_skew_signal_and_snapshot_only_near_its_time():
    lk = op.OptionsLookup(_history([60.0] * 10, snap_skew=4.0))
    f = op.features_at(lk, NOW_MS / 1000 + 600, "LONG")
    assert f["optSkew"] == 4.0 and f["signalOptSkew"] == "SHORT" and f["optPutCallOi"] == 1.1
    assert op.features_at(lk, NOW_MS / 1000 - 600, "LONG")["optSkew"] is None  # przed migawką
    assert op.features_at(lk, NOW_MS / 1000 + 4 * 3600, "LONG")["optSkew"] is None  # za stara
    assert op.skew_signal(-3.0) == "LONG" and op.skew_signal(0.5) == "HOLD" and op.skew_signal(None) is None
    assert op.features_at(None, NOW_MS / 1000, "LONG") == op.empty_features()
