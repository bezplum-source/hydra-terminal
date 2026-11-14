"""Testy funding rate + open interest (Hyperliquid ETH-PERP):
`hydra_signals/data_sources/hyperliquid_derivs.py` (I/O, sieć podmieniona) i
`live/derivs.py` (historia + pola na świecy)."""

from __future__ import annotations

import pytest

from hydra_signals.data_sources import hyperliquid_derivs as hd
from live import derivs
from live import state as st

HOUR = derivs.HOUR_MS
DAY = derivs.DAY_MS
T0 = 1_800_000_000_000  # arbitralny "teraz" w ms


def _meta(coins=("BTC", "ETH", "SOL"), eth_ctx=None):
    ctxs = []
    for c in coins:
        if c == "ETH":
            ctxs.append(
                eth_ctx
                or {
                    "funding": "0.0000125",
                    "openInterest": "812345.5",
                    "markPx": "2000.5",
                    "oraclePx": "2001.0",
                    "premium": "-0.00025",
                    "dayNtlVlm": "1234567890.1",
                }
            )
        else:
            ctxs.append({"funding": "0.1", "openInterest": "1", "markPx": "1"})
    return [{"universe": [{"name": c} for c in coins]}, ctxs]


# ---------- hyperliquid_derivs ----------


def test_parse_asset_ctx_picks_eth_by_name_not_position():
    out = hd.parse_asset_ctx(_meta(coins=("SOL", "ETH", "BTC")), "ETH")
    assert out == {
        "funding": 0.0000125,
        "openInterest": 812345.5,
        "markPx": 2000.5,
        "oraclePx": 2001.0,
        "premium": -0.00025,
        "dayNtlVlm": 1234567890.1,
    }


@pytest.mark.parametrize(
    "raw",
    [None, [], [{"universe": []}, []], "x", [{"universe": [{"name": "BTC"}]}, [{"funding": "1"}]]],
)
def test_parse_asset_ctx_returns_none_on_bad_shape_or_missing_coin(raw):
    assert hd.parse_asset_ctx(raw, "ETH") is None


def test_parse_asset_ctx_none_when_required_numbers_missing():
    bad = _meta(eth_ctx={"funding": "0.1", "markPx": "2000"})  # brak openInterest
    assert hd.parse_asset_ctx(bad, "ETH") is None


def test_fetch_asset_ctx_swallows_network_errors():
    def boom(_body):
        raise OSError("down")

    assert hd.fetch_asset_ctx(post=boom) is None


def _rows(start_t, n):
    return [
        {"coin": "ETH", "fundingRate": f"{0.00001 * (i % 7):.8f}", "premium": "0.0001", "time": start_t + i * HOUR}
        for i in range(n)
    ]


def test_fetch_funding_history_paginates_and_dedupes():
    calls = []
    all_rows = _rows(T0, 520)

    def post(body):
        calls.append(body["startTime"])
        rows = [r for r in all_rows if r["time"] >= body["startTime"]][:500]
        return rows

    out = hd.fetch_funding_history(T0, post=post)
    assert len(out) == 520
    assert [r["t"] for r in out] == sorted(r["t"] for r in out)
    assert calls == [T0, T0 + 499 * HOUR + 1]  # druga strona zaczyna tuż za ostatnim wierszem pierwszej
    assert out[0] == {"t": T0, "rate": 0.0, "premium": 0.0001}


def test_fetch_funding_history_first_page_failure_raises_but_later_page_returns_partial():
    def boom(_body):
        raise OSError("down")

    with pytest.raises(OSError):
        hd.fetch_funding_history(T0, post=boom)

    state = {"n": 0}

    def flaky(body):
        state["n"] += 1
        if state["n"] == 1:
            return _rows(T0, 500)
        raise OSError("down")

    out = hd.fetch_funding_history(T0, post=flaky)
    assert len(out) == 500


def test_fetch_funding_history_skips_malformed_rows():
    rows = _rows(T0, 3) + [{"coin": "ETH", "time": T0 + 10 * HOUR}, {"fundingRate": "x", "time": 1}]
    out = hd.fetch_funding_history(T0, post=lambda b: rows)
    assert len(out) == 3


# ---------- live.derivs.update_history ----------


def _ctx(oi=1000.0, mark=2000.0, funding=0.00001, premium=0.0001):
    return {"funding": funding, "openInterest": oi, "markPx": mark, "oraclePx": mark, "premium": premium, "dayNtlVlm": 5.0}


def test_update_history_backfills_empty_history_from_backfill_days():
    asked = []

    def fetch_funding(start_ms):
        asked.append(start_ms)
        return [{"t": T0 - i * HOUR, "rate": 0.00001, "premium": None} for i in range(5, 0, -1)]

    h = derivs.empty_history()
    c = derivs.update_history(h, T0, fetch_ctx=lambda: _ctx(), fetch_funding=fetch_funding, backfill_days=120)
    assert asked == [T0 - 120 * DAY]
    assert c["funding_new"] == 5 and c["funding_total"] == 5
    assert c["snapshot_added"] and c["snapshot_total"] == 1 and c["errors"] == []
    assert h["snapshots"][0]["oi"] == 1000.0 and h["snapshots"][0]["mark"] == 2000.0


def test_update_history_resumes_after_last_row_and_never_duplicates():
    h = derivs.empty_history()
    h["funding"] = [{"t": T0 - 2 * HOUR, "rate": 0.1, "premium": None}, {"t": T0 - HOUR, "rate": 0.2, "premium": None}]
    asked = []

    def fetch_funding(start_ms):
        asked.append(start_ms)
        return [  # serwer mógłby oddać też już znany wiersz - nie wolno go dublować
            {"t": T0 - HOUR, "rate": 0.2, "premium": None},
            {"t": T0, "rate": 0.3, "premium": None},
        ]

    c = derivs.update_history(h, T0 + 10, fetch_ctx=lambda: _ctx(), fetch_funding=fetch_funding)
    assert asked == [T0 - HOUR + 1]
    assert c["funding_new"] == 1
    assert [r["t"] for r in h["funding"]] == [T0 - 2 * HOUR, T0 - HOUR, T0]


def test_update_history_one_source_failing_does_not_block_the_other():
    h = derivs.empty_history()

    def boom(_):
        raise OSError("funding down")

    c = derivs.update_history(h, T0, fetch_ctx=lambda: _ctx(), fetch_funding=boom)
    assert c["snapshot_added"] and c["funding_new"] == 0
    assert len(c["errors"]) == 1 and c["errors"][0].startswith("funding:")

    h2 = derivs.empty_history()
    c2 = derivs.update_history(
        h2, T0, fetch_ctx=lambda: None, fetch_funding=lambda s: [{"t": T0, "rate": 0.1, "premium": None}]
    )
    assert c2["funding_new"] == 1 and not c2["snapshot_added"]
    assert c2["errors"] == ["asset_ctx: no data"]

    def ctx_boom():
        raise RuntimeError("x")

    c3 = derivs.update_history(derivs.empty_history(), T0, fetch_ctx=ctx_boom, fetch_funding=lambda s: [])
    assert c3["errors"][0].startswith("asset_ctx: RuntimeError")


def test_update_history_snapshot_gap_dedupes_quick_reruns():
    h = derivs.empty_history()
    calls = {"n": 0}

    def ctx():
        calls["n"] += 1
        return _ctx()

    derivs.update_history(h, T0, fetch_ctx=ctx, fetch_funding=lambda s: [])
    c = derivs.update_history(h, T0 + 60_000, fetch_ctx=ctx, fetch_funding=lambda s: [])
    assert not c["snapshot_added"] and calls["n"] == 1 and len(h["snapshots"]) == 1
    c = derivs.update_history(h, T0 + HOUR, fetch_ctx=ctx, fetch_funding=lambda s: [])
    assert c["snapshot_added"] and len(h["snapshots"]) == 2


def test_update_history_prunes_old_rows():
    h = derivs.empty_history()
    h["funding"] = [
        {"t": T0 - 300 * DAY, "rate": 0.1, "premium": None},
        {"t": T0 - 10 * DAY, "rate": 0.2, "premium": None},
    ]
    h["snapshots"] = [
        {"t": T0 - 200 * DAY, "oi": 1.0, "mark": 1.0},
        {"t": T0 - 2 * DAY, "oi": 1.0, "mark": 1.0},
    ]
    derivs.update_history(h, T0, fetch_ctx=lambda: None, fetch_funding=lambda s: [])
    assert [r["t"] for r in h["funding"]] == [T0 - 10 * DAY]
    assert [s["t"] for s in h["snapshots"]] == [T0 - 2 * DAY]


# ---------- live.derivs.features_at ----------


def _hist(n_hours=24 * 40, rate_fn=lambda i: 0.00001, with_snapshots=True):
    h = derivs.empty_history()
    h["funding"] = [{"t": T0 + i * HOUR, "rate": rate_fn(i), "premium": 0.0002} for i in range(n_hours)]
    if with_snapshots:
        h["snapshots"] = [
            {"t": T0 + i * HOUR, "oi": 1000.0 + i, "mark": 2000.0, "premium": 0.0003}
            for i in range(0, n_hours, 1)
        ]
    return h


def test_features_none_for_missing_timestamp_or_empty_history():
    assert derivs.features_at(derivs.empty_history(), 1_800_000_000) == {k: None for k in derivs.FEATURE_KEYS}
    assert derivs.features_at(_hist(), None) == {k: None for k in derivs.FEATURE_KEYS}


def test_features_funding_rate_apr_and_tolerance():
    h = _hist(n_hours=48, rate_fn=lambda i: 0.0001, with_snapshots=False)
    ts = (T0 + 10 * HOUR + 20 * 60_000) / 1000  # 10h20m po starcie -> bierze wiersz z godziny 10
    f = derivs.features_at(h, ts)
    assert f["fundingRate"] == 0.0001
    assert f["fundingApr"] == pytest.approx(0.0001 * 8760 * 100, abs=0.01)  # interwał 1h
    assert f["fundingZ"] is None  # za mało wierszy (< Z_MIN_ROWS) / zerowa wariancja
    far = (T0 + 48 * HOUR + 5 * HOUR) / 1000  # 5h po ostatnim wierszu > tolerancja 3h
    assert derivs.features_at(h, far)["fundingRate"] is None
    before = (T0 - HOUR) / 1000
    assert derivs.features_at(h, before)["fundingRate"] is None


def test_features_funding_zscore_uses_trailing_window_only():
    # płasko 0.00001, a w ostatniej godzinie skok - z-score musi wyjść mocno dodatni
    h = _hist(n_hours=24 * 40, rate_fn=lambda i: 0.00001 + (0.0002 if i == 24 * 40 - 1 else 0.0) + (1e-7 * (i % 3)), with_snapshots=False)
    last_ts = (T0 + (24 * 40 - 1) * HOUR) / 1000
    z = derivs.features_at(h, last_ts)["fundingZ"]
    assert z is not None and z > 5
    # tuż po starcie historii (poniżej Z_MIN_ROWS wierszy) z-score nie istnieje
    early = (T0 + 10 * HOUR) / 1000
    assert derivs.features_at(h, early)["fundingZ"] is None


def test_features_open_interest_usd_and_24h_change():
    h = _hist(n_hours=72)
    ts = (T0 + 48 * HOUR) / 1000
    f = derivs.features_at(h, ts)
    assert f["openInterest"] == 1048.0
    assert f["openInterestUsd"] == 1048.0 * 2000.0
    assert f["oiChange24h"] == pytest.approx((1048.0 / 1024.0 - 1) * 100, abs=0.01)
    assert f["perpPremium"] == 0.0003  # snapshot ma pierwszeństwo przed premią z funding


def test_features_oi_none_when_no_snapshot_near_candle_and_change_needs_old_snapshot():
    h = _hist(n_hours=72, with_snapshots=False)
    h["snapshots"] = [{"t": T0 + 60 * HOUR, "oi": 1000.0, "mark": 2000.0}]
    # świeca z godziny 10: snapshot jest 50h później -> brak
    assert derivs.features_at(h, (T0 + 10 * HOUR) / 1000)["openInterest"] is None
    # świeca w momencie snapshotu: OI jest, ale nie ma snapshotu sprzed 24h -> zmiana None
    f = derivs.features_at(h, (T0 + 60 * HOUR) / 1000)
    assert f["openInterest"] == 1000.0 and f["oiChange24h"] is None


def test_features_apr_uses_actual_interval_when_funding_is_not_hourly():
    h = derivs.empty_history()
    h["funding"] = [{"t": T0 + i * 8 * HOUR, "rate": 0.0001, "premium": None} for i in range(10)]
    f = derivs.features_at(h, (T0 + 9 * 8 * HOUR) / 1000)
    assert f["fundingApr"] == pytest.approx(0.0001 * (8760 / 8) * 100, abs=0.01)


# ---------- state I/O ----------


def test_state_roundtrip_and_corrupt_file_falls_back_to_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "DATA_DIR", tmp_path)
    monkeypatch.setattr(st, "DERIVS_HISTORY_PATH", tmp_path / "derivs_history.json")
    assert st.load_derivs_history() == {"version": 1, "funding": [], "snapshots": []}
    h = _hist(n_hours=5)
    st.save_derivs_history(h)
    again = st.load_derivs_history()
    assert again["funding"] == h["funding"] and again["snapshots"] == h["snapshots"]
    (tmp_path / "derivs_history.json").write_text("{not json", encoding="utf-8")
    assert st.load_derivs_history() == {"version": 1, "funding": [], "snapshots": []}
    (tmp_path / "derivs_history.json").write_text('["x"]', encoding="utf-8")
    assert st.load_derivs_history() == {"version": 1, "funding": [], "snapshots": []}
