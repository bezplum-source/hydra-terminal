"""Testy Etapu 2 planu "trend": godzinowe ceny BTC/ETH z Hyperliquid
(`hydra_signals/data_sources/hyperliquid_market.py`) i cechy na świecach
(`live/market.py`). Sieć zawsze podmieniona."""

from __future__ import annotations

import pytest

from hydra_signals import strategies as strat
from hydra_signals.data_sources import hyperliquid_market as hm
from live import market
from live import state as st

H_MS = 3_600_000
D_MS = 86_400_000
NOW = 1_800_000_000_000


def _raw(coin, closes, start_close_ms):
    return [
        {"t": start_close_ms + i * H_MS - H_MS + 1, "T": start_close_ms + i * H_MS, "s": coin, "i": "1h",
         "o": str(c), "h": str(c), "l": str(c), "c": str(c), "v": "1", "n": 5}
        for i, c in enumerate(closes)
    ]


# ---------- hyperliquid_market ----------


def test_parse_candles_keeps_only_closed_dedupes_and_skips_bad_rows():
    raw = _raw("BTC", [100.0, 101.0, 102.0], NOW - 2 * H_MS)  # ostatnia zamyka się dokładnie "teraz"
    raw.append({"T": NOW + H_MS, "c": "999"})  # jeszcze otwarta
    raw.append({"T": NOW - 2 * H_MS, "c": "100"})  # duplikat
    raw.append({"T": "x", "c": "1"})
    raw.append({"T": NOW - 10 * H_MS, "c": "0"})
    assert hm.parse_candles(raw, NOW) == [[NOW - 2 * H_MS, 100.0], [NOW - H_MS, 101.0], [NOW, 102.0]]
    assert hm.parse_candles(None, NOW) == [] and hm.parse_candles({"a": 1}, NOW) == []


def test_fetch_closes_sends_candle_snapshot_request():
    seen = {}

    def post(body):
        seen.update(body)
        return _raw("ETH", [2000.0], NOW - H_MS)

    assert hm.fetch_closes("ETH", NOW - D_MS, NOW, post=post) == [[NOW - H_MS, 2000.0]]
    assert seen == {"type": "candleSnapshot", "req": {"coin": "ETH", "interval": "1h", "startTime": NOW - D_MS, "endTime": NOW}}


# ---------- live.market.update_history ----------


def test_update_history_backfills_then_appends_without_duplicates():
    calls = []

    def fetch(coin, start, now):
        calls.append((coin, start))
        return [[now - 2 * H_MS, 1.0], [now - H_MS, 2.0]]

    h = market.empty_history()
    c = market.update_history(h, NOW, fetch=fetch, backfill_days=200)
    assert calls == [("BTC", NOW - 200 * D_MS), ("ETH", NOW - 200 * D_MS)]
    assert c["BTC"] == {"new": 2, "total": 2} and c["errors"] == []
    calls.clear()
    c = market.update_history(h, NOW + H_MS, fetch=fetch)
    assert calls[0] == ("BTC", NOW - H_MS + 1)  # od ostatniej zapisanej świecy
    assert c["BTC"]["new"] == 1 and len(h["coins"]["BTC"]) == 3


def test_update_history_one_coin_failing_does_not_stop_the_other_and_prunes_old_rows():
    def fetch(coin, start, now):
        if coin == "BTC":
            raise OSError("down")
        return [[now - H_MS, 2000.0]]

    h = market.empty_history()
    h["coins"]["ETH"] = [[NOW - 400 * D_MS, 1.0]]
    c = market.update_history(h, NOW, fetch=fetch)
    assert c["errors"] and c["errors"][0].startswith("BTC: OSError")
    assert h["coins"]["ETH"] == [[NOW - H_MS, 2000.0]]  # stary wiersz wycięty retencją


# ---------- trendy i cechy na świecy ----------


def _history(btc_fn, eth_fn, hours=24 * 20):
    h = market.empty_history()
    start = NOW - (hours - 1) * H_MS
    h["coins"]["BTC"] = [[start + i * H_MS, btc_fn(i)] for i in range(hours)]
    h["coins"]["ETH"] = [[start + i * H_MS, eth_fn(i)] for i in range(hours)]
    return h


def test_features_btc_trend_ratio_and_reference_price():
    # BTC rośnie, ETH stoi -> BTC LONG, ETH/BTC SHORT (ETH słabszy od BTC)
    h = _history(lambda i: 50_000 + 10 * i, lambda i: 2000.0)
    lk = market.build_lookups(h)
    f = market.features_at(lk, NOW / 1000 + 600)  # 10 min po ostatniej zamkniętej świecy
    assert f["btcSignal"] == "LONG" and f["btcTrendDistPct"] > 0
    assert f["btcPrice"] == 50_000 + 10 * (24 * 20 - 1)
    assert f["ethRefPrice"] == 2000.0
    assert f["ethBtcSignal"] == "SHORT" and f["ethBtc"] == pytest.approx(2000.0 / f["btcPrice"], rel=1e-5)


def test_features_none_when_data_too_old_missing_or_too_short():
    h = _history(lambda i: 50_000.0, lambda i: 2000.0)
    lk = market.build_lookups(h)
    assert all(v is None for v in market.features_at(lk, NOW / 1000 + 4 * 3600).values())  # > 3h po ostatniej
    assert all(v is None for v in market.features_at(None, NOW / 1000).values())
    assert all(v is None for v in market.features_at(lk, None).values())
    short = market.build_lookups(_history(lambda i: 1.0 + i, lambda i: 1.0, hours=24 * 5))
    f = market.features_at(short, NOW / 1000)
    assert f["btcPrice"] is not None and f["btcSignal"] is None  # za mało historii na trend
    empty = market.build_lookups(market.empty_history())
    assert market.features_at(empty, NOW / 1000)["btcPrice"] is None


def test_combined_signal():
    c = market.combined_signal
    assert c("LONG", "LONG") == "LONG" and c("SHORT", "SHORT") == "SHORT"
    assert c("LONG", "SHORT") == "HOLD" and c("HOLD", "HOLD") == "HOLD"
    assert c("LONG", None) is None


def test_market_state_roundtrip_and_corrupt_file(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "DATA_DIR", tmp_path)
    monkeypatch.setattr(st, "MARKET_HISTORY_PATH", tmp_path / "market_history.json")
    assert st.load_market_history() == {"version": 1, "coins": {"BTC": [], "ETH": []}}
    h = _history(lambda i: 1.0, lambda i: 2.0, hours=3)
    st.save_market_history(h)
    assert st.load_market_history()["coins"]["BTC"] == h["coins"]["BTC"]
    (tmp_path / "market_history.json").write_text("{zle", encoding="utf-8")
    assert st.load_market_history()["coins"]["ETH"] == []


# ---------- strategie: nowe zachowania z etapu 2 ----------


def test_price_guard_reference_price_rejects_far_pool_price():
    g = strat.PriceGuard()
    assert g.check(2000.0, 2010.0)
    assert not g.check(2150.0, 2010.0)  # +7% od Hyperliquid - błąd puli, choć skok < 15%
    assert g.check(2020.0, 2015.0)
    assert g.check(2030.0, None)  # bez ceny odniesienia działa sam skok


def test_strategy_step_none_keeps_position_and_book_start_fields():
    b = strat.new_strategy_book("14.11.2026, 10:00", 123)
    assert b["startTime"] == "14.11.2026, 10:00" and b["startBlock"] == 123
    strat.strategy_step(b, signal="LONG", price=100.0, time="a", block=1, ts=1)
    strat.strategy_step(b, signal=None, price=110.0, time="b", block=2, ts=2)
    assert b["position"] == "LONG" and b["trades"] == []
    assert strat.summarize_book(b)["startTime"] == "14.11.2026, 10:00"
