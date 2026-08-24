"""Testy orkiestracji listenera (`live/hyperliquid_listener.py`) - bez
zadnej sieci. Podmieniamy `hyperliquid_ws.listen` na fake, ktory od razu
"dostarcza" kilka paczek transakcji przez callback `on_trades`, zeby
zweryfikowac, ze main() poprawnie laczy przycinanie starego bufora z nowo
zebranymi danymi i zapisuje wynik na dysk."""

from __future__ import annotations

import asyncio

from hydra_signals.data_sources import hyperliquid_ws as hl_ws
from live import hyperliquid_listener as listener
from live import state as st


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(st, "DATA_DIR", tmp_path)
    monkeypatch.setattr(st, "HYPERLIQUID_TRADES_BUFFER_PATH", tmp_path / "hyperliquid_trades_buffer.jsonl")


def _sample_trade(tid: int, ts_ms: int) -> hl_ws.HyperliquidTrade:
    return hl_ws.HyperliquidTrade(
        coin="ETH",
        aggressor_side=hl_ws.AggressorSide.BUY,
        price_usd=2455.42,
        size_eth=1.0,
        buyer="0xBUYER",
        seller="0xSELLER",
        ts_ms=ts_ms,
        tid=tid,
        tx_hash="0xabc",
    )


def test_main_prunes_old_records_and_appends_new_ones(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)

    now_ms = 1_000_000_000_000
    monkeypatch.setattr(listener.time, "time", lambda: now_ms / 1000)

    hour_ms = 3600 * 1000
    old_record = hl_ws.trade_to_json_record(_sample_trade(1, now_ms - 100 * hour_ms))  # za stary
    recent_record = hl_ws.trade_to_json_record(_sample_trade(2, now_ms - 1 * hour_ms))  # zostaje
    st.save_hyperliquid_trades_buffer([old_record, recent_record])

    async def fake_listen(on_trades, *, duration_seconds, **kwargs):
        on_trades([_sample_trade(3, now_ms)])

    monkeypatch.setattr(listener.hl_ws, "listen", fake_listen)
    monkeypatch.setenv("HYPERLIQUID_LISTEN_SECONDS", "5")
    monkeypatch.setenv("HYPERLIQUID_BUFFER_LOOKBACK_HOURS", "48")
    monkeypatch.setenv("HYPERLIQUID_FLUSH_INTERVAL_SECONDS", "999999")  # brak flushu posredniego w tym tescie

    rc = listener.main()
    assert rc == 0

    final = st.load_hyperliquid_trades_buffer()
    tids = {r["tid"] for r in final}
    assert tids == {2, 3}  # stary (tid=1) przyciety, reszta zostaje + nowa transakcja


def test_main_saves_partial_buffer_even_if_listen_raises(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(listener.time, "time", lambda: 1_000_000_000.0)

    async def failing_listen(on_trades, *, duration_seconds, **kwargs):
        on_trades([_sample_trade(1, 1_000_000_000_000)])
        raise RuntimeError("symulowany krach polaczenia")

    monkeypatch.setattr(listener.hl_ws, "listen", failing_listen)
    monkeypatch.setenv("HYPERLIQUID_LISTEN_SECONDS", "5")

    rc = listener.main()
    assert rc == 0  # main() nie propaguje wyjatku - zapisuje co zebrano i konczy czysto

    final = st.load_hyperliquid_trades_buffer()
    assert len(final) == 1
    assert final[0]["tid"] == 1
