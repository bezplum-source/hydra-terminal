"""Testy trwalego stanu pipeline'u (`live/state.py`) - zapis/odczyt
wszystkich czterech plikow stanu, oraz `price_at_block_factory`.

Kazdy test podmienia sciezki modulu na katalog tymczasowy (`tmp_path`),
zeby NIGDY nie dotykac prawdziwych plikow w `data/` tego repo.
"""

from __future__ import annotations

from hydra_signals.models import Side, Trade
from live import state as st


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(st, "DATA_DIR", tmp_path)
    monkeypatch.setattr(st, "SCORING_STATE_PATH", tmp_path / "scoring_state.json")
    monkeypatch.setattr(st, "TRADE_BUFFER_PATH", tmp_path / "trade_buffer.csv")
    monkeypatch.setattr(st, "WALLETS_SEEN_PATH", tmp_path / "wallets_seen.txt")
    monkeypatch.setattr(st, "CANDLES_HISTORY_PATH", tmp_path / "candles_history.json")


def test_scoring_state_roundtrip(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    assert st.load_scoring_state() == {}

    state = {"good_short": 0.55, "prev_signal": "LONG", "last_processed_block": 12345}
    st.save_scoring_state(state)
    assert st.load_scoring_state() == state


def test_trade_buffer_roundtrip_preserves_values(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    assert st.load_trade_buffer() == []

    trades = [
        Trade(wallet="0xAAA", block=10, side=Side.BUY, price_usd=2000.123456, size_eth=1.5),
        Trade(wallet="0xBBB", block=20, side=Side.SELL, price_usd=1999.5, size_eth=0.0001234),
    ]
    st.save_trade_buffer(trades)
    loaded = st.load_trade_buffer()
    assert len(loaded) == 2
    assert loaded[0].wallet == "0xAAA"
    assert loaded[0].side is Side.BUY
    assert abs(loaded[0].price_usd - 2000.123456) < 1e-6
    assert abs(loaded[1].size_eth - 0.0001234) < 1e-9


def test_wallets_seen_roundtrip_and_growth(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    assert st.load_wallets_seen() == set()

    st.save_wallets_seen({"0xAAA", "0xBBB"})
    assert st.load_wallets_seen() == {"0xAAA", "0xBBB"}

    grown = st.load_wallets_seen() | {"0xCCC"}
    st.save_wallets_seen(grown)
    assert st.load_wallets_seen() == {"0xAAA", "0xBBB", "0xCCC"}


def test_candles_history_roundtrip(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    assert st.load_candles_history() == []

    candles = [{"block": 100, "price": 2000.0, "signal": "LONG"}]
    st.save_candles_history(candles)
    assert st.load_candles_history() == candles


def test_price_at_block_factory_uses_nearest_known_block_leq_target():
    trades = [
        Trade(wallet="w1", block=100, side=Side.BUY, price_usd=1000.0, size_eth=1.0),
        Trade(wallet="w2", block=200, side=Side.BUY, price_usd=2000.0, size_eth=1.0),
        Trade(wallet="w3", block=300, side=Side.BUY, price_usd=3000.0, size_eth=1.0),
    ]
    price_at_block = st.price_at_block_factory(trades)

    assert price_at_block(300) == 3000.0
    assert price_at_block(250) == 2000.0  # najblizszy <= 250 to blok 200
    assert price_at_block(50) == 1000.0  # nic wczesniejszego -> najwczesniejsza znana
    assert price_at_block(1000) == 3000.0  # nic pozniejszego -> ostatnia znana
