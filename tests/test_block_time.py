"""Testy `live/block_time.py` - szacunek/naprawa znacznikow czasu swiec
(2026-10-06, "Uniswap+Pancake bez daty" / "?" w historii sygnalow)."""

from __future__ import annotations

from live import block_time as bt

T0 = 1_790_000_000


def _c(block, ts=None, time="?"):
    return {"block": block, "ts": ts, "time": time}


def test_estimate_uses_nearest_anchor_and_12s_blocks():
    anchors = [(1000, T0), (2000, T0 + 12_000)]
    assert bt.estimate_block_ts(1100, anchors) == T0 + 1200  # bliżej 1000
    assert bt.estimate_block_ts(1900, anchors) == T0 + 12_000 - 1200  # bliżej 2000
    assert bt.estimate_block_ts(5000, anchors) == T0 + 12_000 + 3000 * 12  # za ostatnim
    assert bt.estimate_block_ts(10, anchors) == T0 - 990 * 12  # przed pierwszym
    assert bt.estimate_block_ts(1000, []) is None


def test_approx_label_is_marked_and_question_mark_without_anchors():
    anchors = [(1000, T0)]
    label = bt.approx_label(1010, anchors)
    assert label.startswith(bt.APPROX_PREFIX)
    assert label == bt.APPROX_PREFIX + bt.format_warsaw(T0 + 120)
    assert bt.approx_label(1010, []) == "?"


def test_build_anchors_ignores_candles_without_ts_and_merges_extra():
    candles = [_c(10, T0), _c(20, None), {"time": "x"}, _c(30, T0 + 240)]
    assert bt.build_anchors(candles) == [(10, T0), (30, T0 + 240)]
    assert bt.build_anchors(candles, extra=[(20, T0 + 120)]) == [(10, T0), (20, T0 + 120), (30, T0 + 240)]


def test_repair_fetches_real_ts_newest_first_within_limit_and_estimates_rest():
    candles = [_c(1000, T0, bt.format_warsaw(T0)), _c(1100), _c(1200), _c(1300)]
    seen = []

    def fetch(blocks):
        seen.append(list(blocks))
        return {1300: T0 + 3600, 1200: T0 + 2400}  # 1100 poza limitem

    stats = bt.repair_missing_timestamps(candles, fetch, limit=2)
    assert seen == [[1300, 1200]]  # najnowsze najpierw, tylko `limit`
    assert candles[3]["ts"] == T0 + 3600 and candles[3]["time"] == bt.format_warsaw(T0 + 3600)
    assert candles[2]["ts"] == T0 + 2400
    # 1100: bez pomiaru -> `ts` zostaje None (nie udajemy pomiaru), `time` to szacunek "~"
    assert candles[1]["ts"] is None
    assert candles[1]["time"].startswith("~")
    assert stats == {"missing_before": 3, "fetched": 2, "still_missing": 1, "estimated": 1}


def test_repair_replaces_old_approx_label_once_real_ts_arrives():
    candles = [_c(1000, T0, bt.format_warsaw(T0)), _c(1100, None, "~01.01.2026, 00:00")]
    stats = bt.repair_missing_timestamps(candles, lambda bl: {1100: T0 + 1200}, limit=10)
    assert candles[1]["ts"] == T0 + 1200
    assert not candles[1]["time"].startswith("~")
    assert stats["fetched"] == 1 and stats["still_missing"] == 0


def test_repair_survives_fetch_exception_and_still_estimates():
    candles = [_c(1000, T0, bt.format_warsaw(T0)), _c(1100)]

    def boom(blocks):
        raise RuntimeError("RPC padl")

    stats = bt.repair_missing_timestamps(candles, boom, limit=10)
    assert candles[1]["ts"] is None and candles[1]["time"].startswith("~")
    assert stats["fetched"] == 0 and stats["estimated"] == 1


def test_repair_without_any_anchor_leaves_question_mark():
    candles = [_c(1100)]
    stats = bt.repair_missing_timestamps(candles, lambda b: {}, limit=10)
    assert candles[0]["time"] == "?" and candles[0]["ts"] is None
    assert stats["estimated"] == 0 and stats["still_missing"] == 1


def test_repair_noop_when_nothing_missing():
    candles = [_c(1000, T0, "x")]
    called = []
    stats = bt.repair_missing_timestamps(candles, lambda b: called.append(b) or {}, limit=10)
    assert not called and stats["missing_before"] == 0 and candles[0]["time"] == "x"
