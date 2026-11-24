"""Archiwum raportów (live/report_writer.py) i strona site/reports.html - bez sieci."""

import json

import pytest

from hydra_signals import strategies as strat
from live import build_site as bs
from live import report_card as rc
from live import report_writer as rw
from live import state as st

H = 3600
D = 86400
NOW = 1_795_000_000.0 // D * D + 18 * H  # wtorek 15.11.2026? - dzień tygodnia sprawdzany osobno


def _raw():
    n = 60 * 24
    eth = [[(NOW - (n - i) * H) * 1000, 3000.0 - i * 0.5] for i in range(n + 1)]
    candles = [{"ts": NOW - (100 - i) * H, "price": 2400.0, "signal": "SHORT", "voteShort": 11, "voteLong": 1,
                "voteHold": 2, "trendDistPct": -8.0, "takerImbalance24h": 0.1, "walletSkilledNet7d": -10.0}
               for i in range(100)]
    race = strat.new_race(["vote", "eth"], start_block=0, start_time="t", start_ts=int(NOW - 3 * D), start_price=2400.0)
    return {"market": {"coins": {"ETH": eth, "BTC": eth}}, "candles": candles, "race": race,
            "derivs": {}, "options": {}, "stables": {}, "manifests": [], "commits": [NOW - 600]}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "REPORTS_PATH", tmp_path / "reports.json")
    monkeypatch.setattr(rc, "load", lambda repo: _raw())
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("HYDRA_REPORT_MODEL", raising=False)
    return tmp_path


def _fake_http(reply_text, calls):
    def http(url, body, headers):
        calls.append((url, body, headers))
        if url.endswith("/models?limit=100"):
            return {"data": [{"id": "claude-opus-x", "created_at": "2026-10-01"},
                             {"id": "claude-sonnet-old", "created_at": "2025-01-01"},
                             {"id": "claude-sonnet-new", "created_at": "2026-09-01"}]}
        return {"content": [{"type": "text", "text": reply_text}]}
    return http


GOOD = json.dumps({"stage": "bessa · wyprzedanie", "title": "Spadek rozciągnięty", "text": "Zdanie. Drugie.",
                   "f24": "bok", "f7": "spadek", "conf": "umiarkowana", "track": "Brak rozliczonych prognoz."})


def test_claude_analysis_is_archived_with_newest_sonnet_and_key_only_in_header(env, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-123")
    calls = []
    assert rw.main(["--mode", "daily", "--now", str(NOW)], http=_fake_http("Oto ocena:\n" + GOOD, calls)) == 0
    e = st.load_reports()[-1]
    assert e["type"] == "daily" and e["source"] == "Claude (claude-sonnet-new)"
    assert e["stage"] == "bessa · wyprzedanie" and e["f24"] == "bok" and e["f7"] == "spadek"
    assert e["signal"] == "SHORT" and e["votes"] == {"SHORT": 11, "HOLD": 2, "LONG": 1}
    assert e["price"] is not None and e["race"]["vote"] == 0.0
    body = calls[-1][1]
    assert body["model"] == "claude-sonnet-new" and "sk-test-123" not in json.dumps(body)
    assert calls[-1][2]["x-api-key"] == "sk-test-123"
    assert "DZIENNY" in body["messages"][0]["content"]


def test_dedup_within_6h_and_force(env):
    assert rw.main(["--mode", "daily", "--now", str(NOW)]) == 0
    assert rw.main(["--mode", "daily", "--now", str(NOW + 3600)]) == 0
    assert len(st.load_reports()) == 1
    assert rw.main(["--mode", "weekly", "--now", str(NOW + 3600)]) == 0  # inny typ - osobno
    assert len(st.load_reports()) == 2


def test_no_key_or_bad_answer_falls_back_to_rules(env, monkeypatch):
    assert rw.main(["--mode", "daily", "--now", str(NOW)]) == 0
    e = st.load_reports()[-1]
    assert e["source"] == "reguły" and e["f24"] in ("wzrost", "spadek", "bok") and e["text"].startswith("ETH:")
    assert "." not in e["text"].split("w 24 h")[0].split(":")[1]  # polski przecinek dziesiętny
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    calls = []
    assert rw.main(["--mode", "weekly", "--now", str(NOW)], http=_fake_http('{"f24": "w górę"}', calls)) == 0
    assert st.load_reports()[-1]["source"] == "reguły"


def test_parse_analysis_validation():
    a = rw.parse_analysis("```json\n" + GOOD + "\n```")
    assert a["conf"] == "umiarkowana"
    with pytest.raises(ValueError):
        rw.parse_analysis('{"f24": "bok", "f7": "x", "text": "a"}')
    with pytest.raises(ValueError):
        rw.parse_analysis("brak json")
    assert rw.parse_analysis(GOOD.replace("umiarkowana", "ogromna"))["conf"] == "niska"


def test_auto_mode_is_weekly_on_polish_sunday():
    import datetime as dt

    sunday = dt.datetime(2026, 11, 29, 19, 40, tzinfo=rc.TZ).timestamp()
    assert rw.resolve_mode("auto", sunday) == "weekly"
    assert rw.resolve_mode("auto", sunday - D) == "daily"
    assert rw.resolve_mode("daily", sunday) == "daily"


def test_reports_page_has_entries_outcomes_and_safe_json(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")
    eth = [[(NOW - (200 - i) * H) * 1000, 2000.0 - i] for i in range(201)]  # spadek
    entries = [
        {"id": "a", "type": "daily", "ts": int(NOW - 5 * D), "time": "10.11.2026 19:40", "price": 1900.0,
         "f24": "spadek", "f7": "spadek", "conf": "niska", "signal": "SHORT", "text": "x </script> y", "stage": "s"},
        {"id": "b", "type": "weekly", "ts": int(NOW - H), "time": "15.11.2026 19:40", "price": 1801.0,
         "f24": "bok", "f7": "spadek", "conf": "niska", "signal": "SHORT", "text": "t", "stage": "s"},
    ]
    bs.build_site([], market_history={"coins": {"ETH": eth}}, reports=entries)
    html = (tmp_path / "site" / "reports.html").read_text(encoding="utf-8")
    assert html.startswith("<!doctype html>") and "x <\\/script> y" in html and "</script> y" not in html
    data = json.loads(html.split("const DATA = ", 1)[1].split(";\nconst ARROW", 1)[0])
    a, b = data["entries"]
    assert a["outcome"]["24h"]["status"] == "trafiona" and a["outcome"]["7d"]["status"] == "czeka"
    assert b["outcome"]["24h"]["status"] == "czeka"
    assert data["score"]["24h"]["rozliczone"] == 1 and data["asOf"]
    # strona główna linkuje do archiwum
    assert 'href="./reports.html"' in (tmp_path / "site" / "index.html").read_text(encoding="utf-8")


def test_reports_page_without_archive(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")
    bs.build_site([])
    data_part = (tmp_path / "site" / "reports.html").read_text(encoding="utf-8").split("const DATA = ", 1)[1]
    assert data_part.startswith('{"entries":[],"score":null')


def test_report_card_uses_todays_claude_entry_from_archive(tmp_path, monkeypatch):
    monkeypatch.setattr(rc, "load", lambda repo: _raw())
    (tmp_path / "data").mkdir()
    arch = tmp_path / "data" / "reports.json"
    seen = {}
    monkeypatch.setattr(rc, "render", lambda s, out: seen.update(s))
    arch.write_text(json.dumps([{"type": "daily", "ts": NOW - H, "source": "reguły", "f24": "bok", "f7": "bok"}]))
    assert rc.main(["--repo", str(tmp_path), "--now", str(NOW), "--from-archive"]) == 3
    arch.write_text(json.dumps([{"type": "daily", "ts": NOW - H, "source": "Claude (m)", "stage": "s", "text": "t",
                                 "f24": "bok", "f7": "spadek", "conf": "niska", "track": "x"}]))
    assert rc.main(["--repo", str(tmp_path), "--now", str(NOW), "--from-archive"]) == 0
    assert seen["analysis"]["f7"] == "spadek"
    assert rc.main(["--repo", str(tmp_path), "--now", str(NOW + 5 * H), "--from-archive"]) == 3  # za stary
