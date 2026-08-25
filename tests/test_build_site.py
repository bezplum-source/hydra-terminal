"""Testy generowania statycznej strony (`live/build_site.py`) z historii
świec — bez żadnej sieci ani prawdziwego RPC, tylko poprawność złożenia
HTML z szablonu + danych."""

from __future__ import annotations

import json

from live import build_site as bs


def _sample_candles():
    return [
        {
            "block": 100249,
            "price": 2000.0,
            "signal": "HOLD",
            "composite": 0.0,
            "indGoodShort": 0.5,
            "indGoodLong": 0.5,
            "indBadShort": 0.5,
            "indBadLong": 0.5,
            "goodBuyers": 0,
            "goodSellers": 0,
            "badBuyers": 0,
            "badSellers": 0,
            "pool": 0,
            "active": 0,
            "tracked": 10,
            "time": "01.01.2026, 12:00",
        },
        {
            "block": 100499,
            "price": 2100.0,
            "signal": "LONG",
            "composite": 0.3,
            "indGoodShort": 0.7,
            "indGoodLong": 0.6,
            "indBadShort": 0.4,
            "indBadLong": 0.45,
            "goodBuyers": 5,
            "goodSellers": 1,
            "badBuyers": 1,
            "badSellers": 4,
            "pool": 6,
            "active": 11,
            "tracked": 15,
            "time": "01.01.2026, 13:00",
        },
        {
            "block": 100749,
            "price": 2050.0,
            "signal": "LONG",
            "composite": 0.2,
            "indGoodShort": 0.65,
            "indGoodLong": 0.6,
            "indBadShort": 0.42,
            "indBadLong": 0.46,
            "goodBuyers": 4,
            "goodSellers": 2,
            "badBuyers": 2,
            "badSellers": 3,
            "pool": 6,
            "active": 11,
            "tracked": 16,
            "time": "01.01.2026, 14:00",
        },
    ]


def test_build_streaks_groups_consecutive_same_signal_and_orders_newest_first():
    candles = _sample_candles()
    streaks = bs._build_streaks(candles)
    # HOLD (1 swieca) i LONG (2 swiece) -> 2 streaki, najnowszy (LONG) pierwszy
    assert len(streaks) == 2
    assert streaks[0]["signal"] == "LONG"
    assert streaks[0]["startBlock"] == 100499
    assert streaks[0]["endBlock"] == 100749
    assert streaks[1]["signal"] == "HOLD"


def test_build_streaks_hold_pct_is_none_not_treated_as_long():
    # Faza "NEUTRAL dead-zone": HOLD ("NEUTRALNY") nie ma aktywnej pozycji,
    # wiec liczenie wyniku nie ma sensu - pct=None, NIE direction=+1 jak LONG
    # (dawne zachowanie mylaco liczylo HOLD tak, jakby to byl LONG).
    candles = [
        {**_sample_candles()[0], "signal": "HOLD", "price": 2000.0, "block": 1},
        {**_sample_candles()[0], "signal": "HOLD", "price": 2100.0, "block": 2},
    ]
    streaks = bs._build_streaks(candles)
    assert len(streaks) == 1
    assert streaks[0]["signal"] == "HOLD"
    assert streaks[0]["pct"] is None


def test_build_streaks_short_pct_is_signal_result_not_raw_price_change():
    # Cena ROŚNIE w trakcie SHORT -> to jest STRATA dla sygnału (pct musi być
    # ujemny), mimo że surowa zmiana kursu jest dodatnia. To był bug zgłoszony
    # przez uzytkownika: strona pokazywała +0.39% (kurs) miejsce -0.39% (wynik).
    candles = [
        {**_sample_candles()[0], "signal": "SHORT", "price": 2445.81, "block": 1},
        {**_sample_candles()[0], "signal": "SHORT", "price": 2455.42, "block": 2},
    ]
    streaks = bs._build_streaks(candles)
    assert len(streaks) == 1
    assert streaks[0]["signal"] == "SHORT"
    assert streaks[0]["pct"] == -0.39


def test_build_streaks_long_pct_still_matches_raw_price_change():
    # LONG zarabia, gdy cena rośnie -> znak zostaje bez zmian (kontrola, że
    # fix dla SHORT nie zepsuł LONG).
    candles = [
        {**_sample_candles()[0], "signal": "LONG", "price": 2000.0, "block": 1},
        {**_sample_candles()[0], "signal": "LONG", "price": 2100.0, "block": 2},
    ]
    streaks = bs._build_streaks(candles)
    assert len(streaks) == 1
    assert streaks[0]["pct"] == 5.0


def test_build_site_writes_valid_html_with_embedded_data(tmp_path, monkeypatch):
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)

    bs.build_site(_sample_candles())

    out = site_dir / "index.html"
    assert out.exists()
    html = out.read_text(encoding="utf-8")

    assert html.startswith("<!doctype html>")
    assert "__DATA_JSON__" not in html
    assert "<script>" in html and "</script>" in html

    # Wyciagnij wstrzykniety JSON i zweryfikuj, ze to poprawne dane.
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    assert len(data["candles"]) == 3
    assert data["candles"][-1]["price"] == 2050.0
    assert len(data["streaks"]) == 2


def test_build_site_caps_display_candles(tmp_path, monkeypatch):
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)
    monkeypatch.setattr(bs, "MAX_DISPLAY_CANDLES", 2)

    bs.build_site(_sample_candles())

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    # tylko ostatnie 2 z 3 powinny trafic na strone (limit wyswietlania)
    assert len(data["candles"]) == 2
    assert data["candles"][0]["block"] == 100499
    assert data["candles"][1]["block"] == 100749


def test_build_site_empty_history_does_not_crash(tmp_path, monkeypatch):
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)

    bs.build_site([])

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    assert data["candles"] == []
    assert data["streaks"] == []


# =====================================================================
# Faza "wiarygodna świeżość" - DATA.meta.lastRunUtc (zgłoszenie użytkownika:
# chip świeżości na stronie pokazywał np. "30 min temu" zaraz po realnej
# przerwie ~2h w aktualizacjach, bo liczył się z `latest.ts` - znacznika
# czasu BLOKU, nie z tego, kiedy automatyzacja faktycznie ostatnio zadziałała)
# =====================================================================


def test_build_site_embeds_meta_last_run_utc_when_provided(tmp_path, monkeypatch):
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)

    bs.build_site(_sample_candles(), meta={"lastRunUtc": "2026-08-25T07:16:52+00:00"})

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    assert data["meta"]["lastRunUtc"] == "2026-08-25T07:16:52+00:00"


def test_build_site_meta_defaults_to_empty_dict_when_not_provided(tmp_path, monkeypatch):
    # Wywolania bez `meta` (np. stare wywolania, albo scoring_state jeszcze
    # bez `updated_at_utc`) NIE moga sie wywalic - front-end (renderFreshness)
    # ma wlasny fallback na `latest.ts`, wiec pusty słownik jest bezpieczny.
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)

    bs.build_site(_sample_candles())

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    assert data["meta"] == {}
