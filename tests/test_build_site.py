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


# =====================================================================
# Faza 2 (front-end) "Long term (30d)" - `streaksLt`, drugi (równoległy)
# tor streaków dla zakładki Long term (patrz live/template.html,
# getStreaks()/trackView()). `_build_streaks(signal_key=...)` jest tą samą
# funkcją co dla toru Main - tylko parametr się zmienia - więc testy niżej
# sprawdzają głównie samą parametryzację i graceful degradation dla świec
# sprzed wdrożenia Fazy 1 (backend), które nie mają w ogóle pola "signalLt".
# =====================================================================


def test_build_streaks_accepts_custom_signal_key():
    # Ta sama funkcja co dla toru Main, tylko czyta inne pole per świeca -
    # `signal_key="signalLt"` zamiast domyślnego "signal".
    candles = [
        {**_sample_candles()[0], "signalLt": "SHORT", "block": 1},
        {**_sample_candles()[0], "signalLt": "SHORT", "block": 2},
        {**_sample_candles()[0], "signalLt": "LONG", "block": 3},
    ]
    streaks = bs._build_streaks(candles, signal_key="signalLt")
    assert len(streaks) == 2
    assert streaks[0]["signal"] == "LONG"
    assert streaks[1]["signal"] == "SHORT"


def test_build_site_streaks_lt_empty_when_no_candle_has_signal_lt(tmp_path, monkeypatch):
    # Rzeczywisty stan "dzień 0" Fazy 2: historia świec sprzed wdrożenia Fazy 1
    # (backend) nie ma w ogóle pola "signalLt" (nie `null` - nieobecne) -
    # `streaksLt` musi być wtedy pustą listą, nie wywalać się KeyError-em na
    # `candles[i][signal_key]` (patrz _build_streaks). Front-end renderuje to
    # jako czytelny stan "zbieramy historię tego toru", nie błąd.
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)

    bs.build_site(_sample_candles())  # żadna świeca nie ma "signalLt"

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    assert data["streaksLt"] == []


def test_build_site_streaks_lt_only_covers_candles_with_signal_lt(tmp_path, monkeypatch):
    # Mieszana historia (realistyczny stan tuż po wdrożeniu Fazy 1 backend):
    # starsze świece bez "signalLt" w ogóle, nowsze już z pełnym zestawem pól
    # Lt - `streaksLt` musi objąć WYŁĄCZNIE te drugie, dokładnie ten sam zbiór
    # świec, jaki front-end wyznacza samodzielnie w getCandles() dla track="lt".
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)
    candles = [
        {**_sample_candles()[0]},  # brak "signalLt" - sprzed Fazy 1 backend
        {**_sample_candles()[1], "signalLt": "LONG"},
        {**_sample_candles()[2], "signalLt": "LONG"},
    ]

    bs.build_site(candles)

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    assert len(data["streaksLt"]) == 1
    assert data["streaksLt"][0]["signal"] == "LONG"
    assert data["streaksLt"][0]["startBlock"] == candles[1]["block"]
    assert data["streaksLt"][0]["endBlock"] == candles[2]["block"]
    # Tor Main pozostaje niezmieniony (3 świece, jak przed ta faza) - Faza 2
    # dokłada streaksLt OBOK istniejącego "streaks", nie zamiast niego.
    assert len(data["streaks"]) == 2
