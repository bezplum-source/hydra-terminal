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


# =====================================================================
# Faza "dzienny bilans GOOD/BAD" (2026-09-17) - `dailyBalanceCandles`,
# osobna, WŁASNA lista świec dla karty "Dzienny bilans GOOD/BAD" w
# live/template.html (patrz obszerny komentarz przy DAILY_BALANCE_FIELDS w
# build_site.py). Dwie własności krytyczne dla zgłoszenia użytkownika ("to
# muszą być prawdziwe dane, nie zmyślaj" / "zróbmy od tego momentu od
# którego mamy dane"): (1) czyta PEŁNĄ `candles_history`, NIE `display_candles`
# obcięte do MAX_DISPLAY_CANDLES - inaczej karta widziałaby mniej realnej
# historii niż faktycznie istnieje; (2) świece bez danego pola (starsze niż
# dana faza backendu) po prostu GO NIE MAJĄ w wyjściu - zero nigdy nie jest
# tu wpisywane przez backend, to front-end (JS, `c.pole || 0`) decyduje, że
# brak = 0.
# =====================================================================


def _sample_candle_with_ts(ts: int, **overrides) -> dict:
    base = {
        "ts": ts,
        "goodBuyers": 3,
        "goodSellers": 1,
        "badBuyers": 2,
        "badSellers": 4,
    }
    base.update(overrides)
    return base


def test_build_daily_balance_candles_keeps_only_relevant_fields():
    candles = [
        _sample_candle_with_ts(
            1_000,
            baseGoodBuyers=1,
            goodBuyUsd=123.45,
            # Pole spoza DAILY_BALANCE_FIELDS (np. "price") - nie powinno
            # trafić do wyjścia, żeby nie rozdymać payloadu strony.
            price=2000.0,
            block=42,
        ),
    ]
    out = bs._build_daily_balance_candles(candles)
    assert len(out) == 1
    assert out[0] == {
        "ts": 1_000,
        "goodBuyers": 3,
        "goodSellers": 1,
        "badBuyers": 2,
        "badSellers": 4,
        "baseGoodBuyers": 1,
        "goodBuyUsd": 123.45,
    }
    assert "price" not in out[0]
    assert "block" not in out[0]


def test_build_daily_balance_candles_skips_candles_without_ts():
    # Bardzo stare świece (sprzed pola "ts") nie mają w ogole tego klucza -
    # muszą być pominięte, nie wywalać KeyError/None-em jako dzień.
    candles = [
        {"goodBuyers": 1, "goodSellers": 1, "badBuyers": 1, "badSellers": 1},
        _sample_candle_with_ts(2_000),
    ]
    out = bs._build_daily_balance_candles(candles)
    assert len(out) == 1
    assert out[0]["ts"] == 2_000


def test_build_daily_balance_candles_omits_fields_missing_on_older_candles():
    # Świeca sprzed fazy Base/Hyperliquid/Usd - te klucze są NIEOBECNE, nie
    # `0` - front-end sam robi `|| 0` (graceful degradation, ten sam wzorzec
    # co "signalLt").
    candles = [_sample_candle_with_ts(3_000)]
    out = bs._build_daily_balance_candles(candles)
    assert "baseGoodBuyers" not in out[0]
    assert "goodBuyUsd" not in out[0]
    assert "perpBadSellUsd" not in out[0]


def test_build_daily_balance_candles_uses_full_history_not_capped_display_candles(tmp_path, monkeypatch):
    # Właściwość krytyczna dla "zróbmy od tego momentu od którego mamy dane":
    # `dailyBalanceCandles` MUSI objąć całą historię, nawet gdy
    # MAX_DISPLAY_CANDLES obcina "candles" (i przez to price-chart/tabelę
    # historii) do dużo krótszego okna.
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)
    monkeypatch.setattr(bs, "MAX_DISPLAY_CANDLES", 1)
    # "signal"/"price"/"block" - wymagane przez _build_streaks() na
    # display_candles (osobna logika od dailyBalanceCandles, ale build_site()
    # woła obie na tej samej wejściowej liście świec).
    candles = [
        _sample_candle_with_ts(t, signal="HOLD", price=2000.0, block=t, time="01.01.2026, 12:00")
        for t in (1_000, 2_000, 3_000)
    ]

    bs.build_site(candles)

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    # "candles" (price-chart) obcięte do 1, ale dailyBalanceCandles widzi
    # wszystkie 3 - to jest cały sens tej fazy.
    assert len(data["candles"]) == 1
    assert len(data["dailyBalanceCandles"]) == 3
    assert [c["ts"] for c in data["dailyBalanceCandles"]] == [1_000, 2_000, 3_000]


def test_build_site_empty_history_gives_empty_daily_balance_candles(tmp_path, monkeypatch):
    site_dir = tmp_path / "site"
    monkeypatch.setattr(bs, "SITE_DIR", site_dir)

    bs.build_site([])

    html = (site_dir / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    end = html.index(";", start)
    data = json.loads(html[start:end])
    assert data["dailyBalanceCandles"] == []


# =====================================================================
# 2026-10-22: moment wyjscia streaka + streaki z PELNEJ historii
# (zgloszenie uzytkownika: podsumowanie "Historii sygnalow" zle liczy
# ilosc zagran, win rate i wynik; wyjscie "po pierwszej swiecy")
# =====================================================================


def _c(block, price, signal, time="t"):
    return {**_sample_candles()[0], "block": block, "price": price, "signal": signal, "time": time}


def test_closed_streak_exits_at_first_candle_of_next_signal_not_at_own_last_candle():
    candles = [
        _c(1, 2000.0, "LONG", "t1"),
        _c(2, 2100.0, "LONG", "t2"),  # ostatnia swieca z LONG
        _c(3, 2050.0, "HOLD", "t3"),  # pierwsza swieca po zmianie = wyjscie
    ]
    streaks = bs._build_streaks(candles)
    long_streak = streaks[1]  # najnowszy pierwszy: [HOLD, LONG]
    assert long_streak["signal"] == "LONG"
    assert long_streak["endPrice"] == 2050.0
    assert long_streak["endBlock"] == 3
    assert long_streak["endTime"] == "t3"
    assert long_streak["pct"] == 2.5  # (2050-2000)/2000, NIE 5.0
    assert long_streak["closed"] is True


def test_closed_short_streak_exit_uses_flip_candle_and_sign_flip():
    candles = [
        _c(1, 2000.0, "SHORT"),
        _c(2, 1900.0, "SHORT"),
        _c(3, 2100.0, "LONG"),  # cena odskoczyla w gore na swiecy zmiany
    ]
    short_streak = bs._build_streaks(candles)[1]
    assert short_streak["signal"] == "SHORT"
    assert short_streak["endPrice"] == 2100.0
    assert short_streak["pct"] == -5.0  # SHORT: cena wzrosla 2000 -> 2100 = strata
    # dawna konwencja (wyjscie na ostatniej swiecy SHORT, 1900) dalaby +5.0
    assert short_streak["closed"] is True


def test_open_streak_is_measured_to_latest_candle_and_marked_not_closed():
    candles = [
        _c(1, 2000.0, "HOLD"),
        _c(2, 2000.0, "LONG"),
        _c(3, 2060.0, "LONG"),
    ]
    open_streak = bs._build_streaks(candles)[0]
    assert open_streak["signal"] == "LONG"
    assert open_streak["endPrice"] == 2060.0
    assert open_streak["endBlock"] == 3
    assert open_streak["pct"] == 3.0
    assert open_streak["closed"] is False


def test_hold_streak_still_has_no_pct_and_closes_at_next_candle():
    candles = [_c(1, 2000.0, "HOLD"), _c(2, 2010.0, "HOLD"), _c(3, 2020.0, "SHORT")]
    hold = bs._build_streaks(candles)[1]
    assert hold["signal"] == "HOLD" and hold["pct"] is None
    assert hold["endBlock"] == 3 and hold["closed"] is True


def _many_candles(n):
    # naprzemiennie LONG, LONG, HOLD, HOLD -> jeden streak co 2 swiece
    out = []
    for k in range(n):
        sig = "LONG" if (k // 2) % 2 == 0 else "HOLD"
        out.append(_c(1000 + k, 2000.0 + k, sig, f"t{k}"))
    return out


def test_build_site_streaks_cover_full_history_not_only_display_candles(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")
    history = _many_candles(bs.MAX_DISPLAY_CANDLES + 200)
    bs.build_site(history)
    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    start = html.index("const DATA = ") + len("const DATA = ") if "const DATA = " in html else None
    assert start is not None
    data, _ = json.JSONDecoder().raw_decode(html[start:])
    assert len(data["candles"]) == bs.MAX_DISPLAY_CANDLES  # wykres nadal przyciety
    assert len(data["streaks"]) == len(bs._build_streaks(history))
    assert len(data["streaks"]) > len(bs._build_streaks(history[-bs.MAX_DISPLAY_CANDLES:])) - 1
    # najstarszy streak z calej historii (nie ucieta polowka z okna wykresu)
    assert data["streaks"][-1]["startBlock"] == history[0]["block"]


def test_build_site_streaks_lt_also_cover_full_history(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")
    history = _many_candles(bs.MAX_DISPLAY_CANDLES + 200)
    for c in history:
        c["signalLt"] = c["signal"]
    bs.build_site(history)
    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    start = html.index("const DATA = ") + len("const DATA = ")
    data, _ = json.JSONDecoder().raw_decode(html[start:])
    assert len(data["streaksLt"]) == len(bs._build_streaks(history, signal_key="signalLt"))
    assert data["streaksLt"][-1]["startBlock"] == history[0]["block"]


# ---------------------------------------------------------------------------
# Karta "Rynek kontraktow ETH" (funding + open interest) - podsumowanie dla
# frontu liczone z data/derivs_history.json, czysto informacyjne.
# ---------------------------------------------------------------------------

_H = 3_600_000
_D = 86_400_000
_T0 = 1_800_000_000_000


def _derivs_history(hours=24 * 40, rate=1.25e-05, with_snaps=True):
    h = {"version": 1, "funding": [], "snapshots": []}
    h["funding"] = [{"t": _T0 + i * _H, "rate": rate, "premium": 0.0} for i in range(hours)]
    if with_snaps:
        h["snapshots"] = [
            {"t": _T0 + i * _H + 600_000, "funding": rate, "oi": 1000.0 + i, "mark": 2000.0, "premium": 0.0}
            for i in range(0, hours, 1)
        ]
    return h


def test_derivs_summary_is_none_without_any_data():
    assert bs._build_derivs_summary(None) is None
    assert bs._build_derivs_summary({}) is None
    assert bs._build_derivs_summary({"funding": [], "snapshots": []}) is None


def test_derivs_summary_current_values_state_and_oi_change():
    s = bs._build_derivs_summary(_derivs_history(hours=72))
    # stawka bazowa 1,25e-05/h -> ok. 10,95% rocznie, stan "neutral"
    assert s["funding"]["apr"] == 10.95 and s["funding"]["state"] == "neutral"
    assert s["baselineApr"] == 10.95
    last = 71
    assert s["oi"]["eth"] == 1000.0 + last
    assert s["oi"]["usd"] == (1000.0 + last) * 2000.0
    # zmiana 24h wzgledem migawki sprzed doby (71 - 24 = 47)
    assert s["oi"]["change24h"] == round(((1000.0 + 71) / (1000.0 + 47) - 1) * 100, 2)
    assert s["asOf"] == _T0 + 71 * _H + 600_000


def test_derivs_summary_uses_fresher_snapshot_rate_over_last_closed_interval():
    h = _derivs_history(hours=10, with_snaps=False)
    h["snapshots"] = [{"t": h["funding"][-1]["t"] + 1800_000, "funding": -3e-05, "oi": 5.0, "mark": 100.0}]
    s = bs._build_derivs_summary(h)
    assert s["funding"]["rate"] == -3e-05 and s["funding"]["state"] == "negative"
    # ale starsza migawka NIE przebija nowszego wiersza historii
    h["snapshots"][0]["t"] = h["funding"][-1]["t"] - 1800_000
    s = bs._build_derivs_summary(h)
    assert s["funding"]["rate"] == 1.25e-05


def test_derivs_summary_oi_change_is_none_without_old_enough_snapshot():
    h = _derivs_history(hours=10)
    s = bs._build_derivs_summary(h)
    assert s["oi"]["change24h"] is None and s["oi"]["eth"] is not None


def test_derivs_summary_without_snapshots_has_funding_but_no_oi():
    s = bs._build_derivs_summary(_derivs_history(hours=48, with_snaps=False))
    assert s["funding"] is not None and s["oi"] is None and s["oiSeries"] == []


def test_derivs_summary_series_cover_only_display_window():
    s = bs._build_derivs_summary(_derivs_history(hours=24 * 40))
    assert 24 * bs.DERIVS_DISPLAY_DAYS <= len(s["fundingSeries"]) <= 24 * bs.DERIVS_DISPLAY_DAYS + 1
    assert s["fundingSeries"][0][0] >= s["asOf"] - bs.DERIVS_DISPLAY_DAYS * _D
    assert all(len(p) == 2 for p in s["fundingSeries"]) and all(len(p) == 2 for p in s["oiSeries"])


def test_funding_state_thresholds():
    f = bs._funding_state
    assert f(-0.1) == "negative"
    assert f(0.0) == "low" and f(4.9) == "low"
    assert f(5.0) == "neutral" and f(10.95) == "neutral" and f(20.0) == "neutral"
    assert f(20.1) == "elevated" and f(50.0) == "elevated"
    assert f(50.1) == "high"


def test_build_site_embeds_derivs_summary_or_null(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")

    def embedded():
        html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
        marker = "const DATA = "
        start = html.index(marker) + len(marker)
        return json.loads(html[start : html.index(";", start)])

    bs.build_site(_sample_candles())
    assert embedded()["derivs"] is None  # brak danych: front pokazuje "zbieramy dane"

    bs.build_site(_sample_candles(), derivs_history=_derivs_history(hours=72))
    d = embedded()["derivs"]
    assert d["funding"]["state"] == "neutral" and d["oi"]["usd"] > 0 and len(d["fundingSeries"]) > 10


# ---------------------------------------------------------------------------
# Etap 1 planu "trend": dane karty "Wyscig strategii".
# ---------------------------------------------------------------------------

from hydra_signals import strategies as _strat  # noqa: E402

# stały punkt czasu dla danych testowych (12.09.2026 00:00 czasu polskiego);
# widok "od początku" i tak zaczyna się od pierwszej świecy historii
_LB = 1_789_164_000


def _race_candles():
    """30 dni historii godzinowej od 25 dni przed LOOKBACK_FROM; cena stała, potem
    rośnie. Pierwsza część z sygnałem portfeli w `signal`, ostatnie świece po
    przełączeniu (signalSource=trend14, portfele w `signalHydra`)."""
    out = []
    start = _LB - 25 * 86400
    for i in range(24 * 30):
        ts = start + i * 3600
        price = 2000.0 if ts < _LB else 2000.0 + (ts - _LB) / 3600
        c = {**_sample_candles()[0], "block": 1000 + i, "ts": ts, "price": price, "time": f"t{i}", "signal": "SHORT"}
        if i >= 24 * 29:
            c.update({"signalSource": "trend14", "signal": "LONG", "signalHydra": "HOLD"})
        out.append(c)
    return out


def test_race_summary_without_race_state_has_no_live_part_but_has_lookback():
    r = bs._build_race_summary(None, _race_candles())
    assert r["live"] is None
    lb = r["lookback"]
    assert lb["fromTime"] is not None and lb["fromPrice"] == 2000.0
    assert set(lb["strategies"]) == {"vote", "trend14", "hydra", "eth", "btc14", "ethbtc14", "hybrid", "pullback", "skilled7", "antiweak7", "taker24", "trendoi", "stables7", "trendstables", "trendcalm", "optskew"}
    # brak pól BTC na świecach -> strategie BTC nigdy nie wchodzą w pozycję
    assert lb["strategies"]["btc14"]["position"] == "HOLD" and lb["strategies"]["btc14"]["trades"] == 0
    # ETH rośnie od startu widoku wstecz -> kup i trzymaj na plusie, trend na plusie
    assert lb["strategies"]["eth"]["resultPct"] > 0
    assert lb["strategies"]["trend14"]["position"] == "LONG"
    # portfele: przed przełączeniem SHORT (z `signal`), po przełączeniu HOLD (z `signalHydra`)
    assert lb["strategies"]["hydra"]["position"] == "HOLD"
    assert lb["strategies"]["hydra"]["resultPct"] < 0
    assert r["feePerSidePct"] == _strat.FEE_PER_SIDE * 100


def test_race_summary_lookback_starts_at_first_candle_and_none_without_timestamps():
    candles = _race_candles()
    lb = bs._build_race_summary(None, candles)["lookback"]
    assert lb["fromTime"] == candles[0]["time"]  # "licz od początku istnienia Hydry"
    no_ts = [{**c, "ts": None} for c in candles]
    assert bs._build_race_summary(None, no_ts)["lookback"] is None
    assert bs._build_race_summary(None, [])["lookback"] is None


def test_race_summary_live_part_from_race_state_with_downsampled_curve():
    race = _strat.new_race(["trend14", "hydra", "eth"], start_block=1, start_time="t0", start_ts=_LB, start_price=100.0)
    for i in range(1000):
        _strat.race_step(race, {"block": 1 + i, "price": 100.0 + i * 0.1, "time": f"t{i}", "ts": _LB + i * 3600},
                         {"trend14": "LONG", "hydra": "SHORT", "eth": "LONG"})
    r = bs._build_race_summary(race, [])
    live = r["live"]
    assert live["start"]["block"] == 1
    assert live["strategies"]["trend14"]["resultPct"] > 0 > live["strategies"]["hydra"]["resultPct"]
    curve = live["strategies"]["eth"]["curve"]
    assert len(curve) <= bs.RACE_CURVE_POINTS + 1 and all(len(p) == 2 for p in curve)


def test_build_site_embeds_race_section(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")
    bs.build_site(_race_candles())
    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    data = json.loads(html[start : html.index(";", start)])
    assert data["race"]["live"] is None and data["race"]["lookback"]["strategies"]["eth"]["trades"] == 0


# ---------------------------------------------------------------------------
# Etap 2 planu "trend": karta "BTC i ETH/BTC" i strategie BTC w widoku wstecz.
# ---------------------------------------------------------------------------


def _market_history(hours=24 * 40, btc=lambda i: 50_000.0 + 5 * i, eth=lambda i: 2000.0):
    end = int(_LB * 1000) + 24 * 3600 * 1000 * 5
    start = end - (hours - 1) * 3_600_000
    return {
        "version": 1,
        "coins": {
            "BTC": [[start + i * 3_600_000, btc(i)] for i in range(hours)],
            "ETH": [[start + i * 3_600_000, eth(i)] for i in range(hours)],
        },
    }


def test_market_summary_none_without_btc_data():
    assert bs._build_market_summary(None) is None
    assert bs._build_market_summary({"coins": {"BTC": [], "ETH": []}}) is None


def test_market_summary_current_trend_and_30_day_series():
    m = bs._build_market_summary(_market_history())
    btc, ratio = m["btc"], m["ethBtc"]
    assert btc["signal"] == "LONG" and btc["distPct"] > 0 and btc["value"] == 50_000.0 + 5 * (24 * 40 - 1)
    assert ratio["signal"] == "SHORT"  # ETH stoi, BTC rośnie
    assert 24 * bs.MARKET_DISPLAY_DAYS <= len(btc["series"]) <= 24 * bs.MARKET_DISPLAY_DAYS + 1
    assert btc["series"][-1][0] == btc["t"] and 0 < len(btc["smaSeries"]) <= len(btc["series"])
    assert btc["smaSeries"][-1][0] == btc["t"]  # średnia liczona do ostatniej świecy


def test_lookback_btc_strategy_uses_candle_btc_signal():
    candles = _race_candles()
    for c in candles:
        c["btcSignal"] = "LONG"
    r = bs._build_race_summary(None, candles)["lookback"]["strategies"]
    assert r["btc14"]["position"] == "LONG" and r["btc14"]["resultPct"] > 0
    assert r["ethbtc14"]["position"] == "LONG"  # na końcu trend ETH też LONG


# ---------------------------------------------------------------------------
# Etap 3 planu "trend": hybryda i "trend + wejście z portfeli" w widoku wstecz.
# ---------------------------------------------------------------------------


def test_pullback_series_enters_only_when_wallets_buy_and_holds_through_trend():
    trend = ["LONG", "LONG", "LONG", "LONG", "SHORT"]
    comps = [0.0, 0.2, -0.5, -0.5, -0.5]
    candles = [{"price": 100.0 + i, "composite": v} for i, v in enumerate(comps)]
    assert bs._pullback_series(trend, candles) == ["HOLD", "LONG", "LONG", "LONG", "SHORT"]


def test_lookback_has_hybrid_and_pullback():
    lb = bs._build_race_summary(None, _race_candles())["lookback"]["strategies"]
    assert "hybrid" in lb and "pullback" in lb
    # w _race_candles cena rośnie prosto w górę po LOOKBACK_FROM -> tryb TREND -> hybryda gra z trendem
    assert lb["hybrid"]["position"] == "LONG"


# ---------------------------------------------------------------------------
# Etap 6 i 7 planu "trend": karta "Stablecoiny i opcje ETH".
# ---------------------------------------------------------------------------


def test_stables_summary_skips_the_last_partial_day_and_gives_changes():
    day0 = 1_789_000_000 // 86400 * 86400
    rows = [[day0 + i * 86400, 1000.0 + i] for i in range(40)]
    s = bs._build_stables_summary({"total": rows})
    assert s["day"] == rows[-2][0] and s["supplyUsd"] == rows[-2][1]
    assert s["change7dUsd"] == 7.0 and s["change30dPct"] > 0
    assert s["series"][0][0] >= s["day"] - 90 * 86400 and s["series"][-1] == [rows[-2][0], rows[-2][1]]
    assert bs._build_stables_summary(None) is None
    assert bs._build_stables_summary({"total": rows[:1]}) is None


def test_options_summary_dvol_with_average_and_latest_snapshot():
    t0 = 1_789_000_000_000
    dvol = [[t0 + i * 3_600_000, 60.0 + (i % 5)] for i in range(24 * 40)]
    snaps = [{"t": t0 + 24 * 40 * 3_600_000, "pcOi": 1.2, "skew": 3.5, "atmIv": 61.0}]
    o = bs._build_options_summary({"dvol": dvol, "snapshots": snaps})
    assert o["dvol"]["value"] == dvol[-1][1] and o["dvol"]["sma"] is not None
    assert o["dvol"]["series"][0][0] >= o["dvol"]["t"] - 30 * 86400
    assert o["snapshot"]["skew"] == 3.5 and o["skewSeries"] == [[snaps[0]["t"], 3.5]]
    assert o["skewBand"] > 0
    only_snap = bs._build_options_summary({"dvol": [], "snapshots": snaps})
    assert only_snap["dvol"] is None and only_snap["snapshot"]["pcOi"] == 1.2
    assert bs._build_options_summary(None) is None
    assert bs._build_options_summary({"dvol": [], "snapshots": []}) is None


def test_build_site_embeds_stables_and_options(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")
    day0 = 1_789_000_000 // 86400 * 86400
    bs.build_site(
        _sample_candles(),
        stables_history={"total": [[day0 + i * 86400, 1000.0 - i] for i in range(10)]},
        options_history={"dvol": [[1_789_000_000_000 + i * 3_600_000, 55.0] for i in range(30)], "snapshots": []},
    )
    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start = html.index(marker) + len(marker)
    data = json.loads(html[start : html.index(";", start)])
    assert data["stables"]["change7dPct"] < 0 and data["options"]["dvol"]["value"] == 55.0
    assert data["options"]["snapshot"] is None
