"""Generuje `site/index.html` z historii świec (`data/candles_history.json`)
i szablonu `live/template.html` — ten sam design co artefakt "Hydra
Terminal" na claude.ai (patrz projekt Claude, dokument
`hydrav2-frontend.md`, sekcja "Design system v2").

Osobny, mały moduł — celowo odseparowany od silnika sygnału
(`hydra_signals`), zgodnie z pkt. 4 sekcji "Co dalej" w głównym README.
"""

from __future__ import annotations

import datetime
import json
import sys
from pathlib import Path

ROOT_FOR_IMPORT = Path(__file__).resolve().parent.parent
if str(ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(ROOT_FOR_IMPORT))

from hydra_signals import strategies as strat  # noqa: E402
from live import market as mkt  # noqa: E402

MAX_DISPLAY_CANDLES = 500

# Karta "Rynek kontraktow ETH" (funding rate + open interest z Hyperliquid,
# czysto informacyjna - nie wplywa na sygnal). Okno wykresow w dniach, oraz
# stawka bazowa funding Hyperliquid: 1,25e-05 NA GODZINE (0,00125%/h, ok. 10,95%
# rocznie) - w ~65% godzin funding stoi dokladnie na tej wartosci, wiec to ona
# jest "norma" odniesienia dla opisu slownego i linii przerywanej na wykresie.
DERIVS_DISPLAY_DAYS = 30
FUNDING_BASELINE_RATE = 1.25e-05
HOURS_PER_YEAR = 8760.0
_HOUR_MS = 3_600_000
_DAY_MS = 86_400_000


def _funding_state(apr_pct: float) -> str:
    """Slowny stan funding wg rocznej stopy w % (patrz `FUNDING_BASELINE_RATE`):
    ujemny = shorty placa longom; ponizej normy; norma (okolice 11%); podwyzszony
    = longi placa wiecej niz zwykle; wysoki. Progi czysto opisowe (nie sa
    sygnalem handlowym)."""
    if apr_pct < 0:
        return "negative"
    if apr_pct < 5:
        return "low"
    if apr_pct <= 20:
        return "neutral"
    if apr_pct <= 50:
        return "elevated"
    return "high"


def _build_derivs_summary(derivs_history: dict | None) -> dict | None:
    """Zbija `data/derivs_history.json` do malego slownika dla frontu (karta
    "Rynek kontraktow ETH"): biezacy funding (rocznie w %) + jego slowny stan,
    open interest (ETH, USD, zmiana 24h), oraz serie do dwoch wykresow z
    ostatnich `DERIVS_DISPLAY_DAYS` dni. Zwraca None, gdy nie ma ZADNYCH danych
    funding (front pokazuje wtedy "zbieramy dane"). Liczy WYLACZNIE z podanej
    historii (zadnego zegara scian), wiec jest deterministyczne i testowalne."""
    if not derivs_history:
        return None
    funding = derivs_history.get("funding") or []
    snaps = derivs_history.get("snapshots") or []
    if not funding and not snaps:
        return None

    last_f = funding[-1] if funding else None
    last_s = snaps[-1] if snaps else None
    as_of = max(x["t"] for x in (last_f, last_s) if x is not None)

    # Biezacy funding: migawka (stopa trwajacego interwalu) jest swiezsza niz
    # ostatni wiersz historii (interwal zamkniety), jesli jest od niego nowsza.
    cur_rate = None
    cur_t = None
    if last_s is not None and last_s.get("funding") is not None and (last_f is None or last_s["t"] >= last_f["t"]):
        cur_rate, cur_t = last_s["funding"], last_s["t"]
    elif last_f is not None:
        cur_rate, cur_t = last_f["rate"], last_f["t"]
    funding_out = None
    if cur_rate is not None:
        apr = cur_rate * HOURS_PER_YEAR * 100.0
        funding_out = {
            "rate": cur_rate,
            "apr": round(apr, 2),
            "state": _funding_state(apr),
            "t": cur_t,
        }

    oi_out = None
    if last_s is not None and last_s.get("oi") is not None:
        mark = last_s.get("mark")
        change = None
        target = last_s["t"] - _DAY_MS
        older = [x for x in snaps if abs(x["t"] - target) <= 3 * _HOUR_MS and x.get("oi")]
        if older:
            ref = min(older, key=lambda x: abs(x["t"] - target))
            if ref is not last_s:
                change = round((last_s["oi"] / ref["oi"] - 1.0) * 100.0, 2)
        oi_out = {
            "eth": round(last_s["oi"], 1),
            "usd": round(last_s["oi"] * mark, 0) if mark else None,
            "change24h": change,
            "t": last_s["t"],
        }

    cut = as_of - DERIVS_DISPLAY_DAYS * _DAY_MS
    funding_series = [
        [r["t"], round(r["rate"] * HOURS_PER_YEAR * 100.0, 2)] for r in funding if r["t"] >= cut
    ]
    oi_series = [
        [x["t"], round(x["oi"] * x["mark"], 0)]
        for x in snaps
        if x["t"] >= cut and x.get("oi") is not None and x.get("mark")
    ]
    return {
        "asOf": as_of,
        "baselineApr": round(FUNDING_BASELINE_RATE * HOURS_PER_YEAR * 100.0, 2),
        "funding": funding_out,
        "oi": oi_out,
        "fundingSeries": funding_series,
        "oiSeries": oi_series,
    }

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = Path(__file__).resolve().parent / "template.html"
SITE_DIR = ROOT / "site"


def _build_streaks(candles: list[dict], signal_key: str = "signal") -> list[dict]:
    """Grupuje kolejne świece o tym samym sygnale w okresy (streaks),
    najnowszy pierwszy — identyczna logika jak w `build_real_data_v2.py`
    użytym do ręcznego zbudowania pierwszych wersji dashboardu.

    `signal_key` — Faza 2 (front-end) "Long term (30d)": ta sama funkcja
    wywoływana drugi raz z `signal_key="signalLt"` buduje równoległą listę
    `streaksLt` dla zakładki Long term (patrz `build_site()` niżej) — bez
    tego front-end nie mógłby zbudować tabeli "Historia sygnałów"/podsumowania
    wynik-winrate dla tego toru samą zmianą pól per świeca (grupowanie w
    okresy to logika, nie pojedyncze odwołanie do pola). `price`/`block`/
    `time` NIE mają odpowiednika "Lt" (to ta sama cena/blok/czas świecy,
    niezależnie od tego, który tor klasyfikacji ją czyta) — więc te trzy
    pola czytane są zawsze pod zwykłą nazwą, tylko `signal_key` się zmienia."""
    streaks: list[dict] = []
    i = 0
    while i < len(candles):
        j = i
        sig = candles[i][signal_key]
        while j + 1 < len(candles) and candles[j + 1][signal_key] == sig:
            j += 1
        start, last = candles[i], candles[j]
        # MOMENT WYJSCIA (zmiana 2026-10-22, zgloszenie uzytkownika: "po
        # pierwszej swiecy" - podsumowanie wyniku/win rate bylo zle): pozycja
        # zamyka sie w chwili, gdy sygnal SIE ZMIENIA, czyli na PIERWSZEJ
        # swiecy z innym sygnalem (`candles[j + 1]`), a nie na ostatniej
        # swiecy, ktora jeszcze miala dany sygnal (`last`). Poprzednia
        # konwencja (wyjscie na `last`) pomijala ruch ceny, ktory wlasnie
        # spowodowal zmiane sygnalu, i systematycznie zanizala wynik (na
        # prawdziwych danych 2026-10-22: 13/10 i -7.6% zamiast 15/8 i -1.8%).
        # Dla OTWARTEGO streaka (ostatni w historii, `j + 1` nie istnieje)
        # nie ma jeszcze swiecy zmiany - wyjsciem jest najnowsza swieca
        # ("wynik na teraz", niezrealizowany), a `closed` = False.
        closed = j + 1 < len(candles)
        end = candles[j + 1] if closed else last
        # Wynik WYGRANEJ/PRZEGRANEJ sygnalu, nie surowa zmiana kursu: dla LONG
        # wzrost ceny = zysk (znak bez zmian), dla SHORT wzrost ceny = strata
        # (znak odwrocony) - bo SHORT zarabia, gdy cena SPADA. Bez tego SHORT
        # trafiony w dol wygladal na stronie jak sukces (zielony +), a to byla
        # w rzeczywistosci pomylka sygnalu.
        # HOLD ("NEUTRALNY", Faza "NEUTRAL dead-zone") NIE jest ani LONG, ani
        # SHORT - nie ma aktywnej pozycji, wiec liczenie "wyniku" w ogole nie
        # ma sensu (poprzednio HOLD dostawal direction=+1, czyli byl
        # traktowany jak LONG - mylaca resztka z czasow, gdy HOLD byl tylko
        # teoretycznym stanem startowym i nigdy realnie nie wystepowal w
        # historii). `pct=None` - front-end renderuje to jako "-", nie 0.00%.
        direction = -1 if sig == "SHORT" else 1 if sig == "LONG" else None
        pct = (
            round(direction * (end["price"] - start["price"]) / start["price"] * 100, 2)
            if direction is not None and start["price"]
            else None
        )
        streaks.append(
            {
                "signal": sig,
                "startBlock": start["block"],
                "startPrice": start["price"],
                "endBlock": end["block"],
                "endPrice": end["price"],
                "pct": pct,
                "startTime": start["time"],
                "endTime": end["time"],
                "closed": closed,
            }
        )
        i = j + 1
    streaks.reverse()
    return streaks


# Faza "dzienny bilans GOOD/BAD" (2026-09-17, zgloszenie uzytkownika: "a moze
# zrobmy od tego momentu od ktorego mamy dane. WAZNE - to musza byc prawdziwe
# dane, nie zmyslaj") - tylko te pola sa potrzebne do zbudowania karty
# "Dzienny bilans GOOD/BAD" (patrz buildDailyBalanceSeries() w
# live/template.html). Celowo WLASNA, osobna lista swiec ZAMIAST reuzycia
# "candles" (ktore MAX_DISPLAY_CANDLES tnie do ostatnich 500, zeby nie
# rozdymac wykresu ceny/tabeli historii) - uzytkownik poprosil o pelna,
# realna historie "od tego momentu od ktorego mamy dane", a candles_history.json
# NIGDY nie jest przycinane (patrz state.py) - wiec ta karta musi czytac PELNA
# `candles_history`, nie `display_candles`. Trzymajac tylko ~24 liczbowe pola
# per swieca (zamiast ~50+ pol pelnej swiecy) rozmiar strony rosnie dużo
# wolniej niz gdyby po prostu wyslac cala historie z "candles".
DAILY_BALANCE_FIELDS = (
    "ts",
    "goodBuyers", "goodSellers", "badBuyers", "badSellers",
    "baseGoodBuyers", "baseGoodSellers", "baseBadBuyers", "baseBadSellers",
    "perpGoodBuyers", "perpGoodSellers", "perpBadBuyers", "perpBadSellers",
    "goodBuyUsd", "goodSellUsd", "badBuyUsd", "badSellUsd",
    "baseGoodBuyUsd", "baseGoodSellUsd", "baseBadBuyUsd", "baseBadSellUsd",
    "perpGoodBuyUsd", "perpGoodSellUsd", "perpBadBuyUsd", "perpBadSellUsd",
)


def _build_daily_balance_candles(candles_history: list[dict]) -> list[dict]:
    # `{k: c[k] for k in DAILY_BALANCE_FIELDS if k in c}` — pola nieobecne w
    # starszych świecach (np. sprzed fazy Base/Hyperliquid/Usd) są PO PROSTU
    # POMIJANE, nie wypełniane zerem tutaj — front-end (JS, `c.pole || 0`)
    # sam decyduje, że brak pola = 0, ten sam wzorzec graceful degradation co
    # reszta strony.
    return [
        {k: c[k] for k in DAILY_BALANCE_FIELDS if k in c}
        for c in candles_history
        if "ts" in c
    ]


# Widok "od poczatku" w karcie "Wyscig strategii": te same reguly gry na
# papierze odegrane na CALEJ zapisanej historii Hydry (od pierwszej swiecy,
# 23.08.2026). Decyzja Jakuba 14.11.2026: "licz od poczatku istnienia hydry.
# ZAWSZE - wszystko" (wczesniej od 12.09). Tylko orientacyjnie - to historia,
# na ktorej wybieralismy strategie, wiec nie jest dowodem na przyszlosc.
RACE_CURVE_POINTS = 400


def _race_view(race: dict) -> dict:
    out = {}
    for name, book in race.get("strategies", {}).items():
        summary = strat.summarize_book(book)
        curve = [[p[0], p[2]] for p in book.get("curve", []) if p[0] is not None]
        summary["curve"] = strat.downsample_curve(curve, RACE_CURVE_POINTS)
        out[name] = summary
    return out


def _pullback_series(trend: list[str], candles: list[dict]) -> list[str]:
    """Sygnaly strategii "Trend + wejscie z portfeli" po kolei (zalezy od
    wlasnej pozycji). Swieca z bledna cena nie zmienia pozycji - tak jak w
    samym wyscigu, ktory ja pomija."""
    out: list[str] = []
    pos = strat.HOLD
    for t, c, ok in zip(trend, candles, strat.price_flags(candles)):
        if ok:
            pos = strat.pullback_signal(pos, t, c.get("composite"))
        out.append(pos)
    return out


def _build_race_summary(race_state: dict | None, candles_history: list[dict]) -> dict:
    """Dane karty "Wyscig strategii": wyscig na zywo (od dnia wdrozenia, z
    `data/race_state.json`) i orientacyjna powtorka tych samych regul na
    calej historii od pierwszej swiecy ze znacznikiem czasu."""
    live = None
    if race_state and race_state.get("strategies"):
        live = {
            "start": race_state.get("start"),
            "strategies": _race_view(race_state),
        }

    lookback = None
    hist = [c for c in candles_history if c.get("price")]
    if hist:
        trend = strat.trend_series(hist)
        start_i = next((i for i, c in enumerate(hist) if c.get("ts") is not None), None)
        if start_i is not None:
            sub = hist[start_i:]
            modes = strat.mode_series(hist)
            hydra_sigs = [
                c.get("signalHydra", c.get("signal", "HOLD")) if c.get("signalSource") in ("trend14", "trend_wallets") else c.get("signal", "HOLD")
                for c in sub
            ]
            replay = strat.replay_race(
                sub,
                {
                    "trend14": trend[start_i:],
                    # przed przelaczeniem `signal` BYL sygnalem portfeli; po
                    # przelaczeniu ten sam sygnal jest w `signalHydra`
                    "hydra": hydra_sigs,
                    "eth": ["LONG"] * len(sub),
                    # Etap 2: pola BTC sa na swiecach (dopelnione wstecz z
                    # historii Hyperliquid); brak danych = None = bez zmiany pozycji
                    "btc14": [c.get("btcSignal") for c in sub],
                    "ethbtc14": [mkt.combined_signal(t, c.get("btcSignal")) for t, c in zip(trend[start_i:], sub)],
                    # Etap 3: tryb rynku liczony od nowa z cen (ta sama funkcja co w pipeline)
                    "hybrid": [strat.hybrid_signal(t, h, m[1]) for t, h, m in zip(trend[start_i:], hydra_sigs, modes[start_i:])],
                    "pullback": _pullback_series(trend[start_i:], sub),
                    # Etap 5: tylko tam, gdzie bufor transakcji ma dane (None = brak gry)
                    "skilled7": [c.get("signalSkilled") for c in sub],
                    "antiweak7": [c.get("signalVsUnskilled") for c in sub],
                },
            )
            lookback = {
                "fromTime": sub[0].get("time"),
                "fromPrice": sub[0]["price"],
                "strategies": _race_view(replay),
            }
    return {"live": live, "lookback": lookback, "feePerSidePct": strat.FEE_PER_SIDE * 100}


MARKET_DISPLAY_DAYS = 30


def _build_market_summary(market_history: dict | None) -> dict | None:
    """Karta "BTC i ETH/BTC" (Etap 2 planu "trend"): biezacy trend BTC i
    stosunku ETH/BTC + serie z ostatnich `MARKET_DISPLAY_DAYS` dni (wartosc i
    srednia 14 dni) do dwoch wykresow. None, gdy brak danych o BTC."""
    if not market_history or not (market_history.get("coins") or {}).get("BTC"):
        return None
    lk = mkt.build_lookups(market_history)

    def view(series: mkt.SeriesTrend, nd: int) -> dict | None:
        if not series.t:
            return None
        i = len(series.t) - 1
        cut = series.t[i] - MARKET_DISPLAY_DAYS * 86400
        j = next(k for k, t in enumerate(series.t) if t >= cut)
        return {
            "value": round(series.v[i], nd),
            "sma": round(series.sma[i], nd) if series.sma[i] is not None else None,
            "distPct": round(series.dist[i], 2) if series.dist[i] is not None else None,
            "signal": series.sig[i] if series.sma[i] is not None else None,
            "t": int(series.t[i]),
            "series": [[int(series.t[k]), round(series.v[k], nd)] for k in range(j, i + 1)],
            "smaSeries": [
                [int(series.t[k]), round(series.sma[k], nd)] for k in range(j, i + 1) if series.sma[k] is not None
            ],
        }

    return {"btc": view(lk["BTC"], 2), "ethBtc": view(lk["ETHBTC"], 6)}


def build_site(
    candles_history: list[dict],
    meta: dict | None = None,
    derivs_history: dict | None = None,
    race_state: dict | None = None,
    market_history: dict | None = None,
) -> None:
    display_candles = candles_history[-MAX_DISPLAY_CANDLES:]
    # Streaki (tabela "Historia sygnalow" + podsumowanie wynik/win rate) sa
    # budowane z PELNEJ historii, NIE z `display_candles` (naprawa
    # 2026-10-22): limit MAX_DISPLAY_CANDLES jest tylko dla wykresu ceny
    # (rozmiar strony) - uzyty do streakow ucinal je do ostatnich ~17 dni
    # (strona pokazywala 8 zagran zamiast 23 od resetu, a pierwszy streak w
    # oknie byl uciety w polowie, wiec jego wynik tez bywal bledny). Streaki
    # to male obiekty (~130 sztuk na ~1700 swiec), wiec rozmiar strony rosnie
    # pomijalnie.
    streaks = _build_streaks(candles_history) if candles_history else []
    daily_balance_candles = _build_daily_balance_candles(candles_history)

    # Faza "Long term (30d)" front-end (Faza 2, patrz też trackView()/
    # getCandles() w live/template.html) — Faza 1 (backend) została wdrożona
    # PÓŹNIEJ niż istniejąca historia świec, więc starsze świece nie mają w
    # ogóle pola "signalLt" (nie `null` — po prostu nieobecne). Filtrujemy do
    # świec, które faktycznie już je mają (świeca ma "signalLt" wtedy i tylko
    # wtedy, gdy ma KOMPLETNY zestaw pól toru Lt — patrz candle w
    # run_incremental.py, wszystkie dopisywane razem, bezwarunkowo) — dokładnie
    # ten sam zbiór, jaki front-end wyznacza samodzielnie w getCandles() dla
    # track="lt". Zanim ta faza ma za sobą własną historię, `lt_candles`
    # (a więc i `streaksLt`) będzie po prostu puste — front-end renderuje to
    # jako czytelny stan "zbieramy historię tego toru", nie jako błąd.
    # Tak samo jak `streaks` wyzej: z PELNEJ historii (nie z `display_candles`).
    lt_candles = [c for c in candles_history if "signalLt" in c]
    streaks_lt = _build_streaks(lt_candles, signal_key="signalLt") if lt_candles else []

    # Faza "wiarygodna swiezosc" (zgloszenie uzytkownika: chip swiezosci na
    # stronie pokazywal np. "30 min temu" zaraz po przerwie ~2h w
    # aktualizacjach) - `meta` to miejsce na fakty o SAMYM PRZEBIEGU
    # automatyzacji (np. `lastRunUtc`, prawdziwy zegar sciany zapisany PRZEZ
    # SKRYPT w momencie zakonczenia), odrebne od danych o swiecach/streakach.
    # Front-end (template.html::renderFreshness) korzysta z tego zamiast z
    # `latest.ts` (znacznik czasu BLOKU, ktory - z powodu mechaniki domykania
    # okien - zawsze wyglada "swiezo" niezaleznie od tego, jak dawno
    # faktycznie uruchomil sie workflow). Domyslnie pusty slownik - stary
    # front-end/stare dane bez tego pola nadal dzialaja (graceful fallback
    # w JS na `latest.ts`).
    data = {
        "candles": display_candles,
        "streaks": streaks,
        "streaksLt": streaks_lt,
        "dailyBalanceCandles": daily_balance_candles,
        "meta": meta or {},
        # Karta "Rynek kontraktow ETH" (funding + open interest) - czysto
        # informacyjna; None, gdy jeszcze brak danych (front pokazuje stan
        # "zbieramy dane").
        "derivs": _build_derivs_summary(derivs_history),
        # Etap 1 planu "trend": karta "Wyscig strategii".
        "race": _build_race_summary(race_state, candles_history),
        # Etap 2 planu "trend": karta "BTC i ETH/BTC".
        "market": _build_market_summary(market_history),
    }
    data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    if "__DATA_JSON__" not in template:
        raise RuntimeError("Szablon nie zawiera placeholdera __DATA_JSON__")
    html = template.replace("__DATA_JSON__", data_json)

    marker = "</style>"
    idx = html.find(marker)
    if idx == -1:
        raise RuntimeError("Szablon nie zawiera znacznika </style> — nie da się złożyć pełnego dokumentu HTML")
    head_tail = html[: idx + len(marker)]
    body_part = html[idx + len(marker):]

    full_html = (
        "<!doctype html>\n<html lang=\"pl\">\n<head>\n"
        "<meta charset=\"UTF-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
        + head_tail
        + "\n</head>\n<body>\n"
        + body_part
        + "\n</body>\n</html>\n"
    )

    SITE_DIR.mkdir(parents=True, exist_ok=True)
    (SITE_DIR / "index.html").write_text(full_html, encoding="utf-8")


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    from live import state as st

    last_run_utc = st.load_scoring_state().get("updated_at_utc")
    stats_reset_from_block = st.load_stats_reset_state().get("reset_from_block")
    build_site(
        st.load_candles_history(),
        meta={
            "lastRunUtc": last_run_utc,
            "statsResetFromBlock": stats_reset_from_block,
        },
        derivs_history=st.load_derivs_history(),
        race_state=st.load_race_state(),
        market_history=st.load_market_history(),
    )
    print(f"site/index.html zaktualizowany ({len(st.load_candles_history())} świec w historii).")
