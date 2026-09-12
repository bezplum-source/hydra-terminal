"""Agregacja okienna (świece 250-blokowe) + wygładzanie EMA + decyzja
LONG/SHORT/HOLD.

To jest właściwy "silnik sygnału" - odpowiednik tego, co na hydra.trading
najwyraźniej odpalane jest co ~250 bloków (~1h) i produkuje linie
"weight" / "Candle" widoczne w wycieknietym debug-dumpie.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Iterable, List

from .models import Cohort, Side, Signal, Trade, WindowScore
from .wallets import classify_wallets, compute_wallet_stats


@dataclass
class ScoringConfig:
    window_blocks: int = 250  # ~1h przy ~14.4s/blok (Ethereum PoS)

    # Ile bloków historii bierzemy pod uwagę przy rankingu portfeli.
    # 250*24*7 = ok. 7 dni ruchomego okna reputacji (Faza "okno reputacji
    # 7 dni" 2026-09-01 - zgloszenie uzytkownika: sygnal LONG/SHORT trzyma
    # sie za krotko, bo klasyfikacja GOOD/BAD portfela liczyla sie z zaledwie
    # ostatnich 24h - przy tylko kilku-kilkunastu aktywnych sklasyfikowanych
    # portfelach na okno, tak krotkie okno reputacji dawalo bardzo hoslowy
    # (szumny) composite. Wydluzenie do 7 dni NIE zmienia mechanizmu decyzji
    # (SignalEngine/histereza zostaje bez zmian - to bylo juz empirycznie
    # sprawdzone jako niewiele dajace), tylko wejsciowe dane, na ktorych ten
    # mechanizm dziala. Poprzednia wartosc: 250*24 (~24h).
    classification_lookback_blocks: int = 250 * 24 * 7

    min_trades_for_classification: int = 5
    good_pct: float = 0.15
    bad_pct: float = 0.15

    # Filtr "dust" (zgloszenie uzytkownika 2026-08-25: "Proponuje, zeby brac
    # pod uwage portfele z min. 1000 dolarow. Czyli zeby odsiac dust.") -
    # doprecyzowane przez dwa pytania AskUserQuestion: (1) prog dotyczy
    # POJEDYNCZEJ TRANSAKCJI, nie lacznego wolumenu portfela - kazdy
    # pojedynczy swap z `Trade.notional_usd` (`price_usd * size_eth`)
    # ponizej tej wartosci jest CALKOWICIE pomijany; (2) filtr obowiazuje
    # symetrycznie na OBU torach - Uniswap (tu) i Hyperliquid ETH-PERP
    # (`hydra_signals.hyperliquid_wallets.HyperliquidScoringConfig.
    # min_trade_notional_usd`, ta sama nazwa i wartosc, zeby jedna zmiana
    # nie rozjechala sie cicho z druga). Stosowany w JEDNYM miejscu -
    # na wejsciu do `ScoringEngine.run()` nizej - zeby dust byl odsiany
    # identycznie zarowno przy klasyfikacji portfeli (GOOD/BAD), jak i przy
    # liczeniu aktywnosci biezacego okna (good_buyers/bad_sellers itd.).
    # WARTOSC STARTOWA (jak kazdy inny prog w tym projekcie) - podana wprost
    # przez uzytkownika, nie wynik optymalizacji/backtestu.
    min_trade_notional_usd: float = 1000.0

    # EMA - liczone w jednostkach "świec" (okien), nie bloków.
    ema_short_span: int = 3
    ema_long_span: int = 12

    # Wagi funkcji łączącej wskaźniki w jeden composite score.
    w_good_short: float = 1.0
    w_good_long: float = 0.5
    w_bad_short: float = 1.0
    w_bad_long: float = 0.5

    # Pasmo NEUTRALNE wokół zera - |composite| <= ten próg -> sygnał to
    # Signal.HOLD (wyświetlane na froncie jako "NEUTRALNY"), nie LONG/SHORT
    # na siłę. ZMIANA (Faza "NEUTRAL dead-zone", zgłoszona przez użytkownika
    # po zobaczeniu żywych danych): pierwotnie 0.0 ("natychmiastowe
    # przełączanie po znaku, bliżej odzwierciedlające częstotliwość zmian z
    # hydra.trading"), ale w praktyce na żywych danych composite bardzo
    # często oscyluje tuż wokół zera (np. +0.015, -0.014, +0.001) - z
    # threshold=0.0 KAŻDE takie przejście przez zero wymusza LONG albo SHORT,
    # co w "Historii sygnałów" produkowało mnóstwo bezsensownych,
    # jednoświecowych wpisów 0.00% (sygnał "otwarty i zamknięty" na tej samej
    # świecy, bo już następna znowu przeskakiwała na drugą stronę zera).
    # 0.2 to WARTOŚĆ STARTOWA (jak każdy inny próg w tym projekcie,
    # nieprzestrojona backtestem) - wybrana empirycznie na żywej historii 45
    # świec: redukuje liczbę zdegenerowanych jednoświecowych streaków LONG/
    # SHORT z 8/16 do 5/11, zamieniając resztę na uczciwe streaki NEUTRALNE,
    # bez eliminowania realnych, silniejszych wychyleń (0.3-1.5 w
    # zaobserwowanej historii).
    signal_threshold: float = 0.2

    # Ile KOLEJNYCH transakcji w JEDNĄ stronę portfel musi wykonać, zanim
    # następna transakcja w przeciwną stronę liczy się jako potwierdzony
    # "wallet flip" (Faza 3, brief regime-detection sekcja 9) - np. przy
    # wartości domyślnej 3: "SELL SELL SELL -> BUY" to flip, ale
    # "SELL SELL -> BUY" (streak tylko 2) już nie. WARTOŚĆ STARTOWA, nie
    # wynik optymalizacji - do przestrojenia dopiero po backteście (Faza 5),
    # analogicznie do progów w `regime.RegimeConfig`.
    min_flip_streak_trades: int = 3

    # =================================================================
    # Faza "wazenie wolumenem SPOT" (2026-09-11)
    # =================================================================
    # Zgloszenie uzytkownika: `SpotPoolEngine` (nizej) wazy KAZDY portfel
    # identycznie (1 portfel = 1 "glos"), niezaleznie od wielkosci jego
    # pozycji w oknie. Uzytkownik zapytany o pomysly na dokladniejsze
    # badanie wskazal wprost wazenie wolumenem, ale zaraz potem trafnie
    # zauwazyl ryzyko: jeden "wieloryb" moglby w ten sposob zdominowac caly
    # wynik jedna transakcja. Odpowiedz - DWIE warstwy ochrony na poziomie
    # pojedynczej transakcji (ponizej), TRZECIA na poziomie `SpotPoolEngine`
    # (`volume_weight_blend` nizej):
    #
    # (1) STLUMIENIE zamiast liniowej wagi - kazdy portfel wnosi
    #     sqrt(notional_usd), nie sam notional_usd. Transakcja za $1M wazy
    #     WIECEJ niz za $1000, ale ~32x, nie 1000x.
    # (2) TWARDY SUFIT na notional PRZED pierwiastkowaniem
    #     (`volume_weight_cap_notional_usd` nizej) - kazda transakcja
    #     powyzej tego progu wazy identycznie jak transakcja DOKLADNIE na
    #     progu.
    #
    # SWIADOMA DECYZJA (odejscie od doslownie omawianego w rozmowie pomyslu
    # "cap na percentylu wolumenu z OKNA"): przy tak malej probce jak u nas
    # (2-40 sklasyfikowanych portfeli na okno - patrz "okno reputacji 7 dni"
    # wyzej) percentyl liczony z SAMEGO OKNA bylby statystycznie
    # bezuzyteczny (przy 3 transakcjach w oknie "95 percentyl" to praktycznie
    # po prostu najwieksza z tych trzech - zaden realny sufit). Zamiast tego:
    # STALA dolarowa (jak `min_trade_notional_usd` - filtr dust - wyzej) -
    # prostsza, odporna na male probki, w tym samym stylu co kazdy inny prog
    # w projekcie. WARTOSC STARTOWA, nieprzestrojona backtestem.
    volume_weight_cap_notional_usd: float = 50_000.0

    # Waga toru "wazonego wolumenem" w SpotPoolEngine (patrz nizej) wzgledem
    # toru "liczba portfeli" (ten z natury odporny na wieloryby - jeden
    # portfel = jeden glos, niezaleznie od wielkosci jego pozycji). To
    # TRZECIA warstwa ochrony: nawet gdyby (1) stlumienie pierwiastkiem i
    # (2) twardy sufit powyzej nie wystarczyly, pojedyncza duza transakcja
    # moze co najwyzej PRZESUNAC finalny wynik (o co najwyzej ta wage), nigdy
    # go w pelni PRZEJAC - druga polowa (1-volume_weight_blend) zawsze
    # pochodzi z toru liczba-portfeli. WARTOSC STARTOWA (50/50), zaakceptowana
    # wprost przez uzytkownika (2026-09-11) jako punkt startowy do
    # ewentualnego przestrojenia pozniej.
    volume_weight_blend: float = 0.5


# =====================================================================
# Faza H2 (brief `hydrav2-hyperliquid-brief.md`) — blend composite_spot/perp
# =====================================================================

# Waga `composite_perp` w zblendowanej wartości - WARTOŚĆ STARTOWA (jak
# `signal_threshold`/`min_flip_streak_trades` powyżej), zaakceptowana wprost
# przez użytkownika 2026-08-24 jako punkt startowy do ewentualnego
# przestrojenia później (backtest albo obserwacja), nie wynik optymalizacji.
DEFAULT_PERP_WEIGHT = 0.5


def blend_composite(
    composite_spot: float,
    composite_perp: float | None,
    *,
    perp_weight: float = DEFAULT_PERP_WEIGHT,
) -> float:
    """Łączy istniejący `composite_spot` (ten moduł, dane Uniswap) z
    `composite_perp` (Faza H2 briefu Hyperliquid,
    `hydra_signals.hyperliquid_wallets.HyperliquidScoringEngine`) w jedną
    wartość - to ONA, wywołana z `live/run_incremental.py`, odtąd decyduje
    o `signal` (LONG/SHORT) pokazywanym w hero (patrz `decide_signal`
    niżej), zgodnie z decyzją użytkownika "od razu wpięte do głównego
    sygnału" (patrz brief, sekcja "Decyzja architektoniczna").

    `composite_perp=None` — dane z Hyperliquid jeszcze NIEDOJRZAŁE (za mało
    sklasyfikowanych portfeli, patrz
    `HyperliquidScoringConfig.min_classified_wallets_for_maturity`) albo
    Hyperliquid jeszcze w ogóle nic nie zebrał/nie sklasyfikował — zwraca
    WYŁĄCZNIE `composite_spot`, bez żadnej zmiany zachowania względem stanu
    sprzed Fazy H2. To jest zamierzone "graceful degradation" z briefu, NIE
    błąd: silnik nigdy nie staje się losowy/niezdefiniowany z powodu
    brakujących danych z nowego, dopiero rozgrzewającego się źródła —
    dokładnie ten sam wzorzec "BRAK DANYCH zamiast błędu", co przy regime
    (`hydra_signals.regime`).
    """
    if composite_perp is None:
        return composite_spot
    return (1.0 - perp_weight) * composite_spot + perp_weight * composite_perp


def _ema_step(current: float | None, new_value: float, span: int) -> float:
    """Jeden krok EMA - wydzielone z `ScoringEngine._update_ema` (Faza
    "wspólna pula SPOT" niżej), żeby `SpotPoolEngine` mógł używać DOKŁADNIE
    tego samego wzoru bez duplikowania go w drugim miejscu. Zachowanie
    `ScoringEngine._update_ema` bez zmian - to czysty refaktor (przeniesienie
    ciała funkcji), nie nowa logika."""
    alpha = 2.0 / (span + 1)
    if current is None:
        return new_value
    return alpha * new_value + (1 - alpha) * current


# =====================================================================
# Faza "wspólna pula SPOT" (2026-09-11) — Uniswap+Base w JEDNEJ puli
# =====================================================================


class SpotPoolEngine:
    """Zastępuje poprzedni mechanizm łączenia Uniswap+Base: "policz
    `composite_spot` (mainnet) i `composite_base` (Base) OSOBNO, każdy z
    WŁASNYM EMA liczonym z RATIO tego venue, potem zblenduj obie liczby
    stałą wagą 50/50" (`blend_composite` + `BASE_SPOT_WEIGHT`, Faza
    "integracja Base B1-B3").

    Zgłoszenie użytkownika (2026-09-11, po zobaczeniu żywych danych): Base
    miało w danym oknie zaledwie 7 sklasyfikowanych transakcji (1 GOOD
    buyer, 1 GOOD seller, 1 BAD buyer, 4 BAD sellers) wobec 31 po stronie
    Uniswapa (2+16+11+2) - a mimo to dostawał DOKŁADNIE tyle samo wagi
    (50%) w połączonej wartości "spot", co widać było w praktyce jako
    kafelek "Spot" pokazujący "Neutralnie", podczas gdy sam Uniswap
    wskazywałby SHORT. Rozwiązanie zaproponowane przez użytkownika: Uniswap
    i Base powinny być w TEJ SAMEJ puli decyzyjnej, nie w dwóch osobnych
    blendowanych stałą wagą.

    Zamiast osobnych EMA per venue: SUMUJEMY surowe liczniki
    (`good_buyers`/`good_sellers`/`bad_buyers`/`bad_sellers`) z obu torów w
    JEDNĄ połączoną pulę (patrz `update()` niżej), i dopiero z NIEJ liczymy
    JEDNO `good_ratio_raw`/`bad_ratio_raw`, przepuszczone przez JEDNO
    wspólne EMA - dokładnie ten sam wzór/te same stałe co
    `ScoringEngine.run()` (patrz `ScoringConfig.w_good_short` i sąsiednie
    pola, celowo używane WPROST, a nie duplikowane, żeby jedna zmiana progu
    nie rozjechała się cicho między dwoma miejscami).

    Efekt: waga każdego venue w wyniku jest teraz proporcjonalna do jego
    RZECZYWISTEJ aktywności w danym oknie, a nie do sztywnej stałej. Gdy
    Base nie wnosi żadnych sklasyfikowanych transakcji w oknie (0/0,
    najczęściej dlatego, że wywołujący celowo wyzerował liczniki - patrz
    `run_incremental.py`, sekcja "Base gate dojrzałości" - zanim je tu
    poda), pula naturalnie redukuje się do "czysty Uniswap" - bez żadnej
    osobnej bramki/specjalnego przypadku wewnątrz tej klasy, dokładnie tak
    samo jak `blend_composite(spot, None)` degradowało wcześniej do samego
    spot.

    Świadome uproszczenie: portfel handlujący W TEJ SAMEJ godzinie na OBU
    łańcuchach liczy się osobno w każdym z nich (nie nettujemy jego pozycji
    między łańcuchami) - Uniswap i Base mają kompletnie niekompatybilne,
    niezależne siatki blokowe (patrz obszerne komentarze w
    `run_incremental.py`), więc prawdziwe zdeduplikowanie wymagałoby
    wspólnych znaczników czasu zamiast numerów bloków - dużo większy
    refaktor, nieuzasadniony na tym etapie (rzadki przypadek: ten sam adres
    aktywny na obu łańcuchach w TEJ SAMEJ godzinie).

    Stan (cztery liczby EMA) wznawia się między uruchomieniami dokładnie
    tak samo jak `ScoringEngine`/`RegimeEngine` - patrz `export_state()`
    niżej i `live/state.py` (`load_spot_pool_state`/`save_spot_pool_state`).

    ROZSZERZENIE (Faza "wazenie wolumenem SPOT", 2026-09-11): oprocz toru
    "liczba portfeli" powyzej (jeden portfel = jeden glos, z natury odporny
    na wieloryby), silnik liczy TERAZ RÓWNOLEGLE drugi, niezalezny tor -
    "wazony wolumenem" - na WLASNYM, OSOBNYM zestawie 4 EMA (`_w` w nazwie
    pol nizej), z tych samych surowych skladnikow co pierwszy tor, ale
    zamiast liczby portfeli uzywa sum sqrt-capped wag (patrz
    `ScoringConfig.volume_weight_cap_notional_usd` i `ScoringEngine.run()`).
    Finalny wynik `update()` to `blend_composite(composite_liczba_portfeli,
    composite_wazony_wolumenem, perp_weight=cfg.volume_weight_blend)` -
    TRZECIE uzycie `blend_composite()` w tym module (po Fazie H2 i Fazie
    "integracja Base B1-B3" - historyczne, juz zastapione przez ta klase),
    zero nowej logiki blendowania. Domyslnie 50/50 - nawet gdyby tor
    wazony wolumenem zostal w pelni zdominowany przez jednego "wieloryba",
    moze on przesunac WYLACZNIE polowe finalnego wyniku, nigdy go w calosci
    przejac.

    GRACEFUL DEGRADATION (analogiczne do `blend_composite(spot, None)`):
    jesli wywolujacy w ogole NIE poda argumentow wagowych (`good_buy_weight`
    itd. pozostaja `None`, patrz `update()` nizej) - np. stary kod sprzed tej
    fazy, albo dowolny test pisany przed jej wprowadzeniem - silnik zwraca
    WYLACZNIE tor "liczba portfeli", DOKLADNIE tak jak przed ta faza. Zero
    ryzyka regresji istniejacych wywolan/testow.
    """

    def __init__(
        self,
        config: ScoringConfig | None = None,
        *,
        initial_ema: dict[str, float | None] | None = None,
    ) -> None:
        self.cfg = config or ScoringConfig()
        ema = initial_ema or {}
        self._ema_good_short: float | None = ema.get("good_short")
        self._ema_good_long: float | None = ema.get("good_long")
        self._ema_bad_short: float | None = ema.get("bad_short")
        self._ema_bad_long: float | None = ema.get("bad_long")

        # Tor "wazony wolumenem" (Faza "wazenie wolumenem SPOT") - OSOBNY
        # zestaw 4 EMA, bo dziala na INNEJ serii wejsciowej (ratio liczone z
        # wag sqrt-capped, nie z surowych liczb portfeli) - mieszanie ich w
        # jednym EMA nie mialoby sensu, to dwa rozne sygnaly az do finalnego
        # blendu w `update()`. Klucze stanu CELOWO inne niz tor liczba-
        # portfeli (`_weighted` w nazwie) - kazdy juz zapisany
        # `spot_pool_state.json` sprzed tej fazy (w tym na zywym repo) nie ma
        # jeszcze tych kluczy, wiec `.get()` zwraca `None` i ten tor startuje
        # "na zimno" (pierwsza obserwowana wartosc ratio od razu staje sie
        # EMA) - DOKLADNIE tak samo jak `ScoringEngine`/`SpotPoolEngine` przy
        # pierwszym uruchomieniu bez wczesniej zapisanego stanu. To bezpieczne
        # i oczekiwane - nie ma zadnej wczesniejszej historii wolumenowej do
        # odziedziczenia (w przeciwienstwie do migracji Base w tor liczba-
        # portfeli wyzej, gdzie celowo chcielismy bajt-w-bajt identycznosc
        # zachowania sprzed/po wdrozeniu).
        self._ema_good_short_w: float | None = ema.get("good_short_weighted")
        self._ema_good_long_w: float | None = ema.get("good_long_weighted")
        self._ema_bad_short_w: float | None = ema.get("bad_short_weighted")
        self._ema_bad_long_w: float | None = ema.get("bad_long_weighted")

        # Faza "diagnostyka wazenia wolumenem w UI" (2026-09-12) - zglosznie
        # uzytkownika "czy gdzies na stronie w UX bede widzial wagi?" - front-
        # end (karta Wallets, patrz live/template.html) chce pokazac OBA
        # skladowe composite OSOBNO, nie tylko juz zblendowany wynik
        # `update()` zwraca. Zamiast duplikowac logike liczenia EMA/composite
        # poza ta klase, `update()` zapamietuje tu swoj ostatni wynik
        # POSREDNI z kazdego z dwoch torow - wywolujacy (`run_incremental.py`)
        # odczytuje je zaraz PO wywolaniu `update()` i dokleja do rekordu
        # swiecy. CELOWO nie w `export_state()`/wznawialnym stanie - to czysto
        # diagnostyczny odczyt "ostatniej wartosci z tego wywolania", nie coś
        # co trzeba pamietac miedzy uruchomieniami procesu (dokladnie tak jak
        # `ScoringEngine` tez nie eksportuje kazdego posredniego wyniku, tylko
        # to, co niezbedne do wznowienia EMA).
        self.last_composite_counts: float | None = None
        self.last_composite_weighted: float | None = None

    def export_state(self) -> dict:
        return {
            "good_short": self._ema_good_short,
            "good_long": self._ema_good_long,
            "bad_short": self._ema_bad_short,
            "bad_long": self._ema_bad_long,
            "good_short_weighted": self._ema_good_short_w,
            "good_long_weighted": self._ema_good_long_w,
            "bad_short_weighted": self._ema_bad_short_w,
            "bad_long_weighted": self._ema_bad_long_w,
        }

    def update(
        self,
        *,
        good_buyers: int,
        good_sellers: int,
        bad_buyers: int,
        bad_sellers: int,
        good_buy_weight: float | None = None,
        good_sell_weight: float | None = None,
        bad_buy_weight: float | None = None,
        bad_sell_weight: float | None = None,
    ) -> float:
        """Jedno wywołanie na jedną nową świecę (w kolejności czasu!) -
        `good_buyers`/itd. to JUŻ POŁĄCZONE liczniki (Uniswap + Base tego
        okna, patrz wywołanie w `run_incremental.py`), nie surowe transakcje
        - ta klasa nie zna nic o blokach/transakcjach, tylko o gotowych
        licznikach z ilu portfeli kupowało/sprzedawało w danej kohorcie
        (`good_buyers`/itd.) i ile "wazonej wolumenem" masy to reprezentuje
        (`good_buy_weight`/itd., Faza "wazenie wolumenem SPOT" - już
        POŁĄCZONE sumy sqrt-capped wag Uniswap+Base tego okna, patrz
        `ScoringEngine.run()`).

        `good_buy_weight`/`good_sell_weight`/`bad_buy_weight`/
        `bad_sell_weight` pozostawione jako `None` (wszystkie cztery) ->
        wywołujący NIE dostarcza danych wolumenowych w ogóle (stary kod/test
        sprzed tej fazy) - silnik zwraca WYŁĄCZNIE tor "liczba portfeli",
        identycznie jak przed tą fazą (patrz docstring klasy, sekcja
        "graceful degradation")."""
        cfg = self.cfg
        good_total = good_buyers + good_sellers
        bad_total = bad_buyers + bad_sellers

        # Brak aktywnosci w danej kohorcie -> neutralne 0.5 (brak
        # przesuniecia), ten sam wzorzec co w ScoringEngine.run().
        good_ratio_raw = good_buyers / good_total if good_total > 0 else 0.5
        bad_ratio_raw = bad_buyers / bad_total if bad_total > 0 else 0.5

        self._ema_good_short = _ema_step(self._ema_good_short, good_ratio_raw, cfg.ema_short_span)
        self._ema_good_long = _ema_step(self._ema_good_long, good_ratio_raw, cfg.ema_long_span)
        self._ema_bad_short = _ema_step(self._ema_bad_short, bad_ratio_raw, cfg.ema_short_span)
        self._ema_bad_long = _ema_step(self._ema_bad_long, bad_ratio_raw, cfg.ema_long_span)

        composite_counts = (
            cfg.w_good_short * (self._ema_good_short - 0.5)
            + cfg.w_good_long * (self._ema_good_long - 0.5)
            - cfg.w_bad_short * (self._ema_bad_short - 0.5)
            - cfg.w_bad_long * (self._ema_bad_long - 0.5)
        )
        self.last_composite_counts = composite_counts

        if (
            good_buy_weight is None
            and good_sell_weight is None
            and bad_buy_weight is None
            and bad_sell_weight is None
        ):
            # Tor wazony wolumenem sie nie aktywowal w tym wywolaniu - nie ma
            # nic diagnostycznego do pokazania z tej strony (patrz front-end,
            # ktory chowa linie diagnostyczna, gdy to pole jest `None`).
            self.last_composite_weighted = None
            return composite_counts

        gbw = good_buy_weight or 0.0
        gsw = good_sell_weight or 0.0
        bbw = bad_buy_weight or 0.0
        bsw = bad_sell_weight or 0.0

        good_total_w = gbw + gsw
        bad_total_w = bbw + bsw
        good_ratio_w = gbw / good_total_w if good_total_w > 0 else 0.5
        bad_ratio_w = bbw / bad_total_w if bad_total_w > 0 else 0.5

        self._ema_good_short_w = _ema_step(self._ema_good_short_w, good_ratio_w, cfg.ema_short_span)
        self._ema_good_long_w = _ema_step(self._ema_good_long_w, good_ratio_w, cfg.ema_long_span)
        self._ema_bad_short_w = _ema_step(self._ema_bad_short_w, bad_ratio_w, cfg.ema_short_span)
        self._ema_bad_long_w = _ema_step(self._ema_bad_long_w, bad_ratio_w, cfg.ema_long_span)

        composite_weighted = (
            cfg.w_good_short * (self._ema_good_short_w - 0.5)
            + cfg.w_good_long * (self._ema_good_long_w - 0.5)
            - cfg.w_bad_short * (self._ema_bad_short_w - 0.5)
            - cfg.w_bad_long * (self._ema_bad_long_w - 0.5)
        )
        self.last_composite_weighted = composite_weighted

        return blend_composite(composite_counts, composite_weighted, perp_weight=cfg.volume_weight_blend)


def decide_signal(composite: float, *, threshold: float) -> Signal:
    """Ta sama reguła co wewnątrz `ScoringEngine.run` niżej
    (`composite > threshold -> LONG`, `< -threshold -> SHORT`, inaczej
    `Signal.HOLD` - patrz "Faza NEUTRAL dead-zone" przy `ScoringConfig.
    signal_threshold`) — wydzielona tutaj jako osobna, czysta funkcja, bo od
    Fazy H2 to ONA (wywołana na `composite` już ZBLENDOWANYM przez
    `blend_composite`) decyduje o polu `signal` w `candles_history.json`/
    hero, a NIE wewnętrzny sygnał liczony przez `ScoringEngine` (ten dalej
    istnieje i jest liczony wyłącznie z `composite_spot` — `live/
    run_incremental.py` zapisuje go teraz osobno pod kluczem
    `signalSpotOnly`, czysto diagnostycznie, patrz brief pkt.
    "Pełna przejrzystość w hero").

    ZMIANA (Faza "NEUTRAL dead-zone"): funkcja NIE trzyma już poprzedniego
    sygnału w paśmie wokół zera (dawny parametr `prev_signal`, usunięty) —
    zamiast "migotania" LONG<->SHORT albo sztucznego trzymania starej
    decyzji, wewnątrz pasma wprost zwraca `Signal.HOLD` ("NEUTRALNY" na
    froncie). Czysta funkcja bez stanu, w pełni zdeterminowana przez
    `composite`/`threshold`.
    """
    if composite > threshold:
        return Signal.LONG
    if composite < -threshold:
        return Signal.SHORT
    return Signal.HOLD


@dataclass
class SignalConfig:
    """Progi histerezy + potwierdzenia dla GŁÓWNEGO sygnału LONG/SHORT
    pokazywanego w hero (ten liczony na `composite` już ZBLENDOWANYM
    spot+perp - patrz `blend_composite` powyżej).

    Zgłoszenie użytkownika (2026-08-31): "Czemu on zmienia sygnał co każdy
    blok? [...] Tak jak robi to hydra.trading? Od 2 tygodni prawie jest tam
    sygnał LONG, a u nas zmienia się co chwilę." Zmierzone empirycznie na
    żywej historii (225 świec, `data/candles_history.json`): stary
    `decide_signal()` z pojedynczym symetrycznym progiem (`signal_threshold
    =0.2`, bez pamięci) dawał 90 przełączeń sygnału na 225 świec (15 w
    samych ostatnich 40) - `composite` bardzo często oscyluje tuż nad/pod
    progiem w obie strony w krótkich odstępach, dokładnie jak w przykładzie
    z pytania użytkownika.

    Rozwiązanie: DOKŁADNIE ten sam wzorzec, co już wdrożony i zaakceptowany
    `regime.RegimeEngine` (BULL/BEAR/NEUTRAL) - asymetryczna histereza
    wejście/wyjście + wymóg kilku kolejnych świec potwierdzających PRZED
    wejściem w LONG/SHORT z HOLD. WYJŚCIE z LONG/SHORT z powrotem do HOLD
    jest CELOWO natychmiastowe (bez potwierdzenia) - ta sama asymetria co w
    `RegimeConfig`: trzymanie pozycji, która już wyraźnie się skończyła, nie
    powinno być sztucznie przeciągane, tylko WEJŚCIE ma być ostrożne.
    Przejście LONG<->SHORT bezpośrednio (bez przejścia przez HOLD) jest
    architektonicznie niemożliwe - dokładnie jak BULL<->BEAR w RegimeEngine
    - co dodatkowo tłumi "migotanie" przy szybkiej zmianie znaku.

    WARTOŚCI STARTOWE (jak `signal_threshold`/`min_flip_streak_trades`
    powyżej i cała `RegimeConfig`) - wybrane EMPIRYCZNIE na tej samej
    żywej historii 225 świec (nie wynik optymalizacji/backtestu):
    `enter=0.35, exit=0.1, confirm=3` redukuje 90 przełączeń do 10 na całej
    historii (15 -> 4 w ostatnich 40 świecach) - podobny rząd wielkości
    redukcji, jak `signal_threshold=0.2` osiągnął w swoim czasie dla
    "dead-zone". Do przestrojenia dopiero po backteście (Faza 5), tak jak
    każdy inny próg w projekcie.
    """

    enter_threshold: float = 0.35
    exit_threshold: float = 0.1
    min_confirmation_periods: int = 3


class SignalEngine:
    """Stateful maszyna stanów HOLD/LONG/SHORT z histerezą i potwierdzeniem
    - architektura 1:1 skopiowana z `regime.RegimeEngine` (patrz
    `SignalConfig` wyżej po uzasadnienie). Wznawialna między osobnymi
    uruchomieniami procesu dokładnie tak samo jak `RegimeEngine`/
    `ScoringEngine` - patrz `export_state()`/`initial_state` niżej i
    `live/state.py` (`load_signal_state`/`save_signal_state`).

    Zastępuje dawne, BEZSTANOWE wywołanie `decide_signal()` jako źródło
    głównego pola `signal` w hero/`candles_history.json`
    (`live/run_incremental.py`). `decide_signal()` zostaje w kodzie
    niezmieniona - dalej używana m.in. przez `signalSpotOnly` liczone
    WEWNĄTRZ `ScoringEngine.run()` (czysto diagnostyczne, patrz tam), które
    świadomie NIE dostaje histerezy - to osobny, uboczny tor, nie ten,
    który steruje właściwym sygnałem."""

    def __init__(self, config: SignalConfig | None = None, *, initial_state: dict | None = None) -> None:
        self.cfg = config or SignalConfig()
        state = initial_state or {}
        self.signal: Signal = Signal(state.get("signal", "HOLD"))
        self.long_streak: int = state.get("long_streak", 0)
        self.short_streak: int = state.get("short_streak", 0)

    def export_state(self) -> dict:
        return {
            "signal": self.signal.value,
            "long_streak": self.long_streak,
            "short_streak": self.short_streak,
        }

    def process(self, composite: float) -> Signal:
        """Przetwarza JEDNĄ nową świecę (w kolejności czasu!), aktualizuje
        wewnętrzny stan, zwraca nowy `signal`. Kolejność ma znaczenie -
        przy wielu nowych świecach w jednym uruchomieniu (np. po dłuższej
        przerwie) trzeba wywołać to raz na każdą, po kolei - dokładnie jak
        `RegimeEngine.process_candle`."""
        cfg = self.cfg

        if self.signal is Signal.LONG:
            if composite < cfg.exit_threshold:
                self.signal = Signal.HOLD
            self.long_streak = 0
            self.short_streak = 0
        elif self.signal is Signal.SHORT:
            if composite > -cfg.exit_threshold:
                self.signal = Signal.HOLD
            self.long_streak = 0
            self.short_streak = 0
        else:  # HOLD
            long_condition = composite > cfg.enter_threshold
            short_condition = composite < -cfg.enter_threshold
            self.long_streak = self.long_streak + 1 if long_condition else 0
            self.short_streak = self.short_streak + 1 if short_condition else 0

            if self.long_streak >= cfg.min_confirmation_periods:
                self.signal = Signal.LONG
                self.long_streak = 0
                self.short_streak = 0
            elif self.short_streak >= cfg.min_confirmation_periods:
                self.signal = Signal.SHORT
                self.long_streak = 0
                self.short_streak = 0

        return self.signal


class ScoringEngine:
    """Stateful silnik: EMA i poprzedni sygnał są trzymane między oknami.

    Domyślna konstrukcja (bez `initial_*`) zachowuje się dokładnie tak jak
    wcześniej — EMA startuje "na zimno" (None), sygnał startowy to HOLD,
    zbiór śledzonych portfeli pusty. Parametry `initial_*` istnieją, żeby
    silnik dało się **wznowić** między osobnymi uruchomieniami procesu (np.
    cykliczny job w GitHub Actions) bez utraty ciągłości EMA i bez zerowania
    licznika portfeli śledzonych "od zawsze" - patrz `export_state()` niżej
    oraz `live/run_incremental.py`, który z tego korzysta.
    """

    def __init__(
        self,
        config: ScoringConfig | None = None,
        *,
        initial_ema: dict[str, float | None] | None = None,
        initial_prev_signal: Signal | None = None,
        initial_total_tracked: Iterable[str] | None = None,
        initial_wallet_flip_state: dict[str, dict] | None = None,
    ) -> None:
        self.cfg = config or ScoringConfig()
        ema = initial_ema or {}
        self._ema_good_short: float | None = ema.get("good_short")
        self._ema_good_long: float | None = ema.get("good_long")
        self._ema_bad_short: float | None = ema.get("bad_short")
        self._ema_bad_long: float | None = ema.get("bad_long")
        self._prev_signal: Signal = initial_prev_signal or Signal.HOLD
        # Zbiór WSZYSTKICH portfeli kiedykolwiek widzianych - narastający
        # między wywołaniami `run()`, a przy wznowieniu - między osobnymi
        # uruchomieniami procesu (patrz `initial_total_tracked`).
        self.total_tracked: set[str] = set(initial_total_tracked or ())

        # Wallet Flip (Faza 3) - stan PER PORTFEL, ograniczony do dwóch
        # małych pól ("ostatni kierunek", "długość bieżącego ciągu") -
        # dokładnie jak `total_tracked` wyżej, rośnie z liczbą portfeli, ale
        # wolno (te same adresy Ethereum ~42 znaki, ten sam rząd wielkości
        # co `wallets_seen.txt`). Musi wznawiać się MIĘDZY uruchomieniami
        # procesu tak samo jak EMA - bez tego każde uruchomienie widziałoby
        # każdy portfel "po raz pierwszy" i nigdy nie wykryłoby żadnego
        # flipa (patrz pętla w `run()` niżej: pierwsza widziana transakcja
        # portfela tylko zakłada streak, nigdy nie liczy się jako flip).
        flip_state = initial_wallet_flip_state or {}
        self._wallet_flip_last_side: dict[str, str] = {
            w: s["side"] for w, s in flip_state.items()
        }
        self._wallet_flip_streak: dict[str, int] = {
            w: s["streak"] for w, s in flip_state.items()
        }

    def export_state(self) -> dict:
        """Serializowalny (do JSON) zrzut stanu EMA/sygnału - do zapisania na
        dysk i podania jako `initial_ema`/`initial_prev_signal` przy
        kolejnym uruchomieniu procesu. `total_tracked` (zbiór portfeli) i
        stan Wallet Flip eksportują się osobno (mogą być duże) - patrz
        `self.total_tracked` i `export_wallet_flip_state()`."""
        return {
            "good_short": self._ema_good_short,
            "good_long": self._ema_good_long,
            "bad_short": self._ema_bad_short,
            "bad_long": self._ema_bad_long,
            "prev_signal": self._prev_signal.value,
        }

    def export_wallet_flip_state(self) -> dict[str, dict]:
        """Serializowalny (do JSON) zrzut stanu Wallet Flip (Faza 3) - jeden
        wpis na KAŻDY portfel, który kiedykolwiek zawarł transakcję:
        `{wallet: {"side": "BUY"|"SELL", "streak": N}}`. Do podania jako
        `initial_wallet_flip_state` przy kolejnym uruchomieniu procesu -
        patrz `live/state.py` (`load_wallet_flip_state`/
        `save_wallet_flip_state`) i `live/run_incremental.py`."""
        return {
            w: {"side": self._wallet_flip_last_side[w], "streak": self._wallet_flip_streak[w]}
            for w in self._wallet_flip_last_side
        }

    def _update_ema(self, current: float | None, new_value: float, span: int) -> float:
        # Refaktor (Faza "wspólna pula SPOT") - cialo funkcji przeniesione do
        # modulowej `_ema_step` powyzej, zeby `SpotPoolEngine` mogl uzywac
        # DOKLADNIE tego samego wzoru bez duplikacji. Zachowanie bez zmian.
        return _ema_step(current, new_value, span)

    def run(
        self,
        trades: Iterable[Trade],
        price_at_block: Callable[[int], float],
        *,
        history_trades: Iterable[Trade] = (),
    ) -> List[WindowScore]:
        """Liczy WindowScore dla okien pokrytych przez `trades`.

        `history_trades` to opcjonalny, dodatkowy zbiór WCZEŚNIEJ już
        zaobserwowanych transakcji (np. z bufora trzymanego na dysku między
        uruchomieniami) - używany WYŁĄCZNIE jako kontekst do klasyfikacji
        portfeli w oknie `classification_lookback_blocks` (żeby okno tuż po
        wznowieniu procesu miało tę samą "rozgrzaną" pulę GOOD/BAD, co przy
        ciągłym działaniu). Sam nie generuje nowych `WindowScore` - tylko
        `trades` definiuje, które okna zostaną w tym wywołaniu policzone.
        Analogicznie NIE zasila stanu Wallet Flip (Faza 3) - ten wznawia się
        wyłącznie przez `initial_wallet_flip_state`/`export_wallet_flip_state`,
        tak samo jak EMA nie jest odtwarzana z `history_trades` - patrz
        `__init__`.

        Numeracja okien jest liczona względem STAŁEJ, globalnej siatki
        (`block // window_blocks`), a nie względem pierwszego bloku w tym
        wywołaniu - inaczej granice świec przesuwałyby się przy każdym
        wznowieniu procesu z innym pierwszym blokiem w porcji danych.
        """
        cfg = self.cfg

        # Filtr dust (patrz ScoringConfig.min_trade_notional_usd) - stosowany
        # TU, na samym wejsciu do run(), zanim cokolwiek inne zobaczy
        # `trades`/`history_trades` - jedno miejsce filtrowania dla OBU
        # zastosowan (biezace okno I lookback klasyfikacji ponizej), zeby
        # nie dalo sie przypadkiem przepuscic dust przez jedna sciezke, a
        # przez druga juz nie.
        trades_sorted = sorted(
            (t for t in trades if t.notional_usd >= cfg.min_trade_notional_usd),
            key=lambda t: t.block,
        )
        if not trades_sorted:
            return []

        buckets: dict[int, list[Trade]] = defaultdict(list)
        for t in trades_sorted:
            idx = t.block // cfg.window_blocks
            buckets[idx].append(t)

        all_trades_so_far: list[Trade] = [
            t for t in history_trades if t.notional_usd >= cfg.min_trade_notional_usd
        ]
        results: list[WindowScore] = []

        for idx in sorted(buckets):
            window_trades = buckets[idx]
            window_end_block = (idx + 1) * cfg.window_blocks - 1

            all_trades_so_far.extend(window_trades)
            self.total_tracked.update(t.wallet for t in window_trades)

            lookback_start = window_end_block - cfg.classification_lookback_blocks
            lookback_trades = [t for t in all_trades_so_far if t.block > lookback_start]

            stats = compute_wallet_stats(
                lookback_trades, min_trades=cfg.min_trades_for_classification
            )
            classify_wallets(stats, good_pct=cfg.good_pct, bad_pct=cfg.bad_pct)

            good_wallets = {w for w, s in stats.items() if s.cohort is Cohort.GOOD}
            bad_wallets = {w for w, s in stats.items() if s.cohort is Cohort.BAD}

            net_direction: dict[str, float] = defaultdict(float)
            for t in window_trades:
                net_direction[t.wallet] += t.size_eth if t.side is Side.BUY else -t.size_eth

            # Notional per portfel w tym oknie (Faza "wazenie wolumenem
            # SPOT") - suma notional_usd WSZYSTKICH transakcji portfela w
            # oknie, bez wzgledu na kierunek ("ile kapitalu portfel poruszyl
            # w tym oknie"). Uzywane WYLACZNIE do zwazenia jego kierunkowego
            # "glosu" (net_direction powyzej) - to co innego niz
            # good_trader_pressure/bad_trader_pressure nizej, ktore licza
            # przewage wolumenu PER KOHORTA, nie per portfel.
            wallet_notional: dict[str, float] = defaultdict(float)
            for t in window_trades:
                wallet_notional[t.wallet] += t.notional_usd

            good_buyers = good_sellers = bad_buyers = bad_sellers = 0
            good_buy_weight = good_sell_weight = 0.0
            bad_buy_weight = bad_sell_weight = 0.0
            for wallet, net in net_direction.items():
                if net == 0:
                    continue
                # Stlumienie + twardy sufit (patrz ScoringConfig.
                # volume_weight_cap_notional_usd) - cap na notional PRZED
                # pierwiastkowaniem, zeby pojedyncza ekstremalnie duza
                # transakcja nie mogla zdominowac wagi.
                weight = min(wallet_notional[wallet], cfg.volume_weight_cap_notional_usd) ** 0.5
                if wallet in good_wallets:
                    if net > 0:
                        good_buyers += 1
                        good_buy_weight += weight
                    else:
                        good_sellers += 1
                        good_sell_weight += weight
                elif wallet in bad_wallets:
                    if net > 0:
                        bad_buyers += 1
                        bad_buy_weight += weight
                    else:
                        bad_sellers += 1
                        bad_sell_weight += weight

            good_total = good_buyers + good_sellers
            bad_total = bad_buyers + bad_sellers

            # Brak aktywności w danej kohorcie w tym oknie -> traktujemy jako
            # neutralne 0.5 (brak przesunięcia), zamiast propagować NaN.
            good_ratio_raw = good_buyers / good_total if good_total > 0 else 0.5
            bad_ratio_raw = bad_buyers / bad_total if bad_total > 0 else 0.5

            # --- Market regime metrics (Faza 0, niezależne od LONG/SHORT) ---
            # W przeciwieństwie do good_ratio_raw/bad_ratio_raw powyżej (liczba
            # portfeli net-buy vs net-sell), to jest przewaga liczona na
            # WOLUMENIE (rozmiar transakcji w ETH) - "Good/Bad Trader
            # Pressure" z briefu regime-detection. Zakres -1.0 (czysty SELL)
            # do +1.0 (czysty BUY), 0.0 gdy w oknie nie było żadnego wolumenu
            # danej kohorty. Nie wpływa na EMA/composite/signal powyżej -
            # to osobny, równoległy tor liczony wyłącznie do zapisu w historii.
            good_buy_volume = good_sell_volume = 0.0
            bad_buy_volume = bad_sell_volume = 0.0
            for t in window_trades:
                if t.wallet in good_wallets:
                    if t.side is Side.BUY:
                        good_buy_volume += t.size_eth
                    else:
                        good_sell_volume += t.size_eth
                elif t.wallet in bad_wallets:
                    if t.side is Side.BUY:
                        bad_buy_volume += t.size_eth
                    else:
                        bad_sell_volume += t.size_eth

            good_total_volume = good_buy_volume + good_sell_volume
            bad_total_volume = bad_buy_volume + bad_sell_volume
            good_trader_pressure = (
                (good_buy_volume - good_sell_volume) / good_total_volume
                if good_total_volume > 0
                else 0.0
            )
            bad_trader_pressure = (
                (bad_buy_volume - bad_sell_volume) / bad_total_volume
                if bad_total_volume > 0
                else 0.0
            )
            # Dobrzy kupują, źli sprzedają jednocześnie -> duża dodatnia
            # rozbieżność (bardzo bycze). Odwrotnie -> duża ujemna (niedźwiedzie).
            smart_money_divergence = good_trader_pressure - bad_trader_pressure

            # --- Wallet Flip (Faza 3, brief regime-detection sekcja 9) ---
            # Przetwarzamy `window_trades` W KOLEJNOŚCI CZASU (już posortowane
            # - patrz `trades_sorted`/`buckets` wyżej) i śledzimy per portfel
            # (`self._wallet_flip_last_side`/`self._wallet_flip_streak`,
            # wznawialne między uruchomieniami - patrz `__init__`): każda
            # transakcja W TĄ SAMĄ stronę co poprzednia wydłuża streak;
            # transakcja W PRZECIWNĄ stronę kończy streak, a jeśli ten streak
            # miał długość >= `cfg.min_flip_streak_trades`, liczymy to jako
            # POTWIERDZONY flip w BIEŻĄCYM oknie (blok transakcji wyzwalającej
            # i tak należy do tego okna, bo iterujemy `window_trades`).
            # Pierwsza transakcja portfela w ogóle (brak zapisanego
            # `last_side`) tylko zakłada streak - NIGDY nie liczy się jako
            # flip (nie ma z czym porównać - odpowiednik "brak look-ahead"
            # z sekcji 21 briefu, zastosowany tu przez analogię: nie
            # zgadujemy kierunku "sprzed początku danych").
            # Kohorta (GOOD/BAD) brana jest z `good_wallets`/`bad_wallets`
            # WYLICZONYCH DLA TEGO OKNA (jak w bloku Fazy 0 powyżej) - portfel
            # NEUTRAL/UNRATED nie jest liczony w żadnej z czterech liczb.
            good_bullish_flips = good_bearish_flips = 0
            bad_bullish_flips = bad_bearish_flips = 0
            for t in window_trades:
                wallet = t.wallet
                new_side = t.side.value
                last_side = self._wallet_flip_last_side.get(wallet)
                streak = self._wallet_flip_streak.get(wallet, 0)

                if last_side is None:
                    self._wallet_flip_last_side[wallet] = new_side
                    self._wallet_flip_streak[wallet] = 1
                    continue

                if new_side == last_side:
                    self._wallet_flip_streak[wallet] = streak + 1
                    continue

                # Zmiana kierunku - potwierdzony flip tylko, jesli PRZED nia
                # portfel mial wystarczajaco dlugi ciag w poprzednia strone.
                if streak >= cfg.min_flip_streak_trades:
                    if wallet in good_wallets:
                        if t.side is Side.BUY:
                            good_bullish_flips += 1
                        else:
                            good_bearish_flips += 1
                    elif wallet in bad_wallets:
                        if t.side is Side.BUY:
                            bad_bullish_flips += 1
                        else:
                            bad_bearish_flips += 1

                self._wallet_flip_last_side[wallet] = new_side
                self._wallet_flip_streak[wallet] = 1

            self._ema_good_short = self._update_ema(
                self._ema_good_short, good_ratio_raw, cfg.ema_short_span
            )
            self._ema_good_long = self._update_ema(
                self._ema_good_long, good_ratio_raw, cfg.ema_long_span
            )
            self._ema_bad_short = self._update_ema(
                self._ema_bad_short, bad_ratio_raw, cfg.ema_short_span
            )
            self._ema_bad_long = self._update_ema(
                self._ema_bad_long, bad_ratio_raw, cfg.ema_long_span
            )

            # Dobrzy kupują -> byczo (+). Źli kupują -> traktujemy jako
            # kontrariański sygnał niedźwiedzi (-). Zgodnie z opisem
            # "Bad traders buy? Sell." z hydra.trading.
            composite = (
                cfg.w_good_short * (self._ema_good_short - 0.5)
                + cfg.w_good_long * (self._ema_good_long - 0.5)
                - cfg.w_bad_short * (self._ema_bad_short - 0.5)
                - cfg.w_bad_long * (self._ema_bad_long - 0.5)
            )

            # Faza "NEUTRAL dead-zone" - w paśmie wokół zera sygnał to teraz
            # Signal.HOLD ("NEUTRALNY"), NIE poprzedni sygnał (`self.
            # _prev_signal` nadal aktualizowany i eksportowany niżej - stan
            # potrzebny do wznowienia procesu między uruchomieniami, patrz
            # `export_state()` - ale nie jest już CZYTANY przy tej decyzji;
            # ten sam trzy-wartościowy próg jak w wydzielonej funkcji
            # `decide_signal()` powyżej, użytej dla composite ZBLENDOWANEGO).
            if composite > cfg.signal_threshold:
                signal = Signal.LONG
            elif composite < -cfg.signal_threshold:
                signal = Signal.SHORT
            else:
                signal = Signal.HOLD
            self._prev_signal = signal

            results.append(
                WindowScore(
                    window_end_block=window_end_block,
                    price_usd=price_at_block(window_end_block),
                    total_wallets_tracked=len(self.total_tracked),
                    active_wallets=len(net_direction),
                    pool_size=good_total,
                    good_buyers=good_buyers,
                    good_sellers=good_sellers,
                    bad_buyers=bad_buyers,
                    bad_sellers=bad_sellers,
                    good_buy_ratio_raw=good_ratio_raw,
                    bad_buy_ratio_raw=bad_ratio_raw,
                    ind_good_short=self._ema_good_short,
                    ind_good_long=self._ema_good_long,
                    ind_bad_short=self._ema_bad_short,
                    ind_bad_long=self._ema_bad_long,
                    composite_score=composite,
                    signal=signal,
                    good_trader_pressure=good_trader_pressure,
                    bad_trader_pressure=bad_trader_pressure,
                    smart_money_divergence=smart_money_divergence,
                    good_trader_breadth=good_ratio_raw,
                    good_trader_bullish_flips=good_bullish_flips,
                    good_trader_bearish_flips=good_bearish_flips,
                    bad_trader_bullish_flips=bad_bullish_flips,
                    bad_trader_bearish_flips=bad_bearish_flips,
                    total_good_classified=len(good_wallets),
                    total_bad_classified=len(bad_wallets),
                    good_buy_weight=good_buy_weight,
                    good_sell_weight=good_sell_weight,
                    bad_buy_weight=bad_buy_weight,
                    bad_sell_weight=bad_sell_weight,
                )
            )

        return results
