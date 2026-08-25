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
    # 250*24 = ok. 24h ruchomego okna reputacji.
    classification_lookback_blocks: int = 250 * 24

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
        alpha = 2.0 / (span + 1)
        if current is None:
            return new_value
        return alpha * new_value + (1 - alpha) * current

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

            good_buyers = good_sellers = bad_buyers = bad_sellers = 0
            for wallet, net in net_direction.items():
                if net == 0:
                    continue
                if wallet in good_wallets:
                    if net > 0:
                        good_buyers += 1
                    else:
                        good_sellers += 1
                elif wallet in bad_wallets:
                    if net > 0:
                        bad_buyers += 1
                    else:
                        bad_sellers += 1

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
                )
            )

        return results
