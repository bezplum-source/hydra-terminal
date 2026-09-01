"""Faza H1 (brief `hydrav2-hyperliquid-brief.md`) — klasyfikacja portfeli
handlujących ETH-PERP na Hyperliquid na kohorty GOOD/BAD/NEUTRAL,
ADAPTUJĄC istniejący silnik PnL z `wallets.py` zamiast pisać nowy od zera.

Komentarz w `wallets.py` wprost mówi, że jego metoda average-cost obsługuje
symetrycznie long i short — "na DEX-ach spotowych short w sensie dosłownym
nie istnieje, ale portfele mogą sprzedawać więcej niż kupiły". Na
Hyperliquid pozycje long/short są PRAWDZIWE (kontrakty perpetual), więc ten
sam silnik pasuje tu nawet lepiej niż do spotu — nie przepisujemy PnL od
zera, tylko przygotowujemy dane wejściowe w kompatybilnym kształcie.

Mapowanie: jedno zdarzenie rynkowe Hyperliquid (`HyperliquidTrade`) niesie
OBIE strony dopasowania (`buyer`, `seller`) — w przeciwieństwie do
pojedynczego Swap na Uniswap, który mówi tylko o jednym portfelu. Rozbijamy
je więc na DWA niezależne wpisy per-portfelowe (`Trade`), w kształcie,
którego oczekuje `wallets.py`: `buyer` dostaje `Trade(side=BUY, ...)`,
`seller` dostaje `Trade(side=SELL, ...)`, oba po tej samej cenie/wielkości.

`Trade.block` w `wallets.py::compute_wallet_stats` jest używane WYŁĄCZNIE
do sortowania chronologicznego (`sorted(trades, key=lambda t: t.block)`,
nic więcej — zweryfikowane czytając kod) — podstawiamy tu `ts_ms`
(milisekundy Unix z Hyperliquid) bezpośrednio jako "block". Te wartości
NIE są numerami bloków Ethereum i NIE POWINNY nigdy trafić do jednego
wywołania `compute_wallet_stats` razem z prawdziwymi transakcjami Uniswap
— to świadomie DWA OSOBNE, równoległe wywołania (dokładnie zgodnie z
decyzją architektoniczną z briefu: dwa niezależne rurociągi danych, łączone
dopiero na końcu jako dwie liczby `composite_spot`/`composite_perp`, w
Fazie H2).

Świadome uproszczenia tej fazy (patrz brief, sekcja Faza H1) — jawnie
nazwane ograniczenia, nie ukryte błędy:

- **PnL liczony WYŁĄCZNIE z ceny wejścia/wyjścia i wielkości pozycji**
  (realizowany PnL z dopasowanych transakcji, ten sam average-cost co
  Uniswap) — BEZ funding payments (ciągły PnL niezwiązany z pojedynczą
  transakcją) i BEZ ważenia dźwignią (wartość nominalna vs zainwestowany
  margines). Do ewentualnego uzupełnienia w przyszłej fazie.
- **Likwidacje traktowane jak zwykłe zamknięcie pozycji** — silnik i tak
  poprawnie policzy wymuszone zamknięcie jako stratę; osobne oznaczenie
  "portfel został zlikwidowany" nie jest tu implementowane (dane z kanału
  WS `trades` i tak go nie niosą — potwierdzone przy pisaniu Fazy H0).

Faza H1 (powyżej) TYLKO klasyfikowała portfele. Faza H2 (dopisana niżej,
`HyperliquidScoringConfig`/`HyperliquidWindowScore`/`HyperliquidScoringEngine`)
liczy z tej klasyfikacji `composite_perp` — dokładnie tym samym wzorcem EMA
z presji kupna/sprzedaży kohorty GOOD/BAD, co istniejący
`hydra_signals.scoring.ScoringEngine` dla Uniswap, ale z WŁASNYM stanem
(`data/hyperliquid_scoring_state.json`, patrz `live/state.py`) i WŁASNYM
oknowaniem — nie po numerze bloku (Hyperliquid nie ma bloków), tylko po
"co nowego przyszło od ostatniego uruchomienia" (patrz docstring
`HyperliquidScoringEngine.run`). `composite_perp` jest następnie BLENDOWANY
z `composite_spot` w `hydra_signals.scoring.blend_composite`, wołanym z
`live/run_incremental.py` — TA klasa i ten moduł same nie wiedzą nic o
blendzie ani o głównym sygnale LONG/SHORT, zgodnie z decyzją architektoniczną
"dwa niezależne rurociągi danych, złączone dopiero na końcu".

Faza H3 (brief, front-end) dopisuje do `HyperliquidScoringEngine` i
`HyperliquidWindowScore` dwa dodatkowe, czysto DIAGNOSTYCZNE pola —
`active_wallets`/`total_wallets_tracked` — analogiczne do `active_wallets`/
`total_wallets_tracked` w `hydra_signals.models.WindowScore`, potrzebne
WYŁĄCZNIE do wyświetlenia w nowej karcie "ETH-PERP · Hyperliquid" na
stronie (`live/template.html`) - NIE wchodzą do formuły `composite_score`
ani do żadnej logiki blendu, czysto liczby do pokazania użytkownikowi.
`total_wallets_tracked` wymaga wznawialnego stanu MIĘDZY uruchomieniami
(jak `wallets_seen.txt` dla Uniswap) - stąd nowy `initial_total_tracked`/
`self.total_tracked` niżej, i nowy plik `data/hyperliquid_wallets_seen.txt`
w `live/state.py`.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from .data_sources.hyperliquid_ws import HyperliquidTrade
from .models import Cohort, Side, Trade, WalletStats
from .wallets import classify_wallets, compute_wallet_stats

# Te same wartości startowe co domyślne w wallets.py/compute_wallet_stats i
# classify_wallets - świadomie NIE duplikujemy innych progów tutaj, tylko
# przekazujemy je dalej, żeby ewentualna zmiana w jednym miejscu (Uniswap)
# nie rozjechała się cicho z drugim (Hyperliquid).
DEFAULT_MIN_TRADES = 5
DEFAULT_GOOD_PCT = 0.15
DEFAULT_BAD_PCT = 0.15
# Ten sam prog i ta sama wartosc co `hydra_signals.scoring.ScoringConfig.
# min_trade_notional_usd` (Faza "dust filter" - patrz komentarz tam po
# pelne uzasadnienie decyzji uzytkownika: prog per-transakcja, oba tory).
# Osobna stala (nie import z scoring.py) z tego samego powodu co
# DEFAULT_MIN_TRADES/DEFAULT_GOOD_PCT/DEFAULT_BAD_PCT wyzej - ten modul
# swiadomie nie zalezy od scoring.py (patrz docstring modulu: "dwa
# niezalezne rurociagi danych").
DEFAULT_MIN_TRADE_NOTIONAL_USD = 1000.0


def hyperliquid_trade_to_wallet_trades(trade: HyperliquidTrade) -> list[Trade]:
    """Rozbija JEDNO zdarzenie rynkowe Hyperliquid (dwie strony transakcji)
    na DWA niezależne wpisy per-portfelowe, w kształcie którego oczekuje
    `wallets.py`. `ts_ms` podstawiony pod `block` — patrz docstring modułu."""
    return [
        Trade(
            wallet=trade.buyer,
            block=trade.ts_ms,
            side=Side.BUY,
            price_usd=trade.price_usd,
            size_eth=trade.size_eth,
        ),
        Trade(
            wallet=trade.seller,
            block=trade.ts_ms,
            side=Side.SELL,
            price_usd=trade.price_usd,
            size_eth=trade.size_eth,
        ),
    ]


def hyperliquid_trades_to_wallet_trades(trades: Iterable[HyperliquidTrade]) -> list[Trade]:
    """Stosuje `hyperliquid_trade_to_wallet_trades` do całej listy transakcji."""
    out: list[Trade] = []
    for t in trades:
        out.extend(hyperliquid_trade_to_wallet_trades(t))
    return out


def classify_hyperliquid_wallets(
    trades: Iterable[HyperliquidTrade],
    *,
    min_trades: int = DEFAULT_MIN_TRADES,
    good_pct: float = DEFAULT_GOOD_PCT,
    bad_pct: float = DEFAULT_BAD_PCT,
    min_trade_notional_usd: float = DEFAULT_MIN_TRADE_NOTIONAL_USD,
) -> dict[str, WalletStats]:
    """Funkcja całościowa Fazy H1: surowe transakcje Hyperliquid -> statystyki
    + kohorty GOOD/BAD/NEUTRAL per portfel.

    Bezstanowa (jak `compute_wallet_stats`/`classify_wallets`) — zakłada, że
    `trades` jest już przycięte do właściwego okna (patrz
    `hydra_signals.data_sources.hyperliquid_ws.prune_trade_records` /
    `DEFAULT_BUFFER_LOOKBACK_HOURS`). Wywołujący (przyszła Faza H2) jest
    odpowiedzialny za wczytanie bufora (`live.state.load_hyperliquid_trades_buffer`)
    i zdekodowanie rekordów JSON z powrotem na `HyperliquidTrade`
    (`hydra_signals.data_sources.hyperliquid_ws.json_record_to_trade`).

    Filtr "dust" (`min_trade_notional_usd`, patrz `DEFAULT_MIN_TRADE_NOTIONAL_USD`
    i odpowiednik `ScoringConfig.min_trade_notional_usd` dla Uniswap) - kazda
    POJEDYNCZA transakcja ponizej progu jest odsiewana PRZED konwersja na
    wpisy per-portfelowe, wiec nie wplywa ani na PnL, ani na liczbe transakcji
    uzywana przy `min_trades`.
    """
    filtered = [t for t in trades if t.notional_usd >= min_trade_notional_usd]
    wallet_trades = hyperliquid_trades_to_wallet_trades(filtered)
    stats = compute_wallet_stats(wallet_trades, min_trades=min_trades)
    return classify_wallets(stats, good_pct=good_pct, bad_pct=bad_pct)


# =====================================================================
# Faza H2 (brief `hydrav2-hyperliquid-brief.md`) — composite_perp
# =====================================================================


@dataclass
class HyperliquidScoringConfig:
    """Odpowiednik `hydra_signals.scoring.ScoringConfig`, ale dla Hyperliquid
    — te same NAZWY i te same wartości startowe tam, gdzie pojęcie jest
    tożsame (wagi w composite, rozpiętości EMA), świadomie osobna klasa
    (nie reużyta ScoringConfig), żeby zmiana progu dla jednego rynku nie
    rozjechała się cicho z drugim (identyczny powód co osobne
    DEFAULT_MIN_TRADES/DEFAULT_GOOD_PCT/DEFAULT_BAD_PCT wyżej)."""

    # W GODZINACH, nie w blokach (Hyperliquid nie ma bloków) - odpowiednik
    # `classification_lookback_blocks` w ScoringConfig (od Fazy "okno
    # reputacji 7 dni", 2026-09-01: spot = 250*24*7 ~ 7 dni). TU CELOWO
    # zostawione na 24h, NIE wydłużone razem ze spotem w tamtej fazie -
    # surowy bufor `hyperliquid_trades_buffer.jsonl` (firehose WS, patrz
    # `DEFAULT_BUFFER_LOOKBACK_HOURS` w `data_sources/hyperliquid_ws.py`) był
    # już ~83MB przy 48h retencji; wydłużenie do 7 dni napompowałoby go do
    # ~250-300MB+ commitowanych co godzinę przez GitHub Actions - osobna
    # decyzja infrastrukturalna, świadomie NIE podjęta bez użytkownika.
    classification_lookback_hours: float = 24.0

    min_trades_for_classification: int = DEFAULT_MIN_TRADES
    good_pct: float = DEFAULT_GOOD_PCT
    bad_pct: float = DEFAULT_BAD_PCT

    # EMA - liczone w jednostkach "wywołań `run()` z niepustym `new_trades`",
    # nie godzin - w praktyce to prawie to samo co "świece" w ScoringConfig,
    # bo `run_incremental.py` odpala się z podobnym, ~godzinnym cadence.
    ema_short_span: int = 3
    ema_long_span: int = 12

    w_good_short: float = 1.0
    w_good_long: float = 0.5
    w_bad_short: float = 1.0
    w_bad_long: float = 0.5

    # Zabezpieczenie "graceful degradation" z briefu (sekcja "Decyzja
    # architektoniczna"): dopóki w oknie klasyfikacji nie ma co najmniej
    # tylu portfeli spełniających `min_trades_for_classification` (GOOD +
    # BAD + NEUTRAL razem - to miara "czy jest w ogóle z czego liczyć",
    # nie miara jakości kohorty), `composite_perp` jest traktowany jako
    # NIEDOJRZAŁY (patrz `HyperliquidWindowScore.is_mature` i
    # `hydra_signals.scoring.blend_composite`) - blend wtedy spada z
    # powrotem do samego `composite_spot`, bez wpływu Hyperliquid. WARTOŚĆ
    # STARTOWA, nieprzestrojona żadnym backtestem, jak każdy inny próg w
    # tym projekcie.
    min_classified_wallets_for_maturity: int = 20

    # Filtr "dust" (Faza "dust filter") - identyczna nazwa i wartosc co
    # `hydra_signals.scoring.ScoringConfig.min_trade_notional_usd` (patrz
    # tamten komentarz po pelne uzasadnienie decyzji uzytkownika). Uzytkownik
    # wybral przez AskUserQuestion "oba tory", wiec Hyperliquid filtruje
    # dokladnie tak samo jak Uniswap: kazda POJEDYNCZA transakcja
    # (`HyperliquidTrade.notional_usd`) ponizej progu jest calkowicie
    # pomijana, zarowno w klasyfikacji portfeli, jak i w liczeniu biezacego
    # okna - patrz `HyperliquidScoringEngine.run()` nizej.
    min_trade_notional_usd: float = DEFAULT_MIN_TRADE_NOTIONAL_USD


@dataclass
class HyperliquidWindowScore:
    """Odpowiednik `hydra_signals.models.WindowScore`, ale dla jednego
    "okna" Hyperliquid — patrz `HyperliquidScoringEngine.run` po definicję
    okna (NIE stała siatka czasowa, tylko "co nowego od ostatniego
    uruchomienia"). Celowo NIE zawiera `signal` — ten moduł nic nie wie o
    głównym sygnale LONG/SHORT (patrz docstring modułu, sekcja Faza H2)."""

    window_end_ts_ms: int
    n_new_trades: int
    n_classified_wallets: int

    good_buyers: int
    good_sellers: int
    bad_buyers: int
    bad_sellers: int

    good_buy_ratio_raw: float
    bad_buy_ratio_raw: float

    ind_good_short: float
    ind_good_long: float
    ind_bad_short: float
    ind_bad_long: float

    composite_score: float
    is_mature: bool

    # --- Faza H3 (front-end) - czysto diagnostyczne, patrz docstring modulu.
    active_wallets: int = 0
    total_wallets_tracked: int = 0


class HyperliquidScoringEngine:
    """Stateful silnik EMA dla Hyperliquid — architektonicznie odpowiednik
    `hydra_signals.scoring.ScoringEngine`, ale mniejszy: bez regime, bez
    Wallet Flip, bez wewnętrznej decyzji `signal` (to wszystko zostaje
    WYŁĄCZNIE po stronie Uniswap/spot - patrz docstring modułu). Wznawialny
    między uruchomieniami procesu przez `initial_ema`/`export_state()`,
    dokładnie jak `ScoringEngine` (patrz `live/run_incremental.py`)."""

    def __init__(
        self,
        config: HyperliquidScoringConfig | None = None,
        *,
        initial_ema: dict[str, float | None] | None = None,
        initial_total_tracked: Iterable[str] | None = None,
    ) -> None:
        self.cfg = config or HyperliquidScoringConfig()
        ema = initial_ema or {}
        self._ema_good_short: float | None = ema.get("good_short")
        self._ema_good_long: float | None = ema.get("good_long")
        self._ema_bad_short: float | None = ema.get("bad_short")
        self._ema_bad_long: float | None = ema.get("bad_long")
        # Faza H3 - zbior WSZYSTKICH portfeli Hyperliquid kiedykolwiek
        # widzianych (buyer LUB seller w jakimkolwiek `new_trades`), narastajacy
        # miedzy wywolaniami `run()` i (przez `initial_total_tracked`) miedzy
        # osobnymi uruchomieniami procesu - identyczny wzorzec co
        # `ScoringEngine.total_tracked`. Publiczny atrybut (nie ma osobnego
        # exportera) - `live/run_incremental.py` odczytuje go bezposrednio po
        # `run()`, dokladnie tak jak juz robi to z `engine.total_tracked` dla
        # silnika spot.
        self.total_tracked: set[str] = set(initial_total_tracked or ())

    def export_state(self) -> dict:
        """Serializowalny (do JSON) zrzut stanu EMA - do zapisania na dysk i
        podania jako `initial_ema` przy kolejnym uruchomieniu procesu.
        `live/run_incremental.py` dokłada do tego słownika dodatkowe klucze
        (`last_processed_ts_ms`, `last_perp_snapshot` - patrz Faza H3) przed
        zapisem - ta metoda zwraca WYŁĄCZNIE cztery liczby EMA.
        `total_tracked` eksportuje się OSOBNO (publiczny atrybut, nie
        wchodzi w skład tego słownika) - patrz `data/hyperliquid_wallets_seen.txt`
        w `live/state.py`, dokładnie jak `ScoringEngine.total_tracked`/
        `wallets_seen.txt` dla Uniswap."""
        return {
            "good_short": self._ema_good_short,
            "good_long": self._ema_good_long,
            "bad_short": self._ema_bad_short,
            "bad_long": self._ema_bad_long,
        }

    def _update_ema(self, current: float | None, new_value: float, span: int) -> float:
        alpha = 2.0 / (span + 1)
        if current is None:
            return new_value
        return alpha * new_value + (1 - alpha) * current

    def run(
        self,
        new_trades: Iterable[HyperliquidTrade],
        *,
        history_trades: Iterable[HyperliquidTrade] = (),
        window_end_ts_ms: int,
    ) -> HyperliquidWindowScore | None:
        """Liczy JEDNO okno = wszystkie `new_trades` (transakcje, które
        przyszły od ostatniego uruchomienia — dostarczający, czyli
        `live/run_incremental.py`, jest odpowiedzialny za ten podział,
        analogicznie do `from_block`/`to_block` przy Uniswap).

        Zwraca `None`, gdy `new_trades` jest puste — CELOWO nie aktualizujemy
        wtedy EMA (wstrzyknięcie neutralnego 0.5 przy braku jakiejkolwiek
        nowej aktywności fałszywie ciągnęłoby composite_perp w stronę zera).
        `live/run_incremental.py` w takim wypadku zostawia zapisany stan
        nietknięty i używa ostatniej znanej wartości `composite_perp`.

        `history_trades` (opcjonalnie, spoza okna `classification_lookback_hours`
        liczonego od `window_end_ts_ms` i tak zostanie odfiltrowane) pełni
        identyczną rolę co w `ScoringEngine.run` - dodatkowy kontekst do
        klasyfikacji portfeli, sam nie generuje wyniku. `new_trades`
        definiuje zarówno okno klasyfikacji (razem z `history_trades`), jak
        i "window_trades" (transakcje, z których liczymy `good_buyers` itd.
        - w przeciwieństwie do `ScoringEngine`, tu window_trades ZAWSZE
        pokrywa się z całym `new_trades`, bo jest tylko jedno okno na
        wywołanie, nie wiele naraz jak przy Uniswap).
        """
        new_trades = list(new_trades)
        if not new_trades:
            return None

        cfg = self.cfg

        # Filtr dust (patrz HyperliquidScoringConfig.min_trade_notional_usd,
        # a takze analogiczny komentarz w ScoringEngine.run dla Uniswap) -
        # stosowany TU, natychmiast na wejsciu, zanim `new_trades`/
        # `history_trades` traf ia gdziekolwiek dalej (klasyfikacja ORAZ
        # liczenie biezacego okna). Jesli PO odsianiu dust nic nie zostalo w
        # `new_trades` - traktujemy to identycznie jak pusty `new_trades` na
        # wejsciu (patrz docstring wyzej: "CELOWO nie aktualizujemy wtedy
        # EMA") - inaczej okno zlozone wylacznie z dustu falszywie ciagnaloby
        # EMA w strone neutralnego 0.5.
        new_trades = [t for t in new_trades if t.notional_usd >= cfg.min_trade_notional_usd]
        if not new_trades:
            return None
        history_trades = [
            t for t in history_trades if t.notional_usd >= cfg.min_trade_notional_usd
        ]

        lookback_start_ms = window_end_ts_ms - int(cfg.classification_lookback_hours * 3600 * 1000)

        lookback_source = [t for t in history_trades if t.ts_ms > lookback_start_ms]
        lookback_source.extend(t for t in new_trades if t.ts_ms > lookback_start_ms)

        wallet_trades = hyperliquid_trades_to_wallet_trades(lookback_source)
        stats = compute_wallet_stats(wallet_trades, min_trades=cfg.min_trades_for_classification)
        classify_wallets(stats, good_pct=cfg.good_pct, bad_pct=cfg.bad_pct)

        good_wallets = {w for w, s in stats.items() if s.cohort is Cohort.GOOD}
        bad_wallets = {w for w, s in stats.items() if s.cohort is Cohort.BAD}

        window_wallet_trades = hyperliquid_trades_to_wallet_trades(new_trades)
        # Faza H3 - aktualizacja PRZED liczeniem net_direction ponizej, zeby
        # kolejnosc byla identyczna z ScoringEngine.run (tam tez
        # `self.total_tracked.update(...)` idzie od razu po zbudowaniu
        # window_trades, przed reszta logiki tego okna).
        self.total_tracked.update(t.wallet for t in window_wallet_trades)

        net_direction: dict[str, float] = defaultdict(float)
        for t in window_wallet_trades:
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
        # Brak aktywnosci danej kohorty w tym oknie -> neutralne 0.5 (brak
        # przesuniecia), identycznie jak w ScoringEngine.run.
        good_ratio_raw = good_buyers / good_total if good_total > 0 else 0.5
        bad_ratio_raw = bad_buyers / bad_total if bad_total > 0 else 0.5

        self._ema_good_short = self._update_ema(self._ema_good_short, good_ratio_raw, cfg.ema_short_span)
        self._ema_good_long = self._update_ema(self._ema_good_long, good_ratio_raw, cfg.ema_long_span)
        self._ema_bad_short = self._update_ema(self._ema_bad_short, bad_ratio_raw, cfg.ema_short_span)
        self._ema_bad_long = self._update_ema(self._ema_bad_long, bad_ratio_raw, cfg.ema_long_span)

        # Ta sama formula co ScoringEngine.run - dobrzy kupuja -> byczo (+),
        # zli kupuja -> kontrariansko niedzwiedzie (-).
        composite = (
            cfg.w_good_short * (self._ema_good_short - 0.5)
            + cfg.w_good_long * (self._ema_good_long - 0.5)
            - cfg.w_bad_short * (self._ema_bad_short - 0.5)
            - cfg.w_bad_long * (self._ema_bad_long - 0.5)
        )

        n_classified = len(stats)

        return HyperliquidWindowScore(
            window_end_ts_ms=window_end_ts_ms,
            n_new_trades=len(new_trades),
            n_classified_wallets=n_classified,
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
            is_mature=n_classified >= cfg.min_classified_wallets_for_maturity,
            active_wallets=len(net_direction),
            total_wallets_tracked=len(self.total_tracked),
        )
