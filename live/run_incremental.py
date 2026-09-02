#!/usr/bin/env python3
"""Cykliczny krok pipeline'u Hydra Terminal — uruchamiany co ok. godzinę
przez GitHub Actions (`.github/workflows/update.yml`).

Co robi, w kolejności:
1. Wczytuje zapisany stan (EMA silnika, ostatni przetworzony blok, bufor
   transakcji z okna lookback, zbiór wszystkich kiedykolwiek widzianych
   portfeli, pełną historię świec).
2. Pyta o aktualne czoło łańcucha i pobiera NOWE transakcje Swap od
   ostatniego przetworzonego bloku (albo robi jednorazowy backfill przy
   pierwszym uruchomieniu — patrz `BACKFILL_BLOCKS`).
3. Doklasyfikowuje portfele i liczy nowe świece (`ScoringEngine`, z
   wznowionym stanem EMA — patrz `hydra_signals/scoring.py`), a od Fazy H2
   blenduje je z równoległym `composite_perp` z Hyperliquid (patrz sekcja
   Hyperliquid niżej w kodzie i `hydrav2-hyperliquid-brief.md`).
4. Zapisuje zaktualizowany stan z powrotem na dysk (w tym stan diagnostyczny
   Hyperliquid — Faza H3 — do wyświetlenia w nowej karcie na stronie).
5. Generuje `site/index.html` (przez `live/build_site.py`).

6. NA SAM KONIEC (po kroku 5, gdy krytyczny tor mainnet+Hyperliquid już
   bezpiecznie zakończył się i zapisał wyniki) — Faza "Base L2, etap B0" —
   zbiera i buforuje transakcje Uniswap V3 z sieci Base (patrz
   `hydra_signals/data_sources/pools.py::BASE_POOLS`), na razie WYŁĄCZNIE do
   `data/base_trade_buffer.csv`, bez wpływu na composite/sygnał/frontend.
   CELOWO na końcu, nie równolegle z resztą — patrz obszerny komentarz przy
   tym bloku kodu niżej (incydent 2026-09-02: throttling Base potrafił
   zepsuć też krytyczny apel mainnetu, gdy blok Base szedł PRZED nim).

Ten skrypt SAM NIE robi `git commit`/`git push` — to celowo zostawione
workflow'owi (`.github/workflows/update.yml`), żeby ten plik dało się
przetestować lokalnie bez ryzyka przypadkowego commitu.

Wymaga zmiennej środowiskowej `ALCHEMY_RPC_URL` (pełny URL RPC z kluczem) —
w GitHub Actions ustawianej jako **Secret** (Settings → Secrets and
variables → Actions), NIGDY nie commitowanej do repo w postaci jawnej.

Opcjonalnie: `ALCHEMY_BASE_RPC_URL` (analogiczny URL, ale dla sieci Base) —
jeśli nieustawiona, sekcja "Base L2" jest po prostu pomijana (loguje
informację i leci dalej), reszta pipeline'u działa bez zmian.
"""

from __future__ import annotations

import datetime
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hydra_signals.data_sources.onchain_rpc import (  # noqa: E402
    JsonRpcClient,
    batch_call_with_retry,
    fetch_trades_from_chain_batched,
)
from hydra_signals.data_sources import hyperliquid_ws as hl_ws  # noqa: E402
from hydra_signals.data_sources.pools import BASE_POOLS, POOLS  # noqa: E402
from hydra_signals.hyperliquid_wallets import (  # noqa: E402
    HyperliquidScoringConfig,
    HyperliquidScoringEngine,
)
from hydra_signals.models import Signal  # noqa: E402
from hydra_signals import regime  # noqa: E402
from hydra_signals.scoring import (  # noqa: E402
    ScoringConfig,
    ScoringEngine,
    SignalEngine,
    blend_composite,
    decide_signal,
)

from live import state as st  # noqa: E402
from live.build_site import build_site  # noqa: E402

WARSAW = ZoneInfo("Europe/Warsaw")

# Ile blokow pobrac jednorazowo przy PIERWSZYM uruchomieniu (brak zapisanego
# stanu) - tyle, ile potrzeba, zeby classification_lookback_blocks (domyslnie
# 42000 = 7 dni od Fazy "okno reputacji 7 dni", wczesniej 6000 = 24h) mial
# od razu pelny, "rozgrzany" bufor, plus niewielki zapas.
BACKFILL_BLOCKS = int(os.environ.get("HYDRA_BACKFILL_BLOCKS", "42500"))
BLOCKS_PER_CALL = int(os.environ.get("HYDRA_BLOCKS_PER_CALL", "10"))
CALLS_PER_BATCH = int(os.environ.get("HYDRA_CALLS_PER_BATCH", "80"))
# Zabezpieczenie na wypadek dlugiej przerwy w dzialaniu joba (np. wylaczony
# na kilka dni) - nie probuj dogonic WSZYSTKIEGO zaleglego w jednym
# uruchomieniu, tylko tyle, ile bezpiecznie miesci sie w jednym jobie.
MAX_NEW_BLOCKS_PER_RUN = int(os.environ.get("HYDRA_MAX_NEW_BLOCKS_PER_RUN", "20000"))

# Faza H2 (brief hydrav2-hyperliquid-brief.md) - waga composite_perp w
# zblendowanej wartosci ktora steruje glownym sygnalem LONG/SHORT
# ("composite = (1-w)*spot + w*perp"). WARTOSC STARTOWA zaakceptowana wprost
# przez uzytkownika (50/50), do przestrojenia pozniej - patrz
# hydra_signals.scoring.DEFAULT_PERP_WEIGHT.
HYPERLIQUID_PERP_WEIGHT = float(os.environ.get("HYDRA_PERP_WEIGHT", "0.5"))

# Faza "Base L2, etap B0: zbieranie danych" (2026-09-02) - stale CELOWO
# ostrozne/male w porownaniu z odpowiednikami mainnetowymi wyzej. Powod:
# badanie przed ta faza (patrz projekt Claude, hydrav2-automation.md)
# ustalilo, ze (a) limit `eth_getLogs` to plaskie 10 blokow/wywolanie NA
# KAZDYM lancuchu niezaleznie od czasu bloku, a (b) budzet
# throughput/compute-units Alchemy jest WSPOLNY dla calego konta, nie
# osobny per-siec - a Base ma ~6x krotszy czas bloku niz mainnet (~2s vs
# ~12s), wiec pokrycie tego samego okresu czasu kosztuje ~6x wiecej wywolan
# RPC. Zeby NIE powtorzyc wczesniejszego incydentu throttlingu Alchemy,
# startujemy switnie mniejszymi zakresami niz na mainnecie - do
# przestrojenia w gore dopiero po obserwacji realnego zuzycia w praktyce.
BASE_BACKFILL_BLOCKS = int(os.environ.get("HYDRA_BASE_BACKFILL_BLOCKS", "10000"))
# OBNIZONE z 5000 do 1000 (2026-09-02) po pierwszym realnym uruchomieniu:
# log pokazal 292/500 nieudanych zakresow eth_getLogs I WSZYSTKIE 1141
# wywolan eth_getTransactionByHash nieudane - throttling byl na tyle silny,
# ze wyczerpal WSPOLNY budzet RPC konta na tyle mocno, by zepsuc takze
# nastepujacy po nim, krytyczny apel eth_blockNumber mainnetu (przerywajac
# CALE uruchomienie, zero commitu). Mniejszy zakres = mniej wywolan RPC =
# mniejsze ryzyko throttlingu, zarowno dla samego Base jak i (po
# przeniesieniu bloku Base na koniec main(), patrz nizej) dla wszystkiego,
# co idzie po nim. Do przestrojenia w gore dopiero po potwierdzeniu, ze
# przy tej wartosci throttling ustal.
BASE_MAX_NEW_BLOCKS_PER_RUN = int(os.environ.get("HYDRA_BASE_MAX_NEW_BLOCKS_PER_RUN", "1000"))
# Okno przycinania bufora `base_trade_buffer.csv` - na etapie B0 (samo
# zbieranie, BEZ jeszcze klasyfikacji portfeli) to tylko zabezpieczenie
# przed nieograniczonym wzrostem pliku, nie realne "okno reputacji" (to
# dopiero Faza B1). Wartosc startowa (~1 dzien przy ~2s/blok) jest
# JAWNIE prowizoryczna - do przeliczenia na podstawie realnego, obserwowanego
# rozmiaru pliku, dokladnie ta sama lekcja co przy buforze Hyperliquid.
BASE_LOOKBACK_BLOCKS = int(os.environ.get("HYDRA_BASE_LOOKBACK_BLOCKS", "43200"))


def log(msg: str) -> None:
    print(f"[hydra] {msg}", flush=True)


def fmt_warsaw(ts: int) -> str:
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).astimezone(WARSAW)
    return dt.strftime("%d.%m.%Y, %H:%M")


def main() -> int:
    rpc_url = os.environ.get("ALCHEMY_RPC_URL")
    if not rpc_url:
        log("BLAD: brak zmiennej srodowiskowej ALCHEMY_RPC_URL (ustaw jako GitHub Actions secret).")
        return 1

    rpc = JsonRpcClient(rpc_url)
    cfg = ScoringConfig()

    scoring_state = st.load_scoring_state()
    trade_buffer = st.load_trade_buffer()
    wallets_seen = st.load_wallets_seen()
    candles_history = st.load_candles_history()
    regime_state = st.load_regime_state()
    signal_state = st.load_signal_state()
    wallet_flip_state = st.load_wallet_flip_state()
    hyperliquid_scoring_state = st.load_hyperliquid_scoring_state()
    hyperliquid_wallets_seen = st.load_hyperliquid_wallets_seen()

    # --- Faza H2/H3 (brief Hyperliquid) - osobny, rownolegly silnik na danych
    # z Hyperliquid (zbieranych przez OSOBNY workflow/listener, patrz
    # hyperliquid_listener.py), wznawiany miedzy uruchomieniami dokladnie
    # jak ScoringEngine/RegimeEngine ponizej. "Okno" tego silnika to NIE
    # stala siatka czasowa, tylko "wszystko, co przyszlo od ostatniego
    # uruchomienia tego skryptu" - patrz HyperliquidScoringEngine.run.
    # Liczone TUTAJ (przed petla po nowych swiecach Uniswap nizej), zeby
    # jego wlasny kursor (`last_processed_ts_ms`) posuwal sie NIEZALEZNIE
    # od tego, czy w tym konkretnym uruchomieniu Uniswap w ogole domknal
    # jakies okno - dokladnie zgodnie z zasada "dwa niezalezne rurociagi".
    hl_raw_records = st.load_hyperliquid_trades_buffer()
    hl_trades = [hl_ws.json_record_to_trade(r) for r in hl_raw_records]

    last_hl_ts_ms = hyperliquid_scoring_state.get("last_processed_ts_ms")
    if last_hl_ts_ms is None:
        new_hl_trades = hl_trades
        history_hl_trades: list = []
    else:
        new_hl_trades = [t for t in hl_trades if t.ts_ms > last_hl_ts_ms]
        history_hl_trades = [t for t in hl_trades if t.ts_ms <= last_hl_ts_ms]

    hl_engine = HyperliquidScoringEngine(
        HyperliquidScoringConfig(),
        initial_ema=hyperliquid_scoring_state,
        initial_total_tracked=hyperliquid_wallets_seen,
    )
    hl_score = None
    if new_hl_trades:
        hl_window_end_ts_ms = max(t.ts_ms for t in new_hl_trades)
        hl_score = hl_engine.run(
            new_hl_trades, history_trades=history_hl_trades, window_end_ts_ms=hl_window_end_ts_ms
        )

    # Faza H3 (front-end) - `perp_snapshot` niesie WSZYSTKO, co karta
    # diagnostyczna "ETH-PERP - Hyperliquid" potrzebuje pokazac, nie tylko
    # sama wartosc do blendu. Trzymane jako jeden slownik (nie osobne
    # zmienne) specjalnie po to, zeby dalo sie go w calosci zapisac/odczytac
    # ze stanu (`last_perp_snapshot`) - dokladnie tak samo traktujemy
    # "ostatnia znana wartosc" niezaleznie od tego, ile pol ona niesie.
    if hl_score is not None:
        perp_snapshot = {
            "composite": hl_score.composite_score if hl_score.is_mature else None,
            "is_mature": hl_score.is_mature,
            "tracked": hl_score.total_wallets_tracked,
            "active": hl_score.active_wallets,
            "classified": hl_score.n_classified_wallets,
            "good_buyers": hl_score.good_buyers,
            "good_sellers": hl_score.good_sellers,
            "bad_buyers": hl_score.bad_buyers,
            "bad_sellers": hl_score.bad_sellers,
        }
        new_hl_state = hl_engine.export_state()
        new_hl_state["last_processed_ts_ms"] = hl_score.window_end_ts_ms
        new_hl_state["last_perp_snapshot"] = perp_snapshot
        st.save_hyperliquid_scoring_state(new_hl_state)
        st.save_hyperliquid_wallets_seen(hl_engine.total_tracked)
        log(
            f"Hyperliquid: {hl_score.n_new_trades} nowych transakcji, "
            f"{hl_score.n_classified_wallets} sklasyfikowanych portfeli, "
            f"{hl_score.total_wallets_tracked} sledzonych lacznie "
            f"({'dojrzale' if hl_score.is_mature else 'jeszcze NIEDOJRZALE - composite_perp=None'})."
        )
    else:
        # Brak nowych transakcji Hyperliquid od ostatniego uruchomienia (np.
        # listener jeszcze sie nie zdazyl odpalic w tej godzinie) - NIE
        # dotykamy zapisanego stanu (EMA zamrozone), tylko odczytujemy
        # OSTATNI znany snapshot z poprzedniego uruchomienia.
        #
        # Zgodnosc wsteczna (Faza H3): stan zapisany JESZCZE PRZED ta faza
        # ma zamiast `last_perp_snapshot` dwa starsze, plaskie klucze
        # (`last_composite_perp`/`last_is_mature`, patrz Faza H2) - bez tej
        # galezi pierwsze uruchomienie po wdrozeniu Fazy H3 (jesli akurat
        # nie trafia na nowe transakcje Hyperliquid) straciloby ciaglosc
        # composite_perp, mimo ze realne dane juz dawno sa dojrzale.
        perp_snapshot = hyperliquid_scoring_state.get("last_perp_snapshot")
        if perp_snapshot is None:
            legacy_is_mature = hyperliquid_scoring_state.get("last_is_mature", False)
            perp_snapshot = {
                "composite": hyperliquid_scoring_state.get("last_composite_perp") if legacy_is_mature else None,
                "is_mature": legacy_is_mature,
                "tracked": len(hyperliquid_wallets_seen),
                "active": 0,
                "classified": 0,
                "good_buyers": 0,
                "good_sellers": 0,
                "bad_buyers": 0,
                "bad_sellers": 0,
            }

    composite_perp = perp_snapshot["composite"]

    # ZNALEZIONY I NAPRAWIONY realny blad (zgloszenie uzytkownika: "realnie
    # nie trwa to do godziny... czesto odswieza po 2h"): to byl JEDYNY
    # nieponawiany, pojedynczy zwykly `rpc.call()` w calym live-pipelinie
    # (potwierdzone grepem `\.call\(` w hydra_signals/ i live/ - kazde inne
    # wywolanie RPC idzie przez `batch_call_with_retry`). Alchemy throttluje
    # (potwierdzone mailem uzytkownika: >10% zapytan rate-limited w ciagu
    # ostatniej godziny) - kiedy TEN konkretny apel dostal 429/blad
    # sieciowy, `rpc.call()` podnosil wyjatek NIEZLAPANY nigdzie w main(),
    # co wywalalo caly krok GitHub Actions PRZED jakimkolwiek zapisem/commitem
    # - ten cykl byl calkowicie pomijany, bez zadnego logu ani retry. Przy
    # rosnacej liczbie sledzonych portfeli (wiecej zapytan/uruchomienie)
    # ryzyko trafienia w ten pojedynczy punkt awarii rosnie z czasem - to
    # tlumaczy, dlaczego przerwy miedzy aktualizacjami realnie rosly (git log
    # potwierdza: z ~10-30 min na poczatku do 100-200+ min ostatnio).
    # Naprawa: ten sam, juz istniejacy i przetestowany mechanizm ponowien co
    # dla `eth_getLogs`/`eth_getBlockByNumber` nizej - `batch_call_with_retry`
    # z lista jednego wywolania. Przy ~10% szansie throttlingu per-zapytanie,
    # szansa ze WSZYSTKIE proby (1 + max_retries=6 domyslnie) zawioda jest
    # astronomicznie mala (~0.1^7), wiec to powinno w praktyce calkowicie
    # wyeliminowac ten konkretny scenariusz calkowicie pomijanego cyklu.
    head_result = batch_call_with_retry(rpc, [("eth_blockNumber", [])], batch_size=CALLS_PER_BATCH)[0]
    if head_result is None:
        log(
            "BLAD: nie udalo sie pobrac aktualnego czola lancucha "
            "(eth_blockNumber) mimo ponowien - Alchemy najprawdopodobniej "
            "throttluje (rate limit) mocniej niz zwykle. Przerywam TO "
            "uruchomienie (bez zadnego zapisu/commitu) - kolejne zaplanowane "
            "uruchomienie sprobuje ponownie od tego samego miejsca."
        )
        return 1
    head = int(head_result, 16)
    log(f"Aktualne czolo lancucha: blok {head}")

    last_processed = scoring_state.get("last_processed_block")
    if last_processed is None:
        from_block = max(0, head - BACKFILL_BLOCKS)
        log(f"Pierwsze uruchomienie - jednorazowy backfill {BACKFILL_BLOCKS} blokow (od {from_block}).")
    else:
        from_block = last_processed + 1

    to_block = head
    if from_block > to_block:
        log("Brak nowych blokow od ostatniego uruchomienia - nic do zrobienia.")
        return 0

    if to_block - from_block + 1 > MAX_NEW_BLOCKS_PER_RUN:
        capped_to = from_block + MAX_NEW_BLOCKS_PER_RUN - 1
        log(
            f"Zakres {from_block}-{to_block} przekracza limit "
            f"{MAX_NEW_BLOCKS_PER_RUN} blokow/uruchomienie - przycinam do "
            f"{capped_to} (reszta zostanie dogoniona w kolejnych uruchomieniach)."
        )
        to_block = capped_to

    log(f"Pobieram nowe transakcje: bloki {from_block}-{to_block} ({to_block - from_block + 1} blokow)")
    new_trades = fetch_trades_from_chain_batched(
        rpc,
        POOLS,
        from_block,
        to_block,
        blocks_per_call=BLOCKS_PER_CALL,
        calls_per_batch=CALLS_PER_BATCH,
        on_progress=log,
    )
    log(f"Nowych transakcji: {len(new_trades)}")

    combined_buffer = trade_buffer + new_trades
    lookback_start = to_block - cfg.classification_lookback_blocks
    trimmed_buffer = [t for t in combined_buffer if t.block > lookback_start]

    # NIE scoruj okna (swiecy), ktore jeszcze sie nie "domknelo" wzgledem
    # aktualnego czola lancucha - inaczej `window_end_block` bedzie w
    # PRZYSZLOSCI (blok jeszcze niewykopany), eth_getBlockByNumber zwroci
    # null (stad "?" zamiast daty), a w kolejnym uruchomieniu te same
    # transakcje zostalyby policzone PONOWNIE jako nowa swieca z tym samym
    # numerem bloku (duplikat w historii). Zamiast tego: transakcje z
    # jeszcze otwartego okna zostaja w buforze (`trimmed_buffer`, ponizej) i
    # doczekaja sie zaliczenia do swiecy w PRZYSZLYM uruchomieniu, gdy okno
    # sie juz domknie - `last_scored_window_end` (osobny od
    # `last_processed_block`!) pamieta, dokad juz faktycznie doliczylismy
    # swiece, wiec nic nie zostanie policzone dwa razy ani pominiete.
    window_blocks = cfg.window_blocks
    last_closed_end = ((to_block + 1) // window_blocks) * window_blocks - 1
    last_scored_end = scoring_state.get("last_scored_window_end", -1)

    scoreable_trades = [t for t in combined_buffer if last_scored_end < t.block <= last_closed_end]
    classification_history = [t for t in combined_buffer if t.block <= last_scored_end]

    has_prior_state = bool(scoring_state)
    engine = ScoringEngine(
        cfg,
        initial_ema=scoring_state if has_prior_state else None,
        initial_prev_signal=Signal(scoring_state.get("prev_signal", "HOLD")),
        initial_total_tracked=wallets_seen,
        initial_wallet_flip_state=wallet_flip_state,
    )

    price_source = trimmed_buffer if trimmed_buffer else new_trades
    price_at_block = st.price_at_block_factory(price_source)

    # Faza 2 (market regime BULL/BEAR/NEUTRAL) - patrz hydra_signals/regime.py.
    # Osobny silnik, osobny stan na dysku - wznawia sie miedzy godzinowymi
    # uruchomieniami dokladnie tak samo jak ScoringEngine powyzej.
    regime_engine = regime.RegimeEngine(initial_state=regime_state)

    # Faza "sygnał z histerezą" (zgłoszenie użytkownika 2026-08-31: "zmienia
    # sygnał co każdy blok... hydra.trading trzyma LONG od 2 tygodni") -
    # maszyna stanów HOLD/LONG/SHORT z histerezą wejście/wyjście +
    # potwierdzeniem, architektura 1:1 skopiowana z RegimeEngine powyżej -
    # patrz hydra_signals/scoring.py::SignalEngine/SignalConfig. Zastępuje
    # dawne, bezstanowe `decide_signal()` jako źródło głównego `signal`
    # niżej. Wznawia się między uruchomieniami dokładnie tak samo jak
    # regime_engine - patrz `signal_state`/`st.save_signal_state` niżej.
    signal_engine = SignalEngine(initial_state=signal_state)

    new_scores = []
    if scoreable_trades:
        new_scores = engine.run(scoreable_trades, price_at_block, history_trades=classification_history)
    log(f"Nowe swiece (okna) w tym uruchomieniu: {len(new_scores)}")
    if new_trades and not scoreable_trades:
        log(
            "Najnowsze transakcje naleza jeszcze do niedomknietego okna - "
            "zostana doliczone do swiecy w kolejnym uruchomieniu."
        )

    if new_scores:
        block_numbers = [s.window_end_block for s in new_scores]
        ts_calls = [("eth_getBlockByNumber", [hex(b), False]) for b in block_numbers]
        block_results = batch_call_with_retry(rpc, ts_calls, batch_size=CALLS_PER_BATCH)
        block_ts: dict[int, int] = {}
        for b, res in zip(block_numbers, block_results):
            if res and "timestamp" in res:
                block_ts[b] = int(res["timestamp"], 16)
            else:
                log(f"UWAGA: nie udalo sie pobrac znacznika czasu dla bloku {b}.")

        for s in new_scores:
            ts = block_ts.get(s.window_end_block)

            # --- Faza H2 (brief Hyperliquid) - to TU nastepuje zlaczenie
            # dwoch niezaleznych torow w jedna decyzje. `s.composite_score`/
            # `s.signal` (silnik ScoringEngine powyzej) NIE sa modyfikowane -
            # zostaja jako `compositeSpot`/`signalSpotOnly`, czysto
            # diagnostyczne (patrz brief, "Pelna przejrzystosc w hero").
            # `composite_perp` policzone raz, PRZED ta petla (patrz wyzej) -
            # ta sama wartosc stosowana do wszystkich nowych swiec w tym
            # uruchomieniu (zwykle jest ich jedna; w rzadkim przypadku
            # nadrabiania zaleglosci - swiadome uproszczenie, jak wiele
            # innych progow/przyblizen w tym projekcie).
            composite_final = blend_composite(
                s.composite_score, composite_perp, perp_weight=HYPERLIQUID_PERP_WEIGHT
            )
            # Faza "sygnał z histerezą" - `signal_engine` (maszyna stanów
            # HOLD/LONG/SHORT z histerezą + potwierdzeniem, patrz wyżej)
            # ZASTĘPUJE dawne bezstanowe `decide_signal()` jako źródło tego
            # pola. Wywoływane PO KOLEI dla każdej nowej świecy w tym
            # uruchomieniu (jak `regime_engine.process_candle` niżej) - przy
            # wielu nowych świecach naraz (np. po dłuższej przerwie) silnik
            # "przeżywa" je jedna po drugiej, tak jakby przyszły w osobnych,
            # godzinowych uruchomieniach. `decide_signal()` zostaje w kodzie
            # niezmieniona - dalej używana WYŁĄCZNIE wewnątrz
            # `ScoringEngine.run()` do policzenia `signalSpotOnly`
            # (diagnostyczne, celowo BEZ histerezy - patrz scoring.py).
            final_signal = signal_engine.process(composite_final)

            candle = {
                "block": s.window_end_block,
                "price": round(s.price_usd, 2),
                "signal": final_signal.value,
                "composite": round(composite_final, 3),
                # Faza "sygnał z histerezą" - front-end (template.html) dalej
                # czyta TO SAMO pole (`signalThreshold`) do kolorowania
                # rozbicia spot/perp w hero i pill BYCZY/NEUTRALNY/NIEDZWIEDZI
                # w karcie ETH-PERP - teraz niesie `enter_threshold`
                # `SignalEngine` (próg, powyżej ktorego wychylenie jest w
                # ogole "warte uwagi"), zamiast starego, wycofanego
                # `cfg.signal_threshold`. Jedno miejsce prawdy, zamiast
                # duplikowania wartosci na twardo w JS (ten sam wzorzec co
                # `perpMaturityThreshold` nizej) - front-end nie musi nic
                # wiedziec o `exit_threshold`/potwierdzeniu, ktore sterują
                # WYŁĄCZNIE samym polem `signal`, nie kolorowaniem liczb.
                "signalThreshold": signal_engine.cfg.enter_threshold,
                # --- Faza H2 (brief Hyperliquid) - rozbicie widoczne juz w
                # danych, zeby przyszla Faza H3 (frontend) mogla to po
                # prostu wyswietlic bez przeliczania niczego dodatkowo.
                "compositeSpot": round(s.composite_score, 3),
                "compositePerp": round(composite_perp, 3) if composite_perp is not None else None,
                "signalSpotOnly": s.signal.value,
                # --- Faza H3 (front-end) - karta diagnostyczna "ETH-PERP -
                # Hyperliquid" (patrz template.html) - te same wartosci
                # `perp_snapshot` niezaleznie od tego, czy hl_score jest
                # swiezy w TYM konkretnym uruchomieniu (patrz komentarz przy
                # budowie `perp_snapshot` wyzej).
                "perpTracked": perp_snapshot["tracked"],
                "perpActive": perp_snapshot["active"],
                "perpClassified": perp_snapshot["classified"],
                "perpIsMature": perp_snapshot["is_mature"],
                "perpGoodBuyers": perp_snapshot["good_buyers"],
                "perpGoodSellers": perp_snapshot["good_sellers"],
                "perpBadBuyers": perp_snapshot["bad_buyers"],
                "perpBadSellers": perp_snapshot["bad_sellers"],
                "perpMaturityThreshold": hl_engine.cfg.min_classified_wallets_for_maturity,
                "indGoodShort": round(s.ind_good_short, 3),
                "indGoodLong": round(s.ind_good_long, 3),
                "indBadShort": round(s.ind_bad_short, 3),
                "indBadLong": round(s.ind_bad_long, 3),
                "goodBuyers": s.good_buyers,
                "goodSellers": s.good_sellers,
                "badBuyers": s.bad_buyers,
                "badSellers": s.bad_sellers,
                "pool": s.pool_size,
                "active": s.active_wallets,
                "tracked": s.total_wallets_tracked,
                "time": fmt_warsaw(ts) if ts is not None else "?",
                "ts": ts,
                # --- Market regime metrics (Faza 0) - patrz scoring.py.
                # Niezalezne od "signal"/"composite" powyzej - osobny,
                # rownolegly tor, jeszcze BEZ wlasnego BULL/BEAR/NEUTRAL
                # (to dopiero Faza 2) - na razie tylko zapisujemy surowe
                # wskazniki do historii, zeby zaczac budowac dane potrzebne
                # pozniej do ustalenia progow.
                "goodPressure": round(s.good_trader_pressure, 4),
                "badPressure": round(s.bad_trader_pressure, 4),
                "divergence": round(s.smart_money_divergence, 4),
                "breadth": round(s.good_trader_breadth, 4),
                # --- Wallet Flip (Faza 3) - patrz hydra_signals/scoring.py.
                # Liczba portfeli, ktore w TYM oknie odwrocily kierunek po
                # wystarczajaco dlugim ciagu transakcji w przeciwna strone -
                # patrz docstring `ScoringEngine.run()`. Liczone osobno dla
                # kohorty GOOD i BAD, niezaleznie od "signal"/"composite" i
                # od reszty pol regime powyzej/ponizej.
                "goodBullishFlips": s.good_trader_bullish_flips,
                "goodBearishFlips": s.good_trader_bearish_flips,
                "badBullishFlips": s.bad_trader_bullish_flips,
                "badBearishFlips": s.bad_trader_bearish_flips,
            }
            # --- Momentum wieloczasowy (Faza 1) - patrz hydra_signals/regime.py.
            # Liczony z JUZ ZAPISANEJ historii (candles_history PRZED
            # dopisaniem tej swiecy), nie z surowych transakcji - stad
            # dziala identycznie na zywo i w przyszlym backtescie offline.
            # Wartosci beda "None" (null w JSON) dopoki historia nie urosnie
            # na tyle, zeby dany horyzont (np. 30d) mial sie z czego liczyc.
            momentum = regime.compute_momentum(
                candles_history, current=candle, window_blocks=window_blocks
            )
            candle.update(regime.momentum_to_json(momentum))
            # --- BULL/BEAR score + regime (Faza 2) - patrz hydra_signals/regime.py.
            # Wywolywane PO doliczeniu momentum (candle ma juz wszystkie pola
            # potrzebne do compute_regime_score), PO KOLEI dla kazdej nowej
            # swiecy w tym uruchomieniu - tak, jakby kazda przyszla osobno,
            # w swoim wlasnym godzinowym uruchomieniu.
            candle.update(regime_engine.process_candle(candle))
            candles_history.append(candle)

    new_last_scored_end = max((s.window_end_block for s in new_scores), default=last_scored_end)

    new_state = engine.export_state()
    new_state["last_processed_block"] = to_block
    new_state["last_scored_window_end"] = new_last_scored_end
    # Faza H2 zapisywala tu `final_prev_signal` - wycofane w Fazie "NEUTRAL
    # dead-zone" (decide_signal() juz go nie potrzebuje, patrz wyzej). Stary
    # klucz mogl zostac w juz-zapisanym scoring_state.json na dysku - to
    # nieszkodliwe, nic go juz nie czyta.
    new_state["updated_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    st.save_scoring_state(new_state)
    st.save_trade_buffer(trimmed_buffer)
    st.save_wallets_seen(engine.total_tracked)
    st.save_candles_history(candles_history)
    st.save_regime_state(regime_engine.export_state())
    st.save_signal_state(signal_engine.export_state())
    st.save_wallet_flip_state(engine.export_wallet_flip_state())

    # Faza "wiarygodna swiezosc" - `lastRunUtc` to zegar SCIANY (kiedy TEN
    # skrypt faktycznie zakonczyl dzialanie), nie znacznik czasu bloku.
    # Front-end (chip swiezosci) uzywa TEGO, zeby uczciwie pokazywac realny
    # odstep miedzy uruchomieniami automatyzacji - patrz komentarz w
    # build_site.py::build_site.
    build_site(candles_history, meta={"lastRunUtc": new_state["updated_at_utc"]})
    log(f"Strona wygenerowana: site/index.html ({len(candles_history)} swiec w pelnej historii).")

    # --- Faza "Base L2, etap B0: zbieranie danych" (2026-09-02) - trzeci,
    # w pelni NIEZALEZNY tor obok Hyperliquid (wyzej) i mainnet Uniswap
    # (wyzej) - patrz hydra_signals/data_sources/pools.py::BASE_POOLS.
    # CELOWO tylko zbieranie i buforowanie transakcji na tym etapie - BEZ
    # wliczania ich do composite/ScoringEngine/frontendu (to dopiero kolejne
    # fazy, B1+), dokladnie tak jak zaczynal Hyperliquid (H0). Numery blokow
    # Base i Ethereum mainnet NIE sa w zaden sposob porownywalne (inny
    # lancuch, inny czas bloku) - stad wlasny klient RPC, wlasny kursor
    # bloku (`base_collector_state`) i wlasny bufor (`base_trade_buffer.csv`),
    # zupelnie osobne od `rpc`/`trade_buffer` wyzej.
    #
    # NAPRAWA REALNEGO INCYDENTU (2026-09-02, pierwszy dzien po wlaczeniu
    # sekretu ALCHEMY_BASE_RPC_URL): ten blok byl pierwotnie umieszczony
    # PRZED krytycznym `head_result = batch_call_with_retry(...)` mainnetu
    # (linia z komentarzem "ZNALEZIONY I NAPRAWIONY realny blad" wyzej).
    # Log z pierwszego prawdziwego uruchomienia pokazal: 292/500 zakresow
    # eth_getLogs dla Base nie powiodlo sie MIMO ponowien, WSZYSTKIE 1141
    # wywolan eth_getTransactionByHash tez sie nie powiodlo (stad 1203
    # realnych logow Swap -> 0 przetworzonych Trade), a zaraz PO TYM blok
    # mainnetu rowniez nie zdolal pobrac `eth_blockNumber` mimo wlasnych
    # ponowien - CALE uruchomienie przerwane (return 1, ZERO commitu, w tym
    # dla Hydra/Hyperliquid, ktore normalnie dzialaja bezawaryjnie). Przyczyna:
    # budzet throughput/compute-units Alchemy jest WSPOLNY dla calego konta
    # (patrz badanie sprzed startu tej fazy) - ~1600+ wywolan RPC dla samego
    # Base w jednym uruchomieniu potrafi wyczerpac ten budzet na tyle mocno,
    # ze NASTEPNY, dla mainnetu KRYTYCZNY apel tez pada. Blok Base byl juz
    # od poczatku owiniety w try/except (patrz nizej) - to chronilo przed
    # WYJATKIEM python, ale NIE przed wyczerpaniem WSPOLNEGO budzetu RPC,
    # ktore psulo kolejne, niepowiazane wywolanie.
    #
    # NAPRAWA: ten caly blok przeniesiony na sam KONIEC main() - PO tym, jak
    # caly krytyczny tor mainnet+Hyperliquid juz zakonczyl sie sukcesem I
    # ZAPISAL stan (`st.save_*` wyzej) I wygenerowal strone (`build_site`
    # wyzej). Base dostaje "resztki" budzetu RPC na sam koniec - jesli go
    # zabraknie (throttling), szkodzi to WYLACZNIE zbieraniu danych Base
    # (nadal bezpiecznie zlapane przez try/except nizej), NIGDY JUZ
    # krytycznemu torowi, ktory w tym momencie juz bezpiecznie skonczyl
    # prace i zapisal wyniki. Dodatkowo `BASE_MAX_NEW_BLOCKS_PER_RUN`
    # obnizone (patrz stala wyzej) - mniejszy zakres blokow na uruchomienie
    # = mniej wywolan RPC = mniejsze ryzyko throttlingu rowniez SAMEGO Base.
    base_rpc_url = os.environ.get("ALCHEMY_BASE_RPC_URL")
    if not base_rpc_url:
        log(
            "Base L2: brak zmiennej ALCHEMY_BASE_RPC_URL - pomijam zbieranie "
            "danych z Base w tym uruchomieniu (reszta pipeline'u dziala "
            "normalnie). Zeby wlaczyc, dodaj sekret ALCHEMY_BASE_RPC_URL w "
            "GitHub Actions (Settings -> Secrets and variables -> Actions), "
            "wlaczajac wczesniej siec Base w tej samej aplikacji Alchemy."
        )
    else:
        try:
            base_rpc = JsonRpcClient(base_rpc_url)
            base_collector_state = st.load_base_collector_state()
            base_trade_buffer = st.load_base_trade_buffer()

            base_head_result = batch_call_with_retry(
                base_rpc, [("eth_blockNumber", [])], batch_size=CALLS_PER_BATCH
            )[0]
            if base_head_result is None:
                log(
                    "Base L2: nie udalo sie pobrac czola lancucha Base mimo "
                    "ponowien - pomijam zbieranie danych z Base w tym "
                    "uruchomieniu (bez zadnego zapisu bufora Base)."
                )
            else:
                base_head = int(base_head_result, 16)
                base_last_processed = base_collector_state.get("last_processed_block")
                if base_last_processed is None:
                    base_from_block = max(0, base_head - BASE_BACKFILL_BLOCKS)
                    log(
                        f"Base L2: pierwsze uruchomienie - jednorazowy backfill "
                        f"{BASE_BACKFILL_BLOCKS} blokow (od {base_from_block})."
                    )
                else:
                    base_from_block = base_last_processed + 1

                base_to_block = base_head
                if base_from_block > base_to_block:
                    log("Base L2: brak nowych blokow od ostatniego uruchomienia.")
                else:
                    if base_to_block - base_from_block + 1 > BASE_MAX_NEW_BLOCKS_PER_RUN:
                        base_capped_to = base_from_block + BASE_MAX_NEW_BLOCKS_PER_RUN - 1
                        log(
                            f"Base L2: zakres {base_from_block}-{base_to_block} "
                            f"przekracza limit {BASE_MAX_NEW_BLOCKS_PER_RUN} "
                            f"blokow/uruchomienie - przycinam do {base_capped_to} "
                            "(reszta zostanie dogoniona w kolejnych uruchomieniach)."
                        )
                        base_to_block = base_capped_to

                    log(
                        f"Base L2: pobieram nowe transakcje - bloki "
                        f"{base_from_block}-{base_to_block} "
                        f"({base_to_block - base_from_block + 1} blokow)"
                    )
                    base_new_trades = fetch_trades_from_chain_batched(
                        base_rpc,
                        BASE_POOLS,
                        base_from_block,
                        base_to_block,
                        blocks_per_call=BLOCKS_PER_CALL,
                        calls_per_batch=CALLS_PER_BATCH,
                        on_progress=log,
                    )
                    log(f"Base L2: nowych transakcji: {len(base_new_trades)}")

                    base_combined_buffer = base_trade_buffer + base_new_trades
                    base_lookback_start = base_to_block - BASE_LOOKBACK_BLOCKS
                    base_trimmed_buffer = [
                        t for t in base_combined_buffer if t.block > base_lookback_start
                    ]

                    st.save_base_trade_buffer(base_trimmed_buffer)
                    st.save_base_collector_state({"last_processed_block": base_to_block})
                    log(
                        f"Base L2: bufor po przycieciu: {len(base_trimmed_buffer)} "
                        "transakcji (zapisano data/base_trade_buffer.csv)."
                    )
        except Exception as exc:  # noqa: BLE001
            # Faza B0 jest swiadomie IZOLOWANA od reszty pipeline'u - blad po
            # stronie Base (throttling, zmiana API, przejsciowy problem
            # sieciowy) NIE MOZE wywrocic calego uruchomienia (mainnet
            # Uniswap + Hyperliquid musza dzialac dalej niezaleznie od tego,
            # co dzieje sie z eksperymentalnym na tym etapie torem Base).
            # Loggujemy i lecimy dalej - kolejne uruchomienie sprobuje
            # ponownie od tego samego zapisanego `last_processed_block`.
            log(f"Base L2: BLAD podczas zbierania danych ({exc!r}) - pomijam ten krok w tym uruchomieniu.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
