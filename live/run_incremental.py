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
    SpotPoolEngine,
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
# co idzie po nim.
#
# PODNIESIONE z 1000 do 2500 (2026-09-14, zgloszenie uzytkownika po
# zauwazeniu na dashboardzie, ze `baseWindowTime` utknal ~4 dni w tyle za
# realnym czasem) - potwierdzenie w praktyce, o ktore prosil komentarz
# wyzej: od integracji Base (2026-09-11) throttling przy 1000 sie NIE
# powtorzyl (w przeciwienstwie do incydentu z 5000 powyzej), ale
# `last_processed_block`
# w `data/base_collector_state.json` rosl PRZY KAZDYM uruchomieniu o
# dokladnie 1000 blokow (bezustannie w limit) - przy czasie bloku Base ~2s
# to zaledwie ~33 min "czasu Base" na uruchomienie, mniej niz realny odstep
# miedzy uruchomieniami (~40-60 min, patrz harmonogram w update.yml), wiec
# zaleglosc TRWALE rosla zamiast sie stabilizowac (potwierdzone: ~4 dni
# realnego opoznienia w momencie tej poprawki). 2500 daje spory zapas ponad
# prog oplacalnosci (~1200/uruchomienie przy tym rytmie) - zaleglosc powinna
# zaczac sie splacac zamiast tylko przestac rosnac - ale swiadomie NIE
# wraca do 5000 (poziomu, ktory juz raz wywolal throttling) - do dalszego
# przestrojenia w gore dopiero po obserwacji logow Actions bez throttlingu
# przy tej wartosci.
BASE_MAX_NEW_BLOCKS_PER_RUN = int(os.environ.get("HYDRA_BASE_MAX_NEW_BLOCKS_PER_RUN", "2500"))
# Faza "integracja Base B1-B3" (2026-09-11, zgloszenie uzytkownika: "Tak,
# wdrozmy Base", po rozmowie o tym, ze portfeli sledzonych po stronie
# Uniswap mainnet jest duzo mniej niz po stronie Hyperliquid) - Base
# przestaje byc WYLACZNIE buforowany (Faza B0) i zaczyna WZMACNIAC strone
# spot (decyzja uzytkownika przez AskUserQuestion: "Base wzmacnia strone
# spot", NIE osobny trzeci tor) - patrz blend_composite w main() nizej.
#
# BASE_WINDOW_BLOCKS: mainnet uzywa window_blocks=250 przy zalozeniu
# ~14.4s/blok (patrz ScoringConfig - 250*14.4s=3600s=1h dokladnie). Base
# (OP-stack, publicznie udokumentowany czas bloku ~2s) potrzebuje
# 3600/2=1800 blokow, zeby "swieca" Base pokrywala ten sam ~1h co swieca
# mainnetu - dzieki temu obie serie danych rosna w podobnym tempie, mimo ze
# to formalnie DWA NIEZALEZNE silniki (bloki Base i mainnet nie sa w zaden
# sposob porownywalne, patrz komentarz przy bloku zbierania Base nizej).
BASE_WINDOW_BLOCKS = int(os.environ.get("HYDRA_BASE_WINDOW_BLOCKS", "1800"))

# 7-dniowe okno reputacji, tak samo jak mainnet/Hyperliquid od Fazy "okno
# reputacji 7 dni" (250*24*7 dla mainnetu) - tutaj przeliczone na wlasna
# siatke blokow Base: 1800*24*7 = 302400 blokow (~7 dni przy zalozeniu
# ~2s/blok). WARTOSC STARTOWA (jak kazdy inny prog w tym projekcie).
BASE_CLASSIFICATION_LOOKBACK_BLOCKS = int(
    os.environ.get("HYDRA_BASE_CLASSIFICATION_LOOKBACK_BLOCKS", str(1800 * 24 * 7))
)

# Bramka dojrzalosci dla composite_base (ANALOGICZNA do
# HyperliquidScoringConfig.min_classified_wallets_for_maturity=20, ten sam
# mechanizm "graceful degradation" - composite_base=None dopoki populacja
# sklasyfikowanych portfeli Base nie urosnie wystarczajaco, blend_composite
# wtedy zwraca WYLACZNIE mainnet, bez zadnej zmiany zachowania). WARTOSC
# STARTOWA, taka sama jak dla Hyperliquid - do przestrojenia po obserwacji.
BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY = int(
    os.environ.get("HYDRA_BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY", "20")
)

# USUNIETE (Faza "wspolna pula SPOT", 2026-09-11) - `BASE_SPOT_WEIGHT`/
# `HYDRA_BASE_SPOT_WEIGHT` istnialy tu wczesniej (Faza "integracja Base
# B1-B3"): stala waga 50/50, z jaka `composite_base` byl blendowany z
# `composite_spot` mainnetu. Zgloszenie uzytkownika po zobaczeniu zywych
# danych: przy zaledwie 7 sklasyfikowanych transakcjach Base w oknie (wobec
# 31 na Uniswapie) Base i tak dostawal pelne 50% wagi - garstka portfeli
# potrafila przeciagnac polaczona wartosc z SHORT do NEUTRALNIE. Zamiast
# stalej wagi: Uniswap i Base sa teraz w JEDNEJ wspolnej puli liczników
# (patrz `SpotPoolEngine` w hydra_signals/scoring.py i jego uzycie w
# main() nizej) - wplyw kazdego venue jest proporcjonalny do jego
# RZECZYWISTEJ aktywnosci w danym oknie, bez zadnej sztywnej stalej. Jesli
# ktos ma jeszcze ustawiona zmienna `HYDRA_BASE_SPOT_WEIGHT` w GitHub
# Actions - jest teraz calkowicie nieszkodliwie ignorowana (nic juz jej nie
# czyta).

# Okno przycinania bufora `base_trade_buffer.csv` - od Fazy "integracja Base
# B1-B3" to JUZ NIE tylko zabezpieczenie przed nieograniczonym wzrostem
# pliku (jak w Fazie B0), tylko REALNE okno reputacji - MUSI byc >=
# max(BASE_CLASSIFICATION_LOOKBACK_BLOCKS, BASE_LT_CLASSIFICATION_LOOKBACK_BLOCKS)
# ponizej, inaczej klasyfikacja portfeli nie mialaby z czego czytac historii
# starszej niz ten bufor. Podniesione z 43200 (~24h, Faza B0) do 310000
# (~7 dni + maly zapas), a od Fazy "Long term (30d)" (2026-09-14) DALEJ
# podniesione do 1330000 (~30 dni + analogiczny maly zapas), zeby ten sam
# bufor mogl zasilic TAKZE nowy, rownolegly `base_lt_engine` (patrz nizej) -
# dokladnie ten sam powod, dla ktorego trzeba bylo poszerzyc rowniez
# `DEFAULT_BUFFER_LOOKBACK_HOURS` w hydra_signals/data_sources/hyperliquid_ws.py.
# W przeciwienstwie do bufora Hyperliquid (ktory z tego samego powodu MUSIAL
# wyjsc poza git, patrz Faza "bufor poza git") - transakcje Base sa duzo
# rzadsze (obecnie ~13000 wierszy/24h), wiec nawet przy oknie 30-dniowym to
# szacunkowo tylko ~30-35MB pliku CSV w repo - wciaz bezpiecznie ponizej
# twardego limitu GitHuba (100MB/blob), bez ryzyka powtorzenia problemu
# rozmiaru z Hyperliquid.
BASE_LOOKBACK_BLOCKS = int(os.environ.get("HYDRA_BASE_LOOKBACK_BLOCKS", "1330000"))

# --- Faza "Long term (30d)" (2026-09-14, zgloszenie uzytkownika: "moglibysmy
# liczyc i to i to - sygnal dla 7d oraz sygnal dla 30d? Na frontendzie
# zakladka Long term") - DRUGI, w pelni rownolegly tor klasyfikacji/scoringu,
# obok istniejacego 7-dniowego wyzej. NIE zastepuje 7-dniowego okna reputacji
# (`cfg.classification_lookback_blocks`/`BASE_CLASSIFICATION_LOOKBACK_BLOCKS`/
# `HyperliquidScoringConfig.classification_lookback_hours` - wszystkie trzy
# zostaja BEZ ZMIANY), tylko dostarcza DRUGI wynik liczony na TYCH SAMYCH juz
# zebranych transakcjach - zero dodatkowych wywolan RPC/Alchemy (patrz
# rozmowa w projekcie: koszt to WYLACZNIE wieksze bufory/stan na dysku, nie
# wiecej zapytan do sieci).
#
# NAZEWNICTWO "Lt" (nie "30d"!) - CELOWA decyzja, zeby uniknac kolizji z juz
# istniejacym, CALKOWICIE NIEZWIAZANYM pojeciem "30d" w
# `hydra_signals/regime.py::HORIZONS_IN_WINDOWS` - tam "30d" to horyzont
# MOMENTUM (o ile zmieniala sie dana metryka w ciagu ostatnich 30 dni), a
# tutaj chodzi o "ile historii transakcji portfela bierzemy pod uwage przy
# klasyfikacji GOOD/BAD" - dwie zupelnie rozne rzeczy, ktore przypadkiem
# maja ta sama liczbe dni. Wszystkie NOWE stale/pola/pliki stanu tej fazy
# uzywaja sufiksu "Lt"/"_lt" w kodzie Python i kluczach JSON - user-facing
# etykieta na stronie moze dalej mowic "Long term (30D)"/"zakladka Long
# term", to dwie oddzielne warstwy nazewnictwa.
#
# WARTOSCI STARTOWE (250*24*30 / 1800*24*30 / 720.0h) - analogiczne do
# istniejacych 7-dniowych odpowiednikow wyzej, tylko *30 zamiast *7 dni -
# ten sam wzor, ta sama logika, po prostu dluzsze okno.
LT_CLASSIFICATION_LOOKBACK_BLOCKS = int(
    os.environ.get("HYDRA_LT_CLASSIFICATION_LOOKBACK_BLOCKS", str(250 * 24 * 30))
)
BASE_LT_CLASSIFICATION_LOOKBACK_BLOCKS = int(
    os.environ.get("HYDRA_BASE_LT_CLASSIFICATION_LOOKBACK_BLOCKS", str(1800 * 24 * 30))
)
# Uzywana bezposrednio przez HyperliquidScoringConfig przy tworzeniu
# drugiego (Lt) silnika Hyperliquid nizej w main() - patrz tez odpowiadajaca
# jej zmiana `DEFAULT_BUFFER_LOOKBACK_HOURS` (168.0 -> 720.0) w
# hydra_signals/data_sources/hyperliquid_ws.py (bufor musi byc co najmniej
# tak dlugi jak najdluzsze uzywane okno klasyfikacji, dokladnie z tego
# samego powodu co BASE_LOOKBACK_BLOCKS wyzej).
HYPERLIQUID_LT_CLASSIFICATION_LOOKBACK_HOURS = float(
    os.environ.get("HYDRA_HYPERLIQUID_LT_CLASSIFICATION_LOOKBACK_HOURS", "720.0")
)


def log(msg: str) -> None:
    print(f"[hydra] {msg}", flush=True)


def fmt_warsaw(ts: int) -> str:
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).astimezone(WARSAW)
    return dt.strftime("%d.%m.%Y, %H:%M")


def _finalize_and_save_manifest(manifest: dict, started_at: datetime.datetime) -> None:
    """Domyka manifest tego uruchomienia (czas zakonczenia, czas trwania) i
    zapisuje go na dysk - wywolywane tuz przed KAZDYM `return` z `main()`
    (rowniez tym "brak nowych blokow - nic do zrobienia"), zeby kazde
    uruchomienie zostawilo slad, nie tylko te, ktore realnie cos zrobily."""
    finished_at = datetime.datetime.now(datetime.timezone.utc)
    manifest["finished_at_utc"] = finished_at.isoformat()
    manifest["duration_s"] = round((finished_at - started_at).total_seconds(), 1)
    st.save_run_manifest(manifest)


def _synthetic_lt_view(candle: dict) -> dict:
    """Faza "Long term (30d)" - `hydra_signals/regime.py` (compute_momentum,
    RegimeEngine.process_candle, detect_special_event) czyta WYLACZNIE
    stale, niesufiksowane nazwy pol (`goodPressure`/`badPressure`/
    `divergence`/`breadth`/`momentum_{h}_good`/`goodBullishFlips`/
    `goodBearishFlips`) - nie wie nic o torze Lt ani o jego sufiksowanych
    polach (`goodPressureLt` itd.), i CELOWO nie powinien: to ta sama,
    juz przetestowana logika regime/momentum, ktora ma dzialac IDENTYCZNIE
    niezaleznie od tego, ktory tor (7d czy Lt) ja karmi.

    Ta funkcja buduje wiec "widok" swiecy, w ktorym pola Lt danej swiecy sa
    podstawione POD zwykle nazwy - dzieki temu `regime.py` mozna wywolac
    BEZ ZADNEJ zmiany, a wynik (rowniez pod zwyklymi nazwami) wywolujacy
    (`main()` nizej) z powrotem PREFIKSUJE sufiksem "Lt" przed dopisaniem do
    prawdziwego rekordu swiecy. Dziala zarowno dla `candle` (swiezo
    policzona swieca, jeszcze przed dopisaniem do historii - moze miec
    tylko goodPressureLt/itd. bez momentum_*), jak i dla kazdego wpisu z
    `candles_history` (juz kompletny, z wlasnym momentum_*_lt policzonym w
    swoim czasie).

    Wallet Flip (`goodBullishFlips`/`goodBearishFlips`) NIE ma odpowiednika
    "Lt" w ogole (patrz obszerny komentarz przy wczytywaniu stanu Lt w
    main()) - ten sam tor jest lookback-niezalezny, wiec widok Lt po prostu
    POZYCZA zwykle pola wprost, bez zadnego przemianowania.
    """
    return {
        "block": candle["block"],
        "goodPressure": candle.get("goodPressureLt", 0.0),
        "badPressure": candle.get("badPressureLt", 0.0),
        "divergence": candle.get("divergenceLt", 0.0),
        "breadth": candle.get("breadthLt", 0.5),
        "goodBullishFlips": candle.get("goodBullishFlips", 0),
        "goodBearishFlips": candle.get("goodBearishFlips", 0),
    }


def main() -> int:
    rpc_url = os.environ.get("ALCHEMY_RPC_URL")
    if not rpc_url:
        log("BLAD: brak zmiennej srodowiskowej ALCHEMY_RPC_URL (ustaw jako GitHub Actions secret).")
        return 1

    rpc = JsonRpcClient(rpc_url)
    cfg = ScoringConfig()

    # Faza "manifesty per-run" - budowany w trakcie calego main() (kazda
    # sekcja ponizej dopisuje swoj fragment w momencie, gdy zna juz wynik),
    # zapisywany przez `_finalize_and_save_manifest()` tuz przed KAZDYM
    # `return` z tej funkcji - patrz definicja tego helpera wyzej.
    run_started_at = datetime.datetime.now(datetime.timezone.utc)
    manifest: dict = {
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "trigger": os.environ.get("GITHUB_EVENT_NAME", "manual"),
        "commit_sha": os.environ.get("GITHUB_SHA", "unknown"),
        "started_at_utc": run_started_at.isoformat(),
        "finished_at_utc": None,
        "duration_s": None,
        "mainnet": {"enabled": True},
        "base": {"enabled": False},
        "hyperliquid": {"enabled": True},
        "config_snapshot": {
            "base_max_new_blocks_per_run": BASE_MAX_NEW_BLOCKS_PER_RUN,
            "max_new_blocks_per_run": MAX_NEW_BLOCKS_PER_RUN,
            "backfill_blocks": BACKFILL_BLOCKS,
            "perp_weight": HYPERLIQUID_PERP_WEIGHT,
        },
        "warnings": [],
    }

    scoring_state = st.load_scoring_state()
    trade_buffer = st.load_trade_buffer()
    wallets_seen = st.load_wallets_seen()
    candles_history = st.load_candles_history()
    regime_state = st.load_regime_state()
    signal_state = st.load_signal_state()
    wallet_flip_state = st.load_wallet_flip_state()
    hyperliquid_scoring_state = st.load_hyperliquid_scoring_state()
    hyperliquid_wallets_seen = st.load_hyperliquid_wallets_seen()
    base_scoring_state = st.load_base_scoring_state()
    base_wallets_seen = st.load_base_wallets_seen()
    spot_pool_state = st.load_spot_pool_state()

    # --- Faza "Long term (30d)" - stan DRUGIEGO, rownoleglego toru (patrz
    # stale LT_* na poczatku pliku). Celowo BRAK odpowiednikow dla
    # `wallets_seen`/`hyperliquid_wallets_seen`/`base_wallets_seen`/
    # `wallet_flip_state` powyzej - te cztery sa MATEMATYCZNIE IDENTYCZNE
    # niezaleznie od dlugosci okna klasyfikacji (zbior "wszystkich
    # kiedykolwiek widzianych portfeli" i dlugosc biezacego ciagu
    # kupno/sprzedaz per portfel nie zaleza od tego, ile historii bierzemy
    # pod uwage przy KLASYFIKACJI GOOD/BAD - patrz rozdzial "Problem Solving"
    # w dokumentacji projektu) - tor Lt POZYCZA je wprost z toru 7d nizej
    # (ten sam obiekt `wallets_seen`/itd.), zero duplikowania plikow stanu.
    scoring_state_lt = st.load_scoring_state_lt()
    base_scoring_state_lt = st.load_base_scoring_state_lt()
    hyperliquid_scoring_state_lt = st.load_hyperliquid_scoring_state_lt()
    spot_pool_state_lt = st.load_spot_pool_state_lt()
    signal_state_lt = st.load_signal_state_lt()
    regime_state_lt = st.load_regime_state_lt()

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

    manifest["hyperliquid"] = {"enabled": True, "new_trades": len(new_hl_trades)}

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
            # Faza "znaczniki czasu w karcie Wallets" (2026-09-13, zgloszenie
            # uzytkownika: "czy moglibysmy dodac... informacje z ktorej
            # godziny sa te dane?") - Hyperliquid juz i tak liczy koniec
            # swojego okna jako znacznik czasu w ms (`window_end_ts_ms`,
            # patrz HyperliquidScoringEngine/hl_engine.run wyzej) - zero
            # nowego zrodla danych, tylko sformatowanie tego, co juz mielismy,
            # tym samym `fmt_warsaw()` co reszta strony (ten sam format,
            # "DD.MM.YYYY, HH:MM", czas polski).
            "window_time": fmt_warsaw(hl_score.window_end_ts_ms / 1000),
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
                "window_time": None,
            }

    composite_perp = perp_snapshot["composite"]

    # --- Faza "Long term (30d)" - DRUGI, w pelni rownolegly silnik
    # Hyperliquid (Lt), karmiony DOKLADNIE TYMI SAMYMI transakcjami
    # (`new_hl_trades`/`history_hl_trades`/`hl_window_end_ts_ms` wyzej) co
    # silnik 7d - jedyna roznica to dluzsze `classification_lookback_hours`
    # w konfiguracji (patrz `HYPERLIQUID_LT_CLASSIFICATION_LOOKBACK_HOURS`
    # na poczatku pliku). Wlasny stan na dysku
    # (`hyperliquid_scoring_state_lt.json`), ale BEZ osobnego
    # `total_tracked`/`hyperliquid_wallets_seen_lt.txt` - `tracked` portfeli
    # nie zalezy od dlugosci okna klasyfikacji (patrz obszerny komentarz
    # przy wczytywaniu stanu Lt wyzej), wiec `initial_total_tracked`
    # POZYCZONE wprost z toru 7d (`hyperliquid_wallets_seen`). Z tego samego
    # powodu `perp_snapshot_lt` NIE powtarza `tracked`/`active` - front-end
    # bedzie mogl bez zadnej straty scislosci uzyc zwyklych
    # `perpTracked`/`perpActive` rowniez dla zakladki Long term.
    hl_engine_lt = HyperliquidScoringEngine(
        HyperliquidScoringConfig(
            classification_lookback_hours=HYPERLIQUID_LT_CLASSIFICATION_LOOKBACK_HOURS
        ),
        initial_ema=hyperliquid_scoring_state_lt,
        initial_total_tracked=hyperliquid_wallets_seen,
    )
    hl_score_lt = None
    if new_hl_trades:
        hl_score_lt = hl_engine_lt.run(
            new_hl_trades, history_trades=history_hl_trades, window_end_ts_ms=hl_window_end_ts_ms
        )

    if hl_score_lt is not None:
        perp_snapshot_lt = {
            "composite": hl_score_lt.composite_score if hl_score_lt.is_mature else None,
            "is_mature": hl_score_lt.is_mature,
            "classified": hl_score_lt.n_classified_wallets,
            "good_buyers": hl_score_lt.good_buyers,
            "good_sellers": hl_score_lt.good_sellers,
            "bad_buyers": hl_score_lt.bad_buyers,
            "bad_sellers": hl_score_lt.bad_sellers,
        }
        new_hl_state_lt = hl_engine_lt.export_state()
        new_hl_state_lt["last_processed_ts_ms"] = hl_score_lt.window_end_ts_ms
        new_hl_state_lt["last_perp_snapshot_lt"] = perp_snapshot_lt
        st.save_hyperliquid_scoring_state_lt(new_hl_state_lt)
        log(
            f"Hyperliquid (Long term/30d): {hl_score_lt.n_classified_wallets} "
            "sklasyfikowanych portfeli "
            f"({'dojrzale' if hl_score_lt.is_mature else 'jeszcze NIEDOJRZALE - compositePerpLt=None'})."
        )
    else:
        # Ta sama logika "brak nowych transakcji - odczytaj ostatni znany
        # snapshot" co dla toru 7d wyzej - bez zadnej galezi zgodnosci
        # wstecznej (to NOWY klucz stanu, nie ma starszego formatu do
        # udzwigniecia).
        perp_snapshot_lt = hyperliquid_scoring_state_lt.get("last_perp_snapshot_lt")
        if perp_snapshot_lt is None:
            perp_snapshot_lt = {
                "composite": None,
                "is_mature": False,
                "classified": 0,
                "good_buyers": 0,
                "good_sellers": 0,
                "bad_buyers": 0,
                "bad_sellers": 0,
            }

    composite_perp_lt = perp_snapshot_lt["composite"]

    # --- Faza "integracja Base B1-B3" (2026-09-11) - `base_snapshot` niesie
    # dokladnie to samo co `perp_snapshot` wyzej (composite/tracked/active/
    # classified/dojrzalosc/buyers-sellers), ale z JEDNA istotna roznica w
    # tym, KIEDY jest liczony: `hl_score` powyzej moze zostac SWIEZO
    # przeliczony w TYM samym uruchomieniu (dane Hyperliquid zbiera OSOBNY
    # workflow, wiec sa juz gotowe na starcie tego skryptu) - `base_snapshot`
    # NIGDY nie jest liczony tutaj. Scoring Base dzieje sie na SAMYM KONCU
    # main() (patrz obszerny komentarz przy bloku "Base L2" nizej) - z tego
    # samego powodu bezpieczenstwa, dla ktorego samo ZBIERANIE danych Base
    # jest tam umieszczone (throttling Base nie moze zaszkodzic krytycznemu
    # torowi mainnet+Hyperliquid). Wartosc uzyta TUTAJ do zblendowania
    # pochodzi wiec z POPRZEDNIEGO uruchomienia - swiadomy, udokumentowany
    # ~1h lag (patrz `hydrav2-automation.md`), nieszkodliwy: Base WZMACNIA
    # prozke spot, nie jest jej jedynym zrodlem.
    base_snapshot = base_scoring_state.get("last_base_snapshot")
    if base_snapshot is None:
        base_snapshot = {
            "composite": None,
            "is_mature": False,
            "tracked": len(base_wallets_seen),
            "active": 0,
            "classified": 0,
            "good_buyers": 0,
            "good_sellers": 0,
            "bad_buyers": 0,
            "bad_sellers": 0,
            "window_time": None,
            "good_buy_weight": 0.0,
            "good_sell_weight": 0.0,
            "bad_buy_weight": 0.0,
            "bad_sell_weight": 0.0,
        }
    composite_base = base_snapshot["composite"]

    # --- Faza "Long term (30d)" - odpowiednik `base_snapshot` powyzej, ale
    # dla toru Lt (`base_scoring_state_lt`, wczytany na poczatku main()).
    # Ten sam ~1h lag (swiezy `new_base_snapshot_lt` liczony na SAMYM KONCU
    # main() - patrz blok "Base L2" nizej), z tego samego powodu
    # bezpieczenstwa co `base_snapshot`. Bez `tracked`/`active` - patrz
    # komentarz przy `perp_snapshot_lt` wyzej (te dwie wartosci sa
    # lookback-niezalezne, front-end reuzyje `baseTracked`/`baseActive`).
    base_snapshot_lt = base_scoring_state_lt.get("last_base_snapshot_lt")
    if base_snapshot_lt is None:
        base_snapshot_lt = {
            "composite": None,
            "is_mature": False,
            "classified": 0,
            "good_buyers": 0,
            "good_sellers": 0,
            "bad_buyers": 0,
            "bad_sellers": 0,
            "good_buy_weight": 0.0,
            "good_sell_weight": 0.0,
            "bad_buy_weight": 0.0,
            "bad_sell_weight": 0.0,
        }
    composite_base_lt = base_snapshot_lt["composite"]

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
        manifest["mainnet"] = {
            "enabled": True,
            "from_block": from_block,
            "to_block": to_block,
            "new_trades": 0,
            "capped": False,
        }
        _finalize_and_save_manifest(manifest, run_started_at)
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

    manifest["mainnet"] = {
        "enabled": True,
        "from_block": from_block,
        "to_block": to_block,
        "new_trades": len(new_trades),
        "capped": to_block < head,
    }

    combined_buffer = trade_buffer + new_trades
    # Faza "Long term (30d)" - przycinanie MUSI uwzgledniac NAJDLUZSZE z obu
    # uzywanych okien klasyfikacji (7d i Lt), inaczej surowa historia
    # transakcji potrzebna nowemu, 30-dniowemu `mainnet_lt_engine` (nizej)
    # zostalaby odrzucona z bufora ZANIM ten silnik zdazylby jej kiedykolwiek
    # uzyc. Tor 7d (`cfg.classification_lookback_blocks`) dziala DALEJ bez
    # zadnej zmiany zachowania - po prostu odfiltrowuje sobie z tego samego,
    # szerszego bufora tylko wlasne, krotsze okno (patrz
    # `ScoringEngine.run()`/`lookback_start` wewnatrz hydra_signals/scoring.py).
    lookback_start = to_block - max(cfg.classification_lookback_blocks, LT_CLASSIFICATION_LOOKBACK_BLOCKS)
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

    # Faza "wspolna pula SPOT" - `SpotPoolEngine` zastepuje wczesniejszy
    # `blend_composite(composite_spot, composite_base, BASE_SPOT_WEIGHT)`
    # (patrz USUNIETY komentarz `BASE_SPOT_WEIGHT` wyzej i docstring klasy
    # w hydra_signals/scoring.py). MIGRACJA: jesli `spot_pool_state.json`
    # jeszcze nie istnieje (pierwsze uruchomienie po wdrozeniu tej fazy),
    # NIE zaczynamy z EMA=None "na zimno" - dziedziczymy JUZ ROZGRZANE EMA
    # z `scoring_state` (mainnet, wczytany wyzej, PRZED aktualizacja w tym
    # uruchomieniu). Dzieki temu, dopoki Base nie wnosi zadnych transakcji
    # do puli (niedojrzaly/nieskonfigurowany - patrz zerowanie licznikow
    # Base nizej), `composite_spot_combined` zostaje BAJT W BAJT identyczne
    # z `compositeSpot` (Uniswap), bez sztucznego okresu rozgrzewania -
    # dokladnie ta sama gwarancja "zero ryzyka", co przy wprowadzeniu
    # samego Base (Faza B1-B3).
    spot_pool_engine = SpotPoolEngine(
        cfg,
        initial_ema=spot_pool_state if spot_pool_state else scoring_state,
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

    # --- Faza "Long term (30d)" - DRUGI, w pelni rownolegly komplet
    # silnikow (mainnet/spot-pool/signal/regime), kazdy identyczny co do
    # klasy z odpowiednikiem 7d powyzej, karmiony DOKLADNIE TYMI SAMYMI
    # `scoreable_trades`/`classification_history`/`price_at_block` -
    # jedyna roznica to `classification_lookback_blocks` w konfiguracji
    # (patrz `LT_CLASSIFICATION_LOOKBACK_BLOCKS`). `window_blocks` NIEZMIENIONE
    # (`cfg.window_blocks` odziedziczone wprost z `cfg` 7d), wiec obie petle
    # `.run()` licza IDENTYCZNE granice okien - gwarantuje to, ze
    # `new_scores`/`new_scores_lt` maja te sama dlugosc i pasujace
    # `window_end_block` na kazdym indeksie (bezpieczne do zipowania w petli
    # nizej). `initial_total_tracked=wallets_seen` POZYCZONE z toru 7d (patrz
    # obszerny komentarz przy wczytywaniu stanu Lt na poczatku main()) - BEZ
    # `initial_wallet_flip_state`: Wallet Flip jest lookback-niezalezny, wiec
    # `mainnet_lt_engine` w ogole nie musi go liczyc/eksportowac - front-end
    # bedzie mogl reuzyc zwykle `goodBullishFlips`/itd. rowniez dla zakladki
    # Long term.
    lt_cfg = ScoringConfig(classification_lookback_blocks=LT_CLASSIFICATION_LOOKBACK_BLOCKS)
    has_prior_state_lt = bool(scoring_state_lt)
    mainnet_lt_engine = ScoringEngine(
        lt_cfg,
        initial_ema=scoring_state_lt if has_prior_state_lt else None,
        initial_prev_signal=Signal(scoring_state_lt.get("prev_signal", "HOLD")),
        initial_total_tracked=wallets_seen,
    )
    spot_pool_engine_lt = SpotPoolEngine(
        cfg,
        initial_ema=spot_pool_state_lt if spot_pool_state_lt else scoring_state_lt,
    )
    regime_engine_lt = regime.RegimeEngine(initial_state=regime_state_lt)
    signal_engine_lt = SignalEngine(initial_state=signal_state_lt)

    new_scores = []
    new_scores_lt = []
    if scoreable_trades:
        new_scores = engine.run(scoreable_trades, price_at_block, history_trades=classification_history)
        new_scores_lt = mainnet_lt_engine.run(
            scoreable_trades, price_at_block, history_trades=classification_history
        )
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
                manifest["warnings"].append(f"mainnet: brak znacznika czasu dla bloku {b}")

        # Faza "Long term (30d)" - `new_scores`/`new_scores_lt` maja
        # GWARANTOWANA identyczna dlugosc i pasujace `window_end_block` na
        # kazdym indeksie (oba silniki dostaly te same `scoreable_trades` i
        # to samo `window_blocks` - patrz komentarz przy `mainnet_lt_engine`
        # wyzej) - bezpieczne zipowanie, `s_lt` to Lt-odpowiednik `s` dla
        # TEJ SAMEJ swiecy/okna.
        for s, s_lt in zip(new_scores, new_scores_lt):
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
            #
            # --- Faza "wspolna pula SPOT" (2026-09-11) - zamiast blendowac
            # `composite_base` (osobne EMA Base) z `s.composite_score`
            # (mainnet) stala waga 50/50 (jak w Fazie "integracja Base
            # B1-B3"), SUMUJEMY surowe liczniki good/bad buyers-sellers
            # OBU torow w JEDNA polaczona pule i liczymy z niej JEDEN
            # wskaznik przez `spot_pool_engine` (patrz jego docstring w
            # hydra_signals/scoring.py). Gdy Base jest niedojrzaly/
            # nieskonfigurowany (`base_snapshot["is_mature"]` False),
            # wnosi ZERO do liczników - pula naturalnie redukuje sie do
            # samego Uniswapa, ta sama gwarancja "graceful degradation" co
            # wczesniej przy `blend_composite(spot, None)`.
            # (nazwa celowo INNA niz `base_is_mature` uzywana pozniej w
            # bloku zbierania Base na koncu main() - to dwie ODREBNE zmienne
            # lokalne w tej samej funkcji, zeby nie sugerowac mylnie, ze to
            # ten sam stan).
            base_counts_mature = base_snapshot["is_mature"]
            pool_good_buyers = s.good_buyers + (base_snapshot["good_buyers"] if base_counts_mature else 0)
            pool_good_sellers = s.good_sellers + (base_snapshot["good_sellers"] if base_counts_mature else 0)
            pool_bad_buyers = s.bad_buyers + (base_snapshot["bad_buyers"] if base_counts_mature else 0)
            pool_bad_sellers = s.bad_sellers + (base_snapshot["bad_sellers"] if base_counts_mature else 0)
            # Faza "wazenie wolumenem SPOT" (2026-09-11) - analogiczne
            # pulowanie dla wag sqrt-capped (patrz WindowScore/
            # ScoringEngine.run w hydra_signals/scoring.py). `.get(...,
            # 0.0)` - stan Base zapisany PRZED ta faze (w tym juz na zywym
            # repo w momencie jej wdrozenia) nie ma jeszcze tych kluczy;
            # brakujace = 0.0, co dla takiego "starego" stanu i tak jest
            # wlasciwa wartoscia startowa (brak wkladu Base do puli
            # wazonej, dopoki Base nie policzy swiezego snapshotu z tymi
            # polami - patrz blok "Base L2" na koncu main()).
            pool_good_buy_weight = s.good_buy_weight + (
                base_snapshot.get("good_buy_weight", 0.0) if base_counts_mature else 0.0
            )
            pool_good_sell_weight = s.good_sell_weight + (
                base_snapshot.get("good_sell_weight", 0.0) if base_counts_mature else 0.0
            )
            pool_bad_buy_weight = s.bad_buy_weight + (
                base_snapshot.get("bad_buy_weight", 0.0) if base_counts_mature else 0.0
            )
            pool_bad_sell_weight = s.bad_sell_weight + (
                base_snapshot.get("bad_sell_weight", 0.0) if base_counts_mature else 0.0
            )
            composite_spot_combined = spot_pool_engine.update(
                good_buyers=pool_good_buyers,
                good_sellers=pool_good_sellers,
                bad_buyers=pool_bad_buyers,
                bad_sellers=pool_bad_sellers,
                good_buy_weight=pool_good_buy_weight,
                good_sell_weight=pool_good_sell_weight,
                bad_buy_weight=pool_bad_buy_weight,
                bad_sell_weight=pool_bad_sell_weight,
            )
            composite_final = blend_composite(
                composite_spot_combined, composite_perp, perp_weight=HYPERLIQUID_PERP_WEIGHT
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

            # Faza "poprawka komunikatu o rozbieznosci" (2026-09-13,
            # zgloszenie uzytkownika po zobaczeniu banera "sam spot (Uniswap)
            # wskazywalby..." na zywo: `signalSpotOnly` powyzej liczy sie
            # WYLACZNIE z mainnetu - dokladnie tak jak przed Faza "integracja
            # Base B1-B3"/"wspolna pula SPOT", ktore od dawna wlaczyly Base DO
            # puli SPOT (`composite_spot_combined` powyzej). Banner uzywajacy
            # `signalSpotOnly` do porownania z finalnym sygnalem byl wiec
            # myslacy: winil "Base i/lub Hyperliquid" za rozbieznosc, mimo ze
            # Base to od dawna CZESC "spotu", nie osobna przyczyna obok niego.
            # To nowe pole liczy ten sam naiwny, bezstanowy prog
            # (`decide_signal()`, jak `signalSpotOnly`), ale na JUZ POLACZONEJ
            # wartosci `composite_spot_combined` (Uniswap+Base) - i uzywa
            # TEGO SAMEGO progu wejscia, co prawdziwy sygnal
            # (`signal_engine.cfg.enter_threshold`), zeby porownanie "czy
            # zblendowany spot sam przekroczylby ten sam prog" bylo
            # rzeczywiscie jabłko-do-jabłka, a nie mieszanie starego progu
            # 0.2 z nowym 0.35. Pozostala niescislosc (SignalEngine ma
            # histereze/potwierdzenie, ta funkcja nie) jest ta sama, ktora
            # baner mial od zawsze - stad wciaz zastrzezenie "uwzgledniajac
            # historie potwierdzen" w jego tresci (patrz template.html).
            # Stare `signalSpotOnly` NIE jest usuwane (wsteczna zgodnosc pola
            # w publicznym candles_history.json) - po prostu front-end
            # przestaje go uzywac do tego konkretnego banera.
            signal_spot_combined_only = decide_signal(
                composite_spot_combined, threshold=signal_engine.cfg.enter_threshold
            )

            # --- Faza "Long term (30d)" - DOKLADNIE ta sama sekwencja
            # obliczen co wyzej (pulowanie Uniswap+Base, SpotPoolEngine,
            # blend z perp, SignalEngine, decide_signal diagnostyczny), ale
            # na skladowych toru Lt (`s_lt`, `base_snapshot_lt`,
            # `composite_perp_lt`, `spot_pool_engine_lt`, `signal_engine_lt`).
            # `base_counts_mature`/gate dojrzalosci Base jest WSPOLNY dla obu
            # torow (ten sam `BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY` -
            # patrz `base_snapshot_lt["is_mature"]`, liczone niezaleznie z
            # WLASNEGO classified count toru Lt w bloku "Base L2" nizej).
            base_counts_mature_lt = base_snapshot_lt["is_mature"]
            pool_good_buyers_lt = s_lt.good_buyers + (
                base_snapshot_lt["good_buyers"] if base_counts_mature_lt else 0
            )
            pool_good_sellers_lt = s_lt.good_sellers + (
                base_snapshot_lt["good_sellers"] if base_counts_mature_lt else 0
            )
            pool_bad_buyers_lt = s_lt.bad_buyers + (
                base_snapshot_lt["bad_buyers"] if base_counts_mature_lt else 0
            )
            pool_bad_sellers_lt = s_lt.bad_sellers + (
                base_snapshot_lt["bad_sellers"] if base_counts_mature_lt else 0
            )
            pool_good_buy_weight_lt = s_lt.good_buy_weight + (
                base_snapshot_lt.get("good_buy_weight", 0.0) if base_counts_mature_lt else 0.0
            )
            pool_good_sell_weight_lt = s_lt.good_sell_weight + (
                base_snapshot_lt.get("good_sell_weight", 0.0) if base_counts_mature_lt else 0.0
            )
            pool_bad_buy_weight_lt = s_lt.bad_buy_weight + (
                base_snapshot_lt.get("bad_buy_weight", 0.0) if base_counts_mature_lt else 0.0
            )
            pool_bad_sell_weight_lt = s_lt.bad_sell_weight + (
                base_snapshot_lt.get("bad_sell_weight", 0.0) if base_counts_mature_lt else 0.0
            )
            composite_spot_combined_lt = spot_pool_engine_lt.update(
                good_buyers=pool_good_buyers_lt,
                good_sellers=pool_good_sellers_lt,
                bad_buyers=pool_bad_buyers_lt,
                bad_sellers=pool_bad_sellers_lt,
                good_buy_weight=pool_good_buy_weight_lt,
                good_sell_weight=pool_good_sell_weight_lt,
                bad_buy_weight=pool_bad_buy_weight_lt,
                bad_sell_weight=pool_bad_sell_weight_lt,
            )
            composite_final_lt = blend_composite(
                composite_spot_combined_lt, composite_perp_lt, perp_weight=HYPERLIQUID_PERP_WEIGHT
            )
            final_signal_lt = signal_engine_lt.process(composite_final_lt)
            signal_spot_combined_only_lt = decide_signal(
                composite_spot_combined_lt, threshold=signal_engine_lt.cfg.enter_threshold
            )

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
                "signalSpotCombinedOnly": signal_spot_combined_only.value,
                # --- Faza "integracja Base B1-B3" - `compositeSpot` powyzej
                # ZOSTAJE nietkniete (wylacznie mainnet, jak przedtem, dla
                # zgodnosci wstecznej znaczenia tego pola). `compositeBase`/
                # `compositeSpotCombined` to NOWE pola: surowa wartosc z
                # Base (albo `null`, dopoki niedojrzaly/nieskonfigurowany) i
                # polaczona wartosc "spot" (mainnet+Base), ktora FAKTYCZNIE
                # idzie dalej do `compositeFinal`/`signal` - patrz blend
                # wyzej. Diagnostyczne pola `base*` nizej - ten sam ksztalt
                # co `perp*` wyzej (karta "Wallets" w Fazie B3, frontend).
                "compositeBase": round(composite_base, 3) if composite_base is not None else None,
                "compositeSpotCombined": round(composite_spot_combined, 3),
                # --- Faza "diagnostyka wazenia wolumenem w UI" (2026-09-12,
                # zgloszenie uzytkownika "czy gdzies na stronie w UX bede
                # widzial wagi?") - `compositeSpotCombined` powyzej to JUZ
                # zblendowany wynik (50/50) dwoch torow liczonych wewnatrz
                # `spot_pool_engine` (patrz SpotPoolEngine w scoring.py);
                # dwa ponizsze pola ujawniaja OBIE skladowe OSOBNO, zeby
                # front-end (karta Wallets) mogl pokazac, jak bardzo dany
                # tor "pociagnal" wynik - `last_composite_*` to zwykle
                # atrybuty ustawiane PRZEZ TO SAMO wywolanie `.update()`
                # wyzej, nie osobne przeliczenie. `spotPoolCompositeWeighted`
                # bedzie `None` tylko gdy caly tor wazony wolumenem nie
                # aktywowal sie w tym wywolaniu (patrz graceful degradation w
                # `SpotPoolEngine.update()`) - w praktyce w tym pipelinie
                # zawsze przekazujemy jawne wagi (choc bywaja zerowe), wiec
                # to pole powinno byc zawsze wypelnione od momentu wdrozenia
                # tej fazy.
                "spotPoolCompositeCounts": round(spot_pool_engine.last_composite_counts, 3)
                if spot_pool_engine.last_composite_counts is not None
                else None,
                "spotPoolCompositeWeighted": round(spot_pool_engine.last_composite_weighted, 3)
                if spot_pool_engine.last_composite_weighted is not None
                else None,
                "baseTracked": base_snapshot["tracked"],
                "baseActive": base_snapshot["active"],
                "baseClassified": base_snapshot["classified"],
                "baseIsMature": base_snapshot["is_mature"],
                "baseGoodBuyers": base_snapshot["good_buyers"],
                "baseGoodSellers": base_snapshot["good_sellers"],
                "baseBadBuyers": base_snapshot["bad_buyers"],
                "baseBadSellers": base_snapshot["bad_sellers"],
                "baseMaturityThreshold": BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY,
                # Faza "znaczniki czasu w karcie Wallets" (2026-09-13) - kiedy
                # (czas polski) zakonczylo sie okno, z ktorego pochodza
                # powyzsze liczby Base. `.get(...)` zamiast `[...]` celowo:
                # `base_snapshot` wczytany ze starego stanu na dysku (sprzed
                # tej fazy) nie bedzie mial klucza "window_time" - wtedy
                # `None`, front-end chowa etykiete (graceful degradation, ten
                # sam wzorzec co reszta projektu).
                "baseWindowTime": base_snapshot.get("window_time"),
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
                # Analogicznie dla Hyperliquid - patrz komentarz przy
                # "baseWindowTime" wyzej (ten sam wzorzec `.get(...)`).
                "perpWindowTime": perp_snapshot.get("window_time"),
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
                # --- Faza "Long term (30d)" - odpowiedniki powyzszych pol dla
                # DRUGIEGO, rownoleglego toru Lt (sufiks "Lt"). CELOWO BRAK
                # "trackedLt"/"activeLt"/"poolLt"/"baseTrackedLt"/
                # "baseActiveLt"/"perpTrackedLt"/"perpActiveLt"/
                # "*BullishFlipsLt"/"*BearishFlipsLt" - wszystkie te wartosci
                # sa MATEMATYCZNIE IDENTYCZNE niezaleznie od dlugosci okna
                # klasyfikacji (patrz obszerny komentarz przy wczytywaniu
                # stanu Lt na poczatku main()) - front-end zakladki Long term
                # ma po prostu reuzyc zwykle, niesufiksowane pola powyzej.
                "signalLt": final_signal_lt.value,
                "compositeLt": round(composite_final_lt, 3),
                "compositeSpotLt": round(s_lt.composite_score, 3),
                "compositePerpLt": round(composite_perp_lt, 3) if composite_perp_lt is not None else None,
                "signalSpotOnlyLt": s_lt.signal.value,
                "signalSpotCombinedOnlyLt": signal_spot_combined_only_lt.value,
                "compositeBaseLt": round(composite_base_lt, 3) if composite_base_lt is not None else None,
                "compositeSpotCombinedLt": round(composite_spot_combined_lt, 3),
                "spotPoolCompositeCountsLt": round(spot_pool_engine_lt.last_composite_counts, 3)
                if spot_pool_engine_lt.last_composite_counts is not None
                else None,
                "spotPoolCompositeWeightedLt": round(spot_pool_engine_lt.last_composite_weighted, 3)
                if spot_pool_engine_lt.last_composite_weighted is not None
                else None,
                "baseClassifiedLt": base_snapshot_lt["classified"],
                "baseIsMatureLt": base_snapshot_lt["is_mature"],
                "baseGoodBuyersLt": base_snapshot_lt["good_buyers"],
                "baseGoodSellersLt": base_snapshot_lt["good_sellers"],
                "baseBadBuyersLt": base_snapshot_lt["bad_buyers"],
                "baseBadSellersLt": base_snapshot_lt["bad_sellers"],
                "perpClassifiedLt": perp_snapshot_lt["classified"],
                "perpIsMatureLt": perp_snapshot_lt["is_mature"],
                "perpGoodBuyersLt": perp_snapshot_lt["good_buyers"],
                "perpGoodSellersLt": perp_snapshot_lt["good_sellers"],
                "perpBadBuyersLt": perp_snapshot_lt["bad_buyers"],
                "perpBadSellersLt": perp_snapshot_lt["bad_sellers"],
                "indGoodShortLt": round(s_lt.ind_good_short, 3),
                "indGoodLongLt": round(s_lt.ind_good_long, 3),
                "indBadShortLt": round(s_lt.ind_bad_short, 3),
                "indBadLongLt": round(s_lt.ind_bad_long, 3),
                "goodBuyersLt": s_lt.good_buyers,
                "goodSellersLt": s_lt.good_sellers,
                "badBuyersLt": s_lt.bad_buyers,
                "badSellersLt": s_lt.bad_sellers,
                "goodPressureLt": round(s_lt.good_trader_pressure, 4),
                "badPressureLt": round(s_lt.bad_trader_pressure, 4),
                "divergenceLt": round(s_lt.smart_money_divergence, 4),
                "breadthLt": round(s_lt.good_trader_breadth, 4),
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

            # --- Faza "Long term (30d)" - odpowiednik momentum/regime bloku
            # powyzej, ale dla toru Lt, przez "syntetyczny widok" (patrz
            # `_synthetic_lt_view` na poczatku pliku) - `compute_momentum`/
            # `regime_engine_lt.process_candle` wywolywane BEZ ZADNEJ zmiany,
            # tylko na slownikach z polami Lt podstawionymi pod zwykle nazwy.
            # Wynik (rowniez pod zwyklymi nazwami, np. "bullScore") jest z
            # powrotem PREFIKSOWANY sufiksem "Lt" ("bullScoreLt") przed
            # dopisaniem do prawdziwego rekordu swiecy - momentum_* dostaje
            # analogicznie sufiks "_lt" (np. "momentum_7d_good_lt"), zeby
            # pasowac do istniejacej konwencji nazewnictwa tych kluczy
            # (ze znakami podkreslenia, nie camelCase).
            synthetic_history_lt = [_synthetic_lt_view(c) for c in candles_history]
            synthetic_now_lt = _synthetic_lt_view(candle)
            momentum_lt = regime.compute_momentum(
                synthetic_history_lt, current=synthetic_now_lt, window_blocks=window_blocks
            )
            momentum_lt_json = regime.momentum_to_json(momentum_lt)
            for key, value in momentum_lt_json.items():
                candle[f"{key}_lt"] = value
            synthetic_now_lt.update(momentum_lt_json)
            regime_out_lt = regime_engine_lt.process_candle(synthetic_now_lt)
            for key, value in regime_out_lt.items():
                candle[f"{key}Lt"] = value

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
    st.save_spot_pool_state(spot_pool_engine.export_state())

    # --- Faza "Long term (30d)" - zapis stanu DRUGIEGO, rownoleglego toru.
    # CELOWO brak odpowiednikow `save_wallets_seen`/`save_wallet_flip_state`
    # dla toru Lt - oba sa lookback-niezalezne i juz zapisane wyzej przez tor
    # 7d (patrz obszerny komentarz przy wczytywaniu stanu Lt w main()).
    # `last_scored_window_end` zapisany tu WYLACZNIE informacyjnie/dla
    # symetrii ksztaltu pliku z `scoring_state.json` - kursor okien jest w
    # praktyce WSPOLNY dla obu torow (oba licza z tych samych
    # `scoreable_trades`, patrz `new_scores_lt = mainnet_lt_engine.run(...)`
    # wyzej), wiec nic go z powrotem nie odczytuje.
    new_state_lt = mainnet_lt_engine.export_state()
    new_state_lt["last_scored_window_end"] = new_last_scored_end
    new_state_lt["updated_at_utc"] = new_state["updated_at_utc"]
    st.save_scoring_state_lt(new_state_lt)
    st.save_regime_state_lt(regime_engine_lt.export_state())
    st.save_signal_state_lt(signal_engine_lt.export_state())
    st.save_spot_pool_state_lt(spot_pool_engine_lt.export_state())

    # Faza "wiarygodna swiezosc" - `lastRunUtc` to zegar SCIANY (kiedy TEN
    # skrypt faktycznie zakonczyl dzialanie), nie znacznik czasu bloku.
    # Front-end (chip swiezosci) uzywa TEGO, zeby uczciwie pokazywac realny
    # odstep miedzy uruchomieniami automatyzacji - patrz komentarz w
    # build_site.py::build_site.
    #
    # Faza "reset licznika wyniku/win rate" (2026-09-06) - opcjonalny,
    # reczenie ustawiany przez uzytkownika plik `data/stats_reset_state.json`
    # (patrz `live/state.py::load_stats_reset_state` docstring). Czysto
    # lokalny odczyt pliku, zero RPC/sieci - bezpieczny tutaj, PRZED blokiem
    # Base L2 nizej, tak jak reszta juz-zaufanej sciezki mainnet+Hyperliquid.
    stats_reset_from_block = st.load_stats_reset_state().get("reset_from_block")
    build_site(
        candles_history,
        meta={
            "lastRunUtc": new_state["updated_at_utc"],
            "statsResetFromBlock": stats_reset_from_block,
        },
    )
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
                manifest["base"] = {
                    "enabled": True,
                    "error": "nie udalo sie pobrac czola lancucha Base",
                }
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
                    manifest["base"] = {
                        "enabled": True,
                        "from_block": base_from_block,
                        "to_block": base_to_block,
                        "new_trades": 0,
                        "capped": False,
                    }
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

                    manifest["base"] = {
                        "enabled": True,
                        "from_block": base_from_block,
                        "to_block": base_to_block,
                        "new_trades": len(base_new_trades),
                        "capped": base_to_block < base_head,
                    }

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

                    # --- Faza "integracja Base B1-B3" (2026-09-11) - scoring
                    # tych samych transakcji (juz w formacie Trade - identyczny
                    # ksztalt co mainnet, patrz BASE_POOLS/decode_swap_log) przez
                    # DRUGA, niezalezna instancje `ScoringEngine` - TA SAMA klasa
                    # co mainnet wyzej, zaden nowy silnik nie byl potrzebny (w
                    # przeciwienstwie do Hyperliquida, ktory mial inny ksztalt
                    # danych i wymagal wlasnej klasy `HyperliquidScoringEngine`).
                    # Wlasne okno (`BASE_WINDOW_BLOCKS`) i wlasny stan na dysku -
                    # bloki Base i mainnet NIE sa porownywalne (patrz obszerny
                    # komentarz na poczatku tego bloku), wiec silniki musza byc
                    # calkowicie niezalezne.
                    #
                    # CELOWY ~1h LAG: `composite_base` uzyty do zblendowania w
                    # TYM uruchomieniu (patrz `base_snapshot` na poczatku main())
                    # pochodzil z POPRZEDNIEGO uruchomienia - dopiero TERAZ (po
                    # tym, jak krytyczny tor mainnet+Hyperliquid juz bezpiecznie
                    # skonczyl i zapisal wyniki) liczymy SWIEZY composite_base,
                    # ktory zostanie uzyty w NASTEPNYM uruchomieniu. Swiadomy
                    # wybor, z tego samego powodu bezpieczenstwa co samo
                    # umieszczenie calego bloku Base na koncu main(): scoring nie
                    # moze poprzedzac ani przeplatac sie z krytycznym torem.
                    # Lag jest nieszkodliwy - Base WZMACNIA prozke spot, nie
                    # jest jej jedynym zrodlem (patrz blend_composite wyzej).
                    base_cfg = ScoringConfig(
                        window_blocks=BASE_WINDOW_BLOCKS,
                        classification_lookback_blocks=BASE_CLASSIFICATION_LOOKBACK_BLOCKS,
                    )
                    base_has_prior_state = bool(base_scoring_state)
                    base_engine = ScoringEngine(
                        base_cfg,
                        initial_ema=base_scoring_state if base_has_prior_state else None,
                        initial_total_tracked=base_wallets_seen,
                    )

                    # --- Faza "Long term (30d)" - DRUGI, rownolegly silnik
                    # Base (Lt), analogicznie do `mainnet_lt_engine` wyzej w
                    # main() - te same `base_scoreable_trades`/
                    # `base_classification_history` (liczone ponizej, WSPOLNE
                    # dla obu torow - okno `BASE_WINDOW_BLOCKS` jest takie
                    # samo), tylko dluzszy
                    # `BASE_LT_CLASSIFICATION_LOOKBACK_BLOCKS`. Bez osobnego
                    # `base_wallets_seen_lt.txt` z tego samego powodu co
                    # `mainnet_lt_engine` (tracked/active lookback-niezalezne).
                    base_lt_cfg = ScoringConfig(
                        window_blocks=BASE_WINDOW_BLOCKS,
                        classification_lookback_blocks=BASE_LT_CLASSIFICATION_LOOKBACK_BLOCKS,
                    )
                    base_has_prior_state_lt = bool(base_scoring_state_lt)
                    base_lt_engine = ScoringEngine(
                        base_lt_cfg,
                        initial_ema=base_scoring_state_lt if base_has_prior_state_lt else None,
                        initial_total_tracked=base_wallets_seen,
                    )
                    base_last_closed_end = (
                        (base_to_block + 1) // BASE_WINDOW_BLOCKS
                    ) * BASE_WINDOW_BLOCKS - 1
                    base_last_scored_end = base_scoring_state.get("last_scored_window_end", -1)
                    base_scoreable_trades = [
                        t
                        for t in base_combined_buffer
                        if base_last_scored_end < t.block <= base_last_closed_end
                    ]
                    base_classification_history = [
                        t for t in base_combined_buffer if t.block <= base_last_scored_end
                    ]

                    if not base_scoreable_trades:
                        log(
                            "Base L2: brak nowo domknietych okien do przeliczenia "
                            "w tym uruchomieniu (composite_base bez zmian)."
                        )
                    else:
                        base_price_source = (
                            base_trimmed_buffer if base_trimmed_buffer else base_new_trades
                        )
                        base_price_at_block = st.price_at_block_factory(base_price_source)
                        base_new_scores = base_engine.run(
                            base_scoreable_trades,
                            base_price_at_block,
                            history_trades=base_classification_history,
                        )
                        # Faza "Long term (30d)" - identyczne wejscie
                        # (`base_scoreable_trades`/`base_classification_history`/
                        # `base_price_at_block`), inny silnik/config (patrz
                        # `base_lt_engine` wyzej) - gwarantowana ta sama
                        # dlugosc/kolejnosc co `base_new_scores` (ten sam
                        # powod co `new_scores`/`new_scores_lt` w glownej
                        # petli mainnetu).
                        base_new_scores_lt = base_lt_engine.run(
                            base_scoreable_trades,
                            base_price_at_block,
                            history_trades=base_classification_history,
                        )
                        if base_new_scores:
                            base_latest = base_new_scores[-1]
                            base_classified = (
                                base_latest.total_good_classified + base_latest.total_bad_classified
                            )
                            base_is_mature = (
                                base_classified >= BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY
                            )

                            # Faza "znaczniki czasu w karcie Wallets" (2026-09-13,
                            # zgloszenie uzytkownika: "czy moglibysmy dodac...
                            # informacje z ktorej godziny sa te dane?"). Base
                            # dotad NIGDZIE nie pobieral wlasnego znacznika czasu
                            # bloku (w odroznieniu od mainnetu, patrz analogiczny
                            # `eth_getBlockByNumber` przy `new_scores` wyzej) -
                            # tu dokladnie ten sam wzorzec, tylko przez `base_rpc`
                            # zamiast `rpc`, dla bloku konczacego okno Base.
                            # Blad/brak odpowiedzi RPC nie jest krytyczny - Base
                            # composite i tak sie liczy, po prostu znacznik
                            # czasu w karcie Wallets bedzie ukryty (graceful
                            # degradation, patrz "window_time": None nizej).
                            base_window_time = None
                            base_ts_results = batch_call_with_retry(
                                base_rpc,
                                [("eth_getBlockByNumber", [hex(base_latest.window_end_block), False])],
                                batch_size=CALLS_PER_BATCH,
                            )
                            if base_ts_results and base_ts_results[0] and "timestamp" in base_ts_results[0]:
                                base_window_time = fmt_warsaw(int(base_ts_results[0]["timestamp"], 16))
                            else:
                                log(
                                    "UWAGA: nie udalo sie pobrac znacznika czasu dla "
                                    f"bloku Base {base_latest.window_end_block} "
                                    "(znacznik czasu Base w karcie Wallets bedzie ukryty)."
                                )
                                manifest["warnings"].append(
                                    "base: brak znacznika czasu dla bloku "
                                    f"{base_latest.window_end_block}"
                                )

                            new_base_snapshot = {
                                "composite": base_latest.composite_score if base_is_mature else None,
                                "is_mature": base_is_mature,
                                "tracked": base_latest.total_wallets_tracked,
                                "active": base_latest.active_wallets,
                                "classified": base_classified,
                                "good_buyers": base_latest.good_buyers,
                                "good_sellers": base_latest.good_sellers,
                                "bad_buyers": base_latest.bad_buyers,
                                "bad_sellers": base_latest.bad_sellers,
                                "window_time": base_window_time,
                                # Faza "wazenie wolumenem SPOT" - te same 4
                                # nowe pola WindowScore co mainnet (patrz
                                # `s.good_buy_weight`/itd. wyzej), pulowane w
                                # NASTEPNYM uruchomieniu razem z mainnetem
                                # (ten sam ~1h lag co reszta base_snapshot -
                                # patrz obszerny komentarz przy jego budowie
                                # na poczatku main()).
                                "good_buy_weight": base_latest.good_buy_weight,
                                "good_sell_weight": base_latest.good_sell_weight,
                                "bad_buy_weight": base_latest.bad_buy_weight,
                                "bad_sell_weight": base_latest.bad_sell_weight,
                            }
                            new_base_state = base_engine.export_state()
                            new_base_state["last_scored_window_end"] = base_latest.window_end_block
                            new_base_state["last_base_snapshot"] = new_base_snapshot
                            new_base_state["updated_at_utc"] = datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat()
                            st.save_base_scoring_state(new_base_state)
                            st.save_base_wallets_seen(base_engine.total_tracked)
                            log(
                                f"Base L2: {base_classified} sklasyfikowanych portfeli, "
                                f"{base_latest.total_wallets_tracked} sledzonych lacznie "
                                f"({'dojrzale' if base_is_mature else 'jeszcze NIEDOJRZALE - composite_base=None'}) "
                                "- wynik zostanie zblendowany ze spot w NASTEPNYM uruchomieniu."
                            )

                            # --- Faza "Long term (30d)" - odpowiednik bloku
                            # powyzej dla toru Lt. Bez `tracked`/`active`/
                            # `window_time` - lookback-niezalezne wzgledem
                            # 7d (ten sam powod co przy `perp_snapshot_lt`/
                            # `base_snapshot_lt` wyzej w main()) - front-end
                            # reuzyje `baseTracked`/`baseActive`/
                            # `baseWindowTime` rowniez dla zakladki Long term.
                            if base_new_scores_lt:
                                base_latest_lt = base_new_scores_lt[-1]
                                base_classified_lt = (
                                    base_latest_lt.total_good_classified
                                    + base_latest_lt.total_bad_classified
                                )
                                base_is_mature_lt = (
                                    base_classified_lt >= BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY
                                )
                                new_base_snapshot_lt = {
                                    "composite": base_latest_lt.composite_score
                                    if base_is_mature_lt
                                    else None,
                                    "is_mature": base_is_mature_lt,
                                    "classified": base_classified_lt,
                                    "good_buyers": base_latest_lt.good_buyers,
                                    "good_sellers": base_latest_lt.good_sellers,
                                    "bad_buyers": base_latest_lt.bad_buyers,
                                    "bad_sellers": base_latest_lt.bad_sellers,
                                    "good_buy_weight": base_latest_lt.good_buy_weight,
                                    "good_sell_weight": base_latest_lt.good_sell_weight,
                                    "bad_buy_weight": base_latest_lt.bad_buy_weight,
                                    "bad_sell_weight": base_latest_lt.bad_sell_weight,
                                }
                                new_base_state_lt = base_lt_engine.export_state()
                                new_base_state_lt["last_scored_window_end"] = (
                                    base_latest_lt.window_end_block
                                )
                                new_base_state_lt["last_base_snapshot_lt"] = new_base_snapshot_lt
                                new_base_state_lt["updated_at_utc"] = new_base_state["updated_at_utc"]
                                st.save_base_scoring_state_lt(new_base_state_lt)
                                log(
                                    f"Base L2 (Long term/30d): {base_classified_lt} "
                                    "sklasyfikowanych portfeli "
                                    f"({'dojrzale' if base_is_mature_lt else 'jeszcze NIEDOJRZALE - compositeBaseLt=None'})."
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
            manifest["base"] = {"enabled": True, "error": repr(exc)}

    _finalize_and_save_manifest(manifest, run_started_at)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
