"""Trwały stan pipeline'u "żywego" dashboardu, trzymany jako zwykłe pliki w
`data/` i commitowany do repo przez workflow GitHub Actions po każdym
uruchomieniu (patrz `.github/workflows/update.yml`).

Sześć plików, każdy z osobnym powodem istnienia:

- `scoring_state.json` — mały: EMA (4 liczby) + ostatni sygnał + numer
  ostatnio przetworzonego bloku. Pozwala `ScoringEngine` wznowić się
  dokładnie tam, gdzie skończył poprzedni proces (patrz
  `hydra_signals.scoring.ScoringEngine.export_state`).
- `trade_buffer.csv` — ROLNIA (nie rośnie w nieskończoność): tylko
  transakcje z ostatnich `classification_lookback_blocks` bloków, potrzebne
  do klasyfikacji portfeli GOOD/BAD w kolejnym oknie. Starsze transakcje są
  przycinane po każdym uruchomieniu.
- `wallets_seen.txt` — jeden adres na linię, WSZYSTKIE unikalne portfele
  kiedykolwiek zaobserwowane (od pierwszego uruchomienia automatyzacji) —
  to źródło rosnącej liczby "śledzonych portfeli" na dashboardzie. Rośnie
  z czasem, ale liniowo i wolno (adresy Ethereum ~42 znaki) — przy typowym
  ruchu tej puli to lata, zanim rozmiar pliku stanie się problemem.
- `candles_history.json` — pełna, narastająca historia świec (WindowScore)
  do wyświetlenia na wykresie. `live/build_site.py` bierze z niej tylko
  ostatnie `MAX_DISPLAY_CANDLES`, żeby strona i tak pozostała lekka nawet
  po miesiącach działania.
- `regime_state.json` — Faza 2 (market regime detection, patrz
  `hydra_signals/regime.py`): mały, jak `scoring_state.json` — bieżący
  regime (BULL/BEAR/NEUTRAL) + liczniki persystencji. Pozwala
  `RegimeEngine` wznowić się dokładnie tam, gdzie skończył poprzedni
  proces, tak samo jak `ScoringEngine`.
- `wallet_flip_state.json` — Faza 3 (wallet flip, patrz
  `hydra_signals/scoring.py`, sekcja "Wallet Flip" w `ScoringEngine`):
  jeden wpis na KAŻDY portfel kiedykolwiek widziany (`{wallet: {"side":
  ..., "streak": ...}}`) — rośnie tak samo wolno jak `wallets_seen.txt`
  (ten sam zbiór adresów, tylko z dwoma dodatkowymi małymi polami), ale w
  przeciwieństwie do `wallets_seen.txt` MUSI wznawiać się między
  uruchomieniami, żeby nie "zapominać" w połowie ciągu transakcji portfela
  przy każdym restarcie procesu (patrz `ScoringEngine.__init__`).

Siódmy plik, dodany w Fazie H0 briefu Hyperliquid (`hydrav2-hyperliquid-brief.md`),
zapisywany przez OSOBNY workflow (`.github/workflows/hyperliquid-update.yml`),
nie przez `update.yml`:

- `hyperliquid_trades_buffer.jsonl` — surowe transakcje ETH-PERP z Hyperliquid
  (jedna transakcja = jedna linia JSON), ROLNIA jak `trade_buffer.csv`:
  przycinana do `hydra_signals.data_sources.hyperliquid_ws.DEFAULT_BUFFER_LOOKBACK_HOURS`
  przy każdym uruchomieniu listenera. Zbierany przez OSOBNY workflow
  (`.github/workflows/hyperliquid-update.yml`), CZYTANY przez `update.yml`
  (`live/run_incremental.py`) — patrz ósmy plik niżej.

Ósmy plik, dodany w Fazie H2 briefu Hyperliquid — zapisywany i czytany
przez `update.yml`/`run_incremental.py` (NIE przez `hyperliquid-update.yml`,
który tylko zbiera surowe transakcje, patrz plik siódmy wyżej):

- `hyperliquid_scoring_state.json` — mały, jak `scoring_state.json`: cztery
  liczby EMA (`hydra_signals.hyperliquid_wallets.HyperliquidScoringEngine`)
  + `last_processed_ts_ms` (dokąd już policzono — odpowiednik
  `last_processed_block` dla Uniswap) + `last_perp_snapshot` (słownik z
  ostatnią znaną wartością `composite_perp`/dojrzałości/liczników
  diagnostycznych — patrz Faza H3 niżej), żeby `live/run_incremental.py`
  miał czym blendować i czym wypełnić kartę diagnostyczną nawet w
  uruchomieniu, w którym Hyperliquid nie dorzucił żadnej nowej transakcji
  (patrz `HyperliquidScoringEngine.run`, zwraca `None` w takim wypadku -
  stan EMA zostaje wtedy nietknięty). **Uwaga migracyjna**: pliki zapisane
  jeszcze PRZED Fazą H3 mają zamiast `last_perp_snapshot` dwa starsze,
  płaskie klucze `last_composite_perp`/`last_is_mature` — `live/run_incremental.py`
  obsługuje oba warianty (patrz komentarz w `main()`), więc nie trzeba nic
  ręcznie migrować na dysku.

Dziewiąty plik, dodany w Fazie H3 briefu Hyperliquid (front-end) — zapisywany
i czytany przez `update.yml`/`run_incremental.py`, analogicznie do
`wallets_seen.txt` dla Uniswap:

- `hyperliquid_wallets_seen.txt` — jeden adres na linię, WSZYSTKIE unikalne
  portfele Hyperliquid kiedykolwiek zaobserwowane — źródło liczby "śledzone
  portfele" w nowej karcie diagnostycznej "ETH-PERP · Hyperliquid" na
  stronie. Czysto diagnostyczny, NIE wpływa na `composite_perp` ani na
  żadną logikę scoringu (patrz `HyperliquidScoringEngine.total_tracked`).

Dziesiąty plik, dodany w Fazie "sygnał z histerezą" (zgłoszenie użytkownika
2026-08-31: "zmienia sygnał co każdy blok... hydra.trading trzyma LONG od 2
tygodni") — mały, jak `regime_state.json`:

- `signal_state.json` — bieżący `signal` (LONG/SHORT/HOLD) głównego,
  zblendowanego sygnału pokazywanego w hero + dwa liczniki potwierdzenia
  (`long_streak`/`short_streak`). Pozwala `hydra_signals.scoring.
  SignalEngine` (maszyna stanów z histerezą wejście/wyjście, architektura
  1:1 skopiowana z `RegimeEngine`) wznowić się dokładnie tam, gdzie
  skończył poprzedni proces — bez tego KAŻDE uruchomienie zaczynałoby od
  stanu HOLD i zerowych liczników, tracąc ciągłość dokładnie tak samo, jak
  straciłby ją `RegimeEngine` bez `regime_state.json`.
"""

from __future__ import annotations

import bisect
import csv
import datetime
import json
from pathlib import Path

from hydra_signals.models import Side, Trade

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SCORING_STATE_PATH = DATA_DIR / "scoring_state.json"
TRADE_BUFFER_PATH = DATA_DIR / "trade_buffer.csv"
WALLETS_SEEN_PATH = DATA_DIR / "wallets_seen.txt"
CANDLES_HISTORY_PATH = DATA_DIR / "candles_history.json"
REGIME_STATE_PATH = DATA_DIR / "regime_state.json"
WALLET_FLIP_STATE_PATH = DATA_DIR / "wallet_flip_state.json"
HYPERLIQUID_TRADES_BUFFER_PATH = DATA_DIR / "hyperliquid_trades_buffer.jsonl"
HYPERLIQUID_SCORING_STATE_PATH = DATA_DIR / "hyperliquid_scoring_state.json"
HYPERLIQUID_WALLETS_SEEN_PATH = DATA_DIR / "hyperliquid_wallets_seen.txt"
SIGNAL_STATE_PATH = DATA_DIR / "signal_state.json"
BASE_TRADE_BUFFER_PATH = DATA_DIR / "base_trade_buffer.csv"
BASE_COLLECTOR_STATE_PATH = DATA_DIR / "base_collector_state.json"
STATS_RESET_STATE_PATH = DATA_DIR / "stats_reset_state.json"
BASE_SCORING_STATE_PATH = DATA_DIR / "base_scoring_state.json"
BASE_WALLETS_SEEN_PATH = DATA_DIR / "base_wallets_seen.txt"
SPOT_POOL_STATE_PATH = DATA_DIR / "spot_pool_state.json"

# Faza "Long term (30d)" (2026-09-14, zgloszenie uzytkownika: "moglibysmy
# liczyc i to i to - sygnal dla 7d oraz sygnal dla 30d?") - DRUGI, w pelni
# rownolegly komplet stanu silnika, identyczny architektonicznie do
# powyzszych szesciu plikow (scoring/base_scoring/hyperliquid_scoring/
# spot_pool/signal/regime), tylko liczony z DLUZSZYM oknem reputacji
# portfeli (30 dni zamiast 7). Zero nowej logiki w tym module - to
# dokladnie ten sam ksztalt/wzorzec co odpowiedniki 7-dniowe wyzej, wiec
# `run_incremental.py` moze uruchomic dwie NIEZALEZNE instancje kazdego
# silnika (jedna z konfiguracja 7d, jedna z 30d) na TYCH SAMYCH juz
# zebranych transakcjach (zero dodatkowych zapytan RPC) i zapisac ich stan
# do osobnych plikow ponizej. Sufiks "_lt" ("long term") celowo UNIKA
# literalnego "_30d" w nazwach state - `hydra_signals/regime.py` ma juz
# WCZESNIEJSZY, ZUPELNIE INNY koncept o tej samej nazwie (`HORIZONS_IN_
# WINDOWS["30d"]` - "jak zmienil sie composite w ciagu ostatnich 30 dni",
# momentum w czasie), a to tu to "z jak dlugiej historii transakcji portfela
# liczymy jego etykiete GOOD/BAD" - dwa zupelnie rozne pojecia, ktore
# przypadkiem dzieliloby ta sama liczbe "30". `_lt` jednoznacznie odrozniania
# oba w kodzie/nazwach plikow, mimo ze na stronie beda pokazane jako
# zakladka "Long term (30D)".
HYPERLIQUID_SCORING_STATE_LT_PATH = DATA_DIR / "hyperliquid_scoring_state_lt.json"
BASE_SCORING_STATE_LT_PATH = DATA_DIR / "base_scoring_state_lt.json"
SPOT_POOL_STATE_LT_PATH = DATA_DIR / "spot_pool_state_lt.json"
SCORING_STATE_LT_PATH = DATA_DIR / "scoring_state_lt.json"
SIGNAL_STATE_LT_PATH = DATA_DIR / "signal_state_lt.json"
REGIME_STATE_LT_PATH = DATA_DIR / "regime_state_lt.json"

# Faza "manifesty per-run" (2026-09-15, w odpowiedzi na przeglad
# infrastruktury zaproponowany przez kolege uzytkownika) - jeden maly plik
# JSON per uruchomienie `run_incremental.py`, zamiast tego, co dzisiaj:
# zeby dowiedziec sie co konkretnie zrobilo dane uruchomienie (jaki zakres
# blokow, ile nowych transakcji, jakie ostrzezenia RPC), trzeba recznie
# przekopywac `git log`/`git show` po plikach stanu (patrz diagnoza
# zaleglosci Base z 2026-09-14). Manifest robi to za darmo, bez zadnej
# nowej infrastruktury.
RUN_MANIFESTS_DIR = DATA_DIR / "manifests"
# Ile ostatnich manifestow trzymac w gicie. Jeden manifest to ok. 1-2KB, ale
# przy 24 uruchomieniach/dobe nieograniczona retencja tylko pogleblialaby
# rozdecie `.git` (217MB przy 14MB zywych danych - patrz analiza
# infrastruktury 2026-09-14). Krotka historia w gicie wystarcza do biezacej
# diagnostyki; pelny, dlugoterminowy zapis ma docelowo przejac jezioro
# danych (R2 + Parquet, kolejny etap tego samego planu), gdzie miejsce
# kosztuje ulamki centa.
MAX_RETAINED_RUN_MANIFESTS = 30


def load_scoring_state() -> dict:
    if not SCORING_STATE_PATH.exists():
        return {}
    return json.loads(SCORING_STATE_PATH.read_text(encoding="utf-8"))


def save_scoring_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SCORING_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_trade_buffer() -> list[Trade]:
    if not TRADE_BUFFER_PATH.exists():
        return []
    trades: list[Trade] = []
    with TRADE_BUFFER_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append(
                Trade(
                    wallet=row["wallet"],
                    block=int(row["block"]),
                    side=Side(row["side"]),
                    price_usd=float(row["price_usd"]),
                    size_eth=float(row["size_eth"]),
                )
            )
    return trades


def save_trade_buffer(trades: list[Trade]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with TRADE_BUFFER_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "block", "side", "price_usd", "size_eth"])
        for t in sorted(trades, key=lambda t: t.block):
            writer.writerow(
                [t.wallet, t.block, t.side.value, f"{t.price_usd:.8f}", f"{t.size_eth:.10f}"]
            )


def load_wallets_seen() -> set[str]:
    if not WALLETS_SEEN_PATH.exists():
        return set()
    text = WALLETS_SEEN_PATH.read_text(encoding="utf-8").strip()
    return set(text.split()) if text else set()


def save_wallets_seen(wallets: set[str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    WALLETS_SEEN_PATH.write_text("\n".join(sorted(wallets)) + "\n", encoding="utf-8")


def load_candles_history() -> list[dict]:
    if not CANDLES_HISTORY_PATH.exists():
        return []
    return json.loads(CANDLES_HISTORY_PATH.read_text(encoding="utf-8"))


def save_candles_history(candles: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CANDLES_HISTORY_PATH.write_text(
        json.dumps(candles, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )


def load_regime_state() -> dict:
    if not REGIME_STATE_PATH.exists():
        return {}
    return json.loads(REGIME_STATE_PATH.read_text(encoding="utf-8"))


def save_regime_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REGIME_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_wallet_flip_state() -> dict:
    if not WALLET_FLIP_STATE_PATH.exists():
        return {}
    return json.loads(WALLET_FLIP_STATE_PATH.read_text(encoding="utf-8"))


def save_wallet_flip_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    WALLET_FLIP_STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )


def load_hyperliquid_trades_buffer() -> list[dict]:
    """Zwraca surowe rekordy (dict, patrz `hyperliquid_ws.HyperliquidTrade`)
    - jeden na linię JSONL. Linie, których nie da się sparsować jako JSON,
    są pomijane (nie przerywają wczytywania reszty pliku) - defensywnie,
    tak jak reszta parsowania danych zewnętrznych w tym projekcie."""
    if not HYPERLIQUID_TRADES_BUFFER_PATH.exists():
        return []
    records: list[dict] = []
    with HYPERLIQUID_TRADES_BUFFER_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def save_hyperliquid_trades_buffer(records: list[dict]) -> None:
    """Nadpisuje CAŁY plik (nie append) — wywołujący jest odpowiedzialny za
    wcześniejsze przycięcie starych rekordów (patrz
    `hyperliquid_ws.prune_trade_records`) i doklejenie nowych do listy
    przed wywołaniem tej funkcji."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with HYPERLIQUID_TRADES_BUFFER_PATH.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def load_hyperliquid_scoring_state() -> dict:
    if not HYPERLIQUID_SCORING_STATE_PATH.exists():
        return {}
    return json.loads(HYPERLIQUID_SCORING_STATE_PATH.read_text(encoding="utf-8"))


def save_hyperliquid_scoring_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HYPERLIQUID_SCORING_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_hyperliquid_wallets_seen() -> set[str]:
    if not HYPERLIQUID_WALLETS_SEEN_PATH.exists():
        return set()
    text = HYPERLIQUID_WALLETS_SEEN_PATH.read_text(encoding="utf-8").strip()
    return set(text.split()) if text else set()


def save_hyperliquid_wallets_seen(wallets: set[str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HYPERLIQUID_WALLETS_SEEN_PATH.write_text("\n".join(sorted(wallets)) + "\n", encoding="utf-8")


def load_signal_state() -> dict:
    if not SIGNAL_STATE_PATH.exists():
        return {}
    return json.loads(SIGNAL_STATE_PATH.read_text(encoding="utf-8"))


def save_signal_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SIGNAL_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_stats_reset_state() -> dict:
    """Faza "reset licznika wyniku/win rate" (zgloszenie uzytkownika
    2026-09-06: chce wyzerowac TYLKO blok "Laczny wynik / Trafione-
    nietrafione / Win rate" na froncie, bez kasowania wykresu ceny ani
    historii swiec/sygnalow).

    To NIE jest stan zarzadzany przez pipeline (w odroznieniu od reszty
    plikow w tym module) - to reczny "pokretlo" konfiguracyjne, edytowane
    bezposrednio przez uzytkownika na GitHubie, gdy chce przesunac punkt,
    od ktorego licza sie zamkniete sygnaly LONG/SHORT w podsumowaniu
    (np. po naprawie buga, ktory zeprul wczesniejsze wyniki). Brak pliku
    lub brak/`None` w polu `reset_from_block` = brak filtrowania (100%
    wstecznie kompatybilne z zachowaniem sprzed tej fazy).

    Format: `{"reset_from_block": <int blok>}` - streaki z `startBlock <
    reset_from_block` NIE licza sie do "Lacznego wyniku"/"Win rate" (patrz
    `live/template.html::renderAggregateStats()`), ale nadal sa widoczne w
    tabeli "Historia sygnalow" i na wykresie ceny - filtrowanie dotyczy
    WYLACZNIE tego jednego podsumowania.
    """
    if not STATS_RESET_STATE_PATH.exists():
        return {}
    return json.loads(STATS_RESET_STATE_PATH.read_text(encoding="utf-8"))


def save_stats_reset_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATS_RESET_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_base_trade_buffer() -> list[Trade]:
    """Jedenasty plik (Faza "Base L2, etap B0", 2026-09-02) — bufor
    surowych transakcji Uniswap V3 z sieci Base (ROLNIA, jak
    `trade_buffer.csv` dla mainnetu, patrz `BASE_TRADE_BUFFER_LOOKBACK_BLOCKS`
    w `live/run_incremental.py`). CELOWO osobny plik, nie współdzielony z
    `trade_buffer.csv` — numery bloków Base i Ethereum nie są ze sobą w
    żaden sposób porównywalne, więc mieszanie ich w jednym buforze byłoby
    błędem. Ten etap (B0) TYLKO zbiera i przycina - nic jeszcze nie liczy
    klasyfikacji/composite z tych danych (patrz komentarz w
    `run_incremental.py`, sekcja "Faza Base L2")."""
    if not BASE_TRADE_BUFFER_PATH.exists():
        return []
    trades: list[Trade] = []
    with BASE_TRADE_BUFFER_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append(
                Trade(
                    wallet=row["wallet"],
                    block=int(row["block"]),
                    side=Side(row["side"]),
                    price_usd=float(row["price_usd"]),
                    size_eth=float(row["size_eth"]),
                )
            )
    return trades


def save_base_trade_buffer(trades: list[Trade]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with BASE_TRADE_BUFFER_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "block", "side", "price_usd", "size_eth"])
        for t in sorted(trades, key=lambda t: t.block):
            writer.writerow(
                [t.wallet, t.block, t.side.value, f"{t.price_usd:.8f}", f"{t.size_eth:.10f}"]
            )


def load_base_collector_state() -> dict:
    """Dwunasty plik (Faza "Base L2, etap B0") — mały: tylko
    `last_processed_block` NA SIECI BASE (osobna siatka numeracji bloków
    niż `scoring_state.json`, który śledzi Ethereum mainnet). Pozwala
    kolektorowi Base wznowić się dokładnie tam, gdzie skończył poprzedni
    proces, tym samym wzorcem co `scoring_state.json`."""
    if not BASE_COLLECTOR_STATE_PATH.exists():
        return {}
    return json.loads(BASE_COLLECTOR_STATE_PATH.read_text(encoding="utf-8"))


def save_base_collector_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BASE_COLLECTOR_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_base_scoring_state() -> dict:
    """Faza "integracja Base B1-B3" (2026-09-11) — analogiczny do
    `scoring_state.json` (mainnet)/`hyperliquid_scoring_state.json` (perp):
    EMA silnika `ScoringEngine` dedykowanego dla transakcji z Base
    (`good_short`/`good_long`/`bad_short`/`bad_long`/`prev_signal`), własny
    kursor `last_scored_window_end` (NIE mylić z `last_processed_block` w
    `base_collector_state.json` — ten śledzi, dokąd pobrano surowe bloki,
    ten tutaj dokąd doliczono ŚWIECE), oraz `last_base_snapshot` — ten sam
    wzorzec co `last_perp_snapshot` w `hyperliquid_scoring_state.json`:
    ostatnia policzona wartość `composite_base` (albo `None`, gdy populacja
    sklasyfikowanych portfeli Base jest jeszcze za mała, patrz
    `BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY` w `run_incremental.py`) plus
    liczniki tracked/active/classified/good-bad buyers-sellers do
    wyświetlenia w karcie "Wallets". Liczony na SAMYM KOŃCU
    `run_incremental.py` (po zebraniu nowych bloków Base) — CELOWY ~1h lag:
    wartość zapisana TU jest tą, którą NASTĘPNE uruchomienie odczyta jako
    `base_snapshot` (patrz `main()` w `run_incremental.py`). Od Fazy
    "wspólna pula SPOT" (2026-09-11) to liczniki dobrych/złych kupujących-
    sprzedających z TEGO słownika (nie `composite_base` samo w sobie) są
    tym, co faktycznie trafia do wspólnej puli SPOT (`SpotPoolEngine`,
    patrz `spot_pool_state.json`/`load_spot_pool_state` niżej) —
    `composite_base` zostaje wyłącznie jako pole diagnostyczne."""
    if not BASE_SCORING_STATE_PATH.exists():
        return {}
    return json.loads(BASE_SCORING_STATE_PATH.read_text(encoding="utf-8"))


def save_base_scoring_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BASE_SCORING_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_base_wallets_seen() -> set[str]:
    """Faza "integracja Base B1-B3" — analogiczny do `wallets_seen.txt`
    (mainnet)/`hyperliquid_wallets_seen.txt` (perp): WSZYSTKIE unikalne
    adresy kiedykolwiek widziane w transakcjach Base, narastające między
    uruchomieniami (`ScoringEngine.total_tracked`)."""
    if not BASE_WALLETS_SEEN_PATH.exists():
        return set()
    text = BASE_WALLETS_SEEN_PATH.read_text(encoding="utf-8").strip()
    return set(text.split()) if text else set()


def save_base_wallets_seen(wallets: set[str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BASE_WALLETS_SEEN_PATH.write_text("\n".join(sorted(wallets)) + "\n", encoding="utf-8")


def load_spot_pool_state() -> dict:
    """Faza "wspólna pula SPOT" (2026-09-11, zgłoszenie użytkownika: "Base i
    Uniswap powinny być w tej samej puli decyzyjnej") — ZASTĘPUJE poprzedni
    mechanizm ("policz `composite_spot`/`composite_base` OSOBNO, zblenduj
    stałą wagą `BASE_SPOT_WEIGHT`") wspólną pulą liczników Uniswap+Base,
    patrz `hydra_signals.scoring.SpotPoolEngine`.

    Mały, jak `scoring_state.json`/`base_scoring_state.json`: tylko cztery
    liczby EMA (`good_short`/`good_long`/`bad_short`/`bad_long`) — POZWALA
    `SpotPoolEngine` wznowić się dokładnie tam, gdzie skończył poprzedni
    proces, tym samym wzorcem co pozostałe silniki EMA w tym projekcie.

    **Migracja (pierwsze uruchomienie po wdrożeniu tej fazy)**: gdy ten
    plik jeszcze nie istnieje, `run_incremental.py` NIE zaczyna z EMA=None
    ("na zimno") — zamiast tego dziedziczy JUŻ ROZGRZANE EMA z bieżącego
    (w tym momencie jeszcze mainnet-only) `scoring_state.json`. Dzięki temu,
    dopóki Base nie wnosi żadnych transakcji do puli (niedojrzały/
    nieskonfigurowany — dokładnie jak wcześniej), połączona wartość "spot"
    zostaje BAJT W BAJT identyczna z samym Uniswapem, bez sztucznego okresu
    rozgrzewania EMA od zera — patrz komentarz w `main()`."""
    if not SPOT_POOL_STATE_PATH.exists():
        return {}
    return json.loads(SPOT_POOL_STATE_PATH.read_text(encoding="utf-8"))


def save_spot_pool_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SPOT_POOL_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


# --- Faza "Long term (30d)" - sześć nowych plików stanu, patrz komentarz
# przy stałych ścieżek `*_LT_PATH` wyżej. Każda funkcja to bajt-w-bajt ten
# sam wzorzec co jej 7-dniowy odpowiednik powyżej — celowo bez żadnej
# wspólnej abstrakcji/pętli po nazwach plików, żeby zostać spójnym ze
# stylem reszty tego modułu (jawne, jedna funkcja na plik).


def load_scoring_state_lt() -> dict:
    if not SCORING_STATE_LT_PATH.exists():
        return {}
    return json.loads(SCORING_STATE_LT_PATH.read_text(encoding="utf-8"))


def save_scoring_state_lt(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SCORING_STATE_LT_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_base_scoring_state_lt() -> dict:
    if not BASE_SCORING_STATE_LT_PATH.exists():
        return {}
    return json.loads(BASE_SCORING_STATE_LT_PATH.read_text(encoding="utf-8"))


def save_base_scoring_state_lt(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BASE_SCORING_STATE_LT_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_hyperliquid_scoring_state_lt() -> dict:
    if not HYPERLIQUID_SCORING_STATE_LT_PATH.exists():
        return {}
    return json.loads(HYPERLIQUID_SCORING_STATE_LT_PATH.read_text(encoding="utf-8"))


def save_hyperliquid_scoring_state_lt(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HYPERLIQUID_SCORING_STATE_LT_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_spot_pool_state_lt() -> dict:
    if not SPOT_POOL_STATE_LT_PATH.exists():
        return {}
    return json.loads(SPOT_POOL_STATE_LT_PATH.read_text(encoding="utf-8"))


def save_spot_pool_state_lt(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SPOT_POOL_STATE_LT_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_signal_state_lt() -> dict:
    if not SIGNAL_STATE_LT_PATH.exists():
        return {}
    return json.loads(SIGNAL_STATE_LT_PATH.read_text(encoding="utf-8"))


def save_signal_state_lt(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SIGNAL_STATE_LT_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_regime_state_lt() -> dict:
    if not REGIME_STATE_LT_PATH.exists():
        return {}
    return json.loads(REGIME_STATE_LT_PATH.read_text(encoding="utf-8"))


def save_regime_state_lt(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REGIME_STATE_LT_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def save_run_manifest(manifest: dict) -> None:
    """Zapisuje manifest jednego uruchomienia jako osobny plik w
    `data/manifests/` (nazwa = znacznik czasu uruchomienia + `run_id`, wiec
    pliki sortuja sie chronologicznie same z siebie) i przycina katalog do
    `MAX_RETAINED_RUN_MANIFESTS` najnowszych (patrz uzasadnienie retencji
    przy stalej wyzej)."""
    RUN_MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)

    started_raw = manifest.get("started_at_utc")
    started_at = None
    if started_raw:
        try:
            started_at = datetime.datetime.fromisoformat(started_raw)
        except ValueError:
            started_at = None
    if started_at is None:
        started_at = datetime.datetime.now(datetime.timezone.utc)

    # Mikrosekundy w znaczniku - bez nich dwa uruchomienia w tej samej
    # sekundzie zegarowej (w praktyce: lokalne uruchomienia bez prawdziwego,
    # unikalnego GITHUB_RUN_ID) nadpisywalyby ten sam plik zamiast zostawic
    # oba slady.
    stamp = started_at.strftime("%Y-%m-%d_%H%M%S%f")
    run_id = manifest.get("run_id") or "local"
    path = RUN_MANIFESTS_DIR / f"{stamp}_{run_id}.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    _prune_old_run_manifests()


def _prune_old_run_manifests() -> None:
    files = sorted(RUN_MANIFESTS_DIR.glob("*.json"))
    excess = len(files) - MAX_RETAINED_RUN_MANIFESTS
    # UWAGA: `excess` moze byc UJEMNY (mniej plikow niz limit) - bez tego
    # zabezpieczenia `files[:excess]` z ujemnym indeksem po cichu kasowaloby
    # NIEWLASCIWY, znacznie wiekszy zakres (Python interpretuje ujemny
    # koniec wycinka jako "wszystko oprocz ostatnich |excess|"), zamiast nic
    # nie robic ponizej limitu - realny bug znaleziony i naprawiony przy
    # pisaniu testu retencji (`test_run_manifest_retention_prunes_oldest_files`).
    if excess <= 0:
        return
    for stale in files[:excess]:
        stale.unlink()


def list_run_manifests() -> list[Path]:
    """Zwraca sciezki do zapisanych manifestow, od najstarszego do
    najnowszego (nazwy plikow sortuja sie chronologicznie - patrz
    `save_run_manifest`)."""
    if not RUN_MANIFESTS_DIR.exists():
        return []
    return sorted(RUN_MANIFESTS_DIR.glob("*.json"))


def load_latest_run_manifest() -> dict | None:
    files = list_run_manifests()
    if not files:
        return None
    return json.loads(files[-1].read_text(encoding="utf-8"))


def price_at_block_factory(trades: list[Trade]):
    """Buduje funkcję `price_at_block(block) -> float`: cena z najbliższego
    znanego bloku <= target (albo najwcześniejsza znana cena, jeśli target
    jest wcześniejszy niż wszystko, co znamy) — ta sama logika co w
    `run_live_pipeline.py`, tylko z binary search zamiast liniowego
    przeszukania (bufor bywa większy niż w jednorazowych, ręcznych
    przebiegach)."""
    prices_by_block: dict[int, float] = {}
    for t in sorted(trades, key=lambda t: t.block):
        prices_by_block[t.block] = t.price_usd

    if not prices_by_block:
        def _empty(block: int) -> float:
            raise ValueError("Brak znanych cen — pusty bufor transakcji.")

        return _empty

    sorted_blocks = sorted(prices_by_block)

    def price_at_block(block: int) -> float:
        i = bisect.bisect_right(sorted_blocks, block) - 1
        if i < 0:
            i = 0
        return prices_by_block[sorted_blocks[i]]

    return price_at_block
