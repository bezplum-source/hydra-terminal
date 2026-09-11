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
    wartość zapisana TU jest tą, którą NASTĘPNE uruchomienie zblenduje ze
    spot (patrz komentarz przy `BASE_SPOT_WEIGHT`/blend_composite w
    `run_incremental.py`)."""
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
