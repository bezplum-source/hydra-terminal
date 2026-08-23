"""Trwały stan pipeline'u "żywego" dashboardu, trzymany jako zwykłe pliki w
`data/` i commitowany do repo przez workflow GitHub Actions po każdym
uruchomieniu (patrz `.github/workflows/update.yml`).

Cztery pliki, każdy z osobnym powodem istnienia:

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
