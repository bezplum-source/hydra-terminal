"""Regime-detection, Faza 1: momentum wieloczasowy Good/Bad Trader Pressure
i Smart Money Divergence, liczony z JUŻ ZAPISANEJ historii świec
(`candles_history.json`), nie z surowych transakcji.

Celowo odseparowane od `scoring.py` (który liczy WYŁĄCZNIE metryki "na
teraz", per-okno, z surowych `Trade`) — to jest osobna warstwa, patrząca
WSTECZ po historii, żeby porównać "teraz" z "N okien temu". Ma działać
identycznie w `live/run_incremental.py` (na żywo, co godzinę) i w
przyszłym backteście offline (Faza 5 briefu regime-detection) - stąd
zależy WYŁĄCZNIE od listy słowników w formacie zapisywanym przez
`live/state.py`, nie od `Trade`/RPC/sieci.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass

# Horyzonty z briefu regime-detection, wyrażone w liczbie OKIEN (świec)
# wstecz. Każde okno = `ScoringConfig.window_blocks` bloków (domyślnie 250
# ~= 1h dla Ethereum PoS) — te wartości wprost odpowiadają godzinom/dniom
# TYLKO przy domyślnym window_blocks=250. Nie są automatycznie skalowane
# przy zmianie window_blocks — celowo proste, czytelne wprost stałe.
HORIZONS_IN_WINDOWS: dict[str, int] = {
    "1h": 1,
    "4h": 4,
    "12h": 12,
    "1d": 24,
    "3d": 72,
    "7d": 168,
    "14d": 336,
    "30d": 720,
}

DEFAULT_WINDOW_BLOCKS = 250


@dataclass
class MomentumSnapshot:
    """Momentum trzech metryk Fazy 0 dla JEDNEGO horyzontu - pola są
    `None`, gdy historia jest za krótka, żeby to policzyć (patrz
    `compute_momentum`), CELOWO zamiast podstawienia zera (zero wyglądałoby
    jak "brak zmiany", a nie "nie wiemy jeszcze")."""

    good_pressure_momentum: float | None
    bad_pressure_momentum: float | None
    divergence_momentum: float | None


def _find_reference_candle(candles_history: list[dict], target_block: int) -> dict | None:
    """Świeca o najwyższym `block` <= `target_block` (wyszukiwanie binarne)
    - ten sam wzorzec co `live.state.price_at_block_factory`.

    Liczymy po BLOKACH, nie po pozycji na liście: pojedyncze puste okno
    (brak transakcji w danej godzinie - rzadkie dla płynnej puli, ale
    możliwe) przesunęłoby "N-tą świecę wstecz" liczoną po indeksie względem
    faktycznie upływającego czasu. Zwraca `None`, jeśli CAŁA historia jest
    późniejsza niż `target_block` (za mało danych wstecz).

    Zakłada `candles_history` posortowaną rosnąco po `block` - to jest już
    założenie całego pipeline'u (świece dopisywane w kolejności czasu).
    """
    if not candles_history:
        return None
    blocks = [c["block"] for c in candles_history]
    if blocks[0] > target_block:
        return None
    i = bisect.bisect_right(blocks, target_block) - 1
    return candles_history[i] if i >= 0 else None


def compute_momentum(
    candles_history: list[dict],
    *,
    current: dict,
    window_blocks: int = DEFAULT_WINDOW_BLOCKS,
    horizons: dict[str, int] = HORIZONS_IN_WINDOWS,
) -> dict[str, MomentumSnapshot]:
    """Liczy momentum (teraz - N okien temu) trzech metryk Fazy 0
    (`goodPressure`, `badPressure`, `divergence`) dla KAŻDEGO horyzontu z
    briefu regime-detection.

    `candles_history` to historia BEZ `current` (typowo: świeżo policzona
    świeca, jeszcze przed dopisaniem do historii - patrz
    `live/run_incremental.py`).

    Zwraca `None` dla horyzontu, dla którego historia jest za krótka -
    CELOWO nie podstawia "najbliższego dostępnego" innego okresu, żeby np.
    etykieta "30d" nigdy nie kryła w sobie faktycznie policzonych 3 dni
    danych (uczciwiej pokazać brak wartości niż mylącą). To się samo
    naprawi w miarę narastania historii.
    """
    result: dict[str, MomentumSnapshot] = {}
    current_block = current["block"]

    for label, n_windows in horizons.items():
        target_block = current_block - n_windows * window_blocks
        reference = _find_reference_candle(candles_history, target_block)
        if reference is None:
            result[label] = MomentumSnapshot(None, None, None)
            continue
        result[label] = MomentumSnapshot(
            good_pressure_momentum=round(
                current.get("goodPressure", 0.0) - reference.get("goodPressure", 0.0), 4
            ),
            bad_pressure_momentum=round(
                current.get("badPressure", 0.0) - reference.get("badPressure", 0.0), 4
            ),
            divergence_momentum=round(
                current.get("divergence", 0.0) - reference.get("divergence", 0.0), 4
            ),
        )

    return result


def momentum_to_json(snapshots: dict[str, MomentumSnapshot]) -> dict[str, float | None]:
    """Spłaszcza wynik `compute_momentum` do płaskiego słownika gotowego do
    zapisania w rekordzie świecy w `candles_history.json` - klucze
    `momentum_{horyzont}_{metryka}`, np. `momentum_7d_divergence`."""
    out: dict[str, float | None] = {}
    for label, snap in snapshots.items():
        out[f"momentum_{label}_good"] = snap.good_pressure_momentum
        out[f"momentum_{label}_bad"] = snap.bad_pressure_momentum
        out[f"momentum_{label}_divergence"] = snap.divergence_momentum
    return out
