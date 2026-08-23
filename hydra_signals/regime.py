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


# ---------------------------------------------------------------------------
# Faza 2: BULL_SCORE / BEAR_SCORE + maszyna stanów regime (sekcje 11-17
# briefu regime-detection). Buduje na polach z Fazy 0 (goodPressure,
# badPressure, divergence, breadth) i Fazy 1 (momentum_*) już zapisanych w
# rekordzie świecy - nie liczy niczego bezpośrednio z surowych transakcji.
# ---------------------------------------------------------------------------


@dataclass
class RegimeConfig:
    """Progi i wagi maszyny stanów regime - WARTOŚCI STARTOWE, NIE WYNIK
    OPTYMALIZACJI (brief, sekcje 13/17, wprost tego wymaga) - do
    przestrojenia dopiero po backteście (Faza 5), gdy będzie wystarczająco
    dużo historii. Stąd dataclass, nie stałe modułowe - łatwo podać inne
    wartości do testów/backtestu bez zmiany kodu.

    Wagi składowych BULL/BEAR score sumują się do 90, NIE 100: składowa
    "E: Capital Flow" z sekcji 10 briefu jest świadomie ODŁOŻONA na
    późniejszą fazę - wymaga osobnej logiki normalizacji wolumenu względem
    historycznego poziomu aktywności (przeciwko dominacji pojedynczego
    wieloryba), której jeszcze nie zbudowaliśmy. To świadomy,
    udokumentowany brak, nie błąd - `bull_score`/`bear_score` mają obecnie
    sufit 90, nie 100.
    """

    w_pressure: float = 25.0  # A: Good Trader Pressure
    w_opposite_pressure: float = 20.0  # B: Bad Trader Pressure (odwrócone)
    w_momentum: float = 20.0  # C: Good Trader Pressure Momentum
    w_divergence: float = 15.0  # D: Smart Money Divergence
    w_breadth: float = 10.0  # F: Breadth
    # (E: Capital Flow, 10 pkt - odłożone, patrz docstring wyżej)

    # Horyzont momentum użyty w składowej C - wybieramy NAJDŁUŻSZY
    # dostępny w danym momencie (patrz `_pick_momentum`), bo krótka
    # historia na starcie automatyzacji i tak wymusi degradację do
    # krótszych horyzontów przez pierwsze tygodnie działania - lepiej
    # świadomie użyć najlepszego dostępnego niż całkowicie zablokować
    # scoring do czasu, aż "30d" będzie miało pełne dane.
    momentum_horizon_preference: tuple[str, ...] = (
        "30d", "14d", "7d", "3d", "1d", "12h", "4h", "1h",
    )

    bull_enter_threshold: float = 75.0
    bull_exit_threshold: float = 55.0
    bear_enter_threshold: float = 75.0
    bear_exit_threshold: float = 55.0
    # Ile KOLEJNYCH świec musi spełniać warunek wejścia, zanim faktycznie
    # zmienimy regime (sekcja 16 - "nie generuj eventu na podstawie
    # pojedynczego spike'a"). WYJŚCIE z regime (spadek poniżej progu exit)
    # jest CELOWO natychmiastowe, bez wymogu persystencji - to świadoma
    # asymetria: wejście w nowy reżim wymaga potwierdzenia, ale trwanie w
    # reżimie, który już wyraźnie się skończył, nie powinno być sztucznie
    # przeciągane.
    min_confirmation_periods: int = 3

    # --- Faza 4 (CAPITULATION/DISTRIBUTION, sekcje 18-19 briefu) ---
    # WARTOŚCI STARTOWE jak reszta progów w tej klasie. Świadomie MNIEJ
    # ekstremalne niż przykładowe liczby z briefu (Good +0.60/Bad -0.70) -
    # przy tak rzadkich zdarzeniach i wciąż małej bazie portfeli (patrz
    # `hydrav2-automation.md`), dosłowne przykładowe progi z briefu
    # prawdopodobnie nigdy by się nie uaktywniły; 0.4 to świadomy
    # kompromis do przestrojenia po backteście (Faza 5).
    capitulation_good_pressure_threshold: float = 0.4
    capitulation_bad_pressure_threshold: float = -0.4
    distribution_good_pressure_threshold: float = -0.4
    distribution_bad_pressure_threshold: float = 0.4
    # Bonus pewności, gdy zdarzenie jest dodatkowo potwierdzone przez
    # Wallet Flip (Faza 3) - "zwiększona aktywność dobrych traderów" z
    # sekcji 19 briefu, zoperacjonalizowana jako: przynajmniej jeden
    # potwierdzony flip dobrego tradera W TĄ SAMĄ stronę co zdarzenie
    # (bullish flip dla CAPITULATION, bearish flip dla DISTRIBUTION).
    special_event_flip_confidence_bonus: float = 10.0


def _linear_score(value: float, lo: float, hi: float, weight: float) -> float:
    """Mapuje `value` liniowo z zakresu [lo, hi] na [0, weight], z clampem
    dla wartości poza zakresem (np. divergence teoretycznie >2.0 przy
    ekstremalnych, jednostronnych oknach)."""
    if hi == lo:
        return weight / 2
    frac = (value - lo) / (hi - lo)
    frac = max(0.0, min(1.0, frac))
    return frac * weight


def _pick_momentum(candle: dict, horizons: tuple[str, ...]) -> tuple[str | None, float | None]:
    """Zwraca (horyzont, wartość) dla PIERWSZEGO dostępnego (nie-`None`)
    `momentum_{h}_good` w kolejności preferencji (od najdłuższego).
    `(None, None)` tylko gdy żaden horyzont nie ma jeszcze wystarczającej
    historii - możliwe wyłącznie na samym początku działania automatyzacji."""
    for h in horizons:
        v = candle.get(f"momentum_{h}_good")
        if v is not None:
            return h, v
    return None, None


@dataclass
class RegimeScore:
    bull_score: float
    bear_score: float
    momentum_horizon_used: str | None


def compute_regime_score(candle: dict, cfg: RegimeConfig | None = None) -> RegimeScore:
    """Liczy BULL_SCORE i BEAR_SCORE (sekcje 11-12 briefu) dla JEDNEJ
    świecy, na podstawie metryk już policzonych w Fazach 0-1 (`goodPressure`,
    `badPressure`, `divergence`, `breadth`, `momentum_*`) - nic tu nie
    dotyka surowych transakcji.

    `BEAR_SCORE` liczony NIEZALEŻNĄ, lustrzaną formułą (nie przez odjęcie
    od `BULL_SCORE`) - przy obecnych, liniowych funkcjach składowych
    wychodzi matematycznie równoważne `90 - BULL_SCORE`, ale trzymane jako
    osobny kod CELOWO: ułatwi to później dodanie asymetrycznych elementów
    (np. bonus z CAPITULATION wyłącznie po stronie byczej, Faza 4) bez
    ryzyka cichego popsucia drugiej strony formuły.
    """
    cfg = cfg or RegimeConfig()
    good_pressure = candle.get("goodPressure", 0.0)
    bad_pressure = candle.get("badPressure", 0.0)
    divergence = candle.get("divergence", 0.0)
    breadth = candle.get("breadth", 0.5)

    horizon_used, momentum = _pick_momentum(candle, cfg.momentum_horizon_preference)
    momentum = momentum if momentum is not None else 0.0  # brak historii -> neutralnie, nie karzemy ani nie premiujemy

    bull = (
        _linear_score(good_pressure, -1.0, 1.0, cfg.w_pressure)
        + _linear_score(-bad_pressure, -1.0, 1.0, cfg.w_opposite_pressure)
        + _linear_score(momentum, -1.0, 1.0, cfg.w_momentum)
        + _linear_score(divergence, -2.0, 2.0, cfg.w_divergence)
        + _linear_score(breadth, 0.0, 1.0, cfg.w_breadth)
    )
    bear = (
        _linear_score(-good_pressure, -1.0, 1.0, cfg.w_pressure)
        + _linear_score(bad_pressure, -1.0, 1.0, cfg.w_opposite_pressure)
        + _linear_score(-momentum, -1.0, 1.0, cfg.w_momentum)
        + _linear_score(-divergence, -2.0, 2.0, cfg.w_divergence)
        + _linear_score(1.0 - breadth, 0.0, 1.0, cfg.w_breadth)
    )
    return RegimeScore(
        bull_score=round(bull, 2), bear_score=round(bear, 2), momentum_horizon_used=horizon_used
    )


class RegimeEngine:
    """Maszyna stanów BULL/BEAR/NEUTRAL z histerezą i persystencją (sekcje
    13, 16, 17 briefu) - STATEFUL, wznawialna między oddzielnymi
    uruchomieniami procesu (dokładnie ten sam wymóg co `ScoringEngine` z
    `scoring.py` - patrz `export_state()`/`initial_state` niżej, i
    `live/state.py` po odpowiadający plik stanu na dysku)."""

    def __init__(self, config: RegimeConfig | None = None, *, initial_state: dict | None = None) -> None:
        self.cfg = config or RegimeConfig()
        state = initial_state or {}
        self.regime: str = state.get("regime", "NEUTRAL")
        self.bull_streak: int = state.get("bull_streak", 0)
        self.bear_streak: int = state.get("bear_streak", 0)

    def export_state(self) -> dict:
        return {"regime": self.regime, "bull_streak": self.bull_streak, "bear_streak": self.bear_streak}

    def process_candle(self, candle: dict) -> dict:
        """Przetwarza JEDNĄ nową świecę (w kolejności czasu!), aktualizuje
        wewnętrzny stan, zwraca płaski słownik gotowy do dopisania do
        rekordu świecy w `candles_history.json`.

        Kolejność świec ma znaczenie - przy wielu nowych świecach w jednym
        uruchomieniu (np. po dłuższej przerwie) trzeba wywołać to raz na
        każdą, po kolei, żeby maszyna stanów "przeżyła" je tak, jakby
        przyszły w osobnych, godzinowych uruchomieniach."""
        score = compute_regime_score(candle, self.cfg)
        event = None

        if self.regime == "BULL":
            if score.bull_score < self.cfg.bull_exit_threshold:
                self.regime = "NEUTRAL"
                # "END" nie jest wprost w briefie (sekcje 14-15 opisuja
                # tylko eventy START) - dodane bo hydra.trading realnie
                # pokazuje pary START/END BULL MARKET (patrz dochodzenie w
                # dokumencie projektu) - tani, spojny z tym, co juz wiemy.
                event = "END_BULL_MARKET"
            self.bull_streak = 0
            self.bear_streak = 0
        elif self.regime == "BEAR":
            if score.bear_score < self.cfg.bear_exit_threshold:
                self.regime = "NEUTRAL"
                event = "END_BEAR_MARKET"
            self.bull_streak = 0
            self.bear_streak = 0
        else:  # NEUTRAL
            bull_condition = (
                score.bull_score > self.cfg.bull_enter_threshold
                and score.bull_score > score.bear_score
            )
            bear_condition = (
                score.bear_score > self.cfg.bear_enter_threshold
                and score.bear_score > score.bull_score
            )
            self.bull_streak = self.bull_streak + 1 if bull_condition else 0
            self.bear_streak = self.bear_streak + 1 if bear_condition else 0

            if self.bull_streak >= self.cfg.min_confirmation_periods:
                self.regime = "BULL"
                event = "START_BULL_MARKET"
                self.bull_streak = 0
                self.bear_streak = 0
            elif self.bear_streak >= self.cfg.min_confirmation_periods:
                self.regime = "BEAR"
                event = "START_BEAR_MARKET"
                self.bull_streak = 0
                self.bear_streak = 0

        # Heurystyka "pewności" - NIE jest skalibrowaną probabilistyką,
        # tylko czytelnym wskaźnikiem "jak mocno przekroczyliśmy próg
        # wejścia" (w BULL/BEAR) albo "jak blisko remisu są oba score'y"
        # (w NEUTRAL). Do prawdziwej kalibracji potrzebny byłby backtest
        # (Faza 5).
        if self.regime == "BULL":
            confidence = min(100.0, round(score.bull_score / self.cfg.bull_enter_threshold * 100, 1))
        elif self.regime == "BEAR":
            confidence = min(100.0, round(score.bear_score / self.cfg.bear_enter_threshold * 100, 1))
        else:
            confidence = round(max(0.0, 100.0 - abs(score.bull_score - score.bear_score)), 1)

        special = detect_special_event(candle, self.cfg)

        return {
            "bullScore": score.bull_score,
            "bearScore": score.bear_score,
            "regime": self.regime,
            "regimeEvent": event,
            "regimeConfidence": confidence,
            "regimeMomentumHorizon": score.momentum_horizon_used,
            "specialEvent": special.event,
            "specialEventConfidence": special.confidence,
        }


# ---------------------------------------------------------------------------
# Faza 4: CAPITULATION / DISTRIBUTION (sekcje 18-19 briefu regime-detection).
# Buduje WYŁĄCZNIE na goodPressure/badPressure (Faza 0, JUŻ w rekordzie
# świecy) i opcjonalnie na liczbach flipów dobrych traderów (Faza 3) jako
# sygnale WZMACNIAJĄCYM pewność, nie jako twardym warunku - przy wciąż
# małej bazie portfeli (patrz `hydrav2-automation.md`) liczba flipów w
# pojedynczym oknie bywa zerem nawet przy realnym capitulation/distribution,
# więc wymaganie flipa jako WARUNKU KONIECZNEGO zablokowałoby event prawie
# zawsze - stąd tylko bonus do confidence, zgodnie z duchem sekcji 19
# briefu ("szczególnie gdy występuje zwiększona aktywność...").
#
# CELOWO stateless, w przeciwieństwie do `RegimeEngine` - w odróżnieniu od
# BULL/BEAR (sekcja 16 briefu wprost wymaga persystencji/potwierdzenia
# przez kilka świec z rzędu), CAPITULATION/DISTRIBUTION z definicji opisują
# POJEDYNCZY, gwałtowny moment (sekcja 18: "słabe ręce sprzedają, podczas
# gdy dobrzy absorbują podaż") - wymaganie kilku świec z rzędu byłoby
# sprzeczne z naturą zjawiska. Brief WPROST zabrania też wiązania tego z
# maszyną stanów BULL/BEAR ("Nie traktuj tego automatycznie jako
# START_BULL") - stąd `detect_special_event` nie zmienia `self.regime` w
# `RegimeEngine.process_candle` powyżej, tylko dokleja dwa dodatkowe,
# czysto informacyjne pola.
#
# CAPITULATION wymaga good_pressure >= capitulation_good_pressure_threshold
# ORAZ bad_pressure <= capitulation_bad_pressure_threshold jednoczesnie;
# DISTRIBUTION wymaga dokladnie odwrotnych znakow - przy sensownej
# konfiguracji (progi tego samego typu po przeciwnych stronach zera) te
# dwa warunki są WZAJEMNIE WYKLUCZAJĄCE SIĘ (good_pressure nie może być
# jednocześnie >= dodatniego progu i <= ujemnego progu), więc jedno pole
# `specialEvent` (zamiast dwóch osobnych booleanów) wystarcza.
# ---------------------------------------------------------------------------


@dataclass
class SpecialEvent:
    event: str | None  # "CAPITULATION" | "DISTRIBUTION" | None
    confidence: float  # 0-100, heurystyka jak `regimeConfidence` - NIE skalibrowana probabilistyka


def detect_special_event(candle: dict, cfg: RegimeConfig | None = None) -> SpecialEvent:
    """Wykrywa CAPITULATION/DISTRIBUTION (sekcje 18-19 briefu) dla JEDNEJ
    świecy - czysta funkcja, bez stanu (patrz komentarz nad `SpecialEvent`
    wyżej po uzasadnienie). Wywoływana z `RegimeEngine.process_candle()`,
    ale nie wpływa na `self.regime`/`self.bull_streak`/`self.bear_streak`.
    """
    cfg = cfg or RegimeConfig()
    good_pressure = candle.get("goodPressure", 0.0)
    bad_pressure = candle.get("badPressure", 0.0)
    good_bullish_flips = candle.get("goodBullishFlips", 0) or 0
    good_bearish_flips = candle.get("goodBearishFlips", 0) or 0

    is_capitulation = (
        good_pressure >= cfg.capitulation_good_pressure_threshold
        and bad_pressure <= cfg.capitulation_bad_pressure_threshold
    )
    is_distribution = (
        good_pressure <= cfg.distribution_good_pressure_threshold
        and bad_pressure >= cfg.distribution_bad_pressure_threshold
    )

    if is_capitulation:
        good_component = _linear_score(
            good_pressure, cfg.capitulation_good_pressure_threshold, 1.0, 50.0
        )
        bad_component = _linear_score(
            -bad_pressure, -cfg.capitulation_bad_pressure_threshold, 1.0, 50.0
        )
        flip_bonus = cfg.special_event_flip_confidence_bonus if good_bullish_flips > 0 else 0.0
        confidence = round(min(100.0, good_component + bad_component + flip_bonus), 1)
        return SpecialEvent(event="CAPITULATION", confidence=confidence)

    if is_distribution:
        good_component = _linear_score(
            -good_pressure, -cfg.distribution_good_pressure_threshold, 1.0, 50.0
        )
        bad_component = _linear_score(
            bad_pressure, cfg.distribution_bad_pressure_threshold, 1.0, 50.0
        )
        flip_bonus = cfg.special_event_flip_confidence_bonus if good_bearish_flips > 0 else 0.0
        confidence = round(min(100.0, good_component + bad_component + flip_bonus), 1)
        return SpecialEvent(event="DISTRIBUTION", confidence=confidence)

    return SpecialEvent(event=None, confidence=0.0)
