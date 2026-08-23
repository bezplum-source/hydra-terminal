"""Backtest offline regime-detection (Faza 5, brief regime-detection sekcje
20-22) — REPLAY już zapisanej historii świec (`data/candles_history.json`)
przez `regime.RegimeEngine`/`regime.detect_special_event` z DOWOLNYM
`RegimeConfig`, żeby ocenić, czy eventy (START_BULL_MARKET, START_BEAR_MARKET,
CAPITULATION, DISTRIBUTION) faktycznie poprzedzają przewidywalne ruchy ceny.

Świadomie NIE mylić z `hydra_signals/backtest.py` (starszy, CAŁKOWICIE INNY
moduł — testuje sygnał LONG/SHORT na SYNTETYCZNYM rynku, sprzed briefu
regime-detection, zero związku z tym modułem). Ten moduł działa WYŁĄCZNIE na
PRAWDZIWEJ, już zapisanej historii świec i dotyczy WYŁĄCZNIE toru regime
BULL/BEAR/CAPITULATION/DISTRIBUTION z Faz 0-4.

Kluczowa decyzja projektowa: backtest NIGDY nie korzysta z pól
`regime`/`regimeEvent`/`specialEvent`/`momentum_*` już zapisanych w
`candles_history.json` — te pola odzwierciedlają KONKRETNY config, który był
aktywny w produkcji w danym momencie (a config zmieniał się z każdą fazą, i
ma się zmieniać dalej po przyszłym strojeniu). Zamiast tego backtest
REPLAYUJE świece od zera: dla każdej świecy w kolejności czasu liczy
momentum WYŁĄCZNIE z wcześniejszych świec w TYM SAMYM przebiegu
(`regime.compute_momentum`), potem puszcza wynik przez ŚWIEŻY
`RegimeEngine(cfg)` — dokładnie tak, jak robi to `live/run_incremental.py`,
tylko z configiem podanym przez wywołującego, nie z globalnym stanem na
dysku. Dzięki temu: (a) można bezpiecznie testować DOWOLNY `RegimeConfig`, w
tym różne progi/wagi/`window_blocks`, bez ryzyka mieszania configów z
różnych faz produkcyjnych; (b) mechanizm jest z DEFINICJI odporny na
look-ahead bias (sekcja 21 briefu) — każda świeca "widzi" tylko świece PRZED
sobą w przebiegu, tak samo jak w produkcji. Pokryte wprost testem
(`test_replay_never_uses_future_candles`).

Metodologiczne zastrzeżenie (WAŻNE, przeczytaj przed wyciąganiem wniosków z
raportu): przy nakładających się na siebie oknach "patrz do przodu" dla
eventów występujących blisko siebie w czasie, obserwacje NIE SĄ statystycznie
niezależne — `n_occurrences` w raporcie to liczba eventów, nie liczba
niezależnych próbek. Przy obecnej (2026-08-23), wciąż bardzo krótkiej
historii świec (patrz `hydrav2-automation.md`) każdy wynik tego modułu ma
charakter WYŁĄCZNIE demonstracyjny/infrastrukturalny — realna walidacja
predykcyjności wymaga miesięcy zebranej historii (albo płatnego głębokiego
backfillu Alchemy).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from . import regime as regime_module
from .regime import RegimeConfig, RegimeEngine

# Eventy objęte sekcją 20 briefu ("Testuj przede wszystkim +5/+10/+20/+30%...
# po wystąpieniu START BULL / START BEAR / CAPITULATION / DISTRIBUTION").
# END_BULL_MARKET/END_BEAR_MARKET (własny dodatek Claude z Fazy 2, poza
# literą briefu) świadomie NIE są tu objęte — brief mówi wyłącznie o
# eventach *rozpoczynających* reżim/zdarzenie, nie kończących.
EVENT_KEYS = ("START_BULL_MARKET", "START_BEAR_MARKET", "CAPITULATION", "DISTRIBUTION")

DEFAULT_RETURN_THRESHOLDS: tuple[float, ...] = (0.05, 0.10, 0.20, 0.30)

# Orientacyjny, ŚWIADOMIE ARBITRALNY próg liczby świec, poniżej którego
# raport backtestu jest traktowany jako czysto demonstracyjny (patrz `main()`
# niżej) — NIE wynika z żadnej analizy statystycznej mocy testu, tylko z
# przybliżenia "ile świec potrzeba, żeby zobaczyć choć kilka pełnych cykli
# regime przy oknach rzędu dni-tygodni" (patrz dochodzenie BULL MARKET w
# `hydrav2-automation.md` — pełne cykle trwają 4-5 miesięcy).
MIN_CANDLES_FOR_MEANINGFUL_BACKTEST = 2000


@dataclass
class ReplayedCandle:
    """Jedna świeca po przejściu przez replay — surowe pole wejściowe +
    dopisane pola regime (`bullScore`/`bearScore`/`regime`/`regimeEvent`/
    `specialEvent`/...) policzone ŚWIEŻO dla danego configu, NIE wzięte z
    `candles_history.json`."""

    raw: dict
    regime_fields: dict

    @property
    def block(self) -> int:
        return self.raw["block"]

    @property
    def price(self) -> float:
        return self.raw["price"]


def replay_candles(
    candles: list[dict],
    cfg: RegimeConfig | None = None,
    *,
    window_blocks: int = regime_module.DEFAULT_WINDOW_BLOCKS,
) -> list[ReplayedCandle]:
    """Przepuszcza CAŁĄ podaną listę świec (już posortowaną rosnąco po
    `block` — to samo założenie co w reszcie pipeline'u) przez ŚWIEŻY
    `RegimeEngine(cfg)`, licząc momentum od zera dla każdej pozycji
    WYŁĄCZNIE z wcześniejszych świec W TEJ SAMEJ liście wejściowej (patrz
    docstring modułu) — bez żadnej zależności od pól regime już zapisanych w
    wejściowych słownikach. Nowy `RegimeEngine` na każde wywołanie — kolejne
    wywołania `replay_candles` (np. w `grid_search` niżej) są od siebie
    całkowicie niezależne."""
    cfg = cfg or RegimeConfig()
    engine = RegimeEngine(cfg)
    history_so_far: list[dict] = []
    out: list[ReplayedCandle] = []

    for raw in candles:
        momentum = regime_module.compute_momentum(
            history_so_far, current=raw, window_blocks=window_blocks
        )
        enriched = dict(raw)
        enriched.update(regime_module.momentum_to_json(momentum))
        regime_fields = engine.process_candle(enriched)
        out.append(ReplayedCandle(raw=raw, regime_fields=regime_fields))
        history_so_far.append(enriched)

    return out


@dataclass
class DatasetSplit:
    train: list[dict]
    validation: list[dict]
    out_of_sample: list[dict]


def split_chronologically(
    candles: list[dict],
    *,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
) -> DatasetSplit:
    """Dzieli świece na TRAIN/VALIDATION/OUT-OF-SAMPLE PO CZASIE (sekcja 22
    briefu), NIE losowo — świece #0..N*train_frac to train, kolejne
    N*validation_frac to validation, reszta to out-of-sample. Zachowanie
    kolejności chronologicznej MIĘDZY splitami jest kluczowe: OOS musi być
    zawsze PO validation, które musi być PO train w czasie — inaczej
    optymalizacja na "train" mogłaby pośrednio "zobaczyć" przyszłość
    względem OOS (look-ahead bias na poziomie samego podziału danych, nie
    tylko wewnątrz replayu — patrz też `replay_candles`)."""
    if not (0 < train_frac < 1) or not (0 < validation_frac < 1) or train_frac + validation_frac >= 1:
        raise ValueError("train_frac i validation_frac musza byc w (0, 1), a ich suma < 1.")
    n = len(candles)
    train_end = int(n * train_frac)
    validation_end = train_end + int(n * validation_frac)
    return DatasetSplit(
        train=candles[:train_end],
        validation=candles[train_end:validation_end],
        out_of_sample=candles[validation_end:],
    )


@dataclass
class ThresholdOutcome:
    threshold: float
    hit_direction: str | None  # "up" | "down" | None (nie osiagnieto w horyzoncie)
    windows_to_hit: int | None


@dataclass
class EventOutcome:
    event: str
    block: int
    price_at_event: float
    threshold_outcomes: list[ThresholdOutcome]


def _evaluate_thresholds(
    replayed: list[ReplayedCandle],
    start_idx: int,
    *,
    thresholds: tuple[float, ...],
    max_lookforward_windows: int | None,
) -> list[ThresholdOutcome]:
    """Dla świecy `start_idx` (ta, na której wystąpił event), sprawdza dla
    KAŻDEGO progu z `thresholds` NIEZALEŻNIE, czy i kiedy zwrot od ceny w
    momencie eventu osiągnął +prog (w górę) albo -prog (w dół) jako
    PIERWSZY spośród swiec w horyzoncie `max_lookforward_windows` (albo do
    końca `replayed`, jeśli `None`). Progi liczone NIEZALEŻNIE od siebie
    (nie "pierwszy próg jakikolwiek osiągnięty") — celowo, żeby raport
    pokazywał np. "+5% trafiane w 90% przypadków, ale +30% tylko w 10%",
    zgodnie z duchem sekcji 20 briefu."""
    base_price = replayed[start_idx].price
    end_idx = (
        len(replayed)
        if max_lookforward_windows is None
        else min(len(replayed), start_idx + 1 + max_lookforward_windows)
    )
    results: list[ThresholdOutcome] = []
    for t in thresholds:
        hit_direction: str | None = None
        windows: int | None = None
        for i in range(start_idx + 1, end_idx):
            ret = (replayed[i].price / base_price) - 1.0
            if ret >= t:
                hit_direction, windows = "up", i - start_idx
                break
            if ret <= -t:
                hit_direction, windows = "down", i - start_idx
                break
        results.append(ThresholdOutcome(threshold=t, hit_direction=hit_direction, windows_to_hit=windows))
    return results


def evaluate_events(
    replayed: list[ReplayedCandle],
    *,
    thresholds: tuple[float, ...] = DEFAULT_RETURN_THRESHOLDS,
    max_lookforward_windows: int | None = None,
    min_lookforward_windows_required: int = 0,
) -> list[EventOutcome]:
    """Dla każdego eventu z `EVENT_KEYS` w przereplayowanej historii, mierzy
    dla każdego progu z `thresholds` pierwszy moment osiągnięcia go (patrz
    `_evaluate_thresholds`). `regimeEvent` i `specialEvent` to DWA OSOBNE
    pola zwracane przez `RegimeEngine.process_candle` i MOGĄ wystąpić
    jednocześnie na tej samej świecy (np. CAPITULATION w trakcie trwania już
    istniejącego BULL) — traktowane wtedy jako DWA OSOBNE eventy tej samej
    świecy.

    `min_lookforward_windows_required` pomija (NIE liczy jako "brak
    trafienia") eventy zbyt blisko końca `replayed` — bez tego, event tuż
    przed końcem danego splitu (szczególnie krótkiego, jak obecny
    OUT-OF-SAMPLE przy ~28h historii) byłby systematycznie zaniżany w
    hit-rate tylko dlatego, że zabrakło mu świec "do przodu", nie dlatego,
    że progu faktycznie nie osiągnięto."""
    outcomes: list[EventOutcome] = []
    for idx, rc in enumerate(replayed):
        events_here = [
            e
            for e in (rc.regime_fields.get("regimeEvent"), rc.regime_fields.get("specialEvent"))
            if e in EVENT_KEYS
        ]
        if not events_here:
            continue
        remaining = len(replayed) - 1 - idx
        if remaining < min_lookforward_windows_required:
            continue
        for event_name in events_here:
            threshold_outcomes = _evaluate_thresholds(
                replayed, idx, thresholds=thresholds, max_lookforward_windows=max_lookforward_windows
            )
            outcomes.append(
                EventOutcome(
                    event=event_name,
                    block=rc.block,
                    price_at_event=rc.price,
                    threshold_outcomes=threshold_outcomes,
                )
            )
    return outcomes


@dataclass
class ThresholdSummary:
    threshold: float
    n_up: int
    n_down: int
    n_no_hit: int
    up_rate: float
    down_rate: float


@dataclass
class EventSummary:
    event: str
    n_occurrences: int
    by_threshold: list[ThresholdSummary]


def summarize_outcomes(
    outcomes: list[EventOutcome],
    *,
    thresholds: tuple[float, ...] = DEFAULT_RETURN_THRESHOLDS,
) -> list[EventSummary]:
    """Agreguje surowe obserwacje z `evaluate_events` per typ eventu, per
    próg. Dla eventu 'bullish' (START_BULL_MARKET, CAPITULATION) oczekujemy
    wysokiego `up_rate`; dla 'bearish' (START_BEAR_MARKET, DISTRIBUTION) —
    wysokiego `down_rate`. PATRZ `n_occurrences`, nie tylko rate, zanim
    wyciągnie się jakikolwiek wniosek — patrz też zastrzeżenie o
    nakładających się oknach w docstringu modułu."""
    by_event: dict[str, list[EventOutcome]] = {}
    for o in outcomes:
        by_event.setdefault(o.event, []).append(o)

    summaries: list[EventSummary] = []
    for event_name, items in by_event.items():
        n = len(items)
        by_threshold: list[ThresholdSummary] = []
        for t in thresholds:
            n_up = n_down = 0
            for o in items:
                matching = next((to for to in o.threshold_outcomes if to.threshold == t), None)
                if matching is None:
                    continue
                if matching.hit_direction == "up":
                    n_up += 1
                elif matching.hit_direction == "down":
                    n_down += 1
            n_none = n - n_up - n_down
            by_threshold.append(
                ThresholdSummary(
                    threshold=t,
                    n_up=n_up,
                    n_down=n_down,
                    n_no_hit=n_none,
                    up_rate=n_up / n if n else float("nan"),
                    down_rate=n_down / n if n else float("nan"),
                )
            )
        summaries.append(EventSummary(event=event_name, n_occurrences=n, by_threshold=by_threshold))
    return summaries


@dataclass
class BacktestReport:
    split_name: str
    n_candles: int
    event_summaries: list[EventSummary]
    raw_outcomes: list[EventOutcome] = field(repr=False)


def run_regime_backtest(
    candles: list[dict],
    cfg: RegimeConfig | None = None,
    *,
    split_name: str = "all",
    window_blocks: int = regime_module.DEFAULT_WINDOW_BLOCKS,
    thresholds: tuple[float, ...] = DEFAULT_RETURN_THRESHOLDS,
    max_lookforward_windows: int | None = None,
    min_lookforward_windows_required: int = 0,
) -> BacktestReport:
    """Pełny przebieg: replay `candles` z `cfg` -> wykrycie eventów -> pomiar
    progów zwrotu -> agregacja. To jest GŁÓWNA funkcja wejściowa modułu."""
    replayed = replay_candles(candles, cfg, window_blocks=window_blocks)
    outcomes = evaluate_events(
        replayed,
        thresholds=thresholds,
        max_lookforward_windows=max_lookforward_windows,
        min_lookforward_windows_required=min_lookforward_windows_required,
    )
    summaries = summarize_outcomes(outcomes, thresholds=thresholds)
    return BacktestReport(
        split_name=split_name, n_candles=len(candles), event_summaries=summaries, raw_outcomes=outcomes
    )


def grid_search(
    train_candles: list[dict],
    configs: list[RegimeConfig],
    *,
    score_event: str = "START_BULL_MARKET",
    score_threshold: float = 0.10,
    **run_kwargs,
) -> tuple[RegimeConfig, BacktestReport]:
    """Uruchamia `run_regime_backtest` dla KAŻDEGO configu z `configs` na
    `train_candles` WYŁĄCZNIE (sekcja 22 briefu: optymalizacja tylko na
    danych treningowych), wybiera config z najwyższym `up_rate` (albo
    `down_rate` dla eventów zawierających "BEAR"/"DISTRIBUTION" w nazwie)
    dla pary (`score_event`, `score_threshold`).

    Zwraca `(najlepszy_config, jego_raport)`. Zwycieski config NALEŻY
    następnie zweryfikować na VALIDATION, i dopiero na końcu na
    OUT-OF-SAMPLE — `grid_search` ŚWIADOMIE nie robi tego automatycznie, żeby
    wywołujący miał pełną kontrolę i nie pomylił, który split jest który
    (przypadkowe użycie VALIDATION/OOS wewnątrz optymalizacji unieważniłoby
    cały sens podziału z sekcji 22)."""
    if not configs:
        raise ValueError("Lista configow do przeszukania (`configs`) jest pusta.")
    is_bear_like = "BEAR" in score_event or score_event == "DISTRIBUTION"

    best_cfg: RegimeConfig | None = None
    best_report: BacktestReport | None = None
    best_score = float("-inf")

    for cfg in configs:
        report = run_regime_backtest(train_candles, cfg, split_name="train", **run_kwargs)
        matching_event = next((s for s in report.event_summaries if s.event == score_event), None)
        if matching_event is None or matching_event.n_occurrences == 0:
            continue
        matching_threshold = next(
            (t for t in matching_event.by_threshold if t.threshold == score_threshold), None
        )
        if matching_threshold is None:
            continue
        score = matching_threshold.down_rate if is_bear_like else matching_threshold.up_rate
        if score > best_score:
            best_score = score
            best_cfg = cfg
            best_report = report

    if best_cfg is None or best_report is None:
        raise ValueError(
            f"Zaden z {len(configs)} configow nie wygenerowal ani jednego eventu "
            f"'{score_event}' (z policzalnym progiem {score_threshold}) na danych treningowych."
        )
    return best_cfg, best_report


def print_backtest_report(report: BacktestReport) -> None:
    print(f"=== Backtest regime-detection: split '{report.split_name}' ({report.n_candles} swiec) ===")
    if not report.event_summaries:
        print(
            "Brak jakichkolwiek eventow (START_BULL_MARKET/START_BEAR_MARKET/"
            "CAPITULATION/DISTRIBUTION) w tym oknie danych."
        )
        return
    for s in report.event_summaries:
        print(f"{s.event}  (n={s.n_occurrences})")
        for t in s.by_threshold:
            print(
                f"    prog {t.threshold:>5.0%}:  w gore={t.up_rate:.1%} ({t.n_up})  "
                f"w dol={t.down_rate:.1%} ({t.n_down})  brak trafienia={t.n_no_hit}"
            )


def _load_live_candles() -> list[dict]:
    path = Path(__file__).resolve().parent.parent / "data" / "candles_history.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Brak {path} - uruchom najpierw live/run_incremental.py przynajmniej raz "
            "(albo poczekaj na automatyzacje GitHub Actions)."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    """CLI: `python -m hydra_signals.regime_backtest` — wczytuje
    `data/candles_history.json`, dzieli na train/validation/out-of-sample
    (60/20/20), uruchamia backtest z DOMYŚLNYM `RegimeConfig` na każdym
    splicie osobno i drukuje raport. Jawnie ostrzega, gdy danych jest za
    mało (patrz `MIN_CANDLES_FOR_MEANINGFUL_BACKTEST`), zamiast pozwolić
    przypadkowo wziąć czysto demonstracyjny wynik za realną walidację."""
    candles = _load_live_candles()
    print(f"Wczytano {len(candles)} swiec z data/candles_history.json.\n")
    if len(candles) < MIN_CANDLES_FOR_MEANINGFUL_BACKTEST:
        print(
            f"UWAGA: {len(candles)} swiec to znacznie mniej niz orientacyjny prog "
            f"{MIN_CANDLES_FOR_MEANINGFUL_BACKTEST} potrzebny do choc czesciowo "
            "sensownych wnioskow (patrz hydrav2-automation.md, sekcja Faza 5) - "
            "ponizszy raport jest CZYSTO DEMONSTRACYJNY, nie realna walidacja "
            "predykcyjnosci.\n"
        )

    split = split_chronologically(candles)
    for name, subset in (
        ("TRAIN", split.train),
        ("VALIDATION", split.validation),
        ("OUT-OF-SAMPLE", split.out_of_sample),
    ):
        if not subset:
            print(f"=== split '{name}': brak swiec (za malo danych na 3-way split) ===\n")
            continue
        report = run_regime_backtest(subset, split_name=name)
        print_backtest_report(report)
        print()


if __name__ == "__main__":
    main()
