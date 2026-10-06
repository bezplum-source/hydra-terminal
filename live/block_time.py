"""Pomocnicze funkcje do znacznikow czasu swiec (block -> timestamp).

Powod powstania (2026-10-06, zgloszenie uzytkownika: karta Wallets nie
pokazywala daty przy Uniswap+Pancake; historia sygnalow miala "?" w
START/KONIEC): pobranie `eth_getBlockByNumber` dla bloku konczacego okno
potrafi paść mimo ponowien (wspolny, wyczerpany budzet RPC Alchemy), a
swieca dostawala wtedy NA ZAWSZE `ts: null` / `time: "?"` - nic jej pozniej
nie naprawialo. Stan na 2026-10-06: ~31% swiec bez znacznika czasu (395 z
1259), a w ostatnich 100 swiecach az 59.

Ten modul daje dwie warstwy naprawy, obie CZYSTE (bez I/O, latwe do testu):

1. `repair_missing_timestamps` - samonaprawa historii: przy KAZDYM
   uruchomieniu probuje dociagnac prawdziwy znacznik czasu dla swiec bez
   `ts` (od najnowszych, max `limit` na uruchomienie - zeby nie zuzywac
   wspolnego budzetu RPC) i uzupelnia `ts` + `time`. Znaczniki czasu blokow
   sa niezmienne, wiec mozna je pobrac w dowolnym pozniejszym momencie.
2. Szacunek awaryjny - dla swiec, ktorych NIE udalo sie jeszcze naprawic,
   `time` dostaje szacunek z numeru bloku (stala dlugosc bloku wzgledem
   najblizszej swiecy ze znanym `ts`), z prefiksem "~" (`APPROX_PREFIX`),
   zamiast "?". `ts` zostaje WTEDY `None` - `ts` jest uzywane do analityki
   czasowej (filtr zakresu, dzienny bilans, chip swiezosci), wiec nie wolno
   mu podawac szacunku udawanego za pomiar; wyswietlana etykieta `time` -
   tak.
"""

from __future__ import annotations

import bisect
import datetime
from typing import Callable, Sequence
from zoneinfo import ZoneInfo

WARSAW = ZoneInfo("Europe/Warsaw")
APPROX_PREFIX = "~"
UNKNOWN_TIME = "?"

# Czas bloku mainnetu Ethereum (PoS) - stale 12 s na slot.
MAINNET_SECONDS_PER_BLOCK = 12.0


def format_warsaw(ts: int | float) -> str:
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).astimezone(WARSAW)
    return dt.strftime("%d.%m.%Y, %H:%M")


def build_anchors(candles: Sequence[dict], extra: Sequence[tuple[int, int]] = ()) -> list[tuple[int, int]]:
    """Posortowana (po bloku) lista `(blok, ts)` ze swiec, ktore MAJA
    prawdziwy znacznik czasu, plus ewentualne dodatkowe pary (np. swiezo
    pobrane w tym uruchomieniu)."""
    pairs: dict[int, int] = {}
    for c in candles:
        b, ts = c.get("block"), c.get("ts")
        if isinstance(b, int) and isinstance(ts, int):
            pairs[b] = ts
    for b, ts in extra:
        pairs[b] = ts
    return sorted(pairs.items())


def estimate_block_ts(
    block: int,
    anchors: Sequence[tuple[int, int]],
    seconds_per_block: float = MAINNET_SECONDS_PER_BLOCK,
) -> int | None:
    """Szacunek znacznika czasu bloku: z NAJBLIZSZEGO (co do numeru bloku)
    punktu odniesienia, przeliczajac roznice blokow stala dlugoscia bloku.
    `None`, gdy nie ma zadnego punktu odniesienia."""
    if not anchors:
        return None
    blocks = [a[0] for a in anchors]
    i = bisect.bisect_left(blocks, block)
    candidates = []
    if i < len(anchors):
        candidates.append(anchors[i])
    if i > 0:
        candidates.append(anchors[i - 1])
    anchor_block, anchor_ts = min(candidates, key=lambda a: abs(a[0] - block))
    return int(round(anchor_ts + (block - anchor_block) * seconds_per_block))


def approx_label(
    block: int,
    anchors: Sequence[tuple[int, int]],
    seconds_per_block: float = MAINNET_SECONDS_PER_BLOCK,
) -> str:
    """Etykieta `time` dla swiecy bez zmierzonego `ts`: "~dd.mm.rrrr, HH:MM"
    (szacunek) albo "?" gdy nie ma z czego szacowac."""
    est = estimate_block_ts(block, anchors, seconds_per_block)
    if est is None:
        return UNKNOWN_TIME
    return APPROX_PREFIX + format_warsaw(est)


def repair_missing_timestamps(
    candles: list[dict],
    fetch_block_ts: Callable[[list[int]], dict[int, int]],
    *,
    limit: int = 60,
    seconds_per_block: float = MAINNET_SECONDS_PER_BLOCK,
) -> dict[str, int]:
    """Naprawia (W MIEJSCU) swiece bez `ts`.

    1. Bierze do `limit` NAJNOWSZYCH swiec bez `ts` (ktore maja numer bloku)
       i pyta `fetch_block_ts(lista_blokow) -> {blok: ts}` - zwrocone wartosci
       trafiaja do `ts` i `time` (prawdziwa data, bez "~").
    2. Pozostale swiece bez `ts` dostaja szacunek w `time` ("~...") albo "?"
       gdy nie ma zadnego punktu odniesienia.

    Wyjatek z `fetch_block_ts` NIE przerywa - naprawa jest "best effort"
    (szacunek z kroku 2 i tak sie wykona). Zwraca liczniki do logu/manifestu:
    `missing_before`, `fetched`, `still_missing`, `estimated`."""
    missing = [c for c in candles if c.get("ts") is None and isinstance(c.get("block"), int)]
    stats = {"missing_before": len(missing), "fetched": 0, "still_missing": 0, "estimated": 0}
    if not missing:
        return stats

    newest_first = sorted(missing, key=lambda c: c["block"], reverse=True)[: max(0, limit)]
    fetched: dict[int, int] = {}
    if newest_first:
        try:
            fetched = fetch_block_ts([c["block"] for c in newest_first]) or {}
        except Exception:
            fetched = {}
    for c in newest_first:
        ts = fetched.get(c["block"])
        if isinstance(ts, int):
            c["ts"] = ts
            c["time"] = format_warsaw(ts)
            stats["fetched"] += 1

    anchors = build_anchors(candles)
    for c in missing:
        if c.get("ts") is not None:
            continue
        stats["still_missing"] += 1
        label = approx_label(c["block"], anchors, seconds_per_block)
        c["time"] = label
        if label.startswith(APPROX_PREFIX):
            stats["estimated"] += 1
    return stats
