"""Etap 4 planu "trend" (pkt 5, 6, 9): rynek kontraktów ETH-PERP na Hyperliquid.

1. Agresorzy (pkt 6) - z bufora transakcji listenera Hyperliquid (każda
   transakcja ma stronę, która ją zainicjowała: "B" kupujący, "A" sprzedający).
   Na świecy: wartość agresywnych zakupów i sprzedaży z ostatnich 24 h oraz
   przewaga (kupno - sprzedaż) / (kupno + sprzedaż), od -1 do +1. Strategia
   "Agresorzy 24 h": LONG, gdy przeważają agresywni kupujący, SHORT, gdy
   sprzedający. Listener słucha ~50 z każdych 60 minut, więc to próbka, ale ta
   sama dla obu stron.

2. Open interest razem z ceną (pkt 5) - ze zmian OI z migawek
   (`data/derivs_history.json`, pole `oiChange24h` na świecy) i zmiany ceny ETH
   z Hyperliquid z tych samych 24 h:
     cena rośnie + OI rośnie  -> NEW_LONGS   (nowe pieniądze grają na wzrost)
     cena rośnie + OI spada   -> SHORT_COVER (wzrost z zamykania shortów)
     cena spada  + OI rośnie  -> NEW_SHORTS  (nowe pieniądze grają na spadek)
     cena spada  + OI spada   -> LONG_EXIT   (spadek z zamykania longów)
   Strategia "Trend potwierdzony przez OI": kierunek z trendu 14 dni, ale
   pozycja tylko wtedy, gdy OI rośnie (w trend wchodzą nowe pieniądze); gdy OI
   spada - poza rynkiem.

3. Wymuszone zamknięcia (pkt 9) - PRZYBLIŻENIE. Hyperliquid nie udostępnia za
   darmo strumienia likwidacji dla całego rynku (publiczne API pokazuje je tylko
   per portfel; pełny strumień mają płatni dostawcy). Oznaczamy więc godzinę, w
   której OI spadł o co najmniej FORCED_OI_DROP_PCT przy ruchu ceny o co
   najmniej FORCED_PRICE_MOVE_PCT: spadek ceny = fala zamknięć longów, wzrost =
   shortów. To nie są potwierdzone likwidacje, tylko ich ślad.

Progi ustalone z góry (bez dopasowania do historii - OI zbieramy dopiero od
14.11.2026, więc nie było na czym dopasowywać).
"""

from __future__ import annotations

import bisect
from typing import Iterable, Sequence

DAY_S = 86_400
HOUR_S = 3_600
FLOW_WINDOW_S = DAY_S
FORCED_OI_DROP_PCT = 3.0
FORCED_PRICE_MOVE_PCT = 1.5
FORCED_MAX_SNAPSHOT_GAP_S = 2 * HOUR_S
PRICE_MAX_AGE_S = 3 * HOUR_S

NEW_LONGS, SHORT_COVER, NEW_SHORTS, LONG_EXIT = "NEW_LONGS", "SHORT_COVER", "NEW_SHORTS", "LONG_EXIT"

FEATURE_KEYS = (
    "takerBuyUsd24h",
    "takerSellUsd24h",
    "takerImbalance24h",
    "signalTaker",
    "priceChange24h",
    "oiPriceState",
    "signalTrendOi",
    "forcedClose",
)


def empty_features() -> dict:
    return {k: None for k in FEATURE_KEYS}


class TakerFlow:
    """Agresywne kupno i sprzedaż z transakcji Hyperliquid (sumy prefiksowe)."""

    def __init__(self, hl_trades: Iterable):
        rows = sorted(
            ((t.ts_ms / 1000.0, t.aggressor_side, t.price_usd * t.size_eth) for t in hl_trades),
            key=lambda r: r[0],
        )
        self.ts = [r[0] for r in rows]
        self.buy = [0.0]
        self.sell = [0.0]
        for _ts, side, usd in rows:
            self.buy.append(self.buy[-1] + (usd if side == "B" else 0.0))
            self.sell.append(self.sell[-1] + (usd if side == "A" else 0.0))

    def at(self, ts: float | None) -> dict:
        out = {"takerBuyUsd24h": None, "takerSellUsd24h": None, "takerImbalance24h": None, "signalTaker": None}
        if ts is None or not self.ts or ts - FLOW_WINDOW_S < self.ts[0]:
            return out  # okno 24 h nie mieści się w buforze
        lo = bisect.bisect_left(self.ts, ts - FLOW_WINDOW_S)
        hi = bisect.bisect_left(self.ts, ts)  # tylko transakcje sprzed świecy
        b = self.buy[hi] - self.buy[lo]
        s = self.sell[hi] - self.sell[lo]
        if b + s <= 0:
            return out
        imb = (b - s) / (b + s)
        out.update(
            takerBuyUsd24h=round(b, 0),
            takerSellUsd24h=round(s, 0),
            takerImbalance24h=round(imb, 4),
            signalTaker="LONG" if imb > 0 else "SHORT" if imb < 0 else "HOLD",
        )
        return out


class EthCloses:
    def __init__(self, closes: Sequence[Sequence[float]]):
        self.t = [r[0] / 1000.0 for r in closes]
        self.p = [r[1] for r in closes]

    def at(self, ts: float) -> float | None:
        i = bisect.bisect_right(self.t, ts) - 1
        if i < 0 or ts - self.t[i] > PRICE_MAX_AGE_S:
            return None
        return self.p[i]

    def change_pct(self, ts: float, back_s: float) -> float | None:
        a, b = self.at(ts - back_s), self.at(ts)
        if a is None or b is None or a <= 0:
            return None
        return (b / a - 1.0) * 100.0


def oi_price_state(price_change: float | None, oi_change: float | None) -> str | None:
    if price_change is None or oi_change is None:
        return None
    if price_change >= 0:
        return NEW_LONGS if oi_change > 0 else SHORT_COVER
    return NEW_SHORTS if oi_change > 0 else LONG_EXIT


def trend_oi_signal(trend: str | None, oi_change: float | None) -> str | None:
    """Kierunek z trendu tylko wtedy, gdy OI rośnie. Brak danych o OI = None
    (strategia trzyma pozycję)."""
    if oi_change is None:
        return None
    if trend in ("LONG", "SHORT") and oi_change > 0:
        return trend
    return "HOLD"


def forced_close(snapshots: Sequence[dict], prices: EthCloses, ts: float | None) -> str | None:
    """"LONGS" / "SHORTS" gdy ostatnia para migawek OI przed świecą (odstęp
    <= 2 h, nowsza max 2 h przed świecą) pokazuje spadek OI >= 3% przy ruchu
    ceny >= 1,5%; inaczej None."""
    if ts is None or len(snapshots) < 2:
        return None
    times = [s["t"] / 1000.0 for s in snapshots]
    j = bisect.bisect_right(times, ts) - 1
    if j < 1 or ts - times[j] > FORCED_MAX_SNAPSHOT_GAP_S:
        return None
    cur, prev = snapshots[j], snapshots[j - 1]
    if times[j] - times[j - 1] > FORCED_MAX_SNAPSHOT_GAP_S or not prev.get("oi") or not cur.get("oi"):
        return None
    oi_drop = (1.0 - cur["oi"] / prev["oi"]) * 100.0
    p0, p1 = prices.at(times[j - 1]), prices.at(times[j])
    if p0 is None or p1 is None:
        # zapasowo: cena mark zapisana w samej migawce
        p0, p1 = prev.get("mark"), cur.get("mark")
    if not p0 or not p1:
        return None
    move = (p1 / p0 - 1.0) * 100.0
    if oi_drop >= FORCED_OI_DROP_PCT and abs(move) >= FORCED_PRICE_MOVE_PCT:
        return "LONGS" if move < 0 else "SHORTS"
    return None


def features_at(
    taker: TakerFlow | None,
    prices: EthCloses | None,
    snapshots: Sequence[dict],
    ts: float | None,
    trend: str | None,
    oi_change_24h: float | None,
) -> dict:
    out = empty_features()
    if taker is not None:
        out.update(taker.at(ts))
    if prices is not None and ts is not None:
        pc = prices.change_pct(ts, DAY_S)
        out["priceChange24h"] = round(pc, 2) if pc is not None else None
        out["oiPriceState"] = oi_price_state(pc, oi_change_24h)
        out["forcedClose"] = forced_close(snapshots, prices, ts)
    out["signalTrendOi"] = trend_oi_signal(trend, oi_change_24h)
    return out
