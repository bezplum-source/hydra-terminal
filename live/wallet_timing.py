"""Etap 5 planu "trend" (pkt 3 i 4): nowa definicja dobrego portfela i
akumulacja ("trend z portfeli").

Dotychczasowa klasyfikacja GOOD/BAD (`hydra_signals/wallets.py`) liczy
zrealizowany zysk i odsetek wygranych transakcji. Analiza z 14.11.2026
(dokument projektu `hydrav2-contrarian-diagnosis-and-roadmap.md`) pokazała, że
to wybiera graczy na krótkie odbicia: kupują po spadkach, sprzedają po wzrostach,
a ich przepływ niemal nic nie mówi o przyszłej cenie.

Tu "wyczucie" portfela mierzymy wprost: dla każdej jego transakcji (>= $1000)
sprawdzamy, co cena ETH zrobiła w ciągu następnych 24 godzin, w kierunku tej
transakcji (kupno przed wzrostem = plus, sprzedaż przed spadkiem = plus). Wynik
portfela = suma tych ruchów / (liczba transakcji + SHRINK_K) - ściągnięcie do
zera, żeby portfel z jedną szczęśliwą transakcją nie trafiał na szczyt. Liczą
się tylko transakcje, których 24-godzinny wynik był już znany w chwili
klasyfikacji (bez zaglądania w przyszłość). Górne TOP_SHARE portfeli (z co
najmniej MIN_TRADES transakcjami) to "z wyczuciem", dolne - "bez wyczucia".

Akumulacja (pkt 4): ile ETH netto te grupy kupiły w ostatnich ACCUM_DAYS dniach.
Z tego dwie strategie do wyścigu: "akumulacja portfeli z wyczuciem" (LONG, gdy
kupują netto) i "przeciw portfelom bez wyczucia" (SHORT, gdy one kupują netto).

Ograniczenie: surowe transakcje mamy tylko z bufora (ok. 30 dni wstecz), więc
cechy istnieją dopiero od chwili, gdy bufor ma MIN_HISTORY_DAYS dni ocenionych
transakcji plus pełne okno akumulacji; dla starszych świec są None. Na razie
tylko mainnet (Uniswap + Pancake).

Parametry ustalone 14.11.2026 PO wstępnym teście na 28.10-14.11 (jeden spadek,
17 dni): okno 7 dni wybrane dla obu strategii, choć test sprawdzał 1/3/7 dni -
to nie jest dowód, tylko kandydat do wyścigu na żywo.
"""

from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np

from hydra_signals.models import Side, Trade

DAY_S = 86_400
HORIZON_S = DAY_S  # wynik transakcji mierzony po 24 h
MIN_TRADE_USD = 1000.0  # ten sam filtr "dust" co w klasyfikacji GOOD/BAD
MIN_TRADES = 5
SHRINK_K = 10
TOP_SHARE = 0.10
ACCUM_DAYS = 7
MIN_HISTORY_DAYS = 7
PRICE_MAX_AGE_S = 3 * 3600
SECONDS_PER_BLOCK = 12.0

FEATURE_KEYS = (
    "walletSkilledNet7d",
    "walletUnskilledNet7d",
    "walletSkilledCount",
    "signalSkilled",
    "signalVsUnskilled",
)


def empty_features() -> dict:
    return {k: None for k in FEATURE_KEYS}


class WalletTiming:
    def __init__(
        self,
        trades: Iterable[Trade],
        anchors: Sequence[tuple[int, int]],
        eth_closes: Sequence[Sequence[float]],
    ):
        rows = [t for t in trades if t.price_usd * t.size_eth >= MIN_TRADE_USD]
        self.ok = bool(rows) and len(anchors) >= 2 and len(eth_closes) >= 2
        self._cls_cache: dict[int, tuple[np.ndarray, np.ndarray, int] | None] = {}
        if not self.ok:
            return
        ab = np.array([a[0] for a in anchors], dtype=float)
        at = np.array([a[1] for a in anchors], dtype=float)
        # np.interp spłaszcza wartości poza zakresem punktów odniesienia, a
        # najnowsze transakcje bywają za ostatnią świecą z czasem - dokładamy
        # punkty skrajne z typowym czasem bloku (12 s), żeby czas rósł dalej.
        far = 10_000_000
        ab = np.concatenate(([ab[0] - far], ab, [ab[-1] + far]))
        at = np.concatenate(([at[0] - far * SECONDS_PER_BLOCK], at, [at[-1] + far * SECONDS_PER_BLOCK]))
        blocks = np.array([t.block for t in rows], dtype=float)
        # znacznik czasu transakcji z bloku (interpolacja po świecach z prawdziwym czasem)
        ts = np.interp(blocks, ab, at)
        order = np.argsort(ts, kind="stable")
        ts = ts[order]
        wallets = [rows[i].wallet for i in order]
        self.wallet_names, codes = np.unique(np.array(wallets, dtype=object), return_inverse=True)
        self.codes = codes
        self.ts = ts
        self.signed = np.array(
            [(1.0 if rows[i].side is Side.BUY else -1.0) * rows[i].size_eth for i in order], dtype=float
        )
        direction = np.sign(self.signed)
        et = np.array([r[0] for r in eth_closes], dtype=float) / 1000.0
        ep = np.array([r[1] for r in eth_closes], dtype=float)
        self.price_known_until = et[-1]

        def price_at(x: np.ndarray) -> np.ndarray:
            i = np.searchsorted(et, x, side="right") - 1
            p = ep[np.clip(i, 0, len(ep) - 1)]
            stale = (i < 0) | (x - et[np.clip(i, 0, len(et) - 1)] > PRICE_MAX_AGE_S)
            return np.where(stale, np.nan, p)

        p0 = price_at(ts)
        p1 = price_at(ts + HORIZON_S)
        self.edge = direction * (p1 / p0 - 1.0) * 100.0  # NaN, gdy brak ceny
        self.first_ts = float(ts[0])

    # --- klasyfikacja ---------------------------------------------------------

    def classify(self, cutoff: int) -> tuple[np.ndarray, np.ndarray, int] | None:
        """(kody portfeli z wyczuciem, kody bez wyczucia, ile ocenionych portfeli)
        na moment `cutoff` (unix s). None, gdy za mało historii."""
        if not self.ok:
            return None
        if cutoff in self._cls_cache:
            return self._cls_cache[cutoff]
        known_end = cutoff - HORIZON_S
        out = None
        if known_end - self.first_ts >= MIN_HISTORY_DAYS * DAY_S:
            m = (self.ts < known_end) & ~np.isnan(self.edge)
            codes = self.codes[m]
            edges = self.edge[m]
            n_w = len(self.wallet_names)
            sums = np.bincount(codes, weights=edges, minlength=n_w)
            counts = np.bincount(codes, minlength=n_w)
            eligible = np.nonzero(counts >= MIN_TRADES)[0]
            if len(eligible) >= 20:
                score = sums[eligible] / (counts[eligible] + SHRINK_K)
                hi = np.quantile(score, 1.0 - TOP_SHARE)
                lo = np.quantile(score, TOP_SHARE)
                out = (eligible[score >= hi], eligible[score <= lo], int(len(eligible)))
        self._cls_cache[cutoff] = out
        return out

    # --- cechy na świecy ------------------------------------------------------

    def features_at(self, ts: float | None) -> dict:
        out = empty_features()
        if not self.ok or ts is None:
            return out
        if ts - ACCUM_DAYS * DAY_S < self.first_ts:
            return out  # okno akumulacji nie mieści się w buforze
        cutoff = int(ts // DAY_S) * DAY_S  # klasyfikacja raz na dobę (UTC)
        cls = self.classify(cutoff)
        if cls is None:
            return out
        skilled, unskilled, _n = cls
        m = (self.ts >= ts - ACCUM_DAYS * DAY_S) & (self.ts < ts)
        codes = self.codes[m]
        signed = self.signed[m]
        sk = float(signed[np.isin(codes, skilled)].sum())
        un = float(signed[np.isin(codes, unskilled)].sum())
        out["walletSkilledNet7d"] = round(sk, 2)
        out["walletUnskilledNet7d"] = round(un, 2)
        out["walletSkilledCount"] = int(len(skilled))
        out["signalSkilled"] = "LONG" if sk > 0 else "SHORT" if sk < 0 else "HOLD"
        out["signalVsUnskilled"] = "SHORT" if un > 0 else "LONG" if un < 0 else "HOLD"
        return out
