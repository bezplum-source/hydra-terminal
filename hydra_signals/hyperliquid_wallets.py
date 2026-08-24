"""Faza H1 (brief `hydrav2-hyperliquid-brief.md`) — klasyfikacja portfeli
handlujących ETH-PERP na Hyperliquid na kohorty GOOD/BAD/NEUTRAL,
ADAPTUJĄC istniejący silnik PnL z `wallets.py` zamiast pisać nowy od zera.

Komentarz w `wallets.py` wprost mówi, że jego metoda average-cost obsługuje
symetrycznie long i short — "na DEX-ach spotowych short w sensie dosłownym
nie istnieje, ale portfele mogą sprzedawać więcej niż kupiły". Na
Hyperliquid pozycje long/short są PRAWDZIWE (kontrakty perpetual), więc ten
sam silnik pasuje tu nawet lepiej niż do spotu — nie przepisujemy PnL od
zera, tylko przygotowujemy dane wejściowe w kompatybilnym kształcie.

Mapowanie: jedno zdarzenie rynkowe Hyperliquid (`HyperliquidTrade`) niesie
OBIE strony dopasowania (`buyer`, `seller`) — w przeciwieństwie do
pojedynczego Swap na Uniswap, który mówi tylko o jednym portfelu. Rozbijamy
je więc na DWA niezależne wpisy per-portfelowe (`Trade`), w kształcie,
którego oczekuje `wallets.py`: `buyer` dostaje `Trade(side=BUY, ...)`,
`seller` dostaje `Trade(side=SELL, ...)`, oba po tej samej cenie/wielkości.

`Trade.block` w `wallets.py::compute_wallet_stats` jest używane WYŁĄCZNIE
do sortowania chronologicznego (`sorted(trades, key=lambda t: t.block)`,
nic więcej — zweryfikowane czytając kod) — podstawiamy tu `ts_ms`
(milisekundy Unix z Hyperliquid) bezpośrednio jako "block". Te wartości
NIE są numerami bloków Ethereum i NIE POWINNY nigdy trafić do jednego
wywołania `compute_wallet_stats` razem z prawdziwymi transakcjami Uniswap
— to świadomie DWA OSOBNE, równoległe wywołania (dokładnie zgodnie z
decyzją architektoniczną z briefu: dwa niezależne rurociągi danych, łączone
dopiero na końcu jako dwie liczby `composite_spot`/`composite_perp`, w
Fazie H2).

Świadome uproszczenia tej fazy (patrz brief, sekcja Faza H1) — jawnie
nazwane ograniczenia, nie ukryte błędy:

- **PnL liczony WYŁĄCZNIE z ceny wejścia/wyjścia i wielkości pozycji**
  (realizowany PnL z dopasowanych transakcji, ten sam average-cost co
  Uniswap) — BEZ funding payments (ciągły PnL niezwiązany z pojedynczą
  transakcją) i BEZ ważenia dźwignią (wartość nominalna vs zainwestowany
  margines). Do ewentualnego uzupełnienia w przyszłej fazie.
- **Likwidacje traktowane jak zwykłe zamknięcie pozycji** — silnik i tak
  poprawnie policzy wymuszone zamknięcie jako stratę; osobne oznaczenie
  "portfel został zlikwidowany" nie jest tu implementowane (dane z kanału
  WS `trades` i tak go nie niosą — potwierdzone przy pisaniu Fazy H0).

Ta faza TYLKO klasyfikuje portfele — nic tu jeszcze nie liczy
`composite_perp` ani nie dotyka `scoring.py`/`run_incremental.py`/
frontendu (to Faza H2/H3, patrz brief).
"""

from __future__ import annotations

from typing import Iterable

from .data_sources.hyperliquid_ws import HyperliquidTrade
from .models import Side, Trade, WalletStats
from .wallets import classify_wallets, compute_wallet_stats

# Te same wartości startowe co domyślne w wallets.py/compute_wallet_stats i
# classify_wallets - świadomie NIE duplikujemy innych progów tutaj, tylko
# przekazujemy je dalej, żeby ewentualna zmiana w jednym miejscu (Uniswap)
# nie rozjechała się cicho z drugim (Hyperliquid).
DEFAULT_MIN_TRADES = 5
DEFAULT_GOOD_PCT = 0.15
DEFAULT_BAD_PCT = 0.15


def hyperliquid_trade_to_wallet_trades(trade: HyperliquidTrade) -> list[Trade]:
    """Rozbija JEDNO zdarzenie rynkowe Hyperliquid (dwie strony transakcji)
    na DWA niezależne wpisy per-portfelowe, w kształcie którego oczekuje
    `wallets.py`. `ts_ms` podstawiony pod `block` — patrz docstring modułu."""
    return [
        Trade(
            wallet=trade.buyer,
            block=trade.ts_ms,
            side=Side.BUY,
            price_usd=trade.price_usd,
            size_eth=trade.size_eth,
        ),
        Trade(
            wallet=trade.seller,
            block=trade.ts_ms,
            side=Side.SELL,
            price_usd=trade.price_usd,
            size_eth=trade.size_eth,
        ),
    ]


def hyperliquid_trades_to_wallet_trades(trades: Iterable[HyperliquidTrade]) -> list[Trade]:
    """Stosuje `hyperliquid_trade_to_wallet_trades` do całej listy transakcji."""
    out: list[Trade] = []
    for t in trades:
        out.extend(hyperliquid_trade_to_wallet_trades(t))
    return out


def classify_hyperliquid_wallets(
    trades: Iterable[HyperliquidTrade],
    *,
    min_trades: int = DEFAULT_MIN_TRADES,
    good_pct: float = DEFAULT_GOOD_PCT,
    bad_pct: float = DEFAULT_BAD_PCT,
) -> dict[str, WalletStats]:
    """Funkcja całościowa Fazy H1: surowe transakcje Hyperliquid -> statystyki
    + kohorty GOOD/BAD/NEUTRAL per portfel.

    Bezstanowa (jak `compute_wallet_stats`/`classify_wallets`) — zakłada, że
    `trades` jest już przycięte do właściwego okna (patrz
    `hydra_signals.data_sources.hyperliquid_ws.prune_trade_records` /
    `DEFAULT_BUFFER_LOOKBACK_HOURS`). Wywołujący (przyszła Faza H2) jest
    odpowiedzialny za wczytanie bufora (`live.state.load_hyperliquid_trades_buffer`)
    i zdekodowanie rekordów JSON z powrotem na `HyperliquidTrade`
    (`hydra_signals.data_sources.hyperliquid_ws.json_record_to_trade`).
    """
    wallet_trades = hyperliquid_trades_to_wallet_trades(trades)
    stats = compute_wallet_stats(wallet_trades, min_trades=min_trades)
    return classify_wallets(stats, good_pct=good_pct, bad_pct=bad_pct)
