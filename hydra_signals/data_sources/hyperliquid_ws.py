"""Faza H0 (brief `hydrav2-hyperliquid-brief.md`) — pozyskiwanie surowych
transakcji z rynku ETH-PERP na Hyperliquid przez publiczny WebSocket.

**Tylko ETH, na stałe** — decyzja użytkownika (2026-08-24: "Tylko patrzymy
na ETH zawsze"). Brak configu na inne coiny, `ETH_COIN` jest stałą, nie
parametrem uruchomieniowym.

Dlaczego WebSocket, nie REST (jak dla Ethereum/Uniswap w `onchain_rpc.py`):
Hyperliquid NIE ma publicznego REST-owego "co się stało na rynku w ostatniej
godzinie" — REST (`userFills`/`userFillsByTime`) działa tylko per-znany-adres,
nie nadaje się do ODKRYWANIA nowych portfeli. Jedyny publiczny firehose
wszystkich transakcji na rynku (z adresami obu stron) to kanał WS `trades`
— zweryfikowane w oficjalnej dokumentacji Hyperliquid, sierpień 2026, patrz
brief. To wymaga trzymanego połączenia, w przeciwieństwie do "zapytaj i
zgaś" używanego dla RPC Ethereum — stąd osobny, długo działający listener
(`live/hyperliquid_listener.py`) zamiast rozszerzenia `run_incremental.py`.

Ciekawostka względem Uniswap: pojedyncza wiadomość `trades` niesie OBIE
strony dopasowanej transakcji naraz (`users: [buyer, seller]`) — z jednego
eventu dostajemy dwa portfele, podczas gdy pojedynczy Swap na Uniswap mówi
tylko o jednym traderze wchodzącym w interakcję z pulą.

Ta faza (H0) TYLKO zbiera i buforuje surowe transakcje — nic tu jeszcze nie
liczy skuteczności portfeli ani nie dotyka `scoring.py`/`run_incremental.py`/
frontendu (patrz brief, Faza H0 vs H1/H2).
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable

HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"

# Decyzja uzytkownika 2026-08-24: "Tylko patrzymy na ETH zawsze" - swiadomie
# stala, zadnego configu na inne coiny (w przeciwienstwie np. do progow w
# RegimeConfig, ktore SA parametryzowalne).
ETH_COIN = "ETH"

# Ile godzin surowych transakcji trzymac w buforze (`data/hyperliquid_trades_buffer.jsonl`)
# zanim Faza H1 zdefiniuje wlasciwe okno klasyfikacji (analogiczne do
# `classification_lookback_blocks` w ScoringConfig). Wartosc STARTOWA,
# konserwatywnie hojna (podobnie jak inne progi w tym projekcie) - celowo
# NIE rosniemy w nieskonczonosc od pierwszego dnia, zeby nie napuchnac
# repozytorium, zanim H1 w ogole zacznie ten bufor konsumowac.
DEFAULT_BUFFER_LOOKBACK_HOURS = 48.0

# Jak czesto (w sekundach nasluchu) flushowac zebrane transakcje na dysk w
# trakcie jednego uruchomienia listenera - zabezpieczenie na wypadek, gdyby
# proces zostal ubity przez timeout GitHub Actions przed czystym zakonczeniem.
DEFAULT_FLUSH_INTERVAL_SECONDS = 60.0


class AggressorSide:
    """Surowa strona z API Hyperliquid - agresor (kto zainicjowal trade),
    NIE mylic z hydra_signals.models.Side (ktory opisuje kierunek DANEGO
    portfela we wlasnej transakcji). "B" = Bid = Buy, "A" = Ask = Sell/Short
    (zweryfikowane w oficjalnej dokumentacji notacji Hyperliquid)."""

    BUY = "B"
    SELL = "A"


@dataclass(frozen=True)
class HyperliquidTrade:
    """Pojedyncza sparowana transakcja na rynku ETH-PERP.

    `buyer`/`seller` to DWA rozne portfele (Hyperliquid ujawnia obie strony
    dopasowania) - w przeciwienstwie do `hydra_signals.models.Trade`
    (jeden portfel na transakcje), z jednego `HyperliquidTrade` powstana
    docelowo DWA wpisy per-portfelowe w Fazie H1 (jeden BUY dla `buyer`,
    jeden SELL dla `seller`, oba po tej samej cenie/wielkosci).
    """

    coin: str
    aggressor_side: str  # AggressorSide.BUY / AggressorSide.SELL
    price_usd: float
    size_eth: float
    buyer: str
    seller: str
    ts_ms: int
    tid: int
    tx_hash: str


def build_subscribe_message(coin: str = ETH_COIN) -> dict:
    """Wiadomosc subskrypcji wysylana zaraz po otwarciu polaczenia WS."""
    return {"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}


def parse_trades_message(raw: Any) -> list[HyperliquidTrade]:
    """Parsuje JEDNA zdekodowana (juz po `json.loads`) wiadomosc WS.

    Zwraca pusta liste dla wiadomosci spoza kanalu "trades" (np.
    potwierdzenie subskrypcji, ping/pong, inny kanal) - NIE rzuca wyjatku,
    zeby pojedyncza nieoczekiwana wiadomosc nie ubijala calego listenera.
    Pojedynczy znieksztalcony rekord w batchu jest pomijany, reszta batcha
    parsowana normalnie (ten sam wzorzec obronny co `decode_swap_log` w
    `onchain_rpc.py` - nie ufamy slepo ksztaltowi danych z zewnatrz).
    """
    if not isinstance(raw, dict) or raw.get("channel") != "trades":
        return []

    data = raw.get("data")
    if not isinstance(data, list):
        return []

    out: list[HyperliquidTrade] = []
    for item in data:
        try:
            coin = item["coin"]
            if coin != ETH_COIN:
                # Nie powinno sie zdarzyc (subskrybujemy tylko ETH), ale nie
                # ufamy slepo - lepiej pominac niz zanieczyscic bufor.
                continue
            users = item["users"]
            out.append(
                HyperliquidTrade(
                    coin=coin,
                    aggressor_side=item["side"],
                    price_usd=float(item["px"]),
                    size_eth=float(item["sz"]),
                    buyer=users[0],
                    seller=users[1],
                    ts_ms=int(item["time"]),
                    tid=int(item["tid"]),
                    tx_hash=item.get("hash", ""),
                )
            )
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    return out


def trade_to_json_record(trade: HyperliquidTrade) -> dict:
    return asdict(trade)


def json_record_to_trade(record: dict) -> HyperliquidTrade:
    return HyperliquidTrade(**record)


def prune_trade_records(
    records: Iterable[dict],
    *,
    now_ms: int,
    lookback_hours: float = DEFAULT_BUFFER_LOOKBACK_HOURS,
) -> list[dict]:
    """Odrzuca rekordy starsze niz `lookback_hours` wzgledem `now_ms` -
    ten sam cel co przycinanie `trade_buffer.csv` w `live/state.py`: bufor
    ma zostac ROLNIA, nie rosnac w nieskonczonosc. Rekordy bez poprawnego
    `ts_ms` sa odrzucane (lepiej stracic pojedynczy zle sparsowany wpis niz
    trzymac go w nieskonczonosc, bo nigdy nie "wystarzeje")."""
    cutoff_ms = now_ms - int(lookback_hours * 3600 * 1000)
    kept = []
    for r in records:
        ts = r.get("ts_ms")
        if isinstance(ts, int) and ts >= cutoff_ms:
            kept.append(r)
    return kept


async def listen(
    on_trades: Callable[[list[HyperliquidTrade]], None],
    *,
    coin: str = ETH_COIN,
    duration_seconds: float,
    url: str = HYPERLIQUID_WS_URL,
    connect_factory: Callable[[], Any] = None,
    now_fn: Callable[[], float] = time.monotonic,
    retry_delay_seconds: float = 5.0,
    sleep_fn: Callable[[float], Any] = asyncio.sleep,
) -> None:
    """Laczy sie z Hyperliquid WS, subskrybuje `trades` dla `coin`, i
    wywoluje `on_trades(batch)` synchronicznie dla kazdej wiadomosci z co
    najmniej jedna transakcja, az uplynie `duration_seconds` (mierzone
    wzgledem `now_fn`, domyslnie `time.monotonic`).

    WAZNE (znalezione przez zywy smoke-test w piaskownicy, nie tylko
    testy jednostkowe): NIE mozna polegac na tym, ze
    `async for ws in websockets.connect(url)` samo przezyje KAZDY blad -
    biblioteka `websockets` swiadomie NIE retry'uje bledow, ktore uznaje
    za trwale (np. odpowiedz 403 Forbidden od posredniczacego proxy,
    ktora jest dokladnie tym, co ta piaskownica zwraca dla Hyperliquid -
    ten sam rodzaj blokady siecowej co juz udokumentowana dla RPC
    Ethereum). Taki wyjatek wylatuje z SAMEJ iteracji `async for ws in
    connections`, ZANIM w ogole dostaniemy `ws` - czyli poza wewnetrznym
    try/except, ktory chroni tylko obsluge JUZ nawiazanego polaczenia.
    Dlatego `listen()` samo zarzadza retry na tym zewnetrznym poziomie:
    `connect_factory` TWORZY NOWY obiekt polaczen (nowe
    `websockets.connect(url)`) przy kazdej probie, zamiast ponownie
    iterowac raz juz zepsuty obiekt - bezpieczne niezaleznie od tego, czy
    dany wyjatek jest przez biblioteke uznawany za "do retry" czy nie.

    `connect_factory` jest wstrzykiwalny (domyslnie `lambda:
    websockets.connect(url)`) - w testach podstawiamy fabryke fake
    async-iterable polaczen, zeby NIE laczyc sie z zywym internetem (to
    srodowisko i tak nie ma do niego dostepu, ten sam wzorzec co fake
    transport w `onchain_rpc.py`/`test_onchain_rpc.py`).
    """
    if connect_factory is None:
        import websockets

        connect_factory = lambda: websockets.connect(url)  # noqa: E731

    deadline = now_fn() + duration_seconds
    subscribe_payload = json.dumps(build_subscribe_message(coin))

    while now_fn() < deadline:
        try:
            async for ws in connect_factory():
                if now_fn() >= deadline:
                    return
                await ws.send(subscribe_payload)
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    trades = parse_trades_message(message)
                    if trades:
                        on_trades(trades)
                    if now_fn() >= deadline:
                        return
                # To konkretne polaczenie samo sie zakonczylo bez wyjatku
                # (np. serwer je czysto zamknal) - wracamy do zewnetrznego
                # `async for ws in connect_factory()`, ktore da nam kolejne
                # polaczenie z TEJ SAMEJ fabryki (ten sam obiekt
                # connect_factory() moze sam obslugiwac wiele polaczen pod
                # rzad, tak jak realny `websockets.connect()`).
        except Exception:
            # Nawiazanie polaczenia ALBO obsluga juz otwartego polaczenia
            # zawiodly w sposob, ktorego `connect_factory()` samo nie
            # naprawilo (np. trwaly blad typu 403 Forbidden od proxy, DNS,
            # TLS) - budujemy CALKIEM NOWY obiekt polaczen przy nastepnej
            # probie (petla `while`), zamiast reuzywac ten sam, mozliwie
            # juz "wyczerpany" obiekt. Pojedynczy incydent nie powinien
            # ubic calego 50-minutowego joba.
            if now_fn() >= deadline:
                return
            await sleep_fn(retry_delay_seconds)
            continue
