"""Pobieranie i dekodowanie transakcji swap bezpośrednio z węzła Ethereum
(JSON-RPC), bez pośrednictwa płatnych API typu Etherscan/Alchemy/Dune.

Dlaczego tak: to zero-signup ścieżka (publiczne RPC-y jak
ethereum.publicnode.com czy eth.llamarpc.com nie wymagają klucza API), co
pasuje do opisu z hydra.trading ("Hydra reads trading record of DEX traders
directly from the blockchain"). Cena: więcej pracy własnej przy dekodowaniu
eventów i pilnowaniu limitów zapytań publicznych węzłów.

WAŻNE OGRANICZENIE TEGO ŚRODOWISKA: sesja, w której to piszę, nie ma
ogólnego dostępu do internetu (tylko do wąskiej listy domen jak PyPI) - nie
mogłem więc wykonać żywego smoke-testu przeciwko prawdziwemu RPC. Kod jest
przetestowany jednostkowo z podstawionym (fake) transportem HTTP - żywe
uruchomienie z prawdziwym `endpoint_url` trzeba zrobić na maszynie z
normalnym dostępem do sieci (Twój komputer, docelowy serwer).
"""

from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

from ..models import Side, Trade
from .pools import PoolConfig

# keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)")
# Obliczone lokalnie (pycryptodome) w tej sesji - to publiczna, dobrze
# znana stała Uniswap V3, niezależna od żadnego API key.
SWAP_TOPIC0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

Transport = Callable[[str, dict], dict]


def _default_transport(endpoint_url: str, payload: dict) -> dict:
    """Domyślny transport oparty tylko o stdlib (bez zależności od `requests`)."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        endpoint_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


class RpcError(RuntimeError):
    pass


class JsonRpcClient:
    """Cienki klient JSON-RPC z możliwością podmiany transportu (do testów)."""

    def __init__(self, endpoint_url: str, transport: Transport | None = None) -> None:
        self.endpoint_url = endpoint_url
        self._transport = transport or _default_transport
        self._next_id = 1

    def call(self, method: str, params: list) -> object:
        req_id = self._next_id
        self._next_id += 1
        payload = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        response = self._transport(self.endpoint_url, payload)
        if "error" in response and response["error"]:
            raise RpcError(f"{method} -> {response['error']}")
        return response.get("result")

    def batch_call(self, calls: Sequence[tuple[str, list]]) -> list[object]:
        """Jedno zapytanie HTTP, wiele wywołań JSON-RPC - minimalizuje liczbę
        round-tripów przy rozwiązywaniu nadawców (`from`) wielu transakcji."""
        if not calls:
            return []
        payload = []
        start_id = self._next_id
        for i, (method, params) in enumerate(calls):
            payload.append(
                {"jsonrpc": "2.0", "id": start_id + i, "method": method, "params": params}
            )
        self._next_id = start_id + len(calls)

        response = self._transport(self.endpoint_url, payload)
        by_id = {item["id"]: item for item in response}
        results = []
        for i in range(len(calls)):
            item = by_id.get(start_id + i)
            if item is None or ("error" in item and item["error"]):
                results.append(None)
            else:
                results.append(item.get("result"))
        return results


def batch_call_with_retry(
    rpc: JsonRpcClient,
    calls: Sequence[tuple[str, list]],
    *,
    batch_size: int = 80,
    max_retries: int = 6,
    base_delay: float = 0.6,
    sleep: Callable[[float], None] = time.sleep,
) -> list[object]:
    """Jak `JsonRpcClient.batch_call`, ale dzieli dużą listę wywołań na
    mniejsze paczki (`batch_size`) i automatycznie ponawia TYLKO te
    wywołania, które się nie udały - błąd JSON-RPC, HTTP 429 (throttling
    "compute units per second" na Alchemy free tier) albo zwykły błąd
    sieciowy (timeout/connection reset) - z rosnącym opóźnieniem
    (`base_delay * numer_proby`).

    Odkryte empirycznie przy pierwszym ręcznym uruchomieniu tego pipeline'u
    (przez przeglądarkę użytkownika, patrz `hydrav2-engine-v1.md` w
    projekcie): Alchemy free tier akceptuje wsadowe (batch) zapytania
    JSON-RPC, ale i tak throttluje po compute-units/s, więc pojedyncze
    wywołania W ŚRODKU batcha mogą dostać błąd 429, podczas gdy reszta tego
    samego batcha się powiedzie - stąd retry na poziomie POJEDYNCZEGO
    wywołania, nie całego batcha.

    Zwraca listę wyników w TEJ SAMEJ kolejności co `calls` - `None` tylko
    jeśli wywołanie nie powiodło się mimo wyczerpania wszystkich prób
    (logowane przez wywołującego, nie podnosi wyjątku - część danych lepsza
    niż żadna, przy sporadycznych, pojedynczych błędach)."""
    n = len(calls)
    results: list[object] = [None] * n
    pending = list(range(n))

    attempt = 0
    while pending and attempt <= max_retries:
        if attempt > 0:
            sleep(base_delay * attempt)
        next_pending: list[int] = []
        for chunk_start in range(0, len(pending), batch_size):
            chunk_indices = pending[chunk_start : chunk_start + batch_size]
            chunk_calls = [calls[i] for i in chunk_indices]
            try:
                chunk_results = rpc.batch_call(chunk_calls)
            except Exception:
                # Caly batch nie doszedl (np. blad sieciowy/timeout) -
                # traktujemy WSZYSTKIE wywolania w tej paczce jako do ponowienia.
                next_pending.extend(chunk_indices)
                continue
            for i, res in zip(chunk_indices, chunk_results):
                if res is None:
                    next_pending.append(i)
                else:
                    results[i] = res
        pending = next_pending
        attempt += 1

    return results


@dataclass
class DecodedSwap:
    tx_hash: str
    block_number: int
    log_index: int
    amount0: int
    amount1: int
    sqrt_price_x96: int
    liquidity: int
    tick: int


def decode_swap_log(log: dict) -> DecodedSwap:
    """Dekoduje surowy log `eth_getLogs` eventu Uniswap V3 `Swap`.

    Pole `data` to 5 słów po 32 bajty (bez dodatkowych parametrów w topics
    poza sender/recipient, które i tak pomijamy - realnego tradera i tak
    trzeba wziąć z pola `from` transakcji, patrz `_resolve_tx_senders`).
    """
    raw = bytes.fromhex(log["data"][2:])
    if len(raw) != 32 * 5:
        raise ValueError(f"Nieoczekiwana dlugosc danych logu Swap: {len(raw)} bajtow")

    words = [raw[i * 32 : (i + 1) * 32] for i in range(5)]
    amount0 = int.from_bytes(words[0], "big", signed=True)
    amount1 = int.from_bytes(words[1], "big", signed=True)
    sqrt_price_x96 = int.from_bytes(words[2], "big", signed=False)
    liquidity = int.from_bytes(words[3], "big", signed=False)
    tick = int.from_bytes(words[4], "big", signed=True)

    return DecodedSwap(
        tx_hash=log["transactionHash"],
        block_number=int(log["blockNumber"], 16),
        log_index=int(log["logIndex"], 16),
        amount0=amount0,
        amount1=amount1,
        sqrt_price_x96=sqrt_price_x96,
        liquidity=liquidity,
        tick=tick,
    )


def swap_to_trade(swap: DecodedSwap, pool: PoolConfig, wallet: str) -> Trade | None:
    amount0_real = swap.amount0 / (10**pool.token0_decimals)
    amount1_real = swap.amount1 / (10**pool.token1_decimals)

    eth_amount, _usd_amount = (
        (amount0_real, amount1_real) if pool.eth_is_token0 else (amount1_real, amount0_real)
    )
    usd_amount = amount1_real if pool.eth_is_token0 else amount0_real

    if eth_amount == 0:
        return None

    # Ujemna zmiana ETH w puli = pula oddala ETH -> trader KUPIL ETH.
    side = Side.BUY if eth_amount < 0 else Side.SELL
    size_eth = abs(eth_amount)
    price_usd = abs(usd_amount) / size_eth

    return Trade(
        wallet=wallet,
        block=swap.block_number,
        side=side,
        price_usd=price_usd,
        size_eth=size_eth,
    )


def _resolve_tx_senders(rpc: JsonRpcClient, tx_hashes: Iterable[str]) -> dict[str, str]:
    unique_hashes = sorted(set(tx_hashes))
    calls = [("eth_getTransactionByHash", [h]) for h in unique_hashes]
    results = rpc.batch_call(calls)
    out: dict[str, str] = {}
    for h, res in zip(unique_hashes, results):
        if res and "from" in res:
            out[h] = res["from"]
    return out


def fetch_trades_from_chain(
    rpc: JsonRpcClient,
    pool: PoolConfig,
    from_block: int,
    to_block: int,
    chunk_size: int = 2000,
) -> list[Trade]:
    """Pobiera i dekoduje wszystkie transakcje Swap z danej puli w zadanym
    zakresie bloków, zwracając listę `Trade` gotowych do wpiecia w
    `wallets.compute_wallet_stats` / `scoring.ScoringEngine`.

    `chunk_size` chroni przed przekroczeniem limitu liczby logów/zakresu
    bloków na zapytanie, jaki narzucają publiczne węzły (typowo 2000-10000
    bloków na wywołanie `eth_getLogs`).
    """
    trades: list[Trade] = []

    for chunk_start in range(from_block, to_block + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, to_block)
        logs = rpc.call(
            "eth_getLogs",
            [
                {
                    "address": pool.address,
                    "topics": [SWAP_TOPIC0],
                    "fromBlock": hex(chunk_start),
                    "toBlock": hex(chunk_end),
                }
            ],
        )
        if not logs:
            continue

        tx_from = _resolve_tx_senders(rpc, (log["transactionHash"] for log in logs))

        for log in logs:
            decoded = decode_swap_log(log)
            wallet = tx_from.get(decoded.tx_hash)
            if wallet is None:
                continue
            trade = swap_to_trade(decoded, pool, wallet)
            if trade is not None:
                trades.append(trade)

    return trades


def fetch_trades_from_chain_batched(
    rpc: JsonRpcClient,
    pool: PoolConfig,
    from_block: int,
    to_block: int,
    *,
    blocks_per_call: int = 10,
    calls_per_batch: int = 80,
    max_retries: int = 6,
    on_progress: Callable[[str], None] | None = None,
) -> list[Trade]:
    """Jak `fetch_trades_from_chain`, ale dla dostawców RPC z twardym
    limitem bardzo małego zakresu bloków na jedno wywołanie `eth_getLogs`
    (np. Alchemy free tier: 10 bloków/wywołanie). Zamiast wysyłać każdy
    mikro-zakres jako osobne żądanie HTTP (wolne, łatwo o throttling),
    pakuje wiele wywołań `eth_getLogs` w JEDNO żądanie HTTP (wsadowe
    JSON-RPC) i automatycznie ponawia nieudane wywołania - patrz
    `batch_call_with_retry`.

    Przeznaczone do uruchomienia jako proces z prawdziwym, nieograniczonym
    dostępem do internetu (GitHub Actions runner, własny komputer/serwer) -
    patrz `live/run_incremental.py`.
    """

    def log(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    if from_block > to_block:
        return []

    chunks = [
        (start, min(start + blocks_per_call - 1, to_block))
        for start in range(from_block, to_block + 1, blocks_per_call)
    ]
    log(
        f"eth_getLogs: {len(chunks)} zakres(y) po {blocks_per_call} blokow "
        f"({from_block}-{to_block})"
    )

    calls: list[tuple[str, list]] = [
        (
            "eth_getLogs",
            [
                {
                    "address": pool.address,
                    "topics": [SWAP_TOPIC0],
                    "fromBlock": hex(c[0]),
                    "toBlock": hex(c[1]),
                }
            ],
        )
        for c in chunks
    ]
    results = batch_call_with_retry(
        rpc, calls, batch_size=calls_per_batch, max_retries=max_retries
    )

    logs: list[dict] = []
    failed_chunks = 0
    for res in results:
        if res is None:
            failed_chunks += 1
            continue
        logs.extend(res)
    if failed_chunks:
        log(
            f"UWAGA: {failed_chunks}/{len(chunks)} zakresow eth_getLogs nie "
            "powiodlo sie mimo ponowien - pomijam je (mniej transakcji niz "
            "w rzeczywistosci)."
        )

    log(f"Pobrano {len(logs)} surowych logow Swap, rozwiazuje nadawcow...")

    unique_hashes = sorted({entry["transactionHash"] for entry in logs})
    tx_calls: list[tuple[str, list]] = [
        ("eth_getTransactionByHash", [h]) for h in unique_hashes
    ]
    tx_results = batch_call_with_retry(
        rpc, tx_calls, batch_size=calls_per_batch, max_retries=max_retries
    )
    tx_from: dict[str, str] = {}
    unresolved = 0
    for h, res in zip(unique_hashes, tx_results):
        if res and "from" in res:
            tx_from[h] = res["from"]
        else:
            unresolved += 1
    if unresolved:
        log(
            f"UWAGA: {unresolved}/{len(unique_hashes)} transakcji nie udalo "
            "sie rozwiazac do nadawcy (pominiete)."
        )

    trades: list[Trade] = []
    for entry in logs:
        decoded = decode_swap_log(entry)
        wallet = tx_from.get(decoded.tx_hash)
        if wallet is None:
            continue
        trade = swap_to_trade(decoded, pool, wallet)
        if trade is not None:
            trades.append(trade)

    log(f"Zdekodowano {len(trades)} transakcji Trade.")
    return trades
