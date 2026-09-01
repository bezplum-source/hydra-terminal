"""Testy dekodowania i pobierania danych on-chain - bez zadnego live
polaczenia sieciowego. Uzywamy podstawionego (fake) transportu HTTP, zeby
zweryfikowac logike dekodowania eventow Swap i mapowania na model Trade.

Dane testowe sa recznie zakodowane wg specyfikacji ABI Solidity (nie
pobrane z live RPC - to srodowisko nie ma dostepu do sieci), ale format
(32-bajtowe slowa, big-endian, U2 dla liczb ujemnych) jest zgodny ze
standardem EVM i identyczny z tym, co zwrocilby prawdziwy wezel.
"""

from __future__ import annotations

from hydra_signals.data_sources.onchain_rpc import (
    JsonRpcClient,
    SWAP_TOPIC0,
    batch_call_with_retry,
    decode_swap_log,
    fetch_trades_from_chain,
    fetch_trades_from_chain_batched,
    swap_to_trade,
)
from hydra_signals.data_sources.pools import UNISWAP_V3_USDC_WETH_005, UNISWAP_V3_WETH_USDT_030
from hydra_signals.models import Side


def _encode_int(value: int, bits: int = 256) -> bytes:
    return value.to_bytes(32, byteorder="big", signed=(value < 0))


def _encode_swap_data(amount0: int, amount1: int, sqrt_price_x96: int, liquidity: int, tick: int) -> str:
    raw = (
        _encode_int(amount0)
        + _encode_int(amount1)
        + sqrt_price_x96.to_bytes(32, "big", signed=False)
        + liquidity.to_bytes(32, "big", signed=False)
        + _encode_int(tick)
    )
    return "0x" + raw.hex()


def _make_log(
    tx_hash: str,
    block: int,
    log_index: int,
    amount0: int,
    amount1: int,
    address: str = UNISWAP_V3_USDC_WETH_005.address,
) -> dict:
    return {
        "transactionHash": tx_hash,
        "blockNumber": hex(block),
        "logIndex": hex(log_index),
        "data": _encode_swap_data(amount0, amount1, sqrt_price_x96=2**96, liquidity=10**12, tick=1000),
        "topics": [SWAP_TOPIC0],
        "address": address,
    }


def test_decode_swap_log_roundtrip():
    # amount0 = +3000_000000 (pula DOSTAJE 3000 USDC, 6 decimals)
    # amount1 = -1_000000000000000000 (pula ODDAJE 1 WETH, 18 decimals)
    log = _make_log("0xabc", block=100, log_index=0, amount0=3_000_000_000, amount1=-(10**18))
    decoded = decode_swap_log(log)
    assert decoded.block_number == 100
    assert decoded.amount0 == 3_000_000_000
    assert decoded.amount1 == -(10**18)


def test_swap_to_trade_buy_when_pool_sends_eth():
    log = _make_log("0xabc", block=100, log_index=0, amount0=3_000_000_000, amount1=-(10**18))
    decoded = decode_swap_log(log)
    trade = swap_to_trade(decoded, UNISWAP_V3_USDC_WETH_005, wallet="0xWALLET1")
    assert trade is not None
    assert trade.side is Side.BUY  # pula oddala WETH -> ktos je kupil
    assert abs(trade.size_eth - 1.0) < 1e-9
    assert abs(trade.price_usd - 3000.0) < 1e-6


def test_swap_to_trade_sell_when_pool_receives_eth():
    # amount1 dodatnie = pula DOSTAJE WETH -> trader SPRZEDAL
    log = _make_log("0xdef", block=101, log_index=1, amount0=-2_900_000_000, amount1=10**18)
    decoded = decode_swap_log(log)
    trade = swap_to_trade(decoded, UNISWAP_V3_USDC_WETH_005, wallet="0xWALLET2")
    assert trade is not None
    assert trade.side is Side.SELL
    assert abs(trade.price_usd - 2900.0) < 1e-6


def test_fetch_trades_from_chain_with_fake_transport():
    log1 = _make_log("0xTX1", block=100, log_index=0, amount0=3_000_000_000, amount1=-(10**18))
    log2 = _make_log("0xTX2", block=101, log_index=0, amount0=-2_900_000_000, amount1=10**18)

    def fake_transport(url: str, payload):
        # payload moze byc pojedynczym requestem (dict) albo batchem (list)
        if isinstance(payload, list):
            # batch eth_getTransactionByHash
            out = []
            senders = {"0xTX1": "0xWALLETA", "0xTX2": "0xWALLETB"}
            for item in payload:
                tx_hash = item["params"][0]
                out.append(
                    {"id": item["id"], "result": {"from": senders[tx_hash], "hash": tx_hash}}
                )
            return out

        assert payload["method"] == "eth_getLogs"
        # Faza "wiele pul + batchowanie adresow": filtr eth_getLogs teraz
        # niesie LISTE adresow, nawet gdy monitorujemy tylko jedna pule.
        assert payload["params"][0]["address"] == [UNISWAP_V3_USDC_WETH_005.address]
        return {"id": payload["id"], "result": [log1, log2]}

    rpc = JsonRpcClient("https://fake-rpc.invalid", transport=fake_transport)
    trades = fetch_trades_from_chain(
        rpc, [UNISWAP_V3_USDC_WETH_005], from_block=100, to_block=101
    )

    assert len(trades) == 2
    wallets = {t.wallet for t in trades}
    assert wallets == {"0xWALLETA", "0xWALLETB"}
    sides = {t.wallet: t.side for t in trades}
    assert sides["0xWALLETA"] is Side.BUY
    assert sides["0xWALLETB"] is Side.SELL


def test_batch_call_with_retry_recovers_from_429_and_network_errors():
    # Kazde logiczne wywolanie identyfikujemy po jego wlasnym parametrze
    # (fromBlock), NIE po JSON-RPC `id` - `id` jest przydzielane od nowa
    # (kolejny licznik) przy KAZDYM wywolaniu `rpc.batch_call`, wiec ten sam
    # logiczny call dostaje inny `id` przy kazdej ponawianej probie. Kazde
    # wywolanie dostaje 429 za pierwszym razem i udaje sie dopiero za
    # drugim; jedna konkretna paczka (zawierajaca fromBlock=2) pada
    # calkowicie siecowo (wyjatek) przy swojej pierwszej probie.
    attempts: dict[str, int] = {}
    network_failed_once: set[str] = set()

    def key_of(item):
        return item["params"][0]["fromBlock"]

    def fake_transport(url: str, payload):
        assert isinstance(payload, list)
        keys_in_batch = [key_of(item) for item in payload]
        if hex(2) in keys_in_batch and hex(2) not in network_failed_once:
            network_failed_once.add(hex(2))
            raise TimeoutError("simulated network failure")
        out = []
        for item in payload:
            k = key_of(item)
            attempts[k] = attempts.get(k, 0) + 1
            if attempts[k] == 1:
                out.append({"id": item["id"], "error": {"code": 429, "message": "rate limited"}})
            else:
                out.append({"id": item["id"], "result": [f"ok-{k}"]})
        return out

    rpc = JsonRpcClient("https://fake-rpc.invalid", transport=fake_transport)
    calls = [("eth_getLogs", [{"fromBlock": hex(i)}]) for i in range(5)]
    results = batch_call_with_retry(rpc, calls, batch_size=2, max_retries=5, base_delay=0, sleep=lambda s: None)
    assert all(r is not None for r in results)
    assert [r[0] for r in results] == [f"ok-{hex(i)}" for i in range(5)]


def test_fetch_trades_from_chain_batched_with_fake_transport():
    log1 = _make_log("0xTX1", block=100, log_index=0, amount0=3_000_000_000, amount1=-(10**18))
    log2 = _make_log("0xTX2", block=105, log_index=0, amount0=-2_900_000_000, amount1=10**18)

    def fake_transport(url: str, payload):
        assert isinstance(payload, list)
        out = []
        for item in payload:
            if item["method"] == "eth_getLogs":
                from_block = int(item["params"][0]["fromBlock"], 16)
                to_block = int(item["params"][0]["toBlock"], 16)
                logs = [
                    l for l in (log1, log2)
                    if from_block <= int(l["blockNumber"], 16) <= to_block
                ]
                out.append({"id": item["id"], "result": logs})
            else:
                assert item["method"] == "eth_getTransactionByHash"
                tx_hash = item["params"][0]
                senders = {"0xTX1": "0xWALLETA", "0xTX2": "0xWALLETB"}
                out.append({"id": item["id"], "result": {"from": senders[tx_hash]}})
        return out

    rpc = JsonRpcClient("https://fake-rpc.invalid", transport=fake_transport)
    trades = fetch_trades_from_chain_batched(
        rpc, [UNISWAP_V3_USDC_WETH_005], from_block=100, to_block=109, blocks_per_call=5
    )
    # log1 (block 100) i log2 (block 105) trafiaja do DWOCH roznych
    # 5-blokowych zakresow (100-104, 105-109) - sprawdza, ze wielo-zakresowe
    # wsadowe zapytanie poprawnie sklleja wyniki z obu paczek.
    assert len(trades) == 2
    wallets = {t.wallet for t in trades}
    assert wallets == {"0xWALLETA", "0xWALLETB"}


def test_fetch_trades_from_chain_batched_routes_multiple_pools_by_log_address():
    """Faza "wiele pul WETH + batchowanie adresow": jeden filtr eth_getLogs
    z tablica adresow WSZYSTKICH monitorowanych pul (nie osobne wywolanie
    per pula - to caly sens tej fazy, patrz onchain_rpc.py). Kazdy log musi
    byc zdekodowany z WLASCIWA konfiguracja (decimals/kolejnosc tokenow) tej
    konkretnej puli, rozpoznana po log["address"]."""
    # log1: z puli USDC/WETH 0.05% (token0=USDC 6dec, token1=WETH 18dec)
    log1 = _make_log(
        "0xTX1", block=100, log_index=0, amount0=3_000_000_000, amount1=-(10**18),
        address=UNISWAP_V3_USDC_WETH_005.address,
    )
    # log2: z puli WETH/USDT 0.3% (token0=WETH 18dec, token1=USDT 6dec) -
    # kolejnosc tokenow ODWROTNA wzgledem log1, musi byc zdekodowana inaczej.
    log2 = _make_log(
        "0xTX2", block=100, log_index=1, amount0=-(10**18), amount1=2_950_000_000,
        address=UNISWAP_V3_WETH_USDT_030.address,
    )

    def fake_transport(url: str, payload):
        # fetch_trades_from_chain_batched idzie ZAWSZE przez batch_call_with_retry
        # (nawet dla jednego wywolania eth_getLogs) - payload to zawsze LISTA,
        # rozroznienie eth_getLogs vs eth_getTransactionByHash po item["method"]
        # (ten sam wzorzec co w istniejacym
        # test_fetch_trades_from_chain_batched_with_fake_transport powyzej).
        assert isinstance(payload, list)
        senders = {"0xTX1": "0xWALLETA", "0xTX2": "0xWALLETB"}
        out = []
        for item in payload:
            if item["method"] == "eth_getLogs":
                # Kluczowa asercja tej fazy: JEDEN filtr, tablica DWOCH adresow.
                assert set(item["params"][0]["address"]) == {
                    UNISWAP_V3_USDC_WETH_005.address,
                    UNISWAP_V3_WETH_USDT_030.address,
                }
                out.append({"id": item["id"], "result": [log1, log2]})
            else:
                assert item["method"] == "eth_getTransactionByHash"
                tx_hash = item["params"][0]
                out.append({"id": item["id"], "result": {"from": senders[tx_hash]}})
        return out

    rpc = JsonRpcClient("https://fake-rpc.invalid", transport=fake_transport)
    trades = fetch_trades_from_chain_batched(
        rpc,
        [UNISWAP_V3_USDC_WETH_005, UNISWAP_V3_WETH_USDT_030],
        from_block=100,
        to_block=100,
        blocks_per_call=10,
    )

    assert len(trades) == 2
    by_wallet = {t.wallet: t for t in trades}
    # log1: pula oddala WETH (amount1 ujemne) -> BUY, cena ~3000 USDC/WETH.
    assert by_wallet["0xWALLETA"].side is Side.BUY
    assert abs(by_wallet["0xWALLETA"].price_usd - 3000.0) < 1e-6
    # log2: pula oddala WETH (amount0 ujemne, bo tu token0=WETH) -> BUY,
    # cena ~2950 USDT/WETH - MUSI byc poprawnie odczytana mimo odwroconej
    # kolejnosci tokenow wzgledem log1 (to jest wlasnie test na routing).
    assert by_wallet["0xWALLETB"].side is Side.BUY
    assert abs(by_wallet["0xWALLETB"].price_usd - 2950.0) < 1e-6
