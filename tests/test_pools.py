"""Testy konfiguracji pul w `hydra_signals/data_sources/pools.py` - czysto
strukturalne (adresy, dziesietne miejsca, kierunek token0/token1), bez
zadnego live polaczenia RPC.

Dodane przy okazji Fazy "PancakeSwap V3 na Ethereum mainnet" (2026-09-17,
na wyrazna prosbe uzytkownika "dodaj pancakeswap na ethereum") - pilnuja
przede wszystkim tego, zeby:
1. Wszystkie 7 pul (4 Uniswap V3 + 3 PancakeSwap V3) bylo faktycznie w
   `POOLS` (czyli trafialo do jednego batchowanego `eth_getLogs`, patrz
   `onchain_rpc.py`/`run_incremental.py`) - latwo o pomylke "dodalem
   PoolConfig, ale zapomnialem dopisac do krotki".
2. Kazdy adres byl syntaktycznie poprawnym adresem Ethereum (0x + 40 hex) -
   nie chroni to przed zlym `token0()`/`fee()` (do tego wciaz trzeba
   `eth_call`, patrz docstring modulu), ale lapie oczywisty literowka.
3. Nie bylo przypadkowych duplikatow adresow miedzy pulami.
"""

from __future__ import annotations

import re

from hydra_signals.data_sources.pools import (
    BASE_POOLS,
    PANCAKESWAP_V3_USDC_WETH_001,
    PANCAKESWAP_V3_WETH_USDT_001,
    PANCAKESWAP_V3_WETH_USDT_005,
    POOLS,
    UNISWAP_V3_USDC_WETH_005,
    UNISWAP_V3_USDC_WETH_030,
    UNISWAP_V3_WETH_USDT_005,
    UNISWAP_V3_WETH_USDT_030,
)

_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def test_pools_contains_all_seven_mainnet_pools_uniswap_and_pancakeswap():
    assert len(POOLS) == 7
    assert set(POOLS) == {
        UNISWAP_V3_USDC_WETH_005,
        UNISWAP_V3_USDC_WETH_030,
        UNISWAP_V3_WETH_USDT_005,
        UNISWAP_V3_WETH_USDT_030,
        PANCAKESWAP_V3_USDC_WETH_001,
        PANCAKESWAP_V3_WETH_USDT_001,
        PANCAKESWAP_V3_WETH_USDT_005,
    }


def test_all_pool_addresses_are_well_formed_and_unique():
    addresses = [pool.address for pool in POOLS] + [pool.address for pool in BASE_POOLS]
    for address in addresses:
        assert _ADDRESS_RE.match(address), f"zle sformatowany adres: {address}"
    assert len(addresses) == len(set(a.lower() for a in addresses)), "zduplikowany adres puli"


def test_pancakeswap_pools_have_expected_token_order_and_decimals():
    # token0/token1/fee zweryfikowane on-chain (eth_call przez Etherscan
    # "Read Contract", 2026-09-17, patrz docstring modulu) - nie tylko wg
    # GeckoTermina.
    assert PANCAKESWAP_V3_USDC_WETH_001.token0_symbol == "USDC"
    assert PANCAKESWAP_V3_USDC_WETH_001.token0_decimals == 6
    assert PANCAKESWAP_V3_USDC_WETH_001.token1_symbol == "WETH"
    assert PANCAKESWAP_V3_USDC_WETH_001.token1_decimals == 18
    assert PANCAKESWAP_V3_USDC_WETH_001.eth_is_token0 is False

    for pool in (PANCAKESWAP_V3_WETH_USDT_001, PANCAKESWAP_V3_WETH_USDT_005):
        assert pool.token0_symbol == "WETH"
        assert pool.token0_decimals == 18
        assert pool.token1_symbol == "USDT"
        assert pool.token1_decimals == 6
        assert pool.eth_is_token0 is True
