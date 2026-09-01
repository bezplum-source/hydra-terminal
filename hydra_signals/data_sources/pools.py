"""Konfiguracja pul Uniswap V3, z których czytamy transakcje.

Każdy adres poniżej został **zweryfikowany on-chain** (nie tylko
"powszechnie cytowany") przez bezpośrednie wywołanie `eth_call` na
`token0()`, `token1()` i `fee()` tego kontraktu.

Pierwsza pula (USDC/WETH 0.05%) zweryfikowana przy pierwszym ręcznym
uruchomieniu tego pipeline'u przez przeglądarkę użytkownika (patrz projekt
Claude, dokument `hydrav2-engine-v1.md`, sekcja "Co odkryliśmy po drodze",
punkt 1).

Trzy kolejne pule (Faza "wiele pul WETH + batchowanie adresów",
2026-09-01) zweryfikowane tą samą metodą (`eth_call` przez Claude in Chrome
na `ethereum.publicnode.com`) - WAŻNE ODKRYCIE PRZY OKAZJI: etykiety/tagi
Etherscana ("USDC 2", "USDT 3", "USDT") NIE wskazują wiarygodnie fee tieru -
wstępne założenie (na podstawie samej nazwy) było błędne dla obu pul
WETH/USDT, dopiero `fee()` z kontraktu dało poprawną wartość. Wniosek: przy
dodawaniu kolejnych pul w przyszłości ZAWSZE weryfikować `fee()` on-chain,
nigdy nie ufać samej etykiecie Etherscana.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PoolConfig:
    address: str
    token0_symbol: str
    token0_decimals: int
    token1_symbol: str
    token1_decimals: int
    eth_is_token0: bool


# Uniswap V3, USDC/WETH, fee 0.05% (500). token0=USDC (adres < adres WETH
# numerycznie), token1=WETH - standardowe uporządkowanie Uniswap V3.
UNISWAP_V3_USDC_WETH_005 = PoolConfig(
    address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
    token0_symbol="USDC",
    token0_decimals=6,
    token1_symbol="WETH",
    token1_decimals=18,
    eth_is_token0=False,
)

# Uniswap V3, USDC/WETH, fee 0.3% (3000) - Etherscan tag "Uniswap V3: USDC 2".
# Zweryfikowane eth_call: token0=USDC, token1=WETH, fee()=3000.
UNISWAP_V3_USDC_WETH_030 = PoolConfig(
    address="0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
    token0_symbol="USDC",
    token0_decimals=6,
    token1_symbol="WETH",
    token1_decimals=18,
    eth_is_token0=False,
)

# Uniswap V3, WETH/USDT, fee 0.05% (500) - Etherscan tag "Uniswap V3: USDT 3"
# (MYLĄCA nazwa - "3" w tagu NIE oznacza fee 0.3%). Zweryfikowane eth_call:
# token0=WETH, token1=USDT, fee()=500.
UNISWAP_V3_WETH_USDT_005 = PoolConfig(
    address="0x11b815efB8f581194ae79006d24E0d814B7697F6",
    token0_symbol="WETH",
    token0_decimals=18,
    token1_symbol="USDT",
    token1_decimals=6,
    eth_is_token0=True,
)

# Uniswap V3, WETH/USDT, fee 0.3% (3000) - Etherscan tag "Uniswap V3: USDT"
# (bez sufiksu liczbowego - też nie sugeruje fee tieru). Zweryfikowane
# eth_call: token0=WETH, token1=USDT, fee()=3000.
UNISWAP_V3_WETH_USDT_030 = PoolConfig(
    address="0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
    token0_symbol="WETH",
    token0_decimals=18,
    token1_symbol="USDT",
    token1_decimals=6,
    eth_is_token0=True,
)

# Wszystkie monitorowane pule. `live/run_incremental.py` przekazuje to jako
# JEDNĄ listę do `fetch_trades_from_chain_batched` - dzięki batchowaniu
# adresów w jednym filtrze `eth_getLogs` (patrz `onchain_rpc.py`), dodanie
# kolejnej puli tutaj NIE zwiększa liczby wywołań RPC na uruchomienie,
# tylko rozmiar pojedynczej odpowiedzi. To był świadomy wybór (patrz decyzja
# użytkownika w projekcie Claude, `hydrav2-automation.md`) zamiast migracji
# na subgraph, właśnie żeby uniknąć zwiększenia throttlingu Alchemy przy
# dokładaniu pul.
POOLS: tuple[PoolConfig, ...] = (
    UNISWAP_V3_USDC_WETH_005,
    UNISWAP_V3_USDC_WETH_030,
    UNISWAP_V3_WETH_USDT_005,
    UNISWAP_V3_WETH_USDT_030,
)
