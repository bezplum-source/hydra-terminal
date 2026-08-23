"""Konfiguracja puli Uniswap V3, z której czytamy transakcje.

Adres poniżej został **zweryfikowany on-chain** (nie tylko "powszechnie
cytowany") przez bezpośrednie wywołanie `eth_call` na `token0()`, `token1()`
i `fee()` tego kontraktu - potwierdziło to token0=USDC (6 dec.),
token1=WETH (18 dec.), fee=500 (0.05%), dokładnie zgodnie z konfiguracją
poniżej. Zrobione przy pierwszym ręcznym uruchomieniu tego pipeline'u przez
przeglądarkę użytkownika (patrz projekt Claude, dokument
`hydrav2-engine-v1.md`, sekcja "Co odkryliśmy po drodze", punkt 1).
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
