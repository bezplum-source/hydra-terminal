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

# ============================================================
# BASE (L2) — Faza "Base L2, etap B0: zbieranie danych" (2026-09-02)
# ============================================================
# Świadomie ODDZIELNA lista od `POOLS` powyżej — to inny łańcuch (Base, nie
# Ethereum mainnet), więc adresy poniżej wymagają OSOBNEGO klienta RPC
# (inny URL Alchemy) i OSOBNEGO licznika bloków (numery bloków Base i
# Ethereum nie są w żaden sposób porównywalne - patrz
# `live/run_incremental.py`, sekcja "Faza Base L2").
#
# Uniswap V3, WETH/USDC, fee 0.05% (500), na sieci Base. Zweryfikowane
# on-chain (eth_call token0()/token1()/fee()/tickSpacing() przez Basescan
# "Read Contract", nie przez zaufanie samej etykiecie) - ta sama zasada co
# przy pulach na mainnecie (patrz odkrycie o mylących tagach Etherscana w
# komentarzu wyżej). Kontrakt zweryfikowany źródłowo jako "UniswapV3Pool".
#
# token0=WETH, token1=USDC (natywny, NIE bridgowany USDC.e) - kolejność
# OD WROTNA niż na mainnecie (tam token0=USDC), bo to czysto numeryczne
# sortowanie adresów kontraktów tokenów, różne na każdym łańcuchu. Adres
# USDC token1 (`0x8335...A02913`) zweryfikowany jako natywny USDC Base -
# istnieje teraz nieaktywna, dużo mniej płynna wersja USDC.e (bridgowana),
# celowo pominięta.
BASE_UNISWAP_V3_WETH_USDC_005 = PoolConfig(
    address="0xd0b53D9277642d899DF5C87A3966A349A798F224",
    token0_symbol="WETH",
    token0_decimals=18,
    token1_symbol="USDC",
    token1_decimals=6,
    eth_is_token0=True,
)

# Na razie jedna pula (najbardziej płynna na Base dla tej pary) - świadomie
# mały zakres na start etapu B0, zgodnie z tym samym wzorcem co Hyperliquid
# H0 (najpierw samo zbieranie, dopiero potem rozszerzanie). Kolejne pule
# Base (inne fee tiery, WETH/USDbC itd.) można dołożyć tym samym wzorcem co
# `POOLS` na mainnecie - patrz komentarz wyżej o batchowaniu adresów.
BASE_POOLS: tuple[PoolConfig, ...] = (BASE_UNISWAP_V3_WETH_USDC_005,)
