"""Konfiguracja pul DEX (Uniswap V3 i, od 2026-09-17, PancakeSwap V3 na
Ethereum mainnet), z których czytamy transakcje.

Każdy adres poniżej **powinien być zweryfikowany on-chain** (nie tylko
"powszechnie cytowany") przez bezpośrednie wywołanie `eth_call` na
`token0()`, `token1()` i `fee()` tego kontraktu - to standing rule tego
projektu, patrz odkrycie poniżej o mylących tagach Etherscana.

Pierwsza pula (USDC/WETH 0.05%) zweryfikowana przy pierwszym ręcznym
uruchomieniu tego pipeline'u przez przeglądarkę użytkownika (patrz projekt
Claude, dokument `hydrav2-engine-v1.md`, sekcja "Co odkryliśmy po drodze",
punkt 1).

Trzy kolejne pule Uniswap (Faza "wiele pul WETH + batchowanie adresów",
2026-09-01) zweryfikowane tą samą metodą (`eth_call` przez Claude in Chrome
na `ethereum.publicnode.com`) - WAŻNE ODKRYCIE PRZY OKAZJI: etykiety/tagi
Etherscana ("USDC 2", "USDT 3", "USDT") NIE wskazują wiarygodnie fee tieru -
wstępne założenie (na podstawie samej nazwy) było błędne dla obu pul
WETH/USDT, dopiero `fee()` z kontraktu dało poprawną wartość. Wniosek: przy
dodawaniu kolejnych pul w przyszłości ZAWSZE weryfikować `fee()` on-chain,
nigdy nie ufać samej etykiecie Etherscana.

Dwie kolejne pule Uniswap V3 (fee tier 0.01% dla obu par USDC/WETH i
WETH/USDT, dodane 2026-09-18) - na wyraźną prośbę użytkownika "jakie jeszcze
warto pule dorzucić?" po researchu żywych wolumenów (GeckoTerminal) obu
DEX-ów. To akurat dwie NAJWIĘKSZE nieśledzone dotąd pule mainnet -
USDC/WETH 0.01% ≈$51M/24h (byłaby 2-3 co do wielkości ze wszystkich),
WETH/USDT 0.01% ≈$20.5M/24h (większa niż 3 z 4 wtedy śledzonych pul
Uniswap). Zweryfikowane on-chain (2026-09-18, `eth_call` przez Etherscan
"Read Contract" po podłączeniu przeglądarki użytkownika) - `token0()`/
`token1()`/`fee()` odczytane wprost z każdego z dwóch kontraktów, adresy
tokenów potwierdzone jako kanoniczne USDC/WETH/USDT, wartości w pełni
zgodne z konfiguracją poniżej. WAŻNE: to był świadomy powód do weryfikacji
- pierwsza próba (przez zewnętrzne narzędzia webowe, bez `eth_call`) dała
WEWNĘTRZNIE NIESPÓJNE odczyty fee tieru dla tych samych adresów (raz 0.01%,
raz 0.05%) - dokładnie ten scenariusz, przed którym ostrzega standing rule
tego modułu. `eth_call` przez Etherscan rozstrzygnął jednoznacznie:
fee()=100 dla obu. Rozważane, ale ODRZUCONE jako niewarte: Uniswap V3
DAI/WETH 0.3% (tylko ~$423K/24h - mniej niż nasza najmniejsza obecna pula);
para WETH/WBTC (nawet przy wysokim wolumenie) wymagałaby przebudowy
`swap_to_trade()` w `onchain_rpc.py`, bo dekoder liczy `price_usd` zakładając,
że druga noga swapu to JUŻ jest wartość w dolarach (stablecoin) - WBTC nie
jest stablecoinem, potrzebny byłby osobny feed ceny BTC/USD (ten sam koszt
inżynierski co odrzucone wcześniej Curve).

Trzy pule PancakeSwap V3 (Ethereum mainnet, dodane 2026-09-17 na wyraźną
prośbę użytkownika "dodaj pancakeswap na ethereum", po researchu DEX-ów
alternatywnych do Uniswap - patrz projekt Claude,
`hydrav2-min-trades-3-and-dex-research.md`). Wstępnie skonfigurowane wg
GeckoTermina (indekser czytający realny stan kontraktu), a NASTĘPNIE - w tej
samej sesji, po podłączeniu Claude in Chrome przez użytkownika ("włączyłem
chrome, sprawdź") - w pełni zweryfikowane bezpośrednim `eth_call` przez
Etherscan "Read Contract" dla wszystkich trzech kontraktów: `token0()`,
`token1()` i `fee()` odczytane wprost z każdego kontraktu, adresy tokenów
potwierdzone jako kanoniczne USDC/WETH/USDT, `factory()` każdej puli
zwraca kontrakt zgodny z interfejsem `IPancakeV3Factory` (widoczne wprost w
opisie NatSpec na Etherscan), a każda pula ma pole `lmPool` (rozszerzenie
specyficzne dla PancakeSwap V3, którego Uniswap V3 nie ma) - potwierdza to,
że to faktycznie kontrakty PancakeSwap V3, a nie coś podszywającego się pod
tę nazwę. Wyniki w pełni zgodne z wartościami skonfigurowanymi poniżej -
zero rozbieżności. PancakeSwap V3 to architektonicznie dosłowny fork
Uniswap V3 (identyczna sygnatura zdarzenia
`Swap(address,address,int256,int256,uint160,uint128,int24)`, to samo
`SWAP_TOPIC0` w `onchain_rpc.py`) - dekoder poniżej nie wymagał ŻADNEJ
zmiany kodu, tylko nowych wpisów `PoolConfig`.
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

# Uniswap V3, USDC/WETH, fee 0.01% (100), dodane 2026-09-18 (patrz docstring
# modulu). Zweryfikowane eth_call: token0=USDC (0xA0b8...eB48), token1=WETH
# (0xC02a...6Cc2), fee()=100. Najwiekszy pojedynczy wolumen ze wszystkich
# pul mainnet w momencie dodania (~$51M/24h).
UNISWAP_V3_USDC_WETH_001 = PoolConfig(
    address="0xE0554a476A092703abdB3Ef35c80e0D76d32939F",
    token0_symbol="USDC",
    token0_decimals=6,
    token1_symbol="WETH",
    token1_decimals=18,
    eth_is_token0=False,
)

# Uniswap V3, WETH/USDT, fee 0.01% (100), dodane 2026-09-18 (patrz docstring
# modulu). Zweryfikowane eth_call: token0=WETH (0xC02a...6Cc2), token1=USDT
# (0xdAC1...1ec7), fee()=100. ~$20.5M/24h w momencie dodania.
UNISWAP_V3_WETH_USDT_001 = PoolConfig(
    address="0xc7bbec68d12a0d1830360f8ec58fa599ba1b0e9b",
    token0_symbol="WETH",
    token0_decimals=18,
    token1_symbol="USDT",
    token1_decimals=6,
    eth_is_token0=True,
)

# ============================================================
# PancakeSwap V3, Ethereum mainnet (dodane 2026-09-17)
# ============================================================
# Fork Uniswap V3 - identyczna sygnatura zdarzenia Swap, ten sam SWAP_TOPIC0,
# ten sam decode_swap_log()/swap_to_trade() w onchain_rpc.py (zero zmian
# kodu poza tymi wpisami PoolConfig). Trzy pule wybrane jako najlepsze wg
# 24h wolumenu spośród par USDC/USDT na PancakeSwap V3 Ethereum (research w
# projekcie Claude, `hydrav2-min-trades-3-and-dex-research.md`) - BSC
# świadomie pominięty na razie (osobna, dużo większa decyzja architektoniczna:
# nowy RPC, nowy czas bloku, populacja portfeli w innej przestrzeni adresowej).
#
# ZWERYFIKOWANE on-chain (2026-09-17, przez Etherscan "Read Contract" po
# podłączeniu Claude in Chrome) - patrz docstring modułu wyżej po pełny opis:
# token0()/token1()/fee() odczytane bezpośrednio z każdego z trzech
# kontraktów, zgodne co do joty z wartościami poniżej.

# PancakeSwap V3, USDC/WETH, fee 0.01% (100). Zweryfikowane eth_call (przez
# Etherscan Read Contract): token0=USDC (0xA0b8...eB48), token1=WETH
# (0xC02a...6Cc2), fee()=100.
PANCAKESWAP_V3_USDC_WETH_001 = PoolConfig(
    address="0x1445f32d1A74872bA41F3D8cF4022e9996120b31",
    token0_symbol="USDC",
    token0_decimals=6,
    token1_symbol="WETH",
    token1_decimals=18,
    eth_is_token0=False,
)

# PancakeSwap V3, WETH/USDT, fee 0.01% (100). Zweryfikowane eth_call: token0=WETH
# (0xC02a...6Cc2), token1=USDT (0xdAC1...1ec7), fee()=100.
PANCAKESWAP_V3_WETH_USDT_001 = PoolConfig(
    address="0xACDB27b266142223e1E676841c1E809255fc6d07",
    token0_symbol="WETH",
    token0_decimals=18,
    token1_symbol="USDT",
    token1_decimals=6,
    eth_is_token0=True,
)

# PancakeSwap V3, WETH/USDT, fee 0.05% (500). Zweryfikowane eth_call: token0=WETH
# (0xC02a...6Cc2), token1=USDT (0xdAC1...1ec7), fee()=500.
PANCAKESWAP_V3_WETH_USDT_005 = PoolConfig(
    address="0x6ca298D2983aB03Aa1dA7679389D955A4eFee15c",
    token0_symbol="WETH",
    token0_decimals=18,
    token1_symbol="USDT",
    token1_decimals=6,
    eth_is_token0=True,
)

# Wszystkie monitorowane pule (Uniswap V3 + PancakeSwap V3, Ethereum
# mainnet). `live/run_incremental.py` przekazuje to jako JEDNĄ listę do
# `fetch_trades_from_chain_batched` - dzięki batchowaniu adresów w jednym
# filtrze `eth_getLogs` (patrz `onchain_rpc.py`), dodanie kolejnej puli
# tutaj NIE zwiększa liczby wywołań RPC na uruchomienie, tylko rozmiar
# pojedynczej odpowiedzi (wywołania `eth_getTransactionByHash` per unikalny
# hash transakcji rosną z wolumenem - patrz uwaga o throttlingu Alchemy).
# To był świadomy wybór (patrz decyzja użytkownika w projekcie Claude,
# `hydrav2-automation.md`) zamiast migracji na subgraph, właśnie żeby
# uniknąć zwiększenia throttlingu Alchemy przy dokładaniu pul.
POOLS: tuple[PoolConfig, ...] = (
    UNISWAP_V3_USDC_WETH_005,
    UNISWAP_V3_USDC_WETH_030,
    UNISWAP_V3_WETH_USDT_005,
    UNISWAP_V3_WETH_USDT_030,
    UNISWAP_V3_USDC_WETH_001,
    UNISWAP_V3_WETH_USDT_001,
    PANCAKESWAP_V3_USDC_WETH_001,
    PANCAKESWAP_V3_WETH_USDT_001,
    PANCAKESWAP_V3_WETH_USDT_005,
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
