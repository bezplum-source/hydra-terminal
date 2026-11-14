"""Test end-to-end calego 'zywego' pipeline'u (`live/run_incremental.py`):
symuluje DWA kolejne uruchomienia procesu (dokladnie tak, jak dwa kolejne
odpalenia GitHub Actions co godzine) na fikcyjnym lancuchu, bez zadnej
prawdziwej sieci - i sprawdza, ze:

1. Pierwsze uruchomienie robi backfill i tworzy caly stan od zera.
2. Drugie uruchomienie (nowy proces - nowe obiekty, tylko pliki na dysku
   sa wspolne) POPRAWNIE wznawia sie od zapisanego stanu: nie gubi EMA,
   nie zeruje licznika sledzonych portfeli, nie przetwarza ponownie tych
   samych blokow, i dokleja nowe swiece do istniejacej historii.

To jest najbardziej ryzykowna czesc calej automatyzacji (dokladnie to, co
bedzie sie dzialo bez nadzoru co godzine w GitHub Actions) - stad osobny,
pelny test integracyjny, mimo ze poszczegolne cegielki (retry HTTP,
wznawialnosc ScoringEngine, I/O stanu) sa juz przetestowane osobno.
"""

from __future__ import annotations

import datetime
import json

import pytest

from hydra_signals.data_sources import hyperliquid_ws as hl_ws
from hydra_signals.data_sources.onchain_rpc import JsonRpcClient, SWAP_TOPIC0
from hydra_signals.data_sources.pools import BASE_UNISWAP_V3_WETH_USDC_005, UNISWAP_V3_USDC_WETH_005
from hydra_signals.scoring import blend_composite, decide_signal
from live import build_site as bs
from live import lake
from live import run_incremental as ri
from live import state as st


def _encode_int(value: int) -> bytes:
    return value.to_bytes(32, byteorder="big", signed=(value < 0))


def _encode_swap_data(amount0: int, amount1: int) -> str:
    raw = (
        _encode_int(amount0)
        + _encode_int(amount1)
        + (2**96).to_bytes(32, "big", signed=False)
        + (10**12).to_bytes(32, "big", signed=False)
        + _encode_int(1000)
    )
    return "0x" + raw.hex()


class FakeChain:
    """Minimalny fikcyjny lancuch: lista (block, tx_hash, wallet, amount0,
    amount1) reprezentujacych zdarzenia Swap, plus prosty zegar blokow
    (14s/blok, wystarczy do generowania nierosnacych znacznikow czasu)."""

    def __init__(self, pool_address: str = UNISWAP_V3_USDC_WETH_005.address):
        self.events: list[tuple[int, str, str, int, int]] = []
        self.head = 0
        # Faza "Base L2, etap B0" - domyslnie ta sama pula mainnetowa co
        # wczesniej (zero zmiany zachowania istniejacych testow), ale nowe
        # testy Base ponizej przekazuja tu `BASE_UNISWAP_V3_WETH_USDC_005.address`,
        # zeby fikcyjne logi "naleZaly" do puli Base, ktora fetch_trades_from_chain_batched
        # faktycznie filtruje przy przekazaniu BASE_POOLS.
        self.pool_address = pool_address

    def add_swap(self, block: int, tx_hash: str, wallet: str, amount0: int, amount1: int) -> None:
        self.events.append((block, tx_hash, wallet, amount0, amount1))
        self.head = max(self.head, block)

    def logs_in_range(self, from_block: int, to_block: int) -> list[dict]:
        out = []
        for block, tx_hash, _wallet, amount0, amount1 in self.events:
            if from_block <= block <= to_block:
                out.append(
                    {
                        "transactionHash": tx_hash,
                        "blockNumber": hex(block),
                        "logIndex": "0x0",
                        "data": _encode_swap_data(amount0, amount1),
                        "topics": [SWAP_TOPIC0],
                        # Faza "wiele pul + batchowanie adresow": run_incremental
                        # teraz filtruje po TABLICY adresow (POOLS), wiec kazdy
                        # zwrocony log musi niesc "address", zeby dekoder
                        # zrouotowal go do wlasciwej PoolConfig. Ten fake lancuch
                        # symuluje transakcje tylko z podstawowej puli USDC/WETH.
                        "address": self.pool_address,
                    }
                )
        return out

    def sender_of(self, tx_hash: str) -> str | None:
        for _block, h, wallet, _a0, _a1 in self.events:
            if h == tx_hash:
                return wallet
        return None

    def transport(self, url: str, payload):
        # Faza "eth_blockNumber przez retry" (naprawiony realny bug: byl to
        # jedyny NIEBATCHOWANY, nieponawiany apel w calym pipelinie - patrz
        # komentarz w run_incremental.py przy `head_result`) - teraz idzie
        # przez `batch_call_with_retry`, wiec zawsze przychodzi jako LISTA
        # (nawet gdy niesie tylko jedno wywolanie), tak jak reszta metod
        # ponizej - juz nie potrzeba osobnej galezi dla pojedynczego dict.
        assert isinstance(payload, list), "eth_blockNumber idzie teraz przez batch_call_with_retry (lista)"
        out = []
        for item in payload:
            method = item["method"]
            if method == "eth_blockNumber":
                out.append({"id": item["id"], "result": hex(self.head)})
            elif method == "eth_getLogs":
                p = item["params"][0]
                logs = self.logs_in_range(int(p["fromBlock"], 16), int(p["toBlock"], 16))
                out.append({"id": item["id"], "result": logs})
            elif method == "eth_getTransactionByHash":
                tx_hash = item["params"][0]
                sender = self.sender_of(tx_hash)
                if sender is None:
                    out.append({"id": item["id"], "result": None})
                else:
                    out.append({"id": item["id"], "result": {"from": sender, "hash": tx_hash}})
            elif method == "eth_getBlockByNumber":
                block = int(item["params"][0], 16)
                if block > self.head:
                    # Tak zachowuje sie prawdziwy RPC: blok, ktory jeszcze
                    # nie zostal wykopany, po prostu nie istnieje - `null`,
                    # nie jakis wymyslony znacznik czasu. Kluczowe dla testu
                    # "otwartego okna" ponizej.
                    out.append({"id": item["id"], "result": None})
                else:
                    # czas rosnie liniowo z blokiem - wystarczy do testu (nie
                    # musi byc realistyczne, tylko monotoniczne i deterministyczne)
                    out.append({"id": item["id"], "result": {"timestamp": hex(1_700_000_000 + block * 12)}})
            else:
                raise AssertionError(f"nieobslugiwana metoda w fake transport: {method}")
        return out


def _seed_wallets(chain: FakeChain, start_block: int, end_block: int) -> None:
    """Generuje wzorzec analogiczny do testow scoringu: 'good' portfele
    systematycznie kupuja przed wzrostem ceny (i sprzedaja na gorce), 'bad'
    portfele robia odwrotnie - zeby klasyfikacja miala cokolwiek do
    wykrycia, a sygnal nie zostal plasko HOLD przez caly test."""
    block = start_block
    tx_counter = [0]

    def next_hash() -> str:
        tx_counter[0] += 1
        return f"0xTX{tx_counter[0]:06d}"

    while block < end_block:
        for i in range(4):
            # good kupuje USDC->WETH (pula oddaje WETH: amount1 ujemne)
            chain.add_swap(block, next_hash(), f"good{i}", amount0=3_000_000_000, amount1=-(10**18))
        for i in range(2):
            # bad sprzedaje (pula dostaje WETH: amount1 dodatnie)
            chain.add_swap(block, next_hash(), f"bad{i}", amount0=-2_900_000_000, amount1=10**18)
        block += 10


def _patch_all_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(st, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(st, "SCORING_STATE_PATH", tmp_path / "data" / "scoring_state.json")
    monkeypatch.setattr(st, "TRADE_BUFFER_PATH", tmp_path / "data" / "trade_buffer.csv")
    monkeypatch.setattr(st, "WALLETS_SEEN_PATH", tmp_path / "data" / "wallets_seen.txt")
    monkeypatch.setattr(st, "CANDLES_HISTORY_PATH", tmp_path / "data" / "candles_history.json")
    # BUGFIX (znaleziony 2026-08-24 przy okazji Fazy H0 Hyperliquid): brakowalo
    # tu patchowania REGIME_STATE_PATH/WALLET_FLIP_STATE_PATH (dodanych do
    # live/state.py w Fazach 2/3, PO tym jak ten helper zostal napisany w
    # Fazie 0/1) - testy nizej wywoluja run_incremental.main(), ktore
    # WYWOLUJE regime_engine/wallet_flip i realnie zapisywalo/nadpisywalo
    # PRAWDZIWE pliki data/regime_state.json i data/wallet_flip_state.json
    # w repo (wzgledem biezacego katalogu roboczego), zamiast pisac do
    # tmp_path jak reszta stanu. Bylo to NIESZKODLIWE w workflow'ie
    # `update.yml` (uruchamia potem prawdziwy run_incremental.py, ktory
    # nadpisuje smieci testowe poprawnymi danymi PRZED commitem, patrz
    # hydrav2-automation.md), ale ujawnilo sie w nowym `hyperliquid-
    # update.yml` (waskie `git add` tylko bufora Hyperliquid) jako
    # "cannot rebase: You have unstaged changes" - pozostawiony przez testy
    # brudny, niezacommitowany `data/wallet_flip_state.json` blokowal
    # retry/rebase przy konflikcie pusha.
    monkeypatch.setattr(st, "REGIME_STATE_PATH", tmp_path / "data" / "regime_state.json")
    monkeypatch.setattr(st, "WALLET_FLIP_STATE_PATH", tmp_path / "data" / "wallet_flip_state.json")
    # Faza H2 (brief Hyperliquid) - dopisane OD RAZU przy wprowadzeniu tych
    # dwoch nowych sciezek (nie po fakcie, jak REGIME_STATE_PATH/
    # WALLET_FLIP_STATE_PATH wyzej) - to jest dokladnie ta sama klasa bledu,
    # ktora spowodowala incydent "cannot rebase: You have unstaged changes"
    # opisany w hydrav2-automation.md: brak patcha tutaj oznaczalby, ze
    # run_incremental.main() w testach ponizej czyta/pisze PRAWDZIWE pliki
    # data/hyperliquid_trades_buffer.jsonl / data/hyperliquid_scoring_state.json
    # w repo zamiast do tmp_path.
    monkeypatch.setattr(st, "HYPERLIQUID_TRADES_BUFFER_PATH", tmp_path / "data" / "hyperliquid_trades_buffer.jsonl")
    monkeypatch.setattr(st, "HYPERLIQUID_SCORING_STATE_PATH", tmp_path / "data" / "hyperliquid_scoring_state.json")
    # Faza H3 - dopisane OD RAZU (patrz komentarz wyzej) razem z wprowadzeniem
    # tej trzeciej sciezki Hyperliquid.
    monkeypatch.setattr(st, "HYPERLIQUID_WALLETS_SEEN_PATH", tmp_path / "data" / "hyperliquid_wallets_seen.txt")
    # Faza "Base L2, etap B0" - dopisane OD RAZU (patrz komentarze wyzej przy
    # REGIME_STATE_PATH/HYPERLIQUID_*): bez tego testy ponizej wywolujace
    # ri.main() czytalyby/pisaly PRAWDZIWE pliki data/base_trade_buffer.csv i
    # data/base_collector_state.json w repo zamiast do tmp_path.
    monkeypatch.setattr(st, "BASE_TRADE_BUFFER_PATH", tmp_path / "data" / "base_trade_buffer.csv")
    monkeypatch.setattr(st, "BASE_COLLECTOR_STATE_PATH", tmp_path / "data" / "base_collector_state.json")
    # Faza "reset licznika wyniku/win rate" - dopisane OD RAZU (patrz
    # komentarze wyzej przy REGIME_STATE_PATH/HYPERLIQUID_*/BASE_*): bez
    # tego test ponizej czytalby PRAWDZIWY plik data/stats_reset_state.json
    # z tego repo (ktory realnie istnieje i ma ustawiona wartosc dla
    # zywej strony) zamiast izolowanego tmp_path.
    monkeypatch.setattr(st, "STATS_RESET_STATE_PATH", tmp_path / "data" / "stats_reset_state.json")
    # BUGFIX (znaleziony 2026-09-09 przy okazji redesignu frontu): SIGNAL_STATE_PATH
    # istnieje w live/state.py od Fazy 0/1 (razem z SCORING_STATE_PATH wyzej), ale
    # nigdy nie zostalo tutaj dopatchowane - dokladnie ta sama klasa bledu co przy
    # REGIME_STATE_PATH/WALLET_FLIP_STATE_PATH (patrz komentarz wyzej): testy nizej
    # wywolujace run_incremental.main()/SignalEngine czytaly/pisaly PRAWDZIWY plik
    # data/signal_state.json w tym repo zamiast do tmp_path (zaobserwowana realna
    # zmiana "SHORT" -> "HOLD" po samym odpaleniu `pytest`, bez zadnych innych zmian).
    monkeypatch.setattr(st, "SIGNAL_STATE_PATH", tmp_path / "data" / "signal_state.json")
    # Faza "integracja Base B1-B3" - dopisane OD RAZU (patrz wszystkie
    # komentarze wyzej - dokladnie ta sama klasa bledu za kazdym razem, gdy
    # nowa sciezka stanu w live/state.py nie trafia od razu do tego helpera):
    # bez tego testy ponizej wywolujace ri.main() czytalyby/pisaly PRAWDZIWE
    # pliki data/base_scoring_state.json i data/base_wallets_seen.txt w tym
    # repo zamiast do tmp_path.
    monkeypatch.setattr(st, "BASE_SCORING_STATE_PATH", tmp_path / "data" / "base_scoring_state.json")
    monkeypatch.setattr(st, "BASE_WALLETS_SEEN_PATH", tmp_path / "data" / "base_wallets_seen.txt")
    # Faza "wspolna pula SPOT" - dopisane OD RAZU (patrz wszystkie komentarze
    # wyzej - dokladnie ta sama klasa bledu za kazdym razem, gdy nowa sciezka
    # stanu w live/state.py nie trafia od razu do tego helpera): bez tego
    # testy ponizej wywolujace ri.main() zapisywalyby PRAWDZIWY plik
    # data/spot_pool_state.json w tym repo zamiast do tmp_path (dokladnie to
    # sie stalo przy pierwszym uruchomieniu pytest po dodaniu tej sciezki -
    # zauwazone i naprawione od razu, przed dostawa).
    monkeypatch.setattr(st, "SPOT_POOL_STATE_PATH", tmp_path / "data" / "spot_pool_state.json")
    # Faza "funding + open interest" - dopisane OD RAZU (patrz komentarze wyzej).
    monkeypatch.setattr(st, "DERIVS_HISTORY_PATH", tmp_path / "data" / "derivs_history.json")
    # Etap 1 planu "trend" - dopisane OD RAZU (patrz komentarze wyzej).
    monkeypatch.setattr(st, "RACE_STATE_PATH", tmp_path / "data" / "race_state.json")
    monkeypatch.setattr(st, "MARKET_HISTORY_PATH", tmp_path / "data" / "market_history.json")
    # Etap 6 i 7 planu "trend" - dopisane OD RAZU (patrz komentarze wyzej).
    monkeypatch.setattr(st, "STABLES_HISTORY_PATH", tmp_path / "data" / "stables_history.json")
    monkeypatch.setattr(st, "OPTIONS_HISTORY_PATH", tmp_path / "data" / "options_history.json")
    # Faza "Long term (30d)" - dopisane OD RAZU (patrz wszystkie komentarze
    # wyzej - dokladnie ta sama klasa bledu za kazdym razem, gdy nowa sciezka
    # stanu w live/state.py nie trafia od razu do tego helpera): bez tego
    # testy ponizej wywolujace ri.main() zapisywalyby PRAWDZIWE pliki
    # data/*_lt.json w tym repo zamiast do tmp_path (zaobserwowane i
    # naprawione od razu, przed dostawa - dokladnie tak, jak przewiduje
    # komentarz przy SPOT_POOL_STATE_PATH powyzej).
    monkeypatch.setattr(st, "SCORING_STATE_LT_PATH", tmp_path / "data" / "scoring_state_lt.json")
    monkeypatch.setattr(st, "BASE_SCORING_STATE_LT_PATH", tmp_path / "data" / "base_scoring_state_lt.json")
    monkeypatch.setattr(
        st, "HYPERLIQUID_SCORING_STATE_LT_PATH", tmp_path / "data" / "hyperliquid_scoring_state_lt.json"
    )
    monkeypatch.setattr(st, "SPOT_POOL_STATE_LT_PATH", tmp_path / "data" / "spot_pool_state_lt.json")
    monkeypatch.setattr(st, "SIGNAL_STATE_LT_PATH", tmp_path / "data" / "signal_state_lt.json")
    monkeypatch.setattr(st, "REGIME_STATE_LT_PATH", tmp_path / "data" / "regime_state_lt.json")
    # Faza "manifesty per-run" - dopisane OD RAZU (patrz wszystkie komentarze
    # wyzej - dokladnie ta sama klasa bledu za kazdym razem, gdy nowa sciezka
    # stanu w live/state.py nie trafia od razu do tego helpera): bez tego
    # KAZDY test ponizej wywolujacy ri.main() zapisywalby prawdziwy plik w
    # data/manifests/ w tym repo zamiast do tmp_path.
    monkeypatch.setattr(st, "RUN_MANIFESTS_DIR", tmp_path / "data" / "manifests")
    monkeypatch.setattr(bs, "SITE_DIR", tmp_path / "site")


def test_two_consecutive_runs_resume_correctly(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=1000)

    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    # --- Uruchomienie 1: backfill od zera ---
    rc1 = ri.main()
    assert rc1 == 0

    state_after_1 = st.load_scoring_state()
    assert state_after_1["last_processed_block"] == chain.head
    wallets_after_1 = st.load_wallets_seen()
    assert wallets_after_1 == {"good0", "good1", "good2", "good3", "bad0", "bad1"}
    candles_after_1 = st.load_candles_history()
    assert len(candles_after_1) >= 3  # 1000 blokow / window_blocks=250 -> >=3 pelne okna
    assert (tmp_path / "site" / "index.html").exists()

    ema_after_1 = {k: state_after_1[k] for k in ("good_short", "good_long", "bad_short", "bad_long")}
    assert all(v is not None for v in ema_after_1.values())

    # --- Dokladamy nowe bloki do lancucha (symulacja uplywu czasu) i
    #     uruchamiamy DRUGI, NIEZALEZNY proces (nowe obiekty silnika) ---
    _seed_wallets(chain, start_block=1000, end_block=1500)
    # nowy portfel, ktory pojawia sie DOPIERO w drugim uruchomieniu
    chain.add_swap(1010, "0xTXNEWWALLET", "newcomer", amount0=3_000_000_000, amount1=-(10**18))

    rc2 = ri.main()
    assert rc2 == 0

    state_after_2 = st.load_scoring_state()
    assert state_after_2["last_processed_block"] == chain.head
    assert state_after_2["last_processed_block"] > state_after_1["last_processed_block"]

    wallets_after_2 = st.load_wallets_seen()
    assert "newcomer" in wallets_after_2
    assert wallets_after_2 >= wallets_after_1  # nikt nie zostal "zapomniany"

    candles_after_2 = st.load_candles_history()
    assert len(candles_after_2) > len(candles_after_1)
    # historia z pierwszego uruchomienia NIE zostala nadpisana/utracona
    assert candles_after_2[: len(candles_after_1)] == candles_after_1

    # total_wallets_tracked (skumulowane od poczatku) rosnie monotonicznie
    # w calej polaczonej historii swiec - nigdy nie spada.
    tracked_sequence = [c["tracked"] for c in candles_after_2]
    assert tracked_sequence == sorted(tracked_sequence)

    # EMA NIE zresetowalo sie do "na zimno" (None) przy wznowieniu - trzecie
    # uruchomienie startuje z sensownymi liczbami, nie z pierwszej swiecy.
    ema_after_2 = {k: state_after_2[k] for k in ("good_short", "good_long", "bad_short", "bad_long")}
    assert all(v is not None for v in ema_after_2.values())


def test_second_run_with_no_new_blocks_is_a_safe_noop(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles_after_1 = st.load_candles_history()
    state_after_1 = st.load_scoring_state()

    # Drugie uruchomienie BEZ zadnych nowych blokow w lancuchu.
    assert ri.main() == 0
    candles_after_2 = st.load_candles_history()
    state_after_2 = st.load_scoring_state()

    assert candles_after_2 == candles_after_1
    assert state_after_2["last_processed_block"] == state_after_1["last_processed_block"]


def test_still_open_window_is_deferred_not_dated_question_mark_or_duplicated(tmp_path, monkeypatch):
    """Regresja na blad znaleziony na prawdziwym uruchomieniu (GitHub Actions):
    najnowsza swieca odpowiadala oknu, ktore jeszcze sie nie domknelo wzgledem
    aktualnego czola lancucha (`window_end_block` w PRZYSZLOSCI) -> RPC nie
    mial dla niego znacznika czasu (`"time": "?"` na stronie), a w kolejnym
    uruchomieniu te same transakcje zostalyby policzone PONOWNIE jako nowa
    swieca o tym samym numerze bloku (duplikat w historii).

    Test: w PIERWSZYM uruchomieniu lancuch konczy sie W SRODKU drugiego okna
    (250-blokowego) - to okno NIE powinno zostac zaliczone do zadnej swiecy.
    W DRUGIM uruchomieniu lancuch przesuwa sie na tyle, ze to okno sie
    domyka - powinno zostac policzone DOKLADNIE RAZ, z prawdziwym znacznikiem
    czasu (nie "?")."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "300")

    chain = FakeChain()
    # window_blocks domyslnie = 250. Okno 0 to bloki 0-249 (pelne, zamkniete
    # skoro head >= 249). Okno 1 to bloki 250-499 - lancuch konczy sie na
    # bloku 290, czyli okno 1 jest jeszcze OTWARTE (head < 499).
    _seed_wallets(chain, start_block=0, end_block=291)
    assert chain.head == 290

    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    # --- Uruchomienie 1: okno 1 jest jeszcze otwarte ---
    assert ri.main() == 0
    candles_after_1 = st.load_candles_history()
    blocks_after_1 = [c["block"] for c in candles_after_1]

    assert 249 in blocks_after_1  # okno 0 (zamkniete) - policzone
    assert 499 not in blocks_after_1  # okno 1 (jeszcze otwarte) - NIE policzone
    assert all(b <= chain.head for b in blocks_after_1)  # zaden blok "z przyszlosci"
    assert all(c["time"] != "?" for c in candles_after_1)  # zadnej brakujacej daty

    state_after_1 = st.load_scoring_state()
    # last_processed_block dalej normalnie postepuje do czola lancucha (nic
    # nie jest ponownie pobierane) - tylko SCOROWANIE otwartego okna jest
    # odlozone, sledzone osobnym polem stanu.
    assert state_after_1["last_processed_block"] == chain.head
    assert state_after_1["last_scored_window_end"] == 249

    # --- Uplyw czasu: lancuch przesuwa sie na tyle, ze okno 1 sie domyka ---
    _seed_wallets(chain, start_block=291, end_block=520)
    assert chain.head > 499

    # --- Uruchomienie 2: okno 1 powinno zostac policzone DOKLADNIE RAZ ---
    assert ri.main() == 0
    candles_after_2 = st.load_candles_history()
    blocks_after_2 = [c["block"] for c in candles_after_2]

    assert blocks_after_2.count(499) == 1  # brak duplikatu
    assert all(c["time"] != "?" for c in candles_after_2)  # okno 1 ma juz prawdziwa date
    assert all(b <= chain.head for b in blocks_after_2)
    # historia z pierwszego uruchomienia nie zostala nadpisana/utracona
    assert candles_after_2[: len(candles_after_1)] == candles_after_1


# =====================================================================
# Faza H2 (brief hydrav2-hyperliquid-brief.md) - blend composite_spot/perp
# =====================================================================


def _make_hl_trade(buyer, seller, price, size, ts_ms):
    return hl_ws.HyperliquidTrade(
        coin="ETH",
        aggressor_side=hl_ws.AggressorSide.BUY,
        price_usd=price,
        size_eth=size,
        buyer=buyer,
        seller=seller,
        ts_ms=ts_ms,
        tid=ts_ms,
        tx_hash="0xabc",
    )


def test_without_hyperliquid_buffer_composite_equals_spot_and_signal_matches_it(tmp_path, monkeypatch):
    """Regresja: brak `data/hyperliquid_trades_buffer.jsonl` (dokladnie stan
    sprzed Fazy H2, albo pierwsze uruchomienie zanim listener Hyperliquid
    zdazyl cokolwiek zebrac) -> `composite_perp` musi wyjsc `None`, a
    `blend_composite` musi sie zachowac IDENTYCZNIE jak stary, czysto
    spotowy sygnal (`composite == compositeSpot`) - "graceful degradation"
    z briefu, nie zmiana zachowania.

    ZMIANA (Faza "sygnal z histereza"): `signal` (glowne pole, sterowane
    teraz przez `SignalEngine` z histereza+potwierdzeniem) i `signalSpotOnly`
    (diagnostyczne pole liczone WEWNATRZ `ScoringEngine.run()`, celowo BEZ
    histerezy - patrz scoring.py) juz NIE MUSZA byc rowne nawet gdy
    `composite == compositeSpot` - to dwa niezalezne tory z rozna logika
    decyzyjna, nie duplikaty tej samej wartosci. W tym konkretnym tescie
    (jedna swieca, backfill od zera) `signalSpotOnly` moze od razu pokazac
    LONG/SHORT (stary, natychmiastowy prog), a `signal` zostaje HOLD, bo
    `SignalEngine` startuje ze stanu HOLD i wymaga
    `min_confirmation_periods` (domyslnie 3) kolejnych swiec z rzedu, zanim
    w ogole wejdzie w LONG/SHORT - dokladnie tak samo, jak
    `RegimeEngine`/`regime` przy pierwszej swiecy nie wchodzi od razu w
    BULL/BEAR."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert len(candles) > 0
    for c in candles:
        assert c["compositePerp"] is None
        assert c["composite"] == c["compositeSpot"]
        # Faza H3 - pola diagnostyczne rowniez w bezpiecznym, "brak danych"
        # stanie, nie tylko composite/signal.
        assert c["perpIsMature"] is False
        assert c["perpTracked"] == 0
    # Pierwsza (jedyna w tym tescie) swieca - SignalEngine startuje z HOLD i
    # potrzebuje kilku potwierdzajacych swiec pod rzad, wiec NIE wchodzi od
    # razu w LONG/SHORT nawet jesli compositeSpot juz przekracza enter_threshold.
    assert candles[-1]["signalHydra"] == "HOLD"
    # Etap 1 planu "trend": glowny `signal` to trend 14 dni - przy kilku
    # godzinach historii (mniej niz TREND_MIN_COVERAGE_DAYS) to HOLD, nie zgadywanie.
    assert candles[-1]["signal"] == "HOLD" and candles[-1]["signalSource"] == "trend_wallets"
    assert candles[-1]["signalTrend"] == "HOLD"
    assert candles[-1]["trendSma"] is None
    # Stan Hyperliquid nie zostal utworzony - bufor byl pusty, silnik nigdy
    # nie mial "nowego okna" do policzenia (patrz HyperliquidScoringEngine.run).
    assert st.load_hyperliquid_scoring_state() == {}


def test_mature_hyperliquid_buffer_blends_composite_and_can_flip_signal(tmp_path, monkeypatch):
    """Gdy bufor Hyperliquid ma wystarczajaco duzo sklasyfikowanych portfeli
    (>= `min_classified_wallets_for_maturity`, domyslnie 20), `composite_perp`
    przestaje byc `None` i realnie wplywa na zblendowany `composite`/`signal`
    - to jest sedno Fazy H2 ("od razu wpiete do glownego sygnalu LONG/SHORT",
    decyzja uzytkownika z briefu)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    # Bufor Hyperliquid: 25 portfeli "dobrych" konsekwentnie kupujacych tanio
    # (jako buyer) i sprzedajacych drogo (jako seller w kolejnej transakcji)
    # -> jednoznacznie GOOD, i w OSTATNIM oknie wszyscy sa net-BUY -> composite_perp
    # mocno DODATNI (bycze).
    hl_records = []
    ts = 1_000
    for i in range(25):
        wallet = f"hlgood{i}"
        for _ in range(3):
            hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(wallet, f"cp{ts}", 100.0, 20.0, ts)))
            ts += 1
            hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(f"cp{ts}", wallet, 150.0, 20.0, ts)))
            ts += 1
    # Ostatnie zdarzenie w buforze: kazdy z 25 portfeli jeszcze raz KUPUJE
    # (net-BUY w tym "oknie" - patrz HyperliquidScoringEngine.run, okno to
    # CALY bufor przy pierwszym uruchomieniu).
    for i in range(25):
        wallet = f"hlgood{i}"
        hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(wallet, f"cp{ts}", 100.0, 20.0, ts)))
        ts += 1

    st.save_hyperliquid_trades_buffer(hl_records)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert len(candles) > 0

    hl_state = st.load_hyperliquid_scoring_state()
    perp_snapshot = hl_state["last_perp_snapshot"]
    assert perp_snapshot["is_mature"] is True
    assert perp_snapshot["composite"] > 0  # jednoznacznie bycze
    # "sledzone portfele" liczy WSZYSTKIE adresy aktywne w buforze (jak
    # `tracked` dla spotu) - 25 hlgoodN + 175 roznych, jednorazowych
    # kontrahentow "cpNNNN" (2 na kazdy round-trip x3 + 1 na koncowy zakup,
    # x25 portfeli = 175), NIE tylko te faktycznie sklasyfikowane GOOD/BAD.
    assert perp_snapshot["tracked"] == 200

    last = candles[-1]
    assert last["compositePerp"] == perp_snapshot["composite"]
    assert last["compositePerp"] > 0
    # Faza H3 - pola diagnostyczne karty "ETH-PERP - Hyperliquid" wystawione
    # na tej samej swiecy, zgodne z tym co faktycznie policzyl silnik.
    assert last["perpTracked"] == 200
    assert last["perpIsMature"] is True
    assert last["perpGoodBuyers"] == 25
    assert last["perpGoodSellers"] == 0
    assert last["perpBadBuyers"] == 0 and last["perpBadSellers"] == 0
    # composite zblendowany (waga domyslna 50/50) musi lezec DOKLADNIE
    # posrodku miedzy spotem a perpem - weryfikacja formuly blendu na
    # prawdziwym przebiegu run_incremental.py, nie tylko na jednostce.
    expected = round(0.5 * last["compositeSpot"] + 0.5 * last["compositePerp"], 3)
    assert last["composite"] == expected
    # Perp jest mocno bycze - zblendowany composite powinien byc WYZSZY (albo
    # rowny w skrajnym przypadku) niz sam spot, nigdy odwrotnie.
    assert last["composite"] >= last["compositeSpot"]


def test_legacy_hyperliquid_scoring_state_schema_still_works(tmp_path, monkeypatch):
    """Zgodnosc wsteczna (Faza H3): `data/hyperliquid_scoring_state.json`
    zapisany JESZCZE PRZED ta faza ma zamiast `last_perp_snapshot` dwa
    starsze, plaskie klucze `last_composite_perp`/`last_is_mature` (dokladnie
    tak, jak realnie wyglada plik juz skomitowany do repo po Fazie H2, patrz
    hydrav2-automation.md). Ten test symuluje TAKI stan na dysku i sprawdza,
    ze pierwsze uruchomienie run_incremental.py PO wdrozeniu Fazy H3 (bez
    zadnych nowych transakcji Hyperliquid w buforze - listener jeszcze nie
    zdazyl odpalic sie ponownie) NIE traci ciaglosci composite_perp, mimo ze
    nie ma jeszcze nowego, pelnego `last_perp_snapshot`."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    legacy_state = {
        "good_short": 0.9,
        "good_long": 0.9,
        "bad_short": 0.5,
        "bad_long": 0.5,
        "last_processed_ts_ms": 999_999,
        "last_composite_perp": 0.42,
        "last_is_mature": True,
    }
    (tmp_path / "data" / "hyperliquid_scoring_state.json").write_text(
        json.dumps(legacy_state), encoding="utf-8"
    )
    # Bufor Hyperliquid pusty (albo wszystko juz "przetworzone" wzgledem
    # last_processed_ts_ms=999999) - hl_score bedzie None w tym uruchomieniu,
    # wiec kod MUSI sięgnąć po stary schemat, nie po `last_perp_snapshot`
    # (ktorego tu celowo nie ma).
    st.save_hyperliquid_trades_buffer([])

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert len(candles) > 0
    last = candles[-1]
    assert last["compositePerp"] == 0.42
    assert last["perpIsMature"] is True
    # Liczniki diagnostyczne, ktorych stary schemat NIE niosl - bezpieczne
    # zera zamiast bledu/braku klucza.
    assert last["perpGoodBuyers"] == 0
    assert last["perpTracked"] == 0


def test_eth_block_number_failure_after_retries_aborts_run_without_partial_writes(tmp_path, monkeypatch):
    """Faza "eth_blockNumber przez retry" (zgloszenie uzytkownika: automatyzacja
    "realnie nie trwa to do godziny... czesto odswieza po 2h") - znaleziony
    realny bug: `eth_blockNumber` byl jedynym NIEPONAWIANYM, pojedynczym
    `rpc.call()` w calym live-pipelinie (grep potwierdzil - wszystkie inne
    wywolania RPC ida przez `batch_call_with_retry`). Gdy Alchemy throttlowal
    akurat TEN apel (potwierdzone mailem uzytkownika: >10% zapytan
    rate-limited), caly krok GitHub Actions wywalal sie NIEZLAPANYM wyjatkiem
    PRZED jakimkolwiek zapisem/commitem - ten cykl byl calkowicie i cicho
    pomijany. Naprawa: ten sam apel teraz idzie przez `batch_call_with_retry`
    (jak reszta RPC), a gdy WYCZERPIE wszystkie proby (symulowane tutaj),
    main() musi jawnie zwrocic 1 (niepowodzenie joba - GitHub Actions NIE
    zacommituje niczego) i NIE zapisac zadnego czesciowego/niespojnego
    stanu - zamiast poprzedniego, nieprzewidywalnego zachowania (surowy
    wyjatek gdziekolwiek w trakcie)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    real_batch_call_with_retry = ri.batch_call_with_retry

    def flaky_batch_call_with_retry(rpc, calls, **kwargs):
        if len(calls) == 1 and calls[0][0] == "eth_blockNumber":
            return [None]  # symuluje wyczerpanie wszystkich ponowien (Alchemy throttluje uporczywie)
        return real_batch_call_with_retry(rpc, calls, **kwargs)

    monkeypatch.setattr(ri, "batch_call_with_retry", flaky_batch_call_with_retry)

    assert ri.main() == 1
    # Awaria nastapila PRZED jakimkolwiek zapisem - zaden plik stanu/historii
    # nie powinien powstac (pusty stan = "plik nie istnieje", patrz live/state.py).
    assert st.load_scoring_state() == {}
    assert st.load_candles_history() == []


def test_eth_block_number_recovers_after_one_transient_failure(tmp_path, monkeypatch):
    """Symetryczny do testu wyzej - JEDNORAZOWY, przejsciowy blad na poziomie
    transportu HTTP (dokladnie jak realny, pojedynczy 429 od Alchemy, nie
    trwala awaria) MUSI zostac wchloniety przez juz istniejacy, jednostkowo
    przetestowany retry w `batch_call_with_retry` (patrz test_onchain_rpc.py)
    i NIE powinien przerywac calego uruchomienia. Uzywa PRAWDZIWEJ funkcji
    retry (nie mocka) - to test na to, ze naprawa faktycznie dziala w
    typowym, najczestszym przypadku (pojedynczy przejsciowy blad), nie tylko
    ze main() poprawnie obsluguje calkowita, trwala porazke (test wyzej)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    attempts = {"n": 0}
    real_transport = chain.transport

    def flaky_transport(url, payload):
        if isinstance(payload, list) and len(payload) == 1 and payload[0]["method"] == "eth_blockNumber":
            attempts["n"] += 1
            if attempts["n"] == 1:
                # Pierwsza proba: symulacja pojedynczego 429/bledu JSON-RPC -
                # dokladnie ten scenariusz, ktory `batch_call_with_retry`
                # jest zaprojektowany wchlonac (patrz jego docstring).
                return [{"id": payload[0]["id"], "error": {"code": -32005, "message": "rate limited"}}]
        return real_transport(url, payload)

    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=flaky_transport)
    )

    assert ri.main() == 0
    assert attempts["n"] >= 2  # potwierdzenie, ze retry FAKTYCZNIE sie wydarzyl
    assert st.load_scoring_state()["last_processed_block"] == chain.head


def test_freshness_meta_last_run_utc_is_embedded_in_generated_site(tmp_path, monkeypatch):
    """Faza "wiarygodna świeżość" (zgłoszenie użytkownika: chip pokazywał
    zwodniczo świeży czas zaraz po realnej wielogodzinnej przerwie) -
    `site/index.html` wygenerowany przez pełny bieg `ri.main()` musi
    zawierać `DATA.meta.lastRunUtc` DOKŁADNIE równy `updated_at_utc`
    zapisanemu w `scoring_state.json` w TYM samym uruchomieniu - front-end
    liczy świeżość z tego zegara ściany, nie z timestampu bloku."""
    import json as json_module

    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    state = st.load_scoring_state()
    assert "updated_at_utc" in state

    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start_idx = html.index(marker) + len(marker)
    end_idx = html.index(";", start_idx)
    data = json_module.loads(html[start_idx:end_idx])
    assert data["meta"]["lastRunUtc"] == state["updated_at_utc"]


def test_stats_reset_from_block_meta_is_embedded_in_generated_site(tmp_path, monkeypatch):
    """Faza "reset licznika wyniku/win rate" (zgloszenie uzytkownika
    2026-09-06): gdy `data/stats_reset_state.json` ma ustawiony
    `reset_from_block`, `ri.main()` musi przekazac go do `build_site()`
    tak, ze ladu je do `DATA.meta.statsResetFromBlock` w wygenerowanym
    `site/index.html` - front-end (`renderAggregateStats()`) filtruje
    streaki po tej wartosci."""
    import json as json_module

    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    st.save_stats_reset_state({"reset_from_block": 25878499})

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start_idx = html.index(marker) + len(marker)
    end_idx = html.index(";", start_idx)
    data = json_module.loads(html[start_idx:end_idx])
    assert data["meta"]["statsResetFromBlock"] == 25878499


def test_stats_reset_from_block_is_none_without_reset_state_file(tmp_path, monkeypatch):
    """Bez pliku `data/stats_reset_state.json` `statsResetFromBlock` == None:
    podsumowanie "Historii sygnalow" liczy od poczatku historii Hydry. (Przez
    kilka godzin 14.11.2026 liczylo od przelaczenia zrodla sygnalu - wycofane
    decyzja Jakuba: "licz od poczatku istnienia hydry. ZAWSZE - wszystko".)
    `signalSwitchBlock` zostaje jako informacja: pierwsza swieca z obecnym
    zrodlem glownego sygnalu."""
    import json as json_module

    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start_idx = html.index(marker) + len(marker)
    end_idx = html.index(";", start_idx)
    data = json_module.loads(html[start_idx:end_idx])
    assert data["meta"]["statsResetFromBlock"] is None
    assert data["meta"]["signalSwitchBlock"] == st.load_candles_history()[0]["block"]


def test_signal_threshold_is_exposed_on_every_candle(tmp_path, monkeypatch):
    """Faza "NEUTRAL dead-zone" (kontynuowana przez Faze "sygnal z histereza"):
    front-end (template.html) musi stosowac DOKLADNIE ten sam prog co
    `SignalEngine` przy kolorowaniu rozbicia spot/perp w hero i przy pill
    BYCZY/NEUTRALNY/NIEDZWIEDZI w karcie ETH-PERP - zamiast duplikowac
    wartosc na twardo w JS, kazda swieca dostaje pole `signalThreshold`
    wprost z `signal_engine.cfg.enter_threshold` (ten sam wzorzec co juz
    istniejace `perpMaturityThreshold`)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert len(candles) > 0
    for c in candles:
        # Domyslny SignalConfig().enter_threshold (patrz scoring.py) - 0.35,
        # wartosc startowa wybrana empirycznie na zywej historii (225 swiec),
        # nie wynik backtestu (patrz komentarz przy polu w SignalConfig).
        assert c["signalThreshold"] == 0.35


# =====================================================================
# Faza "dzienny bilans GOOD/BAD" (2026-09-17, zgloszenie uzytkownika: "czy
# moglibysmy zsumowac buy/sell dla good i bad portfeli na dzien? [...]
# wolumen w USD obok liczby transakcji tez chce")
# =====================================================================


def test_daily_balance_usd_fields_exposed_on_mainnet_candle(tmp_path, monkeypatch):
    """`goodBuyUsd`/`goodSellUsd`/`badBuyUsd`/`badSellUsd` musza pojawic sie
    na kazdej swiecy jako PRAWDZIWA suma dolarowa policzona z realnych
    transakcji w tym uruchomieniu (ScoringEngine.run(), patrz
    hydra_signals/scoring.py) - nie zmyslona/placeholderowa wartosc. Ten sam
    fixture (`_seed_wallets`) co test_signal_threshold_is_exposed_on_every_
    candle wyzej, o ktorym juz wiadomo (patrz
    test_long_term_track_diverges_from_7d_when_given_shorter_lookback), ze
    daje `goodBuyers > 0` na ostatniej swiecy - jesli sa net-kupujacy
    portfele GOOD, ich realny wolumen w dolarach MUSI byc dodatni (transakcje
    maja dodatnia cene i wielkosc)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert len(candles) > 0
    last = candles[-1]

    for key in ("goodBuyUsd", "goodSellUsd", "badBuyUsd", "badSellUsd"):
        assert key in last
        assert isinstance(last[key], float)
        assert last[key] >= 0.0

    assert last["goodBuyers"] > 0
    # Portfele GOOD faktycznie net-kupuja w tym oknie -> realny wolumen w
    # dolarach musi byc dodatni, nie zero (to bylby znak, ze pole niesie
    # placeholder, nie prawdziwa sume notional).
    assert last["goodBuyUsd"] > 0.0


def test_daily_balance_usd_fields_default_to_zero_for_pre_phase_snapshots(tmp_path, monkeypatch):
    """Zgodnosc wsteczna: `last_base_snapshot`/`last_perp_snapshot` zapisane
    JESZCZE PRZED ta faza (peny H3-owy ksztalt, ale bez nowych kluczy
    `good_buy_usd`/itd.) nie moga wywalic uruchomienia KeyError-em, gdy w
    TYM konkretnym uruchomieniu nie ma zadnych nowych transakcji Base/
    Hyperliquid (wiec kod siega po snapshot z dysku zamiast liczyc swiezy) -
    ten sam scenariusz co juz istniejacy
    test_legacy_hyperliquid_scoring_state_schema_still_works, tylko o jeden
    poziom "nowszy" schemat (post-H3, nie post-H2)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.delenv("ALCHEMY_BASE_RPC_URL", raising=False)

    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    legacy_base_state = {
        "good_short": 0.9,
        "good_long": 0.9,
        "bad_short": 0.5,
        "bad_long": 0.5,
        "last_scored_window_end": 999_999,
        "last_base_snapshot": {
            "composite": 0.42,
            "is_mature": True,
            "tracked": 5,
            "active": 0,
            "classified": 25,
            "good_buyers": 3,
            "good_sellers": 1,
            "bad_buyers": 0,
            "bad_sellers": 2,
            "window_time": "17.09.2026, 10:00",
            "good_buy_weight": 12.0,
            "good_sell_weight": 4.0,
            "bad_buy_weight": 0.0,
            "bad_sell_weight": 8.0,
            # celowo BRAK good_buy_usd/good_sell_usd/bad_buy_usd/bad_sell_usd
        },
    }
    (tmp_path / "data" / "base_scoring_state.json").write_text(
        json.dumps(legacy_base_state), encoding="utf-8"
    )
    legacy_perp_state = {
        "good_short": 0.9,
        "good_long": 0.9,
        "bad_short": 0.5,
        "bad_long": 0.5,
        "last_processed_ts_ms": 999_999,
        "last_perp_snapshot": {
            "composite": 0.24,
            "is_mature": True,
            "tracked": 30,
            "active": 0,
            "classified": 22,
            "good_buyers": 2,
            "good_sellers": 0,
            "bad_buyers": 0,
            "bad_sellers": 1,
            "window_time": "17.09.2026, 10:00",
            # celowo BRAK good_buy_usd/good_sell_usd/bad_buy_usd/bad_sell_usd
        },
    }
    (tmp_path / "data" / "hyperliquid_scoring_state.json").write_text(
        json.dumps(legacy_perp_state), encoding="utf-8"
    )
    st.save_hyperliquid_trades_buffer([])

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0  # nie KeyError - graceful degradation
    candles = st.load_candles_history()
    assert len(candles) > 0
    last = candles[-1]
    assert last["baseGoodBuyUsd"] == 0.0
    assert last["baseGoodSellUsd"] == 0.0
    assert last["baseBadBuyUsd"] == 0.0
    assert last["baseBadSellUsd"] == 0.0
    assert last["perpGoodBuyUsd"] == 0.0
    assert last["perpGoodSellUsd"] == 0.0
    assert last["perpBadBuyUsd"] == 0.0
    assert last["perpBadSellUsd"] == 0.0
    # Reszta starego snapshotu (bez zwiazku z ta faza) dalej dziala normalnie.
    assert last["baseIsMature"] is True
    assert last["perpIsMature"] is True


# =====================================================================
# Faza "Base L2, etap B0: zbieranie danych" (2026-09-02)
# =====================================================================


def test_base_l2_skipped_gracefully_when_secret_missing(tmp_path, monkeypatch):
    """Brak ALCHEMY_BASE_RPC_URL (dokladnie stan repo PRZED tym, jak
    uzytkownik doda nowy sekret recznie) musi byc calkowicie bezpieczny -
    reszta pipeline'u (mainnet + Hyperliquid) dziala IDENTYCZNIE jak przed
    ta faza, bufor Base po prostu nie powstaje."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.delenv("ALCHEMY_BASE_RPC_URL", raising=False)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    assert st.load_base_trade_buffer() == []
    assert st.load_base_collector_state() == {}
    # Reszta pipeline'u nie zauwaza roznicy.
    assert len(st.load_candles_history()) > 0


def test_base_l2_collects_into_separate_buffer_and_stays_immature_with_few_wallets(
    tmp_path, monkeypatch
):
    """Gdy ALCHEMY_BASE_RPC_URL jest ustawiona, nowe transakcje z Base
    ladunja do WLASNEGO bufora/stanu (`base_trade_buffer.csv`,
    `base_collector_state.json`) - kompletnie osobno od mainnetowego
    `trade_buffer.csv`/`scoring_state.json`. Od Fazy "integracja Base
    B1-B3" (2026-09-11) diagnostyczne pola `compositeBase`/`base*` SA
    obecne na kazdej swiecy (w przeciwienstwie do Fazy B0 sprzed tej
    zmiany) - ale przy zaledwie 2 portfelach Base (znacznie ponizej
    `BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY`) `compositeBase` zostaje
    `None` (niedojrzale), a `compositeSpotCombined` jest IDENTYCZNE z
    `compositeSpot` (mainnet) - `blend_composite` gracefully degraduje,
    dokladnie jak dla `composite_perp` przy niedojrzalym Hyperliquid."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.setenv("HYDRA_BASE_BACKFILL_BLOCKS", "300")

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=500)

    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    base_chain.add_swap(100, "0xBASETX1", "baseWallet1", amount0=3_000_000_000, amount1=-(10**18))
    base_chain.add_swap(150, "0xBASETX2", "baseWallet2", amount0=-2_900_000_000, amount1=10**18)

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            return JsonRpcClient(url, transport=base_chain.transport)
        return JsonRpcClient(url, transport=mainnet_chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    candles_before = st.load_candles_history()
    assert ri.main() == 0

    base_buffer = st.load_base_trade_buffer()
    assert {t.wallet for t in base_buffer} == {"baseWallet1", "baseWallet2"}

    base_state = st.load_base_collector_state()
    assert base_state["last_processed_block"] == base_chain.head

    # Mainnet nie zauwazyl niczego z Base - bufory/kursory calkowicie osobne.
    mainnet_buffer = st.load_trade_buffer()
    assert all(w not in {"baseWallet1", "baseWallet2"} for w in {t.wallet for t in mainnet_buffer})
    assert st.load_scoring_state()["last_processed_block"] == mainnet_chain.head

    # Pola base* SA obecne (Faza "integracja Base B1-B3"), ale nie zmieniaja
    # finalnego sygnalu/composite dopoki populacja Base jest zbyt mala -
    # tylko 2 portfele w tym tescie, wiec composite_base=None (niedojrzale)
    # i compositeSpotCombined == compositeSpot (blend_composite degraduje do
    # samego mainnetu, identycznie jak przy niedojrzalym composite_perp).
    candles_after = st.load_candles_history()
    assert len(candles_after) > len(candles_before)  # normalny przyrost z mainnetu
    for c in candles_after:
        assert c["compositeBase"] is None
        assert c["baseIsMature"] is False
        assert c["compositeSpotCombined"] == c["compositeSpot"]
        assert "baseTracked" in c


def test_base_l2_failure_is_isolated_and_does_not_abort_main_run(tmp_path, monkeypatch):
    """Faza B0 jest swiadomie izolowana (try/except w run_incremental.py) -
    blad specyficzny dla Base (throttling, RPC padniety, cokolwiek) NIE MOZE
    wywrocic calego uruchomienia. Mainnet + Hyperliquid musza dokonczyc
    normalnie, main() musi zwrocic 0, a bufor Base po prostu zostaje
    nietkniety (nic nie zapisane w tym uruchomieniu)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=500)

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            raise ConnectionError("symulowany, trwaly blad sieciowy specyficzny dla Base")
        return JsonRpcClient(url, transport=mainnet_chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    assert ri.main() == 0
    assert st.load_base_trade_buffer() == []
    assert st.load_base_collector_state() == {}
    # Mainnet zakonczyl sie normalnie mimo bledu Base.
    assert st.load_scoring_state()["last_processed_block"] == mainnet_chain.head
    assert len(st.load_candles_history()) > 0


def test_base_l2_runs_after_mainnet_critical_path_is_saved(tmp_path, monkeypatch):
    """Regresja na REALNY incydent (2026-09-02, pierwszy dzien po wlaczeniu
    ALCHEMY_BASE_RPC_URL): blok Base pierwotnie stal PRZED krytycznym
    `eth_blockNumber` mainnetu. Log z prawdziwego uruchomienia pokazal:
    292/500 zakresow eth_getLogs Base nie powiodlo sie mimo ponowien,
    WSZYSTKIE 1141 wywolan eth_getTransactionByHash tez nie - throttling byl
    na tyle silny (wspolny budzet RPC Alchemy dla calego konta), ze zepsul
    TAKZE nastepujacy po nim `eth_blockNumber` mainnetu -> CALE uruchomienie
    przerwane (return 1), zero commitu, w tym dla Hydra/Hyperliquid, ktore
    normalnie dzialaja bezawaryjnie.

    Naprawa: blok Base przeniesiony na sam koniec main(), PO zapisaniu stanu
    mainnetu i wygenerowaniu strony. Ten test zlicza kolejnosc wywolan RPC
    do obu klientow (mainnet/Base) i potwierdza: (1) pierwsze wywolanie RPC
    w ogole idzie do mainnetu, nigdy do Base; (2) stan mainnetu jest juz
    zapisany na dysku ZANIM jakiekolwiek wywolanie RPC do Base w ogole sie
    zaczyna - wiec nawet catastrofalny throttling Base nie ma JUZ jak
    zaszkodzic czemukolwiek krytycznemu."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=500)
    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    base_chain.add_swap(100, "0xBASETX1", "baseWallet1", amount0=3_000_000_000, amount1=-(10**18))

    call_order: list[str] = []
    mainnet_state_saved_before_first_base_call = {"value": None}

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            def base_transport(u, payload):
                if mainnet_state_saved_before_first_base_call["value"] is None:
                    mainnet_state_saved_before_first_base_call["value"] = bool(
                        st.load_scoring_state()
                    )
                call_order.append("base")
                return base_chain.transport(u, payload)

            return JsonRpcClient(url, transport=base_transport)

        def mainnet_transport(u, payload):
            call_order.append("mainnet")
            return mainnet_chain.transport(u, payload)

        return JsonRpcClient(url, transport=mainnet_transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    assert ri.main() == 0
    assert call_order[0] == "mainnet"
    assert "base" in call_order
    assert call_order.index("base") > call_order.index("mainnet")
    # W momencie PIERWSZEGO wywolania RPC do Base, stan mainnetu byl juz
    # zapisany na dysku (nie pusty) - dokladnie ta gwarancja, ktorej
    # zabraklo w realnym incydencie.
    assert mainnet_state_saved_before_first_base_call["value"] is True
    assert st.load_scoring_state()["last_processed_block"] == mainnet_chain.head


def _seed_base_wallets_for_maturity(chain: FakeChain) -> None:
    """Generuje 80 odrebnych portfeli Base (40 systematycznie zyskownych,
    40 systematycznie stratnych), po 3 pelne round-tripy kupno/sprzedaz
    kazdy (6 transakcji/portfel >= `min_trades_for_classification=5`) -
    wystarczajaco duza i wyraznie rozdzielona (rank-based) populacja, zeby
    przy domyslnym `good_pct=bad_pct=0.15` przekroczyc
    `BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY=20`: 0.15*80=12 GOOD + 12 BAD
    = 24 sklasyfikowanych >= 20. Wszystko miesci sie w PIERWSZYM oknie Base
    (blocks 0..1799, `BASE_WINDOW_BLOCKS=1800`) - ostatnia transakcja jest
    celowo na bloku 1799, zeby ustawic `chain.head` na koncu tego okna
    (patrz komentarz w kodzie run_incremental.py: `base_last_closed_end =
    ((base_to_block+1)//BASE_WINDOW_BLOCKS)*BASE_WINDOW_BLOCKS - 1`)."""
    tx = [0]

    def next_hash() -> str:
        tx[0] += 1
        return f"0xBASETXM{tx[0]:06d}"

    block = 10
    for i in range(40):
        wallet = f"basegood{i}"
        for _ in range(3):
            # kupuje 1 WETH za 2000 USDC...
            chain.add_swap(block, next_hash(), wallet, amount0=2_000_000_000, amount1=-(10**18))
            block += 1
            # ...i sprzedaje za 2200 USDC - systematyczny zysk -> GOOD.
            chain.add_swap(block, next_hash(), wallet, amount0=-2_200_000_000, amount1=10**18)
            block += 1
    for j in range(40):
        wallet = f"basebad{j}"
        for _ in range(3):
            chain.add_swap(block, next_hash(), wallet, amount0=2_000_000_000, amount1=-(10**18))
            block += 1
            # sprzedaje za 1800 USDC - systematyczna strata -> BAD.
            chain.add_swap(block, next_hash(), wallet, amount0=-1_800_000_000, amount1=10**18)
            block += 1
    assert block < 1799, "seedowanie nie miesci sie juz w pierwszym oknie Base"
    # domyka pierwsze okno Base (blocks 0..1799) - patrz docstring wyzej.
    chain.add_swap(1799, next_hash(), "basegood0", amount0=2_000_000_000, amount1=-(10**18))


def test_base_maturity_gate_lags_one_run_then_blends_into_spot(tmp_path, monkeypatch):
    """Test na CALY sens integracji Base B1-B3: dojrzalosc (`composite_base`
    niedojrzale wobec `BASE_MIN_CLASSIFIED_WALLETS_FOR_MATURITY`) NIE moze
    wplynac na sygnal w TYM SAMYM uruchomieniu, w ktorym zostala policzona -
    bo blok RPC Base musi zostac na samym koncu `main()` (patrz test
    `test_base_l2_runs_after_mainnet_critical_path_is_saved` wyzej), po
    stronie mainnetu/site juz zapisanej na dysk. Zamiast tego:

    - Uruchomienie 1: seeduje 80 portfeli Base (>= progu dojrzalosci) -
      Base scoring NA KONCU tego uruchomienia poprawnie liczy dojrzaly,
      niepusty `composite_base` i zapisuje go do `base_scoring_state.json`,
      ale swiece WYPRODUKOWANE W TYM uruchomieniu nadal pokazuja
      `compositeBase=None`/`baseIsMature=False` (bo `base_snapshot` do
      blendu zostal odczytany na POCZATKU tego samego uruchomienia, ZANIM
      Base w ogole policzylo cokolwiek nowego - dokladnie zamierzone ~1h
      opoznienie z komentarza w run_incremental.py).
    - Uruchomienie 2 (nowe bloki mainnetu, zeby powstala nowa swieca; Base
      nie ma juz nic nowego do przeliczenia): NOWE swiece powinny nareszcie
      pokazac dojrzaly, niepusty `compositeBase` i `compositeSpotCombined
      != compositeSpot` - dowod, ze blend faktycznie zaczyna dzialac,
      z dokladnie jednego uruchomienia opoznienia."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    # UWAGA: `BASE_BACKFILL_BLOCKS`/`BASE_MAX_NEW_BLOCKS_PER_RUN` (jak i
    # `BACKFILL_BLOCKS` powyzej) sa stalymi MODULOWYMI wyliczonymi z
    # os.environ JUZ PRZY IMPORCIE `run_incremental` - ktory dawno sie
    # dokonal (na gorze tego pliku testowego), zanim ten test w ogole
    # ustawil jakiekolwiek zmienne srodowiskowe. `monkeypatch.setenv` na nie
    # WIec juz nie dziala (cichy no-op) - trzeba podmienic same stale na
    # module przez `monkeypatch.setattr`. Domyslny limit 1000 blokow/
    # uruchomienie przycialby nasze seedowanie (0..1799) na dwa uruchomienia
    # Base - podnosimy go, zeby "Uruchomienie 1" ponizej faktycznie domknelo
    # caly pierwszy zakres Base w jednym kroku (test sprawdza konkretnie "1
    # uruchomienie opoznienia", nie kolejkowanie backfillu Base).
    monkeypatch.setattr(ri, "BASE_BACKFILL_BLOCKS", 2000)
    monkeypatch.setattr(ri, "BASE_MAX_NEW_BLOCKS_PER_RUN", 2000)

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=1000)

    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    _seed_base_wallets_for_maturity(base_chain)

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            return JsonRpcClient(url, transport=base_chain.transport)
        return JsonRpcClient(url, transport=mainnet_chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    # --- Uruchomienie 1 ---
    assert ri.main() == 0

    candles_after_1 = st.load_candles_history()
    assert len(candles_after_1) > 0
    for c in candles_after_1:
        assert c["compositeBase"] is None
        assert c["baseIsMature"] is False
        assert c["compositeSpotCombined"] == c["compositeSpot"]

    # Base scoring NA KONCU uruchomienia 1 policzylo i zapisalo dojrzaly
    # snapshot na dysku - gotowy do uzycia dopiero w NASTEPNYM uruchomieniu.
    base_state_after_1 = st.load_base_scoring_state()
    snapshot_after_1 = base_state_after_1["last_base_snapshot"]
    assert snapshot_after_1["is_mature"] is True
    assert snapshot_after_1["classified"] >= 20
    assert snapshot_after_1["composite"] is not None

    # --- Uruchomienie 2: nowe bloki mainnetu (nowa swieca), Base bez zmian
    #     (juz przeliczone w run 1 - nic nowego do domkniecia) ---
    _seed_wallets(mainnet_chain, start_block=1000, end_block=1500)

    assert ri.main() == 0

    candles_after_2 = st.load_candles_history()
    assert len(candles_after_2) > len(candles_after_1)
    # historia z uruchomienia 1 nie zostala nadpisana/utracona.
    assert candles_after_2[: len(candles_after_1)] == candles_after_1

    new_candles = candles_after_2[len(candles_after_1):]
    assert len(new_candles) > 0
    for c in new_candles:
        assert c["baseIsMature"] is True
        assert c["compositeBase"] is not None
        assert c["compositeBase"] == snapshot_after_1["composite"]
        # blend faktycznie zmienil wartosc uzywana w finalnym sygnale wzgledem
        # samego mainnetu - to jest sedno tej fazy integracji.
        assert c["compositeSpotCombined"] != c["compositeSpot"]

    # Base scoring stan pozostaje niezmieniony w run 2 (zaden nowy blok Base
    # do domkniecia) - ale nadal poprawnie odczytany/zblendowany.
    base_state_after_2 = st.load_base_scoring_state()
    assert base_state_after_2["last_base_snapshot"] == snapshot_after_1


def test_spot_pool_uses_pooled_counts_not_old_fixed_weight_blend(tmp_path, monkeypatch):
    """Test na CALY sens Fazy "wspolna pula SPOT" (2026-09-11, zgloszenie
    uzytkownika po zobaczeniu zywych danych: garstka sklasyfikowanych
    transakcji Base miala DOKLADNIE tyle samo wplywu na `compositeSpot
    Combined` co znacznie wieksza probka Uniswapa, bo stary mechanizm
    (`blend_composite` ze stala waga 50/50) nie znal pojecia "rozmiaru
    proby").

    Ten test uruchamia PRAWDZIWY pipeline (ri.main(), dwa uruchomienia -
    dokladnie jak `test_base_maturity_gate_lags_one_run_then_blends_into_
    spot` powyzej) i weryfikuje, ze `compositeSpotCombined` faktycznie
    produkowany przez pipeline RozNI Sie od tego, co dalby stary,
    porzucony mechanizm (`blend_composite(compositeSpot, compositeBase,
    0.5)`) - dowod, ze pooling faktycznie dziala na prawdziwych danych z
    calego pipeline'u, nie tylko w izolowanym tescie jednostkowym
    `SpotPoolEngine` (patrz test_scoring.py - TAM sprawdzana jest
    dokladna, ilosciowa teza "mniejsza proba = proporcjonalnie mniejszy
    wplyw", na recznie wyliczonych liczbach z prawdziwego zrzutu ekranu
    uzytkownika - ten test tutaj tylko potwierdza integracje, celowo bez
    zakladania z gory, KTORY kierunek/wielkosc rozniczy wyjdzie na
    danym, przypadkowym fixture'ze portfeli Base uzywanym tu tylko do
    ustawienia dojrzalosci)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    monkeypatch.setattr(ri, "BASE_BACKFILL_BLOCKS", 2000)
    monkeypatch.setattr(ri, "BASE_MAX_NEW_BLOCKS_PER_RUN", 2000)

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=1000)

    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    _seed_base_wallets_for_maturity(base_chain)

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            return JsonRpcClient(url, transport=base_chain.transport)
        return JsonRpcClient(url, transport=mainnet_chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    # --- Uruchomienie 1: Base dojrzewa (composite_spot_combined ==
    #     compositeSpot, jeszcze nie ma czym zblendowac - patrz test
    #     wyzej). ---
    assert ri.main() == 0

    # --- Uruchomienie 2: nowe bloki mainnetu, Base juz dojrzaly i wnosi
    #     swoj (mala) sklasyfikowana probke do wspolnej puli. ---
    _seed_wallets(mainnet_chain, start_block=1000, end_block=1500)
    assert ri.main() == 0

    candles = st.load_candles_history()
    mature_candles = [c for c in candles if c["baseIsMature"]]
    assert len(mature_candles) > 0
    for c in mature_candles:
        old_style_blend = blend_composite(c["compositeSpot"], c["compositeBase"], perp_weight=0.5)
        # Pooling (suma licznikow, jedno wspolne EMA) to inny mechanizm niz
        # stary, sztywny blend dwoch juz-gotowych composite - musi wiec dac
        # inna wartosc (dokladna, ilosciowa teza "proporcjonalnie mniejszy
        # wplyw mniejszej proby" jest sprawdzona precyzyjnie w
        # test_scoring.py::test_spot_pool_engine_small_venue_gets_
        # proportional_not_equal_influence, na kontrolowanych liczbach).
        assert c["compositeSpotCombined"] != old_style_blend


# =====================================================================
# Faza "wazenie wolumenem SPOT" (2026-09-11)
# =====================================================================


def test_spot_pool_state_persists_volume_weighted_ema_after_real_run(tmp_path, monkeypatch):
    """Test na wpiecie Fazy "wazenie wolumenem SPOT" w PRAWDZIWY pipeline
    (ri.main()), nie tylko w izolowanym tescie jednostkowym SpotPoolEngine
    (patrz test_scoring.py - TAM sprawdzone sa dokladne, ilosciowe tezy o
    stlumieniu/sufit/blendzie na kontrolowanych liczbach). Tutaj chodzi
    wylacznie o potwierdzenie, ze przewodowanie faktycznie dziala na
    koncu do konca: `data/spot_pool_state.json` po realnym uruchomieniu
    musi zawierac takze 4 NOWE klucze toru wazonego wolumenem (nie tylko
    stare 4 z Fazy "wspolna pula SPOT"), i musza byc rzeczywiscie
    wypelnione (nie None) - dowod, ze `run_incremental.py` faktycznie
    liczy i przekazuje `good_buy_weight`/itd. do `SpotPoolEngine.update()`,
    a nie tylko stare argumenty liczba-portfeli."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    state = st.load_spot_pool_state()
    assert set(state) == {
        "good_short",
        "good_long",
        "bad_short",
        "bad_long",
        "good_short_weighted",
        "good_long_weighted",
        "bad_short_weighted",
        "bad_long_weighted",
    }
    # Realny przebieg z aktywnymi, sklasyfikowanymi portfelami - tor wazony
    # faktycznie zebral dane (aktywowal sie), nie zostal "na zimno"/None jak
    # przy calkowitym braku aktywnosci (patrz test_scoring.py::
    # test_spot_pool_engine_omitted_weight_args_returns_count_path_unchanged).
    assert state["good_short_weighted"] is not None
    assert state["good_long_weighted"] is not None
    assert state["bad_short_weighted"] is not None
    assert state["bad_long_weighted"] is not None


def test_candle_exposes_spot_pool_composite_counts_and_weighted_fields(tmp_path, monkeypatch):
    """Faza "diagnostyka wazenia wolumenem w UI" (2026-09-12, zgloszenie
    uzytkownika "czy gdzies na stronie w UX bede widzial wagi?") -
    front-end (karta Wallets) pokazuje teraz dwie posrednie skladowe
    `SpotPoolEngine` osobno (patrz template.html/`walletsVolumeNote`), wiec
    kazda nowo policzona swieca musi je niesc jako pola
    `spotPoolCompositeCounts`/`spotPoolCompositeWeighted`. To jest test
    end-to-end (prawdziwy `ri.main()`), nie tylko jednostkowy odczyt
    atrybutow `SpotPoolEngine` (patrz test_scoring.py dla tamtej czesci)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    candles = st.load_candles_history()
    assert candles, "oczekiwano co najmniej jednej nowo policzonej swiecy"
    latest = candles[-1]
    assert "spotPoolCompositeCounts" in latest
    assert "spotPoolCompositeWeighted" in latest
    assert latest["spotPoolCompositeCounts"] is not None
    # Realny przebieg z aktywnymi, sklasyfikowanymi portfelami - tor wazony
    # faktycznie sie aktywowal (dokladnie ten sam dowod aktywacji co
    # `state["good_short_weighted"] is not None` w tescie wyzej), wiec pole
    # nie powinno zostac `None`.
    assert latest["spotPoolCompositeWeighted"] is not None
    # `compositeSpotCombined` (juz istniejace pole) to blend 50/50 tych
    # dwoch skladowych - proste sprawdzenie spojnosci, zeby nie rozjechaly
    # sie cicho w przyszlosci.
    expected_combined = 0.5 * latest["spotPoolCompositeCounts"] + 0.5 * latest["spotPoolCompositeWeighted"]
    assert abs(latest["compositeSpotCombined"] - expected_combined) < 1e-3


def test_candle_exposes_signal_spot_combined_only_matching_decide_signal(tmp_path, monkeypatch):
    """Faza "poprawka komunikatu o rozbieznosci" (2026-09-13, zgloszenie
    uzytkownika): baner "sam spot wskazywalby..." w karcie hero porownywal
    finalny sygnal z `signalSpotOnly` (WYLACZNIE mainnet Uniswap) - myslace,
    bo Base jest CZESCIA puli spot od Fazy "integracja Base B1-B3"/"wspolna
    pula SPOT", nie osobna przyczyna obok niego. Nowe pole
    `signalSpotCombinedOnly` ma reprezentowac "co powiedzialby sam
    zblendowany spot (Uniswap+Base), bez perp, bez histerezy" - czyli
    dokladnie `decide_signal(compositeSpotCombined, threshold=enter_threshold)`,
    UZYWAJACY TEGO SAMEGO progu wejscia co prawdziwy `SignalEngine`
    (`signalThreshold` w swiecy), nie starego, wycofanego `signal_threshold`
    (0.2) uzywanego przez `signalSpotOnly`.

    Test end-to-end (prawdziwy `ri.main()`) - sprawdza spojnosc pola z
    `decide_signal()` wywolanym bezposrednio na juz zapisanych
    `compositeSpotCombined`/`signalThreshold`, zeby nie rozjechaly sie cicho
    w przyszlosci (ten sam wzorzec co test_candle_exposes_spot_pool_
    composite_counts_and_weighted_fields wyzej dla blendu 50/50)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    candles = st.load_candles_history()
    assert candles, "oczekiwano co najmniej jednej nowo policzonej swiecy"
    latest = candles[-1]
    assert "signalSpotCombinedOnly" in latest

    expected = decide_signal(
        latest["compositeSpotCombined"], threshold=latest["signalThreshold"]
    ).value
    assert latest["signalSpotCombinedOnly"] == expected

    # Pole musi byc niezalezne od starego `signalSpotOnly` (mainnet-only,
    # stary prog 0.2) - to dwa rozne tory, jak `signalSpotOnly` vs `signal`
    # juz udokumentowane w tescie
    # test_without_hyperliquid_buffer_composite_equals_spot_and_signal_matches_it
    # powyzej. Nie zakladamy tu, ze musza sie roznic (moga bez problemu byc
    # rowne przy niedojrzalym/nieskonfigurowanym Base) - tylko ze
    # `signalSpotCombinedOnly` samo w sobie jest spojne z jego wlasnym,
    # udokumentowanym wzorem.
    assert "signalSpotOnly" in latest


def test_wallets_window_time_fields_gracefully_absent_without_base_or_hyperliquid(
    tmp_path, monkeypatch
):
    """Faza "znaczniki czasu w karcie Wallets" (2026-09-13, zgloszenie
    uzytkownika: "czy moglibysmy dodac... informacje z ktorej godziny sa
    te dane?"). Gdy Base w ogole nie jest skonfigurowany (brak
    ALCHEMY_BASE_RPC_URL) i Hyperliquid nie ma zadnego bufora - dokladnie
    stan juz sprawdzony przez
    test_without_hyperliquid_buffer_composite_equals_spot_and_signal_matches_it
    dla compositePerp/compositeBase - nowe pola `baseWindowTime`/
    `perpWindowTime` musza gracefully zdegradowac do `None` (front-end
    chowa etykiete), zamiast wywalic KeyError albo pokazac cos mylacego.
    Uniswap natomiast ZAWSZE ma wlasny znacznik czasu (`time`, dzielony z
    reszta swiecy) - to pole istnialo juz przed ta faza, wiec tu tylko
    potwierdzamy, ze wciaz jest sensowna, niepusta wartoscia."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert candles, "oczekiwano co najmniej jednej nowo policzonej swiecy"
    for c in candles:
        assert c["time"] not in (None, "?")
        assert c["baseWindowTime"] is None
        assert c["perpWindowTime"] is None


def test_wallets_window_time_fields_reflect_real_per_source_block_timestamps(
    tmp_path, monkeypatch
):
    """Sedno tej fazy: Uniswap/Base/Hyperliquid legalnie pochodza z ROZNYCH
    momentow (Base ma udokumentowany ~1h lag - patrz
    test_base_maturity_gate_lags_one_run_then_blends_into_spot wyzej;
    Hyperliquid zalezy od tego, kiedy listener ostatnio skonczyl sluchac).
    Ten test dowodzi, ze front-end faktycznie dostaje TRZY niezalezne,
    poprawne znaczniki czasu - nie jeden wspolny, i nie przypadkowo takie
    same:

    - `perpWindowTime`: Hyperliquid jest "swiezy" w TYM SAMYM uruchomieniu,
      w ktorym dostarczono nowe transakcje (w przeciwienstwie do Base) -
      wiec juz w Uruchomieniu 1 musi byc obecny i zgodny z
      `fmt_warsaw(hl_window_end_ts_ms / 1000)`.
    - `baseWindowTime`: DOKLADNIE jak `compositeBase`/`baseIsMature` w
      tescie maturity-gate wyzej - w Uruchomieniu 1 (gdy Base dopiero co
      policzyl swiezy snapshot NA KONCU tego samego uruchomienia) swiece
      Z TEGO uruchomienia nadal nie widza go (`None`, celowy ~1h lag).
      Dopiero swiece z Uruchomienia 2 pokazuja prawdziwy, sformatowany
      znacznik czasu bloku konczacego okno Base.
    - `time` (Uniswap): niezmieniony, wlasny znacznik mainnetu, rozny od
      obu powyzszych (rozne fikcyjne lancuchy/bloki w tym tescie)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    # Patrz komentarz w test_base_maturity_gate_lags_one_run_then_blends_into_spot
    # o tym, dlaczego stale modulowe trzeba podmienic przez monkeypatch.setattr,
    # nie setenv.
    monkeypatch.setattr(ri, "BASE_BACKFILL_BLOCKS", 2000)
    monkeypatch.setattr(ri, "BASE_MAX_NEW_BLOCKS_PER_RUN", 2000)

    hl_records = []
    ts = 1_000
    for i in range(25):
        wallet = f"hlgood{i}"
        for _ in range(3):
            hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(wallet, f"cp{ts}", 100.0, 20.0, ts)))
            ts += 1
            hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(f"cp{ts}", wallet, 150.0, 20.0, ts)))
            ts += 1
    for i in range(25):
        wallet = f"hlgood{i}"
        hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(wallet, f"cp{ts}", 100.0, 20.0, ts)))
        ts += 1
    st.save_hyperliquid_trades_buffer(hl_records)
    expected_hl_window_time = ri.fmt_warsaw(max(r["ts_ms"] for r in hl_records) / 1000)

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=1000)

    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    _seed_base_wallets_for_maturity(base_chain)

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            return JsonRpcClient(url, transport=base_chain.transport)
        return JsonRpcClient(url, transport=mainnet_chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    # --- Uruchomienie 1 ---
    assert ri.main() == 0
    candles_after_1 = st.load_candles_history()
    assert len(candles_after_1) > 0
    for c in candles_after_1:
        # Hyperliquid: swiezy w tym samym uruchomieniu -> juz obecny.
        assert c["perpWindowTime"] == expected_hl_window_time
        # Base: celowy ~1h lag -> jeszcze NIE widoczny w tym uruchomieniu.
        assert c["baseWindowTime"] is None
        assert c["time"] not in (None, "?")

    base_state_after_1 = st.load_base_scoring_state()
    base_window_end_block = base_state_after_1["last_scored_window_end"]
    expected_base_window_time = ri.fmt_warsaw(1_700_000_000 + base_window_end_block * 12)

    # --- Uruchomienie 2: nowe bloki mainnetu (nowa swieca), Base bez zmian ---
    _seed_wallets(mainnet_chain, start_block=1000, end_block=1500)
    assert ri.main() == 0

    candles_after_2 = st.load_candles_history()
    assert len(candles_after_2) > len(candles_after_1)
    new_candles = candles_after_2[len(candles_after_1):]
    assert len(new_candles) > 0
    for c in new_candles:
        # Teraz nareszcie widoczny - dokladnie ten sam znacznik czasu, ktory
        # faktycznie odpowiada blokowi konczacemu okno Base (nie jakis inny,
        # przypadkowo podobny format).
        assert c["baseWindowTime"] == expected_base_window_time
        # Hyperliquid bez nowych transakcji w tym uruchomieniu - znacznik
        # czasu z Uruchomienia 1 poprawnie przechodzi dalej (carry-forward
        # razem z reszta `last_perp_snapshot`), nie znika ani sie nie zeruje.
        assert c["perpWindowTime"] == expected_hl_window_time
        # Trzy zrodla, trzy rozne momenty - dowod, ze to NIE jeden wspolny
        # znacznik czasu przypadkiem sklejony w trzech polach.
        assert len({c["time"], c["baseWindowTime"], c["perpWindowTime"]}) == 3


# ---------------------------------------------------------------------------
# Faza "Long term (30d)" - drugi, rownolegly tor klasyfikacji/scoringu (patrz
# obszerne komentarze w run_incremental.py przy stalych LT_*/`_synthetic_lt_view`
# i w live/state.py przy *_LT_PATH). Testy ponizej NIE powtarzaja tego, co juz
# jest przetestowane gdzie indziej (np. ze `classification_lookback_blocks`
# faktycznie zmienia klasyfikacje - to jest sedno hydra_signals/scoring.py i
# ma tam wlasne testy), tylko sprawdzaja konkretnie WIRING w run_incremental.py:
# ze DRUGI silnik faktycznie dostaje INNY config i naprawde dziala niezaleznie
# (nie jest przypadkowym aliasem pierwszego), ze jego stan poprawnie sie
# zapisuje/wznawia, i ze przy braku danych Base/Hyperliquid tor Lt degraduje
# sie tak samo bezpiecznie jak tor 7d.
# ---------------------------------------------------------------------------


def test_long_term_track_diverges_from_7d_when_given_shorter_lookback(tmp_path, monkeypatch):
    """Dowod, ze `mainnet_lt_engine` w run_incremental.py NAPRAWDE dostaje
    WLASNY, ODREBNY `ScoringConfig` (nie jest przypadkowym aliasem/kopia
    silnika 7d) - podmieniamy `LT_CLASSIFICATION_LOOKBACK_BLOCKS` na
    DRASTYCZNIE krotsze okno (5 blokow) niz domyslne 7-dniowe
    (`ScoringConfig.classification_lookback_blocks` = 250*24*7 = 42000) i
    sprawdzamy, ze silnik Lt - majac DOKLADNIE te sama, bogata historie
    transakcji co silnik 7d (`_seed_wallets` generuje transakcje co 10
    blokow przez caly zakres) - efektywnie "glodujue" (za malo historii w
    tak krotkim oknie, zeby jakikolwiek portfel osiagnal
    `min_trades_for_classification=5`), podczas gdy tor 7d (pelna historia)
    normalnie klasyfikuje i pokazuje wyrazny sygnal LONG.

    To NIE jest test mechaniki `classification_lookback_blocks` samej w
    sobie (to juz jest przetestowane w hydra_signals) - to jest test, ze
    run_incremental.py FAKTYCZNIE przekazuje DWIE ROZNE wartosci do DWOCH
    NIEZALEZNYCH silnikow, zamiast np. przypadkiem uzyc tego samego `cfg`
    dla obu."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "3000")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    # Patrz komentarz w test_base_maturity_gate_lags_one_run_then_blends_into_spot
    # o tym, dlaczego stale modulowe trzeba podmienic przez monkeypatch.setattr,
    # nie setenv (moduł jest juz zaimportowany na gorze tego pliku testowego).
    monkeypatch.setattr(ri, "LT_CLASSIFICATION_LOOKBACK_BLOCKS", 5)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=3000)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert len(candles) > 1
    last = candles[-1]

    # Tor 7d: bogata, ciagla historia -> jednoznacznie sklasyfikowani
    # "good"/"bad", wyrazny sygnal.
    assert last["goodBuyers"] > 0
    assert last["compositeSpot"] > 0
    assert last["signalSpotOnly"] == "LONG"

    # Tor Lt (lookback=5 blokow): za malo historii, zeby ktokolwiek
    # osiagnal `min_trades_for_classification` -> zero sklasyfikowanych
    # aktywnych w oknie, neutralny (0.0) composite, HOLD - DOKLADNIE tak,
    # jak przy calkowitym braku Base/Hyperliquid (graceful degradation), ale
    # tu wywolane przez celowo za krotkie okno klasyfikacji, nie przez brak
    # danych.
    assert last["goodBuyersLt"] == 0
    assert last["badBuyersLt"] == 0
    assert last["compositeSpotLt"] == 0.0
    assert last["signalSpotOnlyLt"] == "HOLD"

    # Bezpieczenstwo: to NIE jest przypadkiem to samo pole odczytane dwa
    # razy - realnie ROZNE wartosci na TEJ SAMEJ swiecy.
    assert last["compositeSpot"] != last["compositeSpotLt"]
    assert last["indGoodShort"] != last["indGoodShortLt"] or last["goodBuyers"] != last["goodBuyersLt"]


def test_long_term_state_files_created_and_ema_persists_across_runs(tmp_path, monkeypatch):
    """Odpowiednik `test_two_consecutive_runs_resume_correctly` powyzej, ale
    dla wszystkich SZESCIU nowych plikow stanu Lt - musza powstac po
    pierwszym uruchomieniu i (dokladnie jak ich odpowiedniki 7d) NIE
    zresetowac EMA do "na zimno" (None) przy wznowieniu w drugim,
    niezaleznym procesie."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=1000)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    scoring_lt_1 = st.load_scoring_state_lt()
    regime_lt_1 = st.load_regime_state_lt()
    signal_lt_1 = st.load_signal_state_lt()
    spot_pool_lt_1 = st.load_spot_pool_state_lt()
    # Hyperliquid/Base Lt NIE tworza swoich plikow, dopoki nie dostana
    # jakichkolwiek danych (dokladnie jak ich odpowiedniki 7d,
    # `hyperliquid_scoring_state.json`/`base_scoring_state.json`) - w tym
    # tescie brak ALCHEMY_BASE_RPC_URL i pustego bufora Hyperliquid, wiec
    # oba NIE powinny istniec.
    assert st.load_hyperliquid_scoring_state_lt() == {}
    assert st.load_base_scoring_state_lt() == {}

    assert scoring_lt_1 != {}
    ema_lt_1 = {k: scoring_lt_1[k] for k in ("good_short", "good_long", "bad_short", "bad_long")}
    assert all(v is not None for v in ema_lt_1.values())
    assert regime_lt_1 != {}
    assert signal_lt_1 != {}
    assert spot_pool_lt_1 != {}

    _seed_wallets(chain, start_block=1000, end_block=1500)
    assert ri.main() == 0

    scoring_lt_2 = st.load_scoring_state_lt()
    ema_lt_2 = {k: scoring_lt_2[k] for k in ("good_short", "good_long", "bad_short", "bad_long")}
    # Wznowienie z dysku - EMA NIE wraca do stanu "na zimno".
    assert all(v is not None for v in ema_lt_2.values())

    candles = st.load_candles_history()
    assert len(candles) > 1
    for c in candles:
        assert "signalLt" in c and "compositeLt" in c
        assert "regimeLt" in c and "bullScoreLt" in c and "bearScoreLt" in c
        assert any(k.startswith("momentum_") and k.endswith("_lt") for k in c)


def test_long_term_fields_gracefully_absent_without_base_or_hyperliquid(tmp_path, monkeypatch):
    """Odpowiednik `test_without_hyperliquid_buffer_composite_equals_spot_and_signal_matches_it`
    powyzej, ale dla pol Lt - brak sekretu ALCHEMY_BASE_RPC_URL i pustego
    bufora Hyperliquid musi degradowac tor Lt DOKLADNIE tak samo bezpiecznie
    jak tor 7d (`compositePerpLt`/`compositeBaseLt` -> `None`,
    `perpIsMatureLt`/`baseIsMatureLt` -> `False`), zamiast np. rzucic
    wyjatkiem albo cicho podstawic 0 (co wygladaloby jak realna, neutralna
    wartosc, nie jak "brak danych")."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert len(candles) > 0
    for c in candles:
        assert c["compositePerpLt"] is None
        assert c["perpIsMatureLt"] is False
        assert c["compositeBaseLt"] is None
        assert c["baseIsMatureLt"] is False
        # Bez Base/Hyperliquid "spot combined" Lt musi sprowadzic sie do
        # samego mainnetu Lt - dokladnie ta sama gwarancja graceful
        # degradation co `compositeSpotCombined == compositeSpot` dla 7d.
        assert c["compositeSpotCombinedLt"] == c["compositeSpotLt"]
        assert c["compositeLt"] == c["compositeSpotCombinedLt"]


# =====================================================================
# Faza "manifesty per-run" (2026-09-15, w odpowiedzi na przeglad
# infrastruktury zaproponowany przez kolege uzytkownika) - patrz
# `live/state.py::save_run_manifest` i sekcja "Per-run manifests" w
# site/docs.html.
# =====================================================================


def test_run_manifest_is_written_after_successful_run(tmp_path, monkeypatch):
    """Kazde uruchomienie main() musi zostawic dokladnie jeden nowy plik w
    data/manifests/, ze wszystkimi polami wypelnionymi na podstawie tego,
    co realnie sie wydarzylo w tym uruchomieniu."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.setenv("GITHUB_RUN_ID", "999888777")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("GITHUB_SHA", "deadbeef")
    monkeypatch.delenv("ALCHEMY_BASE_RPC_URL", raising=False)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    assert len(st.list_run_manifests()) == 1

    manifest = st.load_latest_run_manifest()
    assert manifest["run_id"] == "999888777"
    assert manifest["trigger"] == "schedule"
    assert manifest["commit_sha"] == "deadbeef"
    assert manifest["started_at_utc"] is not None
    assert manifest["finished_at_utc"] is not None
    assert manifest["duration_s"] >= 0
    assert manifest["mainnet"]["enabled"] is True
    assert manifest["mainnet"]["new_trades"] > 0
    assert manifest["mainnet"]["capped"] is False
    # Bez sekretu Base i bez bufora Hyperliquid w tym tescie - oba tory
    # musza to odzwierciedlac jednoznacznie, nie milczeniem.
    assert manifest["base"] == {"enabled": False}
    assert manifest["hyperliquid"] == {"enabled": True, "new_trades": 0}
    assert manifest["config_snapshot"]["max_new_blocks_per_run"] == ri.MAX_NEW_BLOCKS_PER_RUN
    assert manifest["config_snapshot"]["base_max_new_blocks_per_run"] == ri.BASE_MAX_NEW_BLOCKS_PER_RUN
    assert manifest["warnings"] == []


def test_run_manifest_written_even_when_no_new_mainnet_blocks(tmp_path, monkeypatch):
    """Drugie uruchomienie bez nowych blokow jest bezpiecznym no-opem dla
    stanu (patrz `test_second_run_with_no_new_blocks_is_a_safe_noop` wyzej),
    ale MUSI mimo to zostawic wlasny manifest - to jest dokladnie ten
    przypadek, dla ktorego zapis manifestu jest wpiety w OBIE sciezki
    `return 0` w main(), nie tylko na samym koncu."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0
    assert len(st.list_run_manifests()) == 1

    assert ri.main() == 0
    assert len(st.list_run_manifests()) == 2

    latest = st.load_latest_run_manifest()
    assert latest["mainnet"] == {
        "enabled": True,
        "from_block": chain.head + 1,
        "to_block": chain.head,
        "new_trades": 0,
        "capped": False,
    }


def test_run_manifest_records_base_new_trades_when_enabled(tmp_path, monkeypatch):
    """Gdy Base jest wlaczone i realnie zbiera transakcje, manifest musi to
    pokazac osobno od mainnetu - dokladnie te liczby, ktore trafily do
    `base_trade_buffer.csv` w tym uruchomieniu (patrz test analogiczny,
    `test_base_l2_collects_into_separate_buffer_and_stays_immature_with_few_wallets`)."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.setenv("HYDRA_BASE_BACKFILL_BLOCKS", "300")

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=500)

    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    base_chain.add_swap(100, "0xBASETX1", "baseWallet1", amount0=3_000_000_000, amount1=-(10**18))
    base_chain.add_swap(150, "0xBASETX2", "baseWallet2", amount0=-2_900_000_000, amount1=10**18)

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            return JsonRpcClient(url, transport=base_chain.transport)
        return JsonRpcClient(url, transport=mainnet_chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    assert ri.main() == 0

    manifest = st.load_latest_run_manifest()
    assert manifest["base"]["enabled"] is True
    assert manifest["base"]["new_trades"] == 2
    assert manifest["base"]["capped"] is False


def test_run_manifest_records_base_error_when_base_fails(tmp_path, monkeypatch):
    """Blad specyficzny dla Base (izolowany przez try/except, patrz
    `test_base_l2_failure_is_isolated_and_does_not_abort_main_run` wyzej)
    musi trafic do manifestu jako `base.error` - inaczej jedyny slad tego,
    ze cos poszlo nie tak, to log GitHub Actions, ktory znika po 90 dniach."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=500)

    def fake_client(url):
        if url == "https://fake-base-rpc.invalid":
            raise ConnectionError("symulowany, trwaly blad sieciowy specyficzny dla Base")
        return JsonRpcClient(url, transport=mainnet_chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    assert ri.main() == 0

    manifest = st.load_latest_run_manifest()
    assert manifest["base"]["enabled"] is True
    assert "ConnectionError" in manifest["base"]["error"]
    # Mainnet i Hyperliquid dokonczyly normalnie mimo bledu Base - manifest
    # to potwierdza wprost, nie tylko kod zwrotny main().
    assert manifest["mainnet"]["new_trades"] > 0


def test_run_manifest_retention_prunes_oldest_files(tmp_path, monkeypatch):
    """`save_run_manifest` musi trzymac co najwyzej MAX_RETAINED_RUN_MANIFESTS
    najnowszych plikow - bez tego 24 uruchomienia/dobe pogleblialyby i tak
    juz odczuwalne rozdecie `.git` w nieskonczonosc (patrz uzasadnienie przy
    definicji stalej w live/state.py)."""
    monkeypatch.setattr(st, "RUN_MANIFESTS_DIR", tmp_path / "data" / "manifests")

    total = st.MAX_RETAINED_RUN_MANIFESTS + 5
    for i in range(total):
        started = datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(
            minutes=i
        )
        st.save_run_manifest({"run_id": str(i), "started_at_utc": started.isoformat()})

    remaining = st.list_run_manifests()
    assert len(remaining) == st.MAX_RETAINED_RUN_MANIFESTS

    # Nie tylko liczba plikow - sprawdzamy PO run_id zapisanym w kazdym
    # pliku, ze to konkretnie najstarsze zostaly usuniete, a najnowsze
    # przetrwaly.
    remaining_ids = {json.loads(p.read_text(encoding="utf-8"))["run_id"] for p in remaining}
    assert remaining_ids == {str(i) for i in range(5, total)}


# =====================================================================
# Faza "jezioro danych" (Etap B: R2 + Parquet, 2026-09-15) - integracja
# `live/lake.py` z `main()`: manifest musi dostac `lake_objects` DOKLADNIE
# wtedy, gdy archiwizacja realnie sie udala - patrz `tests/test_lake.py`
# dla testow samego `live/lake.py` w izolacji.
# =====================================================================


class _FakeR2Client:
    def __init__(self):
        self.put_calls: list[dict] = []

    def put_object(self, Bucket: str, Key: str, Body: bytes):  # noqa: N803
        self.put_calls.append({"Bucket": Bucket, "Key": Key, "Body": Body})


def test_run_manifest_lake_objects_empty_without_r2_secrets(tmp_path, monkeypatch):
    """Bez sekretow R2 (stan repo PRZED tym, jak uzytkownik je doda recznie)
    manifest musi jasno pokazywac `lake_objects: []`, nie milczec na temat
    tego, ze archiwizacja w ogole nie zostala sprobowana."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    manifest = st.load_latest_run_manifest()
    assert manifest["lake_objects"] == []


def test_run_manifest_lake_objects_records_uploads_when_r2_configured(tmp_path, monkeypatch):
    """Z podmienionym klientem R2 (patrz `_FakeR2Client` wyzej - symuluje
    komplet sekretow bez prawdziwej sieci), mainnetowe transakcje z tego
    uruchomienia musza trafic do "jeziora", a manifest musi to zapisac pod
    `lake_objects` - klucz partycji + liczba wierszy, dokladnie tyle, ile
    realnie zarchiwizowano."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.delenv("ALCHEMY_BASE_RPC_URL", raising=False)

    fake_r2 = _FakeR2Client()
    monkeypatch.setattr(lake, "_r2_client", lambda: fake_r2)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(
        ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport)
    )

    assert ri.main() == 0

    manifest = st.load_latest_run_manifest()
    assert len(manifest["lake_objects"]) == 1
    lake_entry = manifest["lake_objects"][0]
    assert lake_entry["source"] == "uniswap_mainnet"
    assert lake_entry["rows"] == manifest["mainnet"]["new_trades"]
    assert lake_entry["key"].startswith("trades/source=uniswap_mainnet/date=")
    assert lake_entry["key"].endswith(".parquet")

    # Realnie wyslane do "R2" (tu: fake klienta) - nie tylko zapisane w
    # manifescie bez odpowiadajacego wywolania.
    assert len(fake_r2.put_calls) == 1
    assert fake_r2.put_calls[0]["Key"] == lake_entry["key"]


# =====================================================================
# Faza "przeskok zaleglosci Base + naprawa dat swiec" (2026-10-06)
# =====================================================================


def _latest_manifest():
    files = sorted(st.RUN_MANIFESTS_DIR.glob("*.json"))
    return json.loads(files[-1].read_text(encoding="utf-8"))


def test_base_lag_beyond_threshold_fast_forwards_and_records_it(tmp_path, monkeypatch):
    """Regresja na realny incydent (2026-10-06): kolektor Base byl ~5 dni w
    tyle (kazde uruchomienie `capped`, zaleglosc rosla w nieskonczonosc).
    Gdy zaleglosc przekracza prog, kolektor przeskakuje do `czolo - KEEP`,
    gubi transakcje z luki, a przeskok jest widoczny w manifescie/warnings."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.setattr(ri, "BASE_MAX_LAG_BLOCKS", 10_000)
    monkeypatch.setattr(ri, "BASE_FASTFORWARD_KEEP_BLOCKS", 2_000)
    monkeypatch.setattr(ri, "BASE_MAX_NEW_BLOCKS_PER_RUN", 50_000)
    monkeypatch.setattr(ri, "TS_FETCH_BASE_DELAY", 0.0)

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=500)
    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    base_chain.add_swap(500, "0xGAP", "gapWallet", amount0=3_000_000_000, amount1=-(10**18))
    base_chain.add_swap(29_500, "0xKEPT", "keptWallet", amount0=3_000_000_000, amount1=-(10**18))
    base_chain.head = 30_000
    st.save_base_collector_state({"last_processed_block": 100})  # zaleglosc 29 900 bl.

    def fake_client(url):
        chain = base_chain if url == "https://fake-base-rpc.invalid" else mainnet_chain
        return JsonRpcClient(url, transport=chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)

    assert ri.main() == 0
    assert st.load_base_collector_state()["last_processed_block"] == 30_000
    assert {t.wallet for t in st.load_base_trade_buffer()} == {"keptWallet"}  # luka pominieta
    m = _latest_manifest()
    ff = m["base"]["fast_forward"]
    assert ff["from_block"] == 101 and ff["to_block"] == 30_000 - 2_000
    assert ff["skipped_blocks"] == 30_000 - 2_000 - 101 + 1
    assert any("przeskok zaleglosci" in w for w in m["warnings"])


def test_base_lag_below_threshold_does_not_fast_forward(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("ALCHEMY_BASE_RPC_URL", "https://fake-base-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.setattr(ri, "BASE_MAX_LAG_BLOCKS", 10_000)
    monkeypatch.setattr(ri, "BASE_MAX_NEW_BLOCKS_PER_RUN", 50_000)

    mainnet_chain = FakeChain()
    _seed_wallets(mainnet_chain, start_block=0, end_block=500)
    base_chain = FakeChain(pool_address=BASE_UNISWAP_V3_WETH_USDC_005.address)
    base_chain.add_swap(150, "0xA", "baseWallet1", amount0=3_000_000_000, amount1=-(10**18))
    base_chain.head = 300
    st.save_base_collector_state({"last_processed_block": 100})

    def fake_client(url):
        chain = base_chain if url == "https://fake-base-rpc.invalid" else mainnet_chain
        return JsonRpcClient(url, transport=chain.transport)

    monkeypatch.setattr(ri, "JsonRpcClient", fake_client)
    assert ri.main() == 0
    m = _latest_manifest()
    assert "fast_forward" not in m["base"]
    assert m["base"]["from_block"] == 101


def test_missing_candle_timestamps_are_repaired_on_next_run(tmp_path, monkeypatch):
    """Swiece z `ts: null` (RPC padl przy pierwszym podejsciu) dostaja
    prawdziwy `ts`/`time` w kolejnym uruchomieniu, a `time` nigdy nie
    zostaje gołym "?" jesli jest z czego szacowac."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    monkeypatch.setattr(ri, "TS_FETCH_BASE_DELAY", 0.0)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=1000)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport))
    assert ri.main() == 0

    candles = st.load_candles_history()
    assert len(candles) >= 3
    real_ts = {c["block"]: c["ts"] for c in candles}
    assert all(v is not None for v in real_ts.values())
    victim_a, victim_b = candles[0], candles[-1]
    for c in (victim_a, victim_b):
        c["ts"], c["time"] = None, "?"
    st.save_candles_history(candles)

    _seed_wallets(chain, start_block=1000, end_block=1500)
    assert ri.main() == 0

    after = {c["block"]: c for c in st.load_candles_history()}
    for victim in (victim_a, victim_b):
        fixed = after[victim["block"]]
        assert fixed["ts"] == real_ts[victim["block"]]
        assert fixed["time"] == ri.fmt_warsaw(real_ts[victim["block"]])
    assert _latest_manifest()["ts_repair"]["fetched"] == 2


def test_unfetchable_new_candle_timestamp_gets_estimated_label_not_question_mark(tmp_path, monkeypatch):
    """Gdy `eth_getBlockByNumber` nie zwraca danych nawet po ponowieniach,
    swieca ma `ts: None` (to POMIAR - nie udajemy go), ale `time` to szacunek
    z numeru bloku z prefiksem "~", dzieki czemu karta Wallets/historia
    sygnalow maja date."""
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    monkeypatch.setattr(ri, "TS_FETCH_BASE_DELAY", 0.0)
    monkeypatch.setattr(ri, "TS_FETCH_MAX_RETRIES", 1)

    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=1000)
    client_ok = JsonRpcClient("https://fake-rpc.invalid", transport=chain.transport)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: client_ok)
    assert ri.main() == 0
    n_before = len(st.load_candles_history())

    # Od teraz KAZDE pobranie znacznika czasu bloku zwraca null (przeciazony RPC).
    original = chain.transport

    def flaky(url, payload):
        out = original(url, payload)
        for item, req in zip(out, payload):
            if req["method"] == "eth_getBlockByNumber":
                item["result"] = None
        return out

    flaky_client = JsonRpcClient("https://fake-rpc.invalid", transport=flaky)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: flaky_client)
    _seed_wallets(chain, start_block=1000, end_block=1500)
    assert ri.main() == 0

    new_candles = st.load_candles_history()[n_before:]
    assert new_candles
    for c in new_candles:
        assert c["ts"] is None
        assert c["time"].startswith("~"), c["time"]


# ---------------------------------------------------------------------------
# Faza "funding + open interest" (Hyperliquid ETH-PERP) - krok ZAPISUJACY dane
# (live/derivs.py), nie wplywajacy na sygnal.
# ---------------------------------------------------------------------------


def _fake_derivs_sources(monkeypatch, *, funding_fail=False, ctx_fail=False):
    """Podmienia siec Hyperliquid na deterministyczne dane i znosi przycinanie
    retencji (fikcyjny lancuch ma znaczniki czasu z 2023, a "teraz" w teście to
    prawdziwy zegar - bez tego retencja wycinalaby wszystko)."""
    from hydra_signals.data_sources import hyperliquid_derivs as hd
    from live import derivs as dv

    monkeypatch.delenv("HYDRA_DERIVS_DISABLED", raising=False)
    monkeypatch.setattr(dv, "FUNDING_RETENTION_DAYS", 100_000)
    monkeypatch.setattr(dv, "SNAPSHOT_RETENTION_DAYS", 100_000)

    def fake_funding(start_ms, *a, **k):
        if funding_fail:
            raise OSError("hyperliquid down")
        base = 1_700_000_000_000 - 48 * 3_600_000
        return [{"t": base + i * 3_600_000, "rate": 0.0001, "premium": 0.0002} for i in range(24 * 14)]

    def fake_ctx(*a, **k):
        if ctx_fail:
            return None
        return {"funding": 0.0001, "openInterest": 500.0, "markPx": 2000.0, "oraclePx": 2000.0, "premium": 0.0, "dayNtlVlm": 1.0}

    monkeypatch.setattr(hd, "fetch_funding_history", fake_funding)
    monkeypatch.setattr(hd, "fetch_asset_ctx", fake_ctx)


def test_derivs_fields_are_recorded_on_candles_and_do_not_touch_the_signal(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=1000)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport))

    # Uruchomienie 1: derivs WYLACZONE (stan "sprzed wdrozenia") - swiece bez danych
    assert ri.main() == 0
    before = st.load_candles_history()
    assert before and all(c["fundingRate"] is None and c["openInterest"] is None for c in before)
    assert not (tmp_path / "data" / "derivs_history.json").exists() or st.load_derivs_history()["funding"] == []

    # Uruchomienie 2: derivs WLACZONE, doszlo troche nowych blokow
    _fake_derivs_sources(monkeypatch)
    _seed_wallets(chain, start_block=1000, end_block=1500)
    assert ri.main() == 0
    after = st.load_candles_history()
    assert len(after) > len(before)

    hist = st.load_derivs_history()
    assert len(hist["funding"]) == 24 * 14 and len(hist["snapshots"]) == 1
    manifest = st.load_latest_run_manifest()
    assert manifest["derivs"]["enabled"] is True
    assert manifest["derivs"]["funding_new"] == 24 * 14 and manifest["derivs"]["snapshot_added"] is True
    assert manifest["derivs"]["candles_filled"] >= len(before)

    # stare swiece z uruchomienia 1 DOSTALY funding (znany dokladnie w czasie),
    # a wszystko inne - w tym sygnal i composite - zostalo nietkniete
    for old, new in zip(before, after):
        assert new["fundingRate"] == 0.0001 and new["fundingApr"] == pytest.approx(0.0001 * 8760 * 100, abs=0.01)
        for key in old:
            if key not in ("fundingRate", "fundingApr", "fundingZ", "openInterest", "openInterestUsd", "oiChange24h", "perpPremium"):
                assert new[key] == old[key], key
    # open interest to snapshot "z teraz" (2026), swiece sa z 2023 -> brak OI, i to jest poprawne
    assert all(c["openInterest"] is None for c in after)

    # Uruchomienie 3: te same wiersze funding z serwera NIE dubluja sie
    assert ri.main() == 0
    assert len(st.load_derivs_history()["funding"]) == 24 * 14


def test_derivs_source_down_never_breaks_the_run(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport))
    _fake_derivs_sources(monkeypatch, funding_fail=True, ctx_fail=True)

    assert ri.main() == 0
    candles = st.load_candles_history()
    assert candles and all(c["fundingRate"] is None for c in candles)
    manifest = st.load_latest_run_manifest()
    assert manifest["derivs"]["enabled"] is True
    assert any(w.startswith("derivs:") for w in manifest["warnings"])


def test_derivs_disabled_by_env_skips_the_step_entirely(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport))

    def must_not_be_called(*a, **k):
        raise AssertionError("siec Hyperliquid nie powinna byc dotykana przy HYDRA_DERIVS_DISABLED")

    from hydra_signals.data_sources import hyperliquid_derivs as hd

    monkeypatch.setattr(hd, "fetch_funding_history", must_not_be_called)
    monkeypatch.setattr(hd, "fetch_asset_ctx", must_not_be_called)
    assert ri.main() == 0
    assert st.load_latest_run_manifest()["derivs"] == {"enabled": False}


# ---------------------------------------------------------------------------
# Etap 1 planu "trend" (14.11.2026): glowny sygnal = trend 14 dni, dotychczasowy
# silnik portfeli w `signalHydra`, "wyscig strategii" w data/race_state.json.
# ---------------------------------------------------------------------------


def test_trend_signal_drives_main_signal_and_race_moves_forward(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "1000")
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=1000)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport))

    # Uruchomienie 1: za malo historii na trend -> HOLD; wyscig startuje
    assert ri.main() == 0
    run1 = st.load_candles_history()
    assert run1 and all(c["signal"] == "HOLD" and c["signalSource"] == "trend_wallets" and c["signalTrend"] == "HOLD" for c in run1)
    assert all(c["signalHydra"] in ("LONG", "SHORT", "HOLD") for c in run1)
    race = st.load_race_state()
    assert race["start"]["block"] == run1[0]["block"]
    assert set(race["strategies"]) == {"trend14", "hydra", "eth", "btc14", "ethbtc14", "hybrid", "pullback", "skilled7", "antiweak7", "taker24", "trendoi", "stables7", "trendstables", "trendcalm", "optskew"}
    assert race["strategies"]["eth"]["position"] == "LONG"
    assert race["strategies"]["trend14"]["position"] == "HOLD"
    assert race["lastBlock"] == run1[-1]["block"]

    # Doklejamy przed nimi 20 dni historii po ~2000 USD (realne ceny swiec z
    # testowego lancucha sa ~3000), zeby trend mial pelne okno 14 dni.
    template = dict(run1[0])
    first_ts = run1[0]["ts"]
    history = []
    for i in range(24 * 20, 0, -1):
        h = dict(template)
        h.update({"ts": first_ts - i * 3600, "price": 2000.0, "block": -i, "time": "historia", "signal": "SHORT"})
        # swiece sprzed przelaczenia nie mialy pol trendu ani `signalHydra`
        for k in ("signalSource", "trendSma", "trendDistPct", "signalHydra", "trendEr14", "marketMode"):
            h.pop(k, None)
        history.append(h)
    # Symulacja stanu sprzed przelaczenia glownego sygnalu na "trend + portfele":
    # swiece z run1 maja jeszcze zrodlo "trend14", a strategia "pullback" w
    # wyscigu jest juz w pozycji LONG. Pierwsza nowa swieca ma przejac te pozycje.
    for c in run1:
        c["signalSource"] = "trend14"
        c.pop("signalTrend", None)
    st.save_candles_history(history + run1)
    race_pre = st.load_race_state()
    race_pre["strategies"]["pullback"]["position"] = "LONG"
    race_pre["strategies"]["pullback"]["entryPrice"] = run1[-1]["price"]
    race_pre["strategies"]["pullback"]["lastPrice"] = run1[-1]["price"]
    st.save_race_state(race_pre)

    # Uruchomienie 2: nowe bloki -> trend LONG (cena ~3000 nad srednia), wyscig idzie dalej
    _seed_wallets(chain, start_block=1000, end_block=1500)
    assert ri.main() == 0
    after = st.load_candles_history()
    new = after[len(history) + len(run1):]
    assert new
    for c in new:
        assert c["signalTrend"] == "LONG" and c["trendSma"] is not None and c["trendDistPct"] > 0
        # glowny sygnal = "trend + wejscie z portfeli": przejal pozycje LONG z wyscigu
        # i trzyma ja, dopoki trend jest wzrostowy
        assert c["signal"] == "LONG" and c["signalSource"] == "trend_wallets"
        assert c["priceSuspect"] is False
        # Etap 3: 20 dni po 2000, potem ~3000 -> ruch "w jedna strone", czysty trend
        assert c["trendEr14"] == pytest.approx(1.0) and c["marketMode"] == "TREND"
    # stare swiece dostaly tylko informacyjne pola trendu (linia sredniej na
    # wykresie); ich sygnal zostal taki, jaki byl
    for old_c, new_c in zip(history, after[: len(history)]):
        assert new_c["signal"] == "SHORT" and "signalHydra" not in new_c and "signalSource" not in new_c
        assert "trendSma" in new_c
    assert after[len(history) - 1]["trendSma"] == 2000.0  # pelne okno 14 dni po 2000
    assert after[0]["trendSma"] is None  # za malo historii na poczatku
    race2 = st.load_race_state()
    assert race2["start"]["block"] == run1[0]["block"]  # start sie nie przesuwa
    assert race2["strategies"]["trend14"]["position"] == "LONG"
    assert race2["lastBlock"] == new[-1]["block"]
    # Etap 3: hybryda w trendzie gra z trendem; "trend + wejscie z portfeli" wchodzi
    # dopiero, gdy portfele kupuja (zalezy od composite testowego lancucha)
    assert race2["strategies"]["hybrid"]["position"] == "LONG"
    assert race2["strategies"]["pullback"]["position"] == "LONG" == new[-1]["signal"]
    assert "trendEr14" in after[0] and st.load_latest_run_manifest()["er_fill"] >= len(history)
    assert len(race2["strategies"]["eth"]["curve"]) == len(run1) + len(new)
    manifest = st.load_latest_run_manifest()
    assert set(manifest["race"]) == {"trend14", "hydra", "eth", "btc14", "ethbtc14", "hybrid", "pullback", "skilled7", "antiweak7", "taker24", "trendoi", "stables7", "trendstables", "trendcalm", "optskew"}

    # strona ma dane wyscigu: na zywo + widok wstecz (tu: brak swiec po 12.09.2026
    # w testowym lancuchu z 2023 r., wiec lookback = None)
    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    start_idx = html.index(marker) + len(marker)
    data = json.loads(html[start_idx : html.index(";", start_idx)])
    assert data["race"]["live"]["strategies"]["trend14"]["position"] == "LONG"
    # przelaczenie na "trend + portfele" nastapilo przy pierwszej swiecy run2
    assert data["meta"]["signalSwitchBlock"] == new[0]["block"]
    assert data["meta"]["statsResetFromBlock"] is None  # podsumowanie liczy od poczatku historii


def test_corrupted_race_state_does_not_break_the_run(tmp_path, monkeypatch):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", "500")
    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=500)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport))
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "race_state.json").write_text("{zepsute", encoding="utf-8")
    assert ri.main() == 0
    assert st.load_race_state()["strategies"]


# ---------------------------------------------------------------------------
# Etap 2 planu "trend": BTC / ETH z Hyperliquid (live/market.py).
# ---------------------------------------------------------------------------


def _fake_market(monkeypatch, *, eth_price=3000.0, fail=False):
    from hydra_signals.data_sources import hyperliquid_market as hm
    from live import market as mk

    monkeypatch.delenv("HYDRA_MARKET_DISABLED", raising=False)
    monkeypatch.setattr(mk, "RETENTION_DAYS", 100_000)  # testowy lancuch ma daty z 2023 r.

    def fake(coin, start_ms, now_ms, *a, **k):
        if fail:
            raise OSError("hyperliquid down")
        base = (1_700_000_000 - 20 * 86400) * 1000
        rows = []
        for i in range(24 * 21):
            t = base + i * 3_600_000
            rows.append([t, 40_000.0 + 10 * i if coin == "BTC" else eth_price])
        return rows

    monkeypatch.setattr(hm, "fetch_closes", fake)


def _run_env(monkeypatch, tmp_path, end_block=1000):
    _patch_all_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("ALCHEMY_RPC_URL", "https://fake-rpc.invalid")
    monkeypatch.setenv("HYDRA_BACKFILL_BLOCKS", str(end_block))
    monkeypatch.setenv("HYDRA_BLOCKS_PER_CALL", "50")
    chain = FakeChain()
    _seed_wallets(chain, start_block=0, end_block=end_block)
    monkeypatch.setattr(ri, "JsonRpcClient", lambda url: JsonRpcClient(url, transport=chain.transport))
    return chain


def test_market_features_on_candles_and_btc_strategies_join_existing_race(tmp_path, monkeypatch):
    chain = _run_env(monkeypatch, tmp_path)
    # wyscig z etapu 1 (tylko 3 strategie) - etap 2 ma do niego dolaczyc, nie zaczynac od nowa
    assert ri.main() == 0
    race1 = st.load_race_state()
    for k in ("btc14", "ethbtc14"):
        race1["strategies"].pop(k)
    st.save_race_state(race1)
    first_candles = st.load_candles_history()
    for c in first_candles:  # stan sprzed etapu 2: swiece bez pol BTC
        for k in ("btcPrice", "btcSma", "btcTrendDistPct", "btcSignal", "ethRefPrice", "ethBtc", "ethBtcSma", "ethBtcDistPct", "ethBtcSignal"):
            c.pop(k, None)
    st.save_candles_history(first_candles)

    _fake_market(monkeypatch)
    _seed_wallets(chain, start_block=1000, end_block=1500)
    assert ri.main() == 0
    candles = st.load_candles_history()
    new = candles[len(first_candles):]
    assert new
    for c in candles:  # nowe swiece i dopelnione stare
        assert c["btcSignal"] == "LONG" and c["ethRefPrice"] == 3000.0 and c["btcTrendDistPct"] > 0
    assert all(c["priceSuspect"] is False for c in new)
    race = st.load_race_state()
    assert race["start"] == race1["start"]
    assert race["strategies"]["btc14"]["startTime"] == new[0]["time"]
    assert race["strategies"]["btc14"]["position"] == "LONG"
    assert race["strategies"]["ethbtc14"]["position"] == "HOLD"  # trend ETH jeszcze HOLD (za malo historii)
    m = st.load_latest_run_manifest()
    assert m["market"]["enabled"] is True and m["market"]["BTC"]["new"] == 24 * 21
    assert m["market_fill"] == len(first_candles)
    assert len(st.load_market_history()["coins"]["ETH"]) == 24 * 21

    # strona dostala dane karty BTC
    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    marker = "const DATA = "
    s_idx = html.index(marker) + len(marker)
    data = json.loads(html[s_idx : html.index(";", s_idx)])
    assert data["market"]["btc"]["signal"] == "LONG" and data["market"]["ethBtc"]["signal"] == "SHORT"


def test_pool_price_far_from_hyperliquid_is_suspect_and_skipped_by_race(tmp_path, monkeypatch):
    _run_env(monkeypatch, tmp_path, end_block=500)
    _fake_market(monkeypatch, eth_price=2000.0)  # pula pokazuje ~3000 -> 50% odchylenia
    assert ri.main() == 0
    candles = st.load_candles_history()
    assert candles and all(c["priceSuspect"] is True for c in candles)
    race = st.load_race_state()
    assert race == {} or race["lastBlock"] is None


def test_market_source_down_never_breaks_the_run(tmp_path, monkeypatch):
    _run_env(monkeypatch, tmp_path, end_block=500)
    _fake_market(monkeypatch, fail=True)
    assert ri.main() == 0
    candles = st.load_candles_history()
    assert candles and all(c["btcSignal"] is None for c in candles)
    m = st.load_latest_run_manifest()
    assert any(w.startswith("market: BTC") for w in m["warnings"])
    assert any(w.startswith("market: ETH") for w in m["warnings"])


# ---------------------------------------------------------------------------
# Etap 5 planu "trend": portfele z wyczuciem (live/wallet_timing.py).
# ---------------------------------------------------------------------------


def test_wallet_timing_fields_exist_and_short_history_gives_none(tmp_path, monkeypatch):
    _run_env(monkeypatch, tmp_path, end_block=1000)
    _fake_market(monkeypatch)
    assert ri.main() == 0
    candles = st.load_candles_history()
    # testowy lancuch to kilka godzin transakcji - za malo na klasyfikacje
    # (7 dni ocenionych transakcji) i okno akumulacji: pola sa, ale None
    for c in candles:
        for k in ("walletSkilledNet7d", "walletUnskilledNet7d", "walletSkilledCount", "signalSkilled", "signalVsUnskilled"):
            assert k in c and c[k] is None
    race = st.load_race_state()
    assert race["strategies"]["skilled7"]["position"] == "HOLD"
    assert race["strategies"]["antiweak7"]["trades"] == []


def test_wallet_timing_failure_never_breaks_the_run(tmp_path, monkeypatch):
    _run_env(monkeypatch, tmp_path, end_block=500)
    from live import wallet_timing as wtm

    class Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("zepsute")

    monkeypatch.setattr(wtm, "WalletTiming", Boom)
    assert ri.main() == 0
    m = st.load_latest_run_manifest()
    assert any(w.startswith("wallet_timing: RuntimeError") for w in m["warnings"])
    assert all(c["signalSkilled"] is None for c in st.load_candles_history())


# ---------------------------------------------------------------------------
# Etap 4 planu "trend": agresorzy na Hyperliquid i OI z cena (live/perp_flow.py).
# ---------------------------------------------------------------------------


def test_taker_flow_fields_from_hyperliquid_buffer(tmp_path, monkeypatch):
    _run_env(monkeypatch, tmp_path, end_block=1000)
    # bufor Hyperliquid: 2 dni transakcji przed testowymi swiecami (czas z 2023 r.),
    # 3 z 4 inicjowane przez kupujacych
    base_ms = (1_700_000_000 - 2 * 86400) * 1000
    recs = []
    for i in range(48 * 4):
        side = hl_ws.AggressorSide.SELL if i % 4 == 0 else hl_ws.AggressorSide.BUY
        tr = hl_ws.HyperliquidTrade(coin="ETH", aggressor_side=side, price_usd=2000.0, size_eth=1.0,
                                    buyer=f"b{i}", seller=f"s{i}", ts_ms=base_ms + i * 900_000, tid=i, tx_hash="0x")
        recs.append(hl_ws.trade_to_json_record(tr))
    st.save_hyperliquid_trades_buffer(recs)
    assert ri.main() == 0
    candles = st.load_candles_history()
    assert candles
    for c in candles:
        assert c["signalTaker"] == "LONG" and c["takerImbalance24h"] > 0.3
        # bez danych OI (derivs wylaczone w testach) i cen Hyperliquid: pola OI = None
        assert c["oiPriceState"] is None and c["signalTrendOi"] is None and c["forcedClose"] is None
    race = st.load_race_state()
    assert race["strategies"]["taker24"]["position"] == "LONG"
    assert race["strategies"]["trendoi"]["position"] == "HOLD"
    assert st.load_latest_run_manifest()["perp_flow"]["taker_imbalance_24h"] > 0.3


# ---------------------------------------------------------------------------
# Etap 6 i 7 planu "trend": stablecoiny (DefiLlama) i opcje ETH (Deribit).
# ---------------------------------------------------------------------------


def _liquidity_env(monkeypatch, *, fail=False):
    from hydra_signals.data_sources import defillama_stables as dls
    from hydra_signals.data_sources import deribit as drb
    from live import options as opt
    from live import stables as stb

    monkeypatch.delenv("HYDRA_STABLES_DISABLED", raising=False)
    monkeypatch.delenv("HYDRA_OPTIONS_DISABLED", raising=False)
    monkeypatch.setattr(stb, "RETENTION_DAYS", 100_000)  # testowy lancuch ma daty z 2023 r.
    monkeypatch.setattr(opt, "RETENTION_DAYS", 100_000)
    day0 = (1_700_000_000 - 30 * 86400) // 86400 * 86400

    def fake_total(*a, **k):
        if fail:
            raise OSError("defillama down")
        return [[day0 + i * 86400, 1.5e11 + i * 1e8] for i in range(35)]

    def fake_dvol(start_ms, end_ms, *a, **k):
        if fail:
            raise OSError("deribit down")
        base = (1_700_000_000 - 20 * 86400) * 1000
        return [[base + i * 3_600_000, 50.0] for i in range(24 * 21)]

    def fake_summary(*a, **k):
        if fail:
            raise OSError("deribit down")
        return [{"instrument_name": "ETH-27DEC30-2000-P", "open_interest": 300.0, "volume": 1.0,
                 "mark_iv": 60.0, "underlying_price": 2000.0},
                {"instrument_name": "ETH-27DEC30-2000-C", "open_interest": 200.0, "volume": 1.0,
                 "mark_iv": 60.0, "underlying_price": 2000.0}]

    monkeypatch.setattr(dls, "fetch_total", fake_total)
    monkeypatch.setattr(drb, "fetch_dvol", fake_dvol)
    monkeypatch.setattr(drb, "fetch_option_summary", fake_summary)


def test_stables_and_options_fields_on_candles_and_new_strategies_in_race(tmp_path, monkeypatch):
    _run_env(monkeypatch, tmp_path)
    _liquidity_env(monkeypatch)
    assert ri.main() == 0
    candles = st.load_candles_history()
    assert candles
    for c in candles:
        # podaz rosnie codziennie -> naplyw, LONG
        assert c["stablesChange7dPct"] > 0 and c["signalStables"] == "LONG"
        assert c["signalTrendStables"] in ("HOLD", "LONG")
        # DVOL staly 50 -> nie wyzej niz srednia -> trend (tu jeszcze HOLD/brak trendu)
        assert c["dvol"] == 50.0 and c["dvolSma14"] == 50.0
        assert c["signalTrendCalm"] == c["signalTrend"]
        # migawka opcji jest z "teraz" (2026), swiece z 2023 -> brak skew
        assert c["optSkew"] is None and c["signalOptSkew"] is None
    race = st.load_race_state()
    assert race["strategies"]["stables7"]["position"] == "LONG"
    assert race["strategies"]["optskew"]["position"] == "HOLD"
    m = st.load_latest_run_manifest()
    assert m["stables"]["enabled"] and m["stables"]["total"] == 35 and not m["stables"]["errors"]
    assert m["options"]["snapshot_added"] and m["options"]["dvol_total"] == 24 * 21
    assert st.load_options_history()["snapshots"][0]["pcOi"] == 1.5


def test_stables_and_options_outage_only_warns(tmp_path, monkeypatch):
    _run_env(monkeypatch, tmp_path)
    _liquidity_env(monkeypatch, fail=True)
    assert ri.main() == 0
    m = st.load_latest_run_manifest()
    assert any(w.startswith("stables:") for w in m["warnings"])
    assert any(w.startswith("options:") for w in m["warnings"])
    for c in st.load_candles_history():
        assert c["signalStables"] is None and c["dvol"] is None and c["signalTrendCalm"] is None
