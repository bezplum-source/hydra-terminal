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

from hydra_signals.data_sources import hyperliquid_ws as hl_ws
from hydra_signals.data_sources.onchain_rpc import JsonRpcClient, SWAP_TOPIC0
from live import build_site as bs
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

    def __init__(self):
        self.events: list[tuple[int, str, str, int, int]] = []
        self.head = 0

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
                    }
                )
        return out

    def sender_of(self, tx_hash: str) -> str | None:
        for _block, h, wallet, _a0, _a1 in self.events:
            if h == tx_hash:
                return wallet
        return None

    def transport(self, url: str, payload):
        if isinstance(payload, dict):
            # eth_blockNumber - jedyne niebatchowane wywolanie w tym pipelinie
            assert payload["method"] == "eth_blockNumber"
            return {"id": payload["id"], "result": hex(self.head)}

        out = []
        for item in payload:
            method = item["method"]
            if method == "eth_getLogs":
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
    `blend_composite`/`decide_signal` musza sie zachowac IDENTYCZNIE jak
    stary, czysto spotowy sygnal (`compositeSpot`/`signalSpotOnly`) -
    "graceful degradation" z briefu, nie zmiana zachowania."""
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
        assert c["signal"] == c["signalSpotOnly"]
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
            hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(wallet, f"cp{ts}", 100.0, 1.0, ts)))
            ts += 1
            hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(f"cp{ts}", wallet, 150.0, 1.0, ts)))
            ts += 1
    # Ostatnie zdarzenie w buforze: kazdy z 25 portfeli jeszcze raz KUPUJE
    # (net-BUY w tym "oknie" - patrz HyperliquidScoringEngine.run, okno to
    # CALY bufor przy pierwszym uruchomieniu).
    for i in range(25):
        wallet = f"hlgood{i}"
        hl_records.append(hl_ws.trade_to_json_record(_make_hl_trade(wallet, f"cp{ts}", 100.0, 1.0, ts)))
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
    assert hl_state["last_is_mature"] is True
    assert hl_state["last_composite_perp"] > 0  # jednoznacznie bycze

    last = candles[-1]
    assert last["compositePerp"] == hl_state["last_composite_perp"]
    assert last["compositePerp"] > 0
    # composite zblendowany (waga domyslna 50/50) musi lezec DOKLADNIE
    # posrodku miedzy spotem a perpem - weryfikacja formuly blendu na
    # prawdziwym przebiegu run_incremental.py, nie tylko na jednostce.
    expected = round(0.5 * last["compositeSpot"] + 0.5 * last["compositePerp"], 3)
    assert last["composite"] == expected
    # Perp jest mocno bycze - zblendowany composite powinien byc WYZSZY (albo
    # rowny w skrajnym przypadku) niz sam spot, nigdy odwrotnie.
    assert last["composite"] >= last["compositeSpot"]
