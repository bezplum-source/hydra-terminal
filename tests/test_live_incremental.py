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
