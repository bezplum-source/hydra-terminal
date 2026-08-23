#!/usr/bin/env python3
"""Cykliczny krok pipeline'u Hydra Terminal — uruchamiany co ok. godzinę
przez GitHub Actions (`.github/workflows/update.yml`).

Co robi, w kolejności:
1. Wczytuje zapisany stan (EMA silnika, ostatni przetworzony blok, bufor
   transakcji z okna lookback, zbiór wszystkich kiedykolwiek widzianych
   portfeli, pełną historię świec).
2. Pyta o aktualne czoło łańcucha i pobiera NOWE transakcje Swap od
   ostatniego przetworzonego bloku (albo robi jednorazowy backfill przy
   pierwszym uruchomieniu — patrz `BACKFILL_BLOCKS`).
3. Doklasyfikowuje portfele i liczy nowe świece (`ScoringEngine`, z
   wznowionym stanem EMA — patrz `hydra_signals/scoring.py`).
4. Zapisuje zaktualizowany stan z powrotem na dysk.
5. Generuje `site/index.html` (przez `live/build_site.py`).

Ten skrypt SAM NIE robi `git commit`/`git push` — to celowo zostawione
workflow'owi (`.github/workflows/update.yml`), żeby ten plik dało się
przetestować lokalnie bez ryzyka przypadkowego commitu.

Wymaga zmiennej środowiskowej `ALCHEMY_RPC_URL` (pełny URL RPC z kluczem) —
w GitHub Actions ustawianej jako **Secret** (Settings → Secrets and
variables → Actions), NIGDY nie commitowanej do repo w postaci jawnej.
"""

from __future__ import annotations

import datetime
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hydra_signals.data_sources.onchain_rpc import (  # noqa: E402
    JsonRpcClient,
    batch_call_with_retry,
    fetch_trades_from_chain_batched,
)
from hydra_signals.data_sources.pools import UNISWAP_V3_USDC_WETH_005  # noqa: E402
from hydra_signals.models import Signal  # noqa: E402
from hydra_signals.scoring import ScoringConfig, ScoringEngine  # noqa: E402

from live import state as st  # noqa: E402
from live.build_site import build_site  # noqa: E402

WARSAW = ZoneInfo("Europe/Warsaw")

# Ile blokow pobrac jednorazowo przy PIERWSZYM uruchomieniu (brak zapisanego
# stanu) - tyle, ile potrzeba, zeby classification_lookback_blocks (domyslnie
# 6000) mial od razu pelny, "rozgrzany" bufor, plus niewielki zapas.
BACKFILL_BLOCKS = int(os.environ.get("HYDRA_BACKFILL_BLOCKS", "6500"))
BLOCKS_PER_CALL = int(os.environ.get("HYDRA_BLOCKS_PER_CALL", "10"))
CALLS_PER_BATCH = int(os.environ.get("HYDRA_CALLS_PER_BATCH", "80"))
# Zabezpieczenie na wypadek dlugiej przerwy w dzialaniu joba (np. wylaczony
# na kilka dni) - nie probuj dogonic WSZYSTKIEGO zaleglego w jednym
# uruchomieniu, tylko tyle, ile bezpiecznie miesci sie w jednym jobie.
MAX_NEW_BLOCKS_PER_RUN = int(os.environ.get("HYDRA_MAX_NEW_BLOCKS_PER_RUN", "20000"))


def log(msg: str) -> None:
    print(f"[hydra] {msg}", flush=True)


def fmt_warsaw(ts: int) -> str:
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).astimezone(WARSAW)
    return dt.strftime("%d.%m.%Y, %H:%M")


def main() -> int:
    rpc_url = os.environ.get("ALCHEMY_RPC_URL")
    if not rpc_url:
        log("BLAD: brak zmiennej srodowiskowej ALCHEMY_RPC_URL (ustaw jako GitHub Actions secret).")
        return 1

    rpc = JsonRpcClient(rpc_url)
    cfg = ScoringConfig()

    scoring_state = st.load_scoring_state()
    trade_buffer = st.load_trade_buffer()
    wallets_seen = st.load_wallets_seen()
    candles_history = st.load_candles_history()

    head = int(rpc.call("eth_blockNumber", []), 16)
    log(f"Aktualne czolo lancucha: blok {head}")

    last_processed = scoring_state.get("last_processed_block")
    if last_processed is None:
        from_block = max(0, head - BACKFILL_BLOCKS)
        log(f"Pierwsze uruchomienie - jednorazowy backfill {BACKFILL_BLOCKS} blokow (od {from_block}).")
    else:
        from_block = last_processed + 1

    to_block = head
    if from_block > to_block:
        log("Brak nowych blokow od ostatniego uruchomienia - nic do zrobienia.")
        return 0

    if to_block - from_block + 1 > MAX_NEW_BLOCKS_PER_RUN:
        capped_to = from_block + MAX_NEW_BLOCKS_PER_RUN - 1
        log(
            f"Zakres {from_block}-{to_block} przekracza limit "
            f"{MAX_NEW_BLOCKS_PER_RUN} blokow/uruchomienie - przycinam do "
            f"{capped_to} (reszta zostanie dogoniona w kolejnych uruchomieniach)."
        )
        to_block = capped_to

    log(f"Pobieram nowe transakcje: bloki {from_block}-{to_block} ({to_block - from_block + 1} blokow)")
    new_trades = fetch_trades_from_chain_batched(
        rpc,
        UNISWAP_V3_USDC_WETH_005,
        from_block,
        to_block,
        blocks_per_call=BLOCKS_PER_CALL,
        calls_per_batch=CALLS_PER_BATCH,
        on_progress=log,
    )
    log(f"Nowych transakcji: {len(new_trades)}")

    combined_buffer = trade_buffer + new_trades
    lookback_start = to_block - cfg.classification_lookback_blocks
    trimmed_buffer = [t for t in combined_buffer if t.block > lookback_start]

    # NIE scoruj okna (swiecy), ktore jeszcze sie nie "domknelo" wzgledem
    # aktualnego czola lancucha - inaczej `window_end_block` bedzie w
    # PRZYSZLOSCI (blok jeszcze niewykopany), eth_getBlockByNumber zwroci
    # null (stad "?" zamiast daty), a w kolejnym uruchomieniu te same
    # transakcje zostalyby policzone PONOWNIE jako nowa swieca z tym samym
    # numerem bloku (duplikat w historii). Zamiast tego: transakcje z
    # jeszcze otwartego okna zostaja w buforze (`trimmed_buffer`, ponizej) i
    # doczekaja sie zaliczenia do swiecy w PRZYSZLYM uruchomieniu, gdy okno
    # sie juz domknie - `last_scored_window_end` (osobny od
    # `last_processed_block`!) pamieta, dokad juz faktycznie doliczylismy
    # swiece, wiec nic nie zostanie policzone dwa razy ani pominiete.
    window_blocks = cfg.window_blocks
    last_closed_end = ((to_block + 1) // window_blocks) * window_blocks - 1
    last_scored_end = scoring_state.get("last_scored_window_end", -1)

    scoreable_trades = [t for t in combined_buffer if last_scored_end < t.block <= last_closed_end]
    classification_history = [t for t in combined_buffer if t.block <= last_scored_end]

    has_prior_state = bool(scoring_state)
    engine = ScoringEngine(
        cfg,
        initial_ema=scoring_state if has_prior_state else None,
        initial_prev_signal=Signal(scoring_state.get("prev_signal", "HOLD")),
        initial_total_tracked=wallets_seen,
    )

    price_source = trimmed_buffer if trimmed_buffer else new_trades
    price_at_block = st.price_at_block_factory(price_source)

    new_scores = []
    if scoreable_trades:
        new_scores = engine.run(scoreable_trades, price_at_block, history_trades=classification_history)
    log(f"Nowe swiece (okna) w tym uruchomieniu: {len(new_scores)}")
    if new_trades and not scoreable_trades:
        log(
            "Najnowsze transakcje naleza jeszcze do niedomknietego okna - "
            "zostana doliczone do swiecy w kolejnym uruchomieniu."
        )

    if new_scores:
        block_numbers = [s.window_end_block for s in new_scores]
        ts_calls = [("eth_getBlockByNumber", [hex(b), False]) for b in block_numbers]
        block_results = batch_call_with_retry(rpc, ts_calls, batch_size=CALLS_PER_BATCH)
        block_ts: dict[int, int] = {}
        for b, res in zip(block_numbers, block_results):
            if res and "timestamp" in res:
                block_ts[b] = int(res["timestamp"], 16)
            else:
                log(f"UWAGA: nie udalo sie pobrac znacznika czasu dla bloku {b}.")

        for s in new_scores:
            ts = block_ts.get(s.window_end_block)
            candles_history.append(
                {
                    "block": s.window_end_block,
                    "price": round(s.price_usd, 2),
                    "signal": s.signal.value,
                    "composite": round(s.composite_score, 3),
                    "indGoodShort": round(s.ind_good_short, 3),
                    "indGoodLong": round(s.ind_good_long, 3),
                    "indBadShort": round(s.ind_bad_short, 3),
                    "indBadLong": round(s.ind_bad_long, 3),
                    "goodBuyers": s.good_buyers,
                    "goodSellers": s.good_sellers,
                    "badBuyers": s.bad_buyers,
                    "badSellers": s.bad_sellers,
                    "pool": s.pool_size,
                    "active": s.active_wallets,
                    "tracked": s.total_wallets_tracked,
                    "time": fmt_warsaw(ts) if ts is not None else "?",
                    "ts": ts,
                }
            )

    new_last_scored_end = max((s.window_end_block for s in new_scores), default=last_scored_end)

    new_state = engine.export_state()
    new_state["last_processed_block"] = to_block
    new_state["last_scored_window_end"] = new_last_scored_end
    new_state["updated_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    st.save_scoring_state(new_state)
    st.save_trade_buffer(trimmed_buffer)
    st.save_wallets_seen(engine.total_tracked)
    st.save_candles_history(candles_history)

    build_site(candles_history)
    log(f"Strona wygenerowana: site/index.html ({len(candles_history)} swiec w pelnej historii).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
