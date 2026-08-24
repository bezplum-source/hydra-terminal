#!/usr/bin/env python3
"""Listener Fazy H0 (brief `hydrav2-hyperliquid-brief.md`) — uruchamiany co
godzinę przez GitHub Actions (`.github/workflows/hyperliquid-update.yml`),
OSOBNO od głównego pipeline'u ETH/Uniswap (`run_incremental.py`).

W przeciwieństwie do `run_incremental.py` (krótkie "zapytaj RPC i zgaś"),
ten skrypt trzyma otwarte połączenie WebSocket do Hyperliquid przez większość
godziny (`HYPERLIQUID_LISTEN_SECONDS`, domyślnie 50 minut) i zbiera surowe
transakcje z rynku ETH-PERP — bez tego nie da się ich odkryć, bo Hyperliquid
nie ma publicznego REST-owego "co się stało w ostatniej godzinie" (patrz
`hydra_signals/data_sources/hyperliquid_ws.py`, docstring modułu).

Co robi, w kolejności:
1. Wczytuje istniejący bufor (`data/hyperliquid_trades_buffer.jsonl`) i
   przycina go do `HYPERLIQUID_BUFFER_LOOKBACK_HOURS` wstecz.
2. Nasłuchuje WS przez `HYPERLIQUID_LISTEN_SECONDS`, dopisując nowe
   transakcje do bufora w pamięci, z okresowym flushem na dysk (co
   `HYPERLIQUID_FLUSH_INTERVAL_SECONDS`) — zabezpieczenie na wypadek, gdyby
   proces został ubity przez timeout GitHub Actions przed czystym końcem.
3. Zapisuje finalny bufor na dysk (w `finally`, więc nawet przy wyjątku
   zapisujemy to, co zdążyliśmy zebrać).

Ten skrypt SAM NIE robi `git commit`/`git push` — to, tak jak w
`run_incremental.py`, celowo zostawione workflow'owi.

Faza H0 TYLKO zbiera dane — nic tu jeszcze nie liczy skuteczności portfeli,
nie dotyka `scoring_state.json`/`candles_history.json`/frontendu. Klasyfikacja
portfeli z tego bufora to Faza H1 (jeszcze niezaimplementowana, patrz brief).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hydra_signals.data_sources import hyperliquid_ws as hl_ws  # noqa: E402

from live import state as st  # noqa: E402

DEFAULT_LISTEN_SECONDS = 50 * 60  # 50 minut - zostawia zapas w timeout-minutes: 55 workflow'a


def log(msg: str) -> None:
    print(f"[hyperliquid] {msg}", flush=True)


def _quiet_dropped_handshake_noise(loop: "asyncio.AbstractEventLoop", context: dict) -> None:
    """Wygasza znany, nieszkodliwy hałas z biblioteki `websockets`: gdy
    połączenie zostaje zerwane (np. przez pośredniczące proxy) W TRAKCIE
    handshake'u WS, jej wewnętrzny callback `connection_lost` próbuje
    odczytać `response.status_code` z odpowiedzi, której jeszcze nie ma
    (`response is None`), co rzuca `AttributeError` bezpośrednio do
    domyślnego handlera wyjątków pętli asyncio (NIE do naszego try/except
    w `main()` - to osobna ścieżka, callback zaplanowany przez pętlę
    zdarzeń). Zaobserwowane w piaskownicy Claude przy realnym połączeniu z
    Hyperliquid (proxy zwraca 403 Forbidden) - samo `listen()` i tak
    poprawnie to obsługuje i ponawia próbę (patrz
    `hydra_signals/data_sources/hyperliquid_ws.py`), więc to wyłącznie
    kosmetyka logów, nie wpływa na działanie."""
    exc = context.get("exception")
    if isinstance(exc, AttributeError) and "status_code" in str(exc):
        log("(pominięto nieszkodliwy wewnętrzny log biblioteki websockets po zerwanym połączeniu)")
        return
    loop.default_exception_handler(context)


async def _listen_with_quiet_logging(on_trades, *, duration_seconds: float) -> None:
    asyncio.get_running_loop().set_exception_handler(_quiet_dropped_handshake_noise)
    await hl_ws.listen(on_trades, duration_seconds=duration_seconds)


def main() -> int:
    listen_seconds = float(os.environ.get("HYPERLIQUID_LISTEN_SECONDS", str(DEFAULT_LISTEN_SECONDS)))
    lookback_hours = float(
        os.environ.get("HYPERLIQUID_BUFFER_LOOKBACK_HOURS", str(hl_ws.DEFAULT_BUFFER_LOOKBACK_HOURS))
    )
    flush_interval = float(
        os.environ.get("HYPERLIQUID_FLUSH_INTERVAL_SECONDS", str(hl_ws.DEFAULT_FLUSH_INTERVAL_SECONDS))
    )

    existing = st.load_hyperliquid_trades_buffer()
    now_ms = int(time.time() * 1000)
    pruned = hl_ws.prune_trade_records(existing, now_ms=now_ms, lookback_hours=lookback_hours)
    log(
        f"Bufor wczytany: {len(existing)} rekordów, {len(pruned)} po przycięciu "
        f"do ostatnich {lookback_hours:.0f}h."
    )

    collected: list[dict] = list(pruned)
    state = {"last_flush_monotonic": time.monotonic(), "n_batches": 0}

    def on_trades(trades: list[hl_ws.HyperliquidTrade]) -> None:
        collected.extend(hl_ws.trade_to_json_record(t) for t in trades)
        state["n_batches"] += 1
        if time.monotonic() - state["last_flush_monotonic"] >= flush_interval:
            st.save_hyperliquid_trades_buffer(collected)
            state["last_flush_monotonic"] = time.monotonic()
            log(f"Flush: {len(collected)} transakcji w buforze (po {state['n_batches']} paczkach WS).")

    log(f"Start nasłuchu ETH-PERP na Hyperliquid przez {listen_seconds:.0f}s...")
    try:
        asyncio.run(_listen_with_quiet_logging(on_trades, duration_seconds=listen_seconds))
    except Exception as exc:  # noqa: BLE001 - nie chcemy stracic juz zebranych danych
        log(f"BŁĄD podczas nasłuchu (zapisuję to, co zebrano do tej pory): {exc!r}")
    finally:
        st.save_hyperliquid_trades_buffer(collected)
        log(f"Zakończono. Bufor końcowy: {len(collected)} transakcji.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
