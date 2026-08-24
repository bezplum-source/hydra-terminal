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
   wznowionym stanem EMA — patrz `hydra_signals/scoring.py`), a od Fazy H2
   blenduje je z równoległym `composite_perp` z Hyperliquid (patrz sekcja
   Hyperliquid niżej w kodzie i `hydrav2-hyperliquid-brief.md`).
4. Zapisuje zaktualizowany stan z powrotem na dysk (w tym stan diagnostyczny
   Hyperliquid — Faza H3 — do wyświetlenia w nowej karcie na stronie).
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
from hydra_signals.data_sources import hyperliquid_ws as hl_ws  # noqa: E402
from hydra_signals.data_sources.pools import UNISWAP_V3_USDC_WETH_005  # noqa: E402
from hydra_signals.hyperliquid_wallets import (  # noqa: E402
    HyperliquidScoringConfig,
    HyperliquidScoringEngine,
)
from hydra_signals.models import Signal  # noqa: E402
from hydra_signals import regime  # noqa: E402
from hydra_signals.scoring import (  # noqa: E402
    ScoringConfig,
    ScoringEngine,
    blend_composite,
    decide_signal,
)

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

# Faza H2 (brief hydrav2-hyperliquid-brief.md) - waga composite_perp w
# zblendowanej wartosci ktora steruje glownym sygnalem LONG/SHORT
# ("composite = (1-w)*spot + w*perp"). WARTOSC STARTOWA zaakceptowana wprost
# przez uzytkownika (50/50), do przestrojenia pozniej - patrz
# hydra_signals.scoring.DEFAULT_PERP_WEIGHT.
HYPERLIQUID_PERP_WEIGHT = float(os.environ.get("HYDRA_PERP_WEIGHT", "0.5"))


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
    regime_state = st.load_regime_state()
    wallet_flip_state = st.load_wallet_flip_state()
    hyperliquid_scoring_state = st.load_hyperliquid_scoring_state()
    hyperliquid_wallets_seen = st.load_hyperliquid_wallets_seen()

    # --- Faza H2/H3 (brief Hyperliquid) - osobny, rownolegly silnik na danych
    # z Hyperliquid (zbieranych przez OSOBNY workflow/listener, patrz
    # hyperliquid_listener.py), wznawiany miedzy uruchomieniami dokladnie
    # jak ScoringEngine/RegimeEngine ponizej. "Okno" tego silnika to NIE
    # stala siatka czasowa, tylko "wszystko, co przyszlo od ostatniego
    # uruchomienia tego skryptu" - patrz HyperliquidScoringEngine.run.
    # Liczone TUTAJ (przed petla po nowych swiecach Uniswap nizej), zeby
    # jego wlasny kursor (`last_processed_ts_ms`) posuwal sie NIEZALEZNIE
    # od tego, czy w tym konkretnym uruchomieniu Uniswap w ogole domknal
    # jakies okno - dokladnie zgodnie z zasada "dwa niezalezne rurociagi".
    hl_raw_records = st.load_hyperliquid_trades_buffer()
    hl_trades = [hl_ws.json_record_to_trade(r) for r in hl_raw_records]

    last_hl_ts_ms = hyperliquid_scoring_state.get("last_processed_ts_ms")
    if last_hl_ts_ms is None:
        new_hl_trades = hl_trades
        history_hl_trades: list = []
    else:
        new_hl_trades = [t for t in hl_trades if t.ts_ms > last_hl_ts_ms]
        history_hl_trades = [t for t in hl_trades if t.ts_ms <= last_hl_ts_ms]

    hl_engine = HyperliquidScoringEngine(
        HyperliquidScoringConfig(),
        initial_ema=hyperliquid_scoring_state,
        initial_total_tracked=hyperliquid_wallets_seen,
    )
    hl_score = None
    if new_hl_trades:
        hl_window_end_ts_ms = max(t.ts_ms for t in new_hl_trades)
        hl_score = hl_engine.run(
            new_hl_trades, history_trades=history_hl_trades, window_end_ts_ms=hl_window_end_ts_ms
        )

    # Faza H3 (front-end) - `perp_snapshot` niesie WSZYSTKO, co karta
    # diagnostyczna "ETH-PERP - Hyperliquid" potrzebuje pokazac, nie tylko
    # sama wartosc do blendu. Trzymane jako jeden slownik (nie osobne
    # zmienne) specjalnie po to, zeby dalo sie go w calosci zapisac/odczytac
    # ze stanu (`last_perp_snapshot`) - dokladnie tak samo traktujemy
    # "ostatnia znana wartosc" niezaleznie od tego, ile pol ona niesie.
    if hl_score is not None:
        perp_snapshot = {
            "composite": hl_score.composite_score if hl_score.is_mature else None,
            "is_mature": hl_score.is_mature,
            "tracked": hl_score.total_wallets_tracked,
            "active": hl_score.active_wallets,
            "classified": hl_score.n_classified_wallets,
            "good_buyers": hl_score.good_buyers,
            "good_sellers": hl_score.good_sellers,
            "bad_buyers": hl_score.bad_buyers,
            "bad_sellers": hl_score.bad_sellers,
        }
        new_hl_state = hl_engine.export_state()
        new_hl_state["last_processed_ts_ms"] = hl_score.window_end_ts_ms
        new_hl_state["last_perp_snapshot"] = perp_snapshot
        st.save_hyperliquid_scoring_state(new_hl_state)
        st.save_hyperliquid_wallets_seen(hl_engine.total_tracked)
        log(
            f"Hyperliquid: {hl_score.n_new_trades} nowych transakcji, "
            f"{hl_score.n_classified_wallets} sklasyfikowanych portfeli, "
            f"{hl_score.total_wallets_tracked} sledzonych lacznie "
            f"({'dojrzale' if hl_score.is_mature else 'jeszcze NIEDOJRZALE - composite_perp=None'})."
        )
    else:
        # Brak nowych transakcji Hyperliquid od ostatniego uruchomienia (np.
        # listener jeszcze sie nie zdazyl odpalic w tej godzinie) - NIE
        # dotykamy zapisanego stanu (EMA zamrozone), tylko odczytujemy
        # OSTATNI znany snapshot z poprzedniego uruchomienia.
        #
        # Zgodnosc wsteczna (Faza H3): stan zapisany JESZCZE PRZED ta faza
        # ma zamiast `last_perp_snapshot` dwa starsze, plaskie klucze
        # (`last_composite_perp`/`last_is_mature`, patrz Faza H2) - bez tej
        # galezi pierwsze uruchomienie po wdrozeniu Fazy H3 (jesli akurat
        # nie trafia na nowe transakcje Hyperliquid) straciloby ciaglosc
        # composite_perp, mimo ze realne dane juz dawno sa dojrzale.
        perp_snapshot = hyperliquid_scoring_state.get("last_perp_snapshot")
        if perp_snapshot is None:
            legacy_is_mature = hyperliquid_scoring_state.get("last_is_mature", False)
            perp_snapshot = {
                "composite": hyperliquid_scoring_state.get("last_composite_perp") if legacy_is_mature else None,
                "is_mature": legacy_is_mature,
                "tracked": len(hyperliquid_wallets_seen),
                "active": 0,
                "classified": 0,
                "good_buyers": 0,
                "good_sellers": 0,
                "bad_buyers": 0,
                "bad_sellers": 0,
            }

    composite_perp = perp_snapshot["composite"]

    final_prev_signal = Signal(scoring_state.get("final_prev_signal", "HOLD"))

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
        initial_wallet_flip_state=wallet_flip_state,
    )

    price_source = trimmed_buffer if trimmed_buffer else new_trades
    price_at_block = st.price_at_block_factory(price_source)

    # Faza 2 (market regime BULL/BEAR/NEUTRAL) - patrz hydra_signals/regime.py.
    # Osobny silnik, osobny stan na dysku - wznawia sie miedzy godzinowymi
    # uruchomieniami dokladnie tak samo jak ScoringEngine powyzej.
    regime_engine = regime.RegimeEngine(initial_state=regime_state)

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

            # --- Faza H2 (brief Hyperliquid) - to TU nastepuje zlaczenie
            # dwoch niezaleznych torow w jedna decyzje. `s.composite_score`/
            # `s.signal` (silnik ScoringEngine powyzej) NIE sa modyfikowane -
            # zostaja jako `compositeSpot`/`signalSpotOnly`, czysto
            # diagnostyczne (patrz brief, "Pelna przejrzystosc w hero").
            # `composite_perp` policzone raz, PRZED ta petla (patrz wyzej) -
            # ta sama wartosc stosowana do wszystkich nowych swiec w tym
            # uruchomieniu (zwykle jest ich jedna; w rzadkim przypadku
            # nadrabiania zaleglosci - swiadome uproszczenie, jak wiele
            # innych progow/przyblizen w tym projekcie).
            composite_final = blend_composite(
                s.composite_score, composite_perp, perp_weight=HYPERLIQUID_PERP_WEIGHT
            )
            final_signal = decide_signal(
                composite_final, threshold=cfg.signal_threshold, prev_signal=final_prev_signal
            )
            final_prev_signal = final_signal

            candle = {
                "block": s.window_end_block,
                "price": round(s.price_usd, 2),
                "signal": final_signal.value,
                "composite": round(composite_final, 3),
                # --- Faza H2 (brief Hyperliquid) - rozbicie widoczne juz w
                # danych, zeby przyszla Faza H3 (frontend) mogla to po
                # prostu wyswietlic bez przeliczania niczego dodatkowo.
                "compositeSpot": round(s.composite_score, 3),
                "compositePerp": round(composite_perp, 3) if composite_perp is not None else None,
                "signalSpotOnly": s.signal.value,
                # --- Faza H3 (front-end) - karta diagnostyczna "ETH-PERP -
                # Hyperliquid" (patrz template.html) - te same wartosci
                # `perp_snapshot` niezaleznie od tego, czy hl_score jest
                # swiezy w TYM konkretnym uruchomieniu (patrz komentarz przy
                # budowie `perp_snapshot` wyzej).
                "perpTracked": perp_snapshot["tracked"],
                "perpActive": perp_snapshot["active"],
                "perpClassified": perp_snapshot["classified"],
                "perpIsMature": perp_snapshot["is_mature"],
                "perpGoodBuyers": perp_snapshot["good_buyers"],
                "perpGoodSellers": perp_snapshot["good_sellers"],
                "perpBadBuyers": perp_snapshot["bad_buyers"],
                "perpBadSellers": perp_snapshot["bad_sellers"],
                "perpMaturityThreshold": hl_engine.cfg.min_classified_wallets_for_maturity,
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
                # --- Market regime metrics (Faza 0) - patrz scoring.py.
                # Niezalezne od "signal"/"composite" powyzej - osobny,
                # rownolegly tor, jeszcze BEZ wlasnego BULL/BEAR/NEUTRAL
                # (to dopiero Faza 2) - na razie tylko zapisujemy surowe
                # wskazniki do historii, zeby zaczac budowac dane potrzebne
                # pozniej do ustalenia progow.
                "goodPressure": round(s.good_trader_pressure, 4),
                "badPressure": round(s.bad_trader_pressure, 4),
                "divergence": round(s.smart_money_divergence, 4),
                "breadth": round(s.good_trader_breadth, 4),
                # --- Wallet Flip (Faza 3) - patrz hydra_signals/scoring.py.
                # Liczba portfeli, ktore w TYM oknie odwrocily kierunek po
                # wystarczajaco dlugim ciagu transakcji w przeciwna strone -
                # patrz docstring `ScoringEngine.run()`. Liczone osobno dla
                # kohorty GOOD i BAD, niezaleznie od "signal"/"composite" i
                # od reszty pol regime powyzej/ponizej.
                "goodBullishFlips": s.good_trader_bullish_flips,
                "goodBearishFlips": s.good_trader_bearish_flips,
                "badBullishFlips": s.bad_trader_bullish_flips,
                "badBearishFlips": s.bad_trader_bearish_flips,
            }
            # --- Momentum wieloczasowy (Faza 1) - patrz hydra_signals/regime.py.
            # Liczony z JUZ ZAPISANEJ historii (candles_history PRZED
            # dopisaniem tej swiecy), nie z surowych transakcji - stad
            # dziala identycznie na zywo i w przyszlym backtescie offline.
            # Wartosci beda "None" (null w JSON) dopoki historia nie urosnie
            # na tyle, zeby dany horyzont (np. 30d) mial sie z czego liczyc.
            momentum = regime.compute_momentum(
                candles_history, current=candle, window_blocks=window_blocks
            )
            candle.update(regime.momentum_to_json(momentum))
            # --- BULL/BEAR score + regime (Faza 2) - patrz hydra_signals/regime.py.
            # Wywolywane PO doliczeniu momentum (candle ma juz wszystkie pola
            # potrzebne do compute_regime_score), PO KOLEI dla kazdej nowej
            # swiecy w tym uruchomieniu - tak, jakby kazda przyszla osobno,
            # w swoim wlasnym godzinowym uruchomieniu.
            candle.update(regime_engine.process_candle(candle))
            candles_history.append(candle)

    new_last_scored_end = max((s.window_end_block for s in new_scores), default=last_scored_end)

    new_state = engine.export_state()
    new_state["last_processed_block"] = to_block
    new_state["last_scored_window_end"] = new_last_scored_end
    # Faza H2 (brief Hyperliquid) - histereza NIEZALEZNA od `prev_signal`
    # powyzej (ten ostatni to nadal wylacznie spot, wewnetrzny stan
    # ScoringEngine) - patrz docstring `hydra_signals.scoring.decide_signal`.
    new_state["final_prev_signal"] = final_prev_signal.value
    new_state["updated_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    st.save_scoring_state(new_state)
    st.save_trade_buffer(trimmed_buffer)
    st.save_wallets_seen(engine.total_tracked)
    st.save_candles_history(candles_history)
    st.save_regime_state(regime_engine.export_state())
    st.save_wallet_flip_state(engine.export_wallet_flip_state())

    build_site(candles_history)
    log(f"Strona wygenerowana: site/index.html ({len(candles_history)} swiec w pelnej historii).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
