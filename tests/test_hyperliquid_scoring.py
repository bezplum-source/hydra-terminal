"""Testy Faz H2/H3 (composite_perp + pola diagnostyczne) - jednostkowe dla
`hydra_signals.hyperliquid_wallets.HyperliquidScoringEngine`, mirror stylu
`tests/test_scoring.py` (odpowiednik dla Uniswap), ale z HyperliquidTrade
(dwie strony na jedno zdarzenie) zamiast pojedynczego Trade."""

from __future__ import annotations

from hydra_signals.data_sources.hyperliquid_ws import AggressorSide, HyperliquidTrade
from hydra_signals.hyperliquid_wallets import (
    HyperliquidScoringConfig,
    HyperliquidScoringEngine,
)


def make_hl_trade(buyer, seller, price, size, ts_ms, tid=0):
    return HyperliquidTrade(
        coin="ETH",
        aggressor_side=AggressorSide.BUY,
        price_usd=price,
        size_eth=size,
        buyer=buyer,
        seller=seller,
        ts_ms=ts_ms,
        tid=tid,
        tx_hash="0xabc",
    )


def test_no_new_trades_returns_none_and_does_not_touch_ema():
    engine = HyperliquidScoringEngine(HyperliquidScoringConfig())
    result = engine.run([], history_trades=[], window_end_ts_ms=1_000_000)
    assert result is None
    # EMA nietkniete - dalej "na zimno" (None), export_state to potwierdza.
    assert all(v is None for v in engine.export_state().values())


def test_all_wallets_below_min_trades_gives_neutral_composite_and_immature():
    # Kazdy portfel ma tylko 1 transakcje (< domyslne min_trades=5) -> nikt
    # sklasyfikowany, ratio domyslnie neutralne 0.5/0.5, composite = 0.0,
    # niedojrzale (0 sklasyfikowanych < 20).
    trades = [make_hl_trade(f"w{i}", f"cp{i}", 100.0, 1.0, ts_ms=1000 + i) for i in range(3)]
    engine = HyperliquidScoringEngine(HyperliquidScoringConfig())
    result = engine.run(trades, history_trades=[], window_end_ts_ms=2_000_000)
    assert result is not None
    assert result.n_classified_wallets == 0
    assert result.composite_score == 0.0
    assert result.is_mature is False


def _make_mixed_good_bad_history(n_each: int = 12):
    """12 portfeli konsekwentnie zyskownych ('good') + 12 konsekwentnie
    stratnych ('bad'), interleaved - potrzebne RAZEM w jednej klasyfikacji,
    bo `classify_wallets` rankuje WZGLEDEM SIEBIE (rank-based percentyle,
    patrz `wallets.py::classify_wallets`): populacja zlozona wylacznie z
    JEDNAKOWO zyskownych (albo wylacznie jednakowo stratnych) portfeli
    wypada w klasyfikacji IDENTYCZNIE (remis na kazdym percentylu) i cala
    zostaje zaklasyfikowana jako GOOD niezaleznie od znaku PnL - to
    wlasciwosc rankingu wzglednego, nie blad. Dwie WYRAZNIE rozne grupy w
    jednej puli sa wiec konieczne, zeby test faktycznie sprawdzal
    rozroznienie GOOD/BAD, nie tylko remis."""
    history = []
    ts = 0
    for i in range(n_each):
        wallet = f"good{i}"
        for _ in range(3):
            history.append(make_hl_trade(wallet, f"cp{ts}", 100.0, 1.0, ts_ms=ts))
            ts += 1
            history.append(make_hl_trade(f"cp{ts}", wallet, 150.0, 1.0, ts_ms=ts))
            ts += 1
    for i in range(n_each):
        wallet = f"bad{i}"
        for _ in range(3):
            history.append(make_hl_trade(wallet, f"cp{ts}", 150.0, 1.0, ts_ms=ts))
            ts += 1
            history.append(make_hl_trade(f"cp{ts}", wallet, 100.0, 1.0, ts_ms=ts))
            ts += 1
    return history, ts


def test_good_cohort_net_buying_gives_positive_composite_and_maturity():
    history, ts = _make_mixed_good_bad_history(n_each=12)
    # W oknie testowym TYLKO "dobrzy" NETTO kupuja - "zli" nie handluja
    # wcale w tym oknie (bad_ratio_raw zostaje neutralne 0.5, brak wplywu).
    window_trades = [make_hl_trade(f"good{i}", f"cpw{i}", 100.0, 1.0, ts_ms=ts + i) for i in range(12)]

    cfg = HyperliquidScoringConfig(good_pct=0.5, bad_pct=0.5)
    engine = HyperliquidScoringEngine(cfg)
    result = engine.run(window_trades, history_trades=history, window_end_ts_ms=ts + 100)

    assert result is not None
    assert result.n_classified_wallets == 24  # 12 good + 12 bad, wszyscy >= min_trades
    assert result.is_mature is True  # 24 >= domyslny prog 20
    assert result.good_buyers == 12
    assert result.good_sellers == 0
    assert result.composite_score > 0


def test_bad_cohort_net_buying_gives_negative_composite_contrarian():
    # "Bad traders buy? Sell." - dokladnie ta sama logika kontrariańska co
    # w hydra_signals.scoring.ScoringEngine.run. W oknie testowym TYLKO
    # "zli" NETTO kupuja -> sygnal kontrariańsko niedzwiedzi (composite < 0).
    history, ts = _make_mixed_good_bad_history(n_each=12)
    window_trades = [make_hl_trade(f"bad{i}", f"cpw{i}", 100.0, 1.0, ts_ms=ts + i) for i in range(12)]

    cfg = HyperliquidScoringConfig(good_pct=0.5, bad_pct=0.5)
    engine = HyperliquidScoringEngine(cfg)
    result = engine.run(window_trades, history_trades=history, window_end_ts_ms=ts + 100)

    assert result is not None
    assert result.is_mature is True
    assert result.bad_buyers == 12
    assert result.bad_sellers == 0
    assert result.composite_score < 0


def test_ema_persists_across_two_separate_engine_instances():
    # Wznawialnosc: drugi silnik (nowe obiekty - symulacja nowego procesu w
    # kolejnym uruchomieniu run_incremental.py) skonstruowany z
    # `initial_ema=poprzedni.export_state()` musi kontynuowac EMA, a NIE
    # startowac "na zimno" (None).
    trades = [make_hl_trade(f"w{i}", f"cp{i}", 100.0, 1.0, ts_ms=i) for i in range(10)]
    history = [
        make_hl_trade(f"w{i}", f"h{i}", 100.0, 1.0, ts_ms=-100 - i) for i in range(10)
    ] + [make_hl_trade(f"h{i}", f"w{i}", 150.0, 1.0, ts_ms=-90 - i) for i in range(10)]

    cfg = HyperliquidScoringConfig(min_trades_for_classification=2, min_classified_wallets_for_maturity=5)
    engine1 = HyperliquidScoringEngine(cfg)
    result1 = engine1.run(trades, history_trades=history, window_end_ts_ms=1000)
    assert result1 is not None

    exported = engine1.export_state()
    assert all(v is not None for v in exported.values())

    engine2 = HyperliquidScoringEngine(cfg, initial_ema=exported)
    assert engine2.export_state() == exported

    # Drugie okno ma PRZECIWNY kierunek (wszyscy NETTO sprzedaja, nie kupuja)
    # wzgledem pierwszego - gdyby EMA startowalo "na zimno" (initial_ema
    # zignorowany), wynik bylby identyczny niezaleznie od stanu wznowionego,
    # bo `_update_ema` z `current=None` zwraca `new_value` wprost.
    more_trades = [make_hl_trade(f"cp2{i}", f"w{i}", 100.0, 1.0, ts_ms=2000 + i) for i in range(10)]
    result_warm = engine2.run(more_trades, history_trades=history + trades, window_end_ts_ms=3000)
    assert result_warm is not None

    # Porownanie z silnikiem "na zimno" (initial_ema=None) przetwarzajacym
    # DOKLADNIE te same dane w jednym kroku - jesli initial_ema bylby
    # ignorowany, oba silniki dalyby IDENTYCZNY wynik (EMA startuje od
    # new_value przy pierwszym wywolaniu niezaleznie od historii). Rozny
    # wynik dowodzi, ze wznowiony stan realnie wplywa na EMA.
    cold_engine = HyperliquidScoringEngine(cfg)
    result_cold = cold_engine.run(more_trades, history_trades=history + trades, window_end_ts_ms=3000)
    assert result_cold is not None
    assert result_warm.ind_good_short != result_cold.ind_good_short


def test_history_trades_outside_lookback_window_are_ignored_for_classification():
    # Portfel z historia SPRZED classification_lookback_hours nie powinien
    # zostac sklasyfikowany na jej podstawie - lookback musi realnie
    # odfiltrowywac stare transakcje, nie tylko przyjmowac wszystko.
    cfg = HyperliquidScoringConfig(classification_lookback_hours=1.0, min_trades_for_classification=2)
    one_hour_ms = 3600 * 1000
    window_end_ts_ms = 10 * one_hour_ms

    # Historia SPRZED okna lookback (ponad godzine wstecz wzgledem window_end) - MA zostac zignorowana.
    old_history = [
        make_hl_trade("stale", "cp1", 100.0, 1.0, ts_ms=window_end_ts_ms - 5 * one_hour_ms),
        make_hl_trade("cp2", "stale", 150.0, 1.0, ts_ms=window_end_ts_ms - 4 * one_hour_ms),
    ]
    new_trades = [make_hl_trade("stale", "cp3", 100.0, 1.0, ts_ms=window_end_ts_ms)]

    engine = HyperliquidScoringEngine(cfg)
    result = engine.run(new_trades, history_trades=old_history, window_end_ts_ms=window_end_ts_ms)

    assert result is not None
    # "stale" ma tylko 1 transakcje W OKNIE LOOKBACK (ta z new_trades) - stara
    # historia zostala odrzucona, wiec ponizej min_trades=2 -> niesklasyfikowany.
    assert result.n_classified_wallets == 0


# =====================================================================
# Faza H3 (front-end) - active_wallets / total_wallets_tracked
# =====================================================================


def test_active_wallets_counts_distinct_wallets_with_nonzero_net_direction():
    # 3 portfele NETTO kupuja/sprzedaja w oknie (buyer+seller par -> 6 wpisow
    # per-portfelowych, ale tylko 3+3=6 ROZNYCH adresow licza sie do
    # active_wallets - kazdy portfel liczony raz, niezaleznie od liczby
    # transakcji w tym oknie).
    trades = [make_hl_trade(f"w{i}", f"cp{i}", 100.0, 1.0, ts_ms=i) for i in range(3)]
    engine = HyperliquidScoringEngine(HyperliquidScoringConfig())
    result = engine.run(trades, history_trades=[], window_end_ts_ms=1000)
    assert result is not None
    assert result.active_wallets == 6  # 3 buyerow + 3 sellerow, wszyscy rozni


def test_total_wallets_tracked_accumulates_across_calls_on_same_engine():
    engine = HyperliquidScoringEngine(HyperliquidScoringConfig())
    r1 = engine.run(
        [make_hl_trade("a", "b", 100.0, 1.0, ts_ms=1)], history_trades=[], window_end_ts_ms=1000
    )
    assert r1 is not None
    assert r1.total_wallets_tracked == 2  # a, b

    # Drugie wywolanie: "a" sie powtarza (juz sledzony), "c" jest nowy ->
    # suma rosnie o JEDEN nowy adres, nie o dwa.
    r2 = engine.run(
        [make_hl_trade("a", "c", 100.0, 1.0, ts_ms=2000)], history_trades=[], window_end_ts_ms=2000
    )
    assert r2 is not None
    assert r2.total_wallets_tracked == 3  # a, b, c
    assert engine.total_tracked == {"a", "b", "c"}


def test_total_wallets_tracked_resumes_from_initial_total_tracked():
    # Wznawialnosc: drugi silnik (nowe obiekty, symulacja kolejnego
    # uruchomienia run_incremental.py) skonstruowany z
    # `initial_total_tracked=poprzedni.total_tracked` MUSI kontynuowac
    # liczenie od zapisanego zbioru, a nie zaczynac liczyc "śledzone
    # portfele" od zera przy kazdym restarcie procesu.
    engine1 = HyperliquidScoringEngine(HyperliquidScoringConfig())
    engine1.run([make_hl_trade("a", "b", 100.0, 1.0, ts_ms=1)], history_trades=[], window_end_ts_ms=1000)
    assert engine1.total_tracked == {"a", "b"}

    engine2 = HyperliquidScoringEngine(HyperliquidScoringConfig(), initial_total_tracked=engine1.total_tracked)
    result = engine2.run(
        [make_hl_trade("c", "d", 100.0, 1.0, ts_ms=2000)], history_trades=[], window_end_ts_ms=2000
    )
    assert result is not None
    assert result.total_wallets_tracked == 4  # a, b (wznowione) + c, d (nowe)
