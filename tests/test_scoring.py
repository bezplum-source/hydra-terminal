from hydra_signals.models import Side, Trade, Signal
from hydra_signals.scoring import ScoringConfig, ScoringEngine, blend_composite, decide_signal


def make_trade(wallet, block, side, price, size):
    return Trade(wallet=wallet, block=block, side=side, price_usd=price, size_eth=size)


def test_no_trades_returns_empty():
    engine = ScoringEngine(ScoringConfig())
    assert engine.run([], lambda b: 2000.0) == []


def test_all_neutral_wallets_no_crash_and_hold_signal():
    # Za malo transakcji per portfel, zeby cokolwiek sklasyfikowac ->
    # wszyscy zostaja UNRATED, pool powinien byc pusty, silnik nie powinien
    # sie wywalic. composite wychodzi dokladnie 0.0 (brak jakiejkolwiek
    # aktywnosci w obu kohortach -> oba ratio domyslnie 0.5, patrz komentarz
    # w ScoringEngine.run) - Faza "NEUTRAL dead-zone": to trafia w pasmo
    # neutralne, sygnal to Signal.HOLD ("NEUTRALNY" na froncie).
    trades = [
        make_trade(f"w{i}", block, Side.BUY, 2000.0, 1.0)
        for i in range(3)
        for block in [0, 250, 500]
    ]
    cfg = ScoringConfig(min_trades_for_classification=100)  # celowo niemozliwy prog
    engine = ScoringEngine(cfg)
    scores = engine.run(trades, lambda b: 2000.0)
    assert len(scores) > 0
    for s in scores:
        assert s.pool_size == 0
        assert s.signal == Signal.HOLD


def test_weak_composite_within_dead_zone_gives_hold_not_long():
    # Faza "NEUTRAL dead-zone": w przeciwienstwie do testu wyzej (composite
    # dokladnie 0.0 z powodu BRAKU jakiejkolwiek aktywnosci), tutaj JEST
    # realna, sklasyfikowana kohorta GOOD/BAD (pool_size > 0) i realna
    # aktywnosc w oknie - composite wychodzi niezerowy (+0.1875), ale nadal
    # wewnatrz domyslnego pasma neutralnego (+-0.2). Ze starym progiem 0.0
    # (sprzed tej fazy) ten sam scenariusz dalby Signal.LONG - to jest
    # dokladnie ten przypadek, ktory produkowal bezsensowne, jednoswiecowe
    # wpisy 0.00% w "Historii sygnalow" (zglosil to uzytkownik).
    trades = []
    # Faza 1 (blok 0): ustala profitowosc przez round-tripy kupno/sprzedaz w
    # TYM SAMYM oknie - zerowy wplyw na net_direction (kupno+sprzedaz tej
    # samej wielkosci sie znosi), ale wystarcza do klasyfikacji GOOD/BAD
    # (patrz lookback w ScoringEngine.run).
    for _ in range(3):
        for i in range(8):
            trades.append(make_trade(f"good{i}", 0, Side.BUY, 100.0, 1.0))
            trades.append(make_trade(f"good{i}", 0, Side.SELL, 110.0, 1.0))
        for i in range(8):
            trades.append(make_trade(f"bad{i}", 0, Side.BUY, 100.0, 1.0))
            trades.append(make_trade(f"bad{i}", 0, Side.SELL, 90.0, 1.0))
    # Faza 2 (nadal blok 0, wiec to samo okno): jednostronna aktywnosc,
    # ktora faktycznie liczy sie do net_direction. good: 5 net-buy / 3
    # net-sell -> good_ratio_raw=5/8=0.625. bad: 4/4 -> bad_ratio_raw=0.5
    # (brak wkladu). composite (EMA "na zimno") = 1.5*(0.625-0.5) = 0.1875.
    for i in range(5):
        trades.append(make_trade(f"good{i}", 0, Side.BUY, 105.0, 1.0))
    for i in range(5, 8):
        trades.append(make_trade(f"good{i}", 0, Side.SELL, 105.0, 1.0))
    for i in range(4):
        trades.append(make_trade(f"bad{i}", 0, Side.BUY, 95.0, 1.0))
    for i in range(4, 8):
        trades.append(make_trade(f"bad{i}", 0, Side.SELL, 95.0, 1.0))

    engine = ScoringEngine(ScoringConfig())  # signal_threshold domyslny = 0.2
    scores = engine.run(trades, lambda b: 2000.0)
    assert len(scores) == 1
    s = scores[0]
    assert s.pool_size == 8  # realna, sklasyfikowana kohorta - nie "brak danych"
    assert 0.0 < s.composite_score < 0.2
    assert s.signal == Signal.HOLD


def test_only_buys_pushes_toward_long_when_good_cohort_exists():
    trades = []
    # Zbuduj historie, w ktorej "dobrzy" (zyskowni) systematycznie kupuja
    # tuz przed wzrostem ceny, zeby zostali sklasyfikowani jako GOOD,
    # a potem w oknie testowym wszyscy kupuja -> oczekujemy sygnalu LONG.
    price_map = {}
    block = 0
    price = 100.0
    for cycle in range(20):
        for wallet_id in range(5):
            trades.append(make_trade(f"good{wallet_id}", block, Side.BUY, price, 1.0))
        price_map[block] = price
        block += 1
        price *= 1.05  # cena rosnie po kazdym zakupie -> "dobrzy" maja racje
        price_map[block] = price
        for wallet_id in range(5):
            trades.append(make_trade(f"good{wallet_id}", block, Side.SELL, price, 1.0))
        block += 5

    def price_at_block(b):
        return price_map.get(b, price)

    cfg = ScoringConfig(
        window_blocks=25,
        classification_lookback_blocks=10_000,
        min_trades_for_classification=5,
        good_pct=0.5,
        bad_pct=0.0,
        ema_short_span=2,
        ema_long_span=4,
    )
    engine = ScoringEngine(cfg)
    scores = engine.run(trades, price_at_block)
    assert len(scores) > 0
    # W co najmniej czesci okien powinnismy zobaczyc wyrazna przewage
    # kupujacych w kohortcie "dobrych" (ind_good_short > 0.5), a niektore
    # z tych okien powinny skutkowac sygnalem LONG - bo skoro nie ma kohorty
    # "zlych", composite score jest wtedy jednoznacznie dodatni.
    assert any(s.ind_good_short > 0.5 for s in scores)
    assert any(s.signal == Signal.LONG for s in scores)


def test_good_bad_pressure_divergence_breadth_are_volume_based_and_independent_of_signal():
    # Faza 0 (market regime metrics) - dobrzy/zli sklasyfikowani na podstawie
    # historii (round-trip: kupno + sprzedaz z zyskiem/strata), potem w
    # oknie testowym dobrzy WYLACZNIE kupuja, zli WYLACZNIE sprzedaja, z
    # jawnie zadanymi wolumenami - zeby dalo sie policzyc oczekiwany wynik
    # recznie i sprawdzic, ze pressure/divergence/breadth licza sie z
    # WOLUMENU, a nie z liczby portfeli (ktora jest tu po 2 na kohorte).
    history = [
        make_trade("g1", 1, Side.BUY, 100.0, 1.0),
        make_trade("g1", 2, Side.SELL, 200.0, 1.0),  # zysk -> GOOD
        make_trade("g2", 1, Side.BUY, 100.0, 1.0),
        make_trade("g2", 2, Side.SELL, 190.0, 1.0),  # zysk -> GOOD
        make_trade("b1", 1, Side.BUY, 100.0, 1.0),
        make_trade("b1", 2, Side.SELL, 75.0, 1.0),  # strata -> BAD
        make_trade("b2", 1, Side.BUY, 100.0, 1.0),
        make_trade("b2", 2, Side.SELL, 70.0, 1.0),  # strata -> BAD
    ]
    window_trades = [
        make_trade("g1", 150, Side.BUY, 150.0, 3.0),
        make_trade("g2", 150, Side.BUY, 150.0, 1.0),
        make_trade("b1", 150, Side.SELL, 150.0, 2.0),
        make_trade("b2", 150, Side.SELL, 150.0, 1.0),
    ]

    cfg = ScoringConfig(
        window_blocks=100,
        classification_lookback_blocks=1000,
        min_trades_for_classification=2,
        good_pct=0.5,
        bad_pct=0.5,
    )
    engine = ScoringEngine(cfg)
    scores = engine.run(window_trades, lambda b: 150.0, history_trades=history)

    assert len(scores) == 1
    s = scores[0]
    # good: 4 ETH BUY, 0 SELL -> pressure = (4-0)/4 = +1.0 (czysty BUY)
    assert s.good_trader_pressure == 1.0
    # bad: 0 ETH BUY, 3 ETH SELL -> pressure = (0-3)/3 = -1.0 (czysty SELL)
    assert s.bad_trader_pressure == -1.0
    # dobrzy kupuja, zli sprzedaja jednoczesnie -> maksymalna rozbieznosc
    assert s.smart_money_divergence == 2.0
    # obaj dobrzy portfele sa net-buyerami w tym oknie -> breadth = 100%
    assert s.good_trader_breadth == 1.0
    # to NIE wplywa na istniejacy tor LONG/SHORT - w tym oknie dobrzy kupuja
    # bez kohorty przeciwnej o wiekszej wadze, wiec sygnal i tak powinien
    # dzialac dokladnie tak jak przed dodaniem tych pol (nie sprawdzamy tu
    # konkretnej wartosci - tylko ze pole istnieje i engine sie nie wywalil).
    assert s.signal in (Signal.LONG, Signal.SHORT, Signal.HOLD)


# =====================================================================
# Faza H2 (brief hydrav2-hyperliquid-brief.md) - blend composite_spot/perp
# =====================================================================


def test_blend_composite_returns_spot_unchanged_when_perp_is_none():
    # "Graceful degradation" z briefu - brak/niedojrzale dane Hyperliquid
    # (composite_perp=None) NIE moga zmienic zachowania wzgledem stanu
    # sprzed Fazy H2, niezaleznie od wagi.
    assert blend_composite(0.42, None) == 0.42
    assert blend_composite(-0.17, None, perp_weight=0.9) == -0.17


def test_blend_composite_averages_with_default_weight():
    assert blend_composite(1.0, 0.0) == 0.5
    assert blend_composite(0.2, 0.6) == 0.4


def test_blend_composite_respects_custom_weight():
    # waga=0 -> czysty spot, waga=1 -> czysty perp.
    assert blend_composite(0.5, -0.5, perp_weight=0.0) == 0.5
    assert blend_composite(0.5, -0.5, perp_weight=1.0) == -0.5
    assert blend_composite(1.0, 0.0, perp_weight=0.25) == 0.75


def test_decide_signal_matches_sign_of_composite():
    assert decide_signal(0.1, threshold=0.0) == Signal.LONG
    assert decide_signal(-0.1, threshold=0.0) == Signal.SHORT


def test_decide_signal_neutral_within_threshold_band():
    # Faza "NEUTRAL dead-zone": wewnatrz pasma (-threshold, +threshold)
    # sygnal to Signal.HOLD ("NEUTRALNY") - NIE trzyma juz poprzedniego
    # sygnalu (prev_signal usuniety z sygnatury, patrz docstring w
    # scoring.py) - niezaleznie od tego, co bylo "wczesniej".
    assert decide_signal(0.05, threshold=0.1) == Signal.HOLD
    assert decide_signal(-0.05, threshold=0.1) == Signal.HOLD
    assert decide_signal(0.0, threshold=0.1) == Signal.HOLD
    # Tuz POZA pasmem - normalna decyzja po znaku.
    assert decide_signal(0.11, threshold=0.1) == Signal.LONG
    assert decide_signal(-0.11, threshold=0.1) == Signal.SHORT
