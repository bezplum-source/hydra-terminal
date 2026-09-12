import pytest

from hydra_signals.models import Side, Trade, Signal
from hydra_signals.scoring import (
    ScoringConfig,
    ScoringEngine,
    SpotPoolEngine,
    blend_composite,
    decide_signal,
)


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
        make_trade(f"w{i}", block, Side.BUY, 2000.0, 20.0)
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
            trades.append(make_trade(f"good{i}", 0, Side.BUY, 100.0, 20.0))
            trades.append(make_trade(f"good{i}", 0, Side.SELL, 110.0, 20.0))
        for i in range(8):
            trades.append(make_trade(f"bad{i}", 0, Side.BUY, 100.0, 20.0))
            trades.append(make_trade(f"bad{i}", 0, Side.SELL, 90.0, 20.0))
    # Faza 2 (nadal blok 0, wiec to samo okno): jednostronna aktywnosc,
    # ktora faktycznie liczy sie do net_direction. good: 5 net-buy / 3
    # net-sell -> good_ratio_raw=5/8=0.625. bad: 4/4 -> bad_ratio_raw=0.5
    # (brak wkladu). composite (EMA "na zimno") = 1.5*(0.625-0.5) = 0.1875.
    for i in range(5):
        trades.append(make_trade(f"good{i}", 0, Side.BUY, 105.0, 20.0))
    for i in range(5, 8):
        trades.append(make_trade(f"good{i}", 0, Side.SELL, 105.0, 20.0))
    for i in range(4):
        trades.append(make_trade(f"bad{i}", 0, Side.BUY, 95.0, 20.0))
    for i in range(4, 8):
        trades.append(make_trade(f"bad{i}", 0, Side.SELL, 95.0, 20.0))

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
            trades.append(make_trade(f"good{wallet_id}", block, Side.BUY, price, 20.0))
        price_map[block] = price
        block += 1
        price *= 1.05  # cena rosnie po kazdym zakupie -> "dobrzy" maja racje
        price_map[block] = price
        for wallet_id in range(5):
            trades.append(make_trade(f"good{wallet_id}", block, Side.SELL, price, 20.0))
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
        make_trade("g1", 1, Side.BUY, 100.0, 20.0),
        make_trade("g1", 2, Side.SELL, 200.0, 20.0),  # zysk -> GOOD
        make_trade("g2", 1, Side.BUY, 100.0, 20.0),
        make_trade("g2", 2, Side.SELL, 190.0, 20.0),  # zysk -> GOOD
        make_trade("b1", 1, Side.BUY, 100.0, 20.0),
        make_trade("b1", 2, Side.SELL, 75.0, 20.0),  # strata -> BAD
        make_trade("b2", 1, Side.BUY, 100.0, 20.0),
        make_trade("b2", 2, Side.SELL, 70.0, 20.0),  # strata -> BAD
    ]
    window_trades = [
        make_trade("g1", 150, Side.BUY, 150.0, 60.0),
        make_trade("g2", 150, Side.BUY, 150.0, 20.0),
        make_trade("b1", 150, Side.SELL, 150.0, 40.0),
        make_trade("b2", 150, Side.SELL, 150.0, 20.0),
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


def test_total_good_bad_classified_reports_whole_cohort_not_just_active_window():
    # Faza "Base L2, integracja B1-B3" - `total_good_classified`/
    # `total_bad_classified` musza liczyc CALA sklasyfikowana populacje
    # kohorty (z historii/lookback), NIE tylko portfele aktywne (net BUY/
    # SELL) w tym konkretnym oknie - potrzebne do bramki dojrzalosci Base
    # (analogicznej do `HyperliquidScoringConfig.min_classified_wallets_
    # for_maturity`). Uzywamy tego samego scenariusza co test wyzej (4
    # sklasyfikowane portfele w historii: g1/g2 -> GOOD, b1/b2 -> BAD), ale
    # w oknie testowym aktywny jest TYLKO b2 (reszta milczy) - jesli pola
    # liczylyby tylko "aktywnych", wyszloby 0/1 zamiast 2/2.
    history = [
        make_trade("g1", 1, Side.BUY, 100.0, 20.0),
        make_trade("g1", 2, Side.SELL, 200.0, 20.0),  # zysk -> GOOD
        make_trade("g2", 1, Side.BUY, 100.0, 20.0),
        make_trade("g2", 2, Side.SELL, 190.0, 20.0),  # zysk -> GOOD
        make_trade("b1", 1, Side.BUY, 100.0, 20.0),
        make_trade("b1", 2, Side.SELL, 75.0, 20.0),  # strata -> BAD
        make_trade("b2", 1, Side.BUY, 100.0, 20.0),
        make_trade("b2", 2, Side.SELL, 70.0, 20.0),  # strata -> BAD
    ]
    # w oknie testowym trada TYLKO b2 - g1/g2/b1 sa nieaktywni w tym oknie,
    # ale nadal nalezy do sklasyfikowanej (historycznej) populacji.
    window_trades = [
        make_trade("b2", 150, Side.SELL, 150.0, 20.0),
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
    assert s.total_good_classified == 2  # g1 + g2
    assert s.total_bad_classified == 2  # b1 + b2
    # `pool_size` (aktywni w TYM oknie z kohorty GOOD - patrz ScoringEngine.
    # run) to co innego niz total_good_classified: tylko b2 (BAD) handlowal
    # w oknie testowym, wiec pool_size=0, mimo ze sklasyfikowana populacja
    # GOOD/BAD (total_*_classified) to nadal pelne 2+2 z historii.
    assert s.pool_size == 0
    assert s.bad_sellers == 1
    assert s.good_buyers == 0 and s.good_sellers == 0


# =====================================================================
# Faza "wazenie wolumenem SPOT" (2026-09-11) - wagi sqrt+cap per portfel
# w ScoringEngine.run() (good_buy_weight/itd. na WindowScore)
# =====================================================================


def test_run_computes_sqrt_of_notional_weight_when_below_cap():
    # Dwa portfele GOOD, oba net-buyerzy w oknie testowym, z notionalami
    # WYRAZNIE ponizej domyslnego sufitu ($50 000) - waga powinna byc po
    # prostu sqrt(notional), bez zadnego capowania.
    history = [
        make_trade("g1", 1, Side.BUY, 100.0, 20.0),
        make_trade("g1", 2, Side.SELL, 200.0, 20.0),  # zysk -> GOOD
        make_trade("g2", 1, Side.BUY, 100.0, 20.0),
        make_trade("g2", 2, Side.SELL, 190.0, 20.0),  # zysk -> GOOD
    ]
    # notional g1 = 100.0*100.0 = 10 000 -> sqrt = 100.0
    # notional g2 = 100.0*25.0 = 2 500 -> sqrt = 50.0
    window_trades = [
        make_trade("g1", 150, Side.BUY, 100.0, 100.0),
        make_trade("g2", 150, Side.BUY, 100.0, 25.0),
    ]
    cfg = ScoringConfig(
        window_blocks=100,
        classification_lookback_blocks=1000,
        min_trades_for_classification=2,
        # good_pct=1.0/bad_pct=0.0 - klasyfikacja jest RANKINGIEM wzgledem
        # innych portfeli w oknie lookback, nie prostym testem "czy portfel
        # byl na plusie" - przy 0.5/0.5 i 2 portfelach zostalby podzielony
        # 1 GOOD/1 BAD wg rankingu (mimo ze OBA byly zyskowne), co zepsuloby
        # ten test. 1.0/0.0 gwarantuje, ze OBA portfele lokuja sie w GOOD.
        good_pct=1.0,
        bad_pct=0.0,
    )
    engine = ScoringEngine(cfg)
    scores = engine.run(window_trades, lambda b: 100.0, history_trades=history)
    assert len(scores) == 1
    s = scores[0]
    assert s.good_buyers == 2
    assert s.good_sellers == 0
    assert s.good_buy_weight == pytest.approx(100.0 + 50.0)
    assert s.good_sell_weight == 0.0
    assert s.bad_buy_weight == 0.0 and s.bad_sell_weight == 0.0


def test_run_caps_notional_before_sqrt_for_whale_trade():
    # Jeden portfel GOOD, transakcja WYRAZNIE powyzej sufitu - waga MUSI
    # byc sqrt(sufit), NIE sqrt(surowy notional) - to jest wlasnie ochrona
    # przed "wielorybem" zdominowanym pojedyncza transakcja.
    history = [
        make_trade("g1", 1, Side.BUY, 100.0, 20.0),
        make_trade("g1", 2, Side.SELL, 200.0, 20.0),  # zysk -> GOOD
    ]
    # notional = 50.0*30.0 = 1500 - powyzej domyslnego filtru dust ($1000,
    # inaczej transakcja zostalaby CALKOWICIE odsiana na wejsciu do run()),
    # ale WYRAZNIE powyzej celowo niskiego sufitu wazenia (400.0) uzytego w
    # tym tescie, zeby capowanie bylo jednoznacznie wymuszone.
    window_trades = [make_trade("g1", 150, Side.BUY, 50.0, 30.0)]
    cfg = ScoringConfig(
        window_blocks=100,
        classification_lookback_blocks=1000,
        min_trades_for_classification=1,
        good_pct=1.0,
        bad_pct=0.0,
        volume_weight_cap_notional_usd=400.0,
    )
    engine = ScoringEngine(cfg)
    scores = engine.run(window_trades, lambda b: 50.0, history_trades=history)
    assert len(scores) == 1
    s = scores[0]
    assert s.good_buyers == 1
    # sqrt(400) = 20.0, NIE sqrt(1500) = ok. 38.7.
    assert s.good_buy_weight == pytest.approx(400.0**0.5)
    assert s.good_buy_weight != pytest.approx(1500.0**0.5)


def test_run_whale_cannot_dominate_weighted_ratio_thanks_to_sqrt_and_cap():
    # Adwersarialny scenariusz: JEDEN "wieloryb" GOOD kupuje za $10 000 000
    # (absurdalnie duzo), obok 5 "zwyklych" GOOD portfeli sprzedajacych po
    # $10 000 kazdy. Bez stlumienia+sufitu (sam surowy notional jako waga)
    # wieloryb calkowicie zdominowalby ratio (ponad 99% wagi). Ze
    # sqrt+cap (domyslny sufit $50 000) - dominacja jest mocno ograniczona,
    # ratio zostaje NAWET PONIZEJ 0.5 (przewaga wciaz po stronie
    # sprzedajacych, nie wieloryba).
    history = [
        make_trade(f"g{i}", 1, Side.BUY, 100.0, 20.0)
        for i in range(6)
    ] + [
        make_trade(f"g{i}", 2, Side.SELL, 200.0, 20.0)  # zysk -> wszyscy GOOD
        for i in range(6)
    ]
    window_trades = [
        # wieloryb: notional = 1000.0 * 10000.0 = 10 000 000
        make_trade("g0", 150, Side.BUY, 1000.0, 10_000.0),
    ] + [
        # 5 "zwyklych" portfeli: notional = 100.0*100.0 = 10 000 kazdy
        make_trade(f"g{i}", 150, Side.SELL, 100.0, 100.0)
        for i in range(1, 6)
    ]
    cfg = ScoringConfig(
        window_blocks=100,
        classification_lookback_blocks=1000,
        min_trades_for_classification=2,
        good_pct=1.0,
        bad_pct=0.0,
    )
    engine = ScoringEngine(cfg)
    scores = engine.run(window_trades, lambda b: 100.0, history_trades=history)
    assert len(scores) == 1
    s = scores[0]
    assert s.good_buyers == 1  # tylko wieloryb net-buyer
    assert s.good_sellers == 5

    cap = cfg.volume_weight_cap_notional_usd  # domyslnie 50 000.0
    assert s.good_buy_weight == pytest.approx(cap**0.5)
    assert s.good_sell_weight == pytest.approx(5 * (10_000.0**0.5))

    ratio_w = s.good_buy_weight / (s.good_buy_weight + s.good_sell_weight)
    # Gdyby wagi liczyc naiwnie (sam surowy notional, bez sqrt/capa),
    # wieloryb calkowicie zdominowalby wynik.
    naive_ratio = 10_000_000.0 / (10_000_000.0 + 5 * 10_000.0)
    assert naive_ratio > 0.99
    # Ze stlumieniem+sufitem: wieloryb NIE dominuje - ratio zostaje ponizej
    # 0.5 (przewaga wciaz po stronie 5 sprzedajacych), zamiast >0.99.
    assert ratio_w < 0.5
    assert ratio_w == pytest.approx(0.309, abs=1e-3)


# =====================================================================
# Faza "wspolna pula SPOT" (2026-09-11) - SpotPoolEngine (Uniswap+Base
# w JEDNEJ puli, zamiast blend_composite ze stala waga BASE_SPOT_WEIGHT)
# =====================================================================


def test_spot_pool_engine_cold_start_matches_single_window_formula():
    # Pierwsze wywolanie (EMA=None) - _ema_step zwraca surowa wartosc
    # niezmieniona, wiec composite to po prostu (w_good_short+w_good_long)*
    # (good_ratio-0.5) - (w_bad_short+w_bad_long)*(bad_ratio-0.5) z
    # domyslnymi wagami (1.0+0.5=1.5 po obu stronach).
    engine = SpotPoolEngine(ScoringConfig())
    composite = engine.update(good_buyers=3, good_sellers=1, bad_buyers=1, bad_sellers=3)
    good_ratio = 3 / 4
    bad_ratio = 1 / 4
    expected = 1.5 * (good_ratio - 0.5) - 1.5 * (bad_ratio - 0.5)
    assert composite == pytest.approx(expected)


def test_spot_pool_engine_no_activity_in_a_cohort_is_neutral_not_nan():
    # Brak aktywnosci w kohorcie (0/0) -> ratio=0.5 (neutralne), tak samo
    # jak w ScoringEngine.run() - zaden dzielenie-przez-zero/NaN.
    engine = SpotPoolEngine(ScoringConfig())
    composite = engine.update(good_buyers=0, good_sellers=0, bad_buyers=2, bad_sellers=0)
    # good_ratio=0.5 (brak aktywnosci) -> skladnik good=0. bad_ratio=1.0
    # (same zakupy w kohorcie BAD - kontrariansko niedzwiedzie).
    expected = 1.5 * (0.5 - 0.5) - 1.5 * (1.0 - 0.5)
    assert composite == pytest.approx(expected)


def test_spot_pool_engine_zero_contribution_matches_single_venue_alone():
    # Rdzen "graceful degradation": dopisanie WENUE z zerowymi licznikami
    # (odpowiednik niedojrzalego/nieskonfigurowanego Base) do puli MUSI dac
    # identyczny wynik, jakby tego venue w ogole nie bylo - dokladnie tak
    # samo jak `blend_composite(spot, None)` degradowalo wczesniej do
    # samego spot.
    solo = SpotPoolEngine(ScoringConfig())
    solo_composite = solo.update(good_buyers=5, good_sellers=2, bad_buyers=1, bad_sellers=4)

    pooled = SpotPoolEngine(ScoringConfig())
    pooled_composite = pooled.update(
        good_buyers=5 + 0, good_sellers=2 + 0, bad_buyers=1 + 0, bad_sellers=4 + 0
    )
    assert pooled_composite == solo_composite


def test_spot_pool_engine_small_venue_gets_proportional_not_equal_influence():
    # Test na REALNY problem zgloszony przez uzytkownika po zobaczeniu
    # zywych danych (2026-09-11): Uniswap mial w oknie 31 sklasyfikowanych
    # transakcji (good 2 kupno/16 sprzedaz, bad 11 kupno/2 sprzedaz),
    # Base zaledwie 7 (good 1/1, bad 1/4) - a stary mechanizm
    # (blend_composite ze stala waga 0.5) dawal Base DOKLADNIE tyle samo
    # wplywu na wynik co Uniswapowi, mimo ~4-5x mniejszej probki.
    #
    # Nowy mechanizm (pula wspolnych licznikow) MUSI dawac wynik BLIZSZY
    # samemu Uniswapowi niz stary, sztywny blend 50/50 - dokladnie to
    # sprawdza ten test, na tych samych liczbach co zrzut ekranu
    # uzytkownika.
    uniswap_only = SpotPoolEngine(ScoringConfig())
    composite_uniswap_alone = uniswap_only.update(
        good_buyers=2, good_sellers=16, bad_buyers=11, bad_sellers=2
    )

    base_only = SpotPoolEngine(ScoringConfig())
    composite_base_alone = base_only.update(good_buyers=1, good_sellers=1, bad_buyers=1, bad_sellers=4)

    old_style_blend = blend_composite(composite_uniswap_alone, composite_base_alone, perp_weight=0.5)

    pooled = SpotPoolEngine(ScoringConfig())
    composite_pooled = pooled.update(
        good_buyers=2 + 1, good_sellers=16 + 1, bad_buyers=11 + 1, bad_sellers=2 + 4
    )

    distance_pooled = abs(composite_pooled - composite_uniswap_alone)
    distance_old_blend = abs(old_style_blend - composite_uniswap_alone)
    assert distance_pooled < distance_old_blend
    # Konkretne liczby (na wypadek regresji formuly) - patrz wyliczenie w
    # komentarzu PR/dostawie: pooled ~ -0.775, stary blend ~ -0.326, sam
    # Uniswap ~ -1.1025.
    assert composite_pooled == pytest.approx(-0.775, abs=1e-3)
    assert old_style_blend == pytest.approx(-0.32615, abs=1e-3)
    assert composite_uniswap_alone == pytest.approx(-1.1025, abs=1e-3)


def test_spot_pool_engine_resumes_from_exported_state():
    # Wznowienie miedzy uruchomieniami (initial_ema) - ten sam wzorzec co
    # ScoringEngine/RegimeEngine/SignalEngine.
    engine = SpotPoolEngine(ScoringConfig())
    engine.update(good_buyers=3, good_sellers=1, bad_buyers=1, bad_sellers=3)
    state = engine.export_state()
    # Faza "wazenie wolumenem SPOT" - export_state() niesie TERAZ takze 4
    # dodatkowe klucze toru wazonego wolumenem (patrz test ponizej) - ten
    # test nie uzywal argumentow wagowych, wiec sa `None` (tor sie nie
    # aktywowal), ale klucze i tak sa obecne w wyeksportowanym slowniku.
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

    resumed = SpotPoolEngine(ScoringConfig(), initial_ema=state)
    # Ta sama kolejna aktualizacja na wznowionym silniku i na oryginalnym
    # (kontynuowanym) silniku musi dac identyczny wynik.
    expected = engine.update(good_buyers=1, good_sellers=1, bad_buyers=1, bad_sellers=1)
    actual = resumed.update(good_buyers=1, good_sellers=1, bad_buyers=1, bad_sellers=1)
    assert actual == expected


# =====================================================================
# Faza "wazenie wolumenem SPOT" (2026-09-11) - drugi, rownolegly tor EMA
# w SpotPoolEngine, blendowany 50/50 z torem "liczba portfeli"
# =====================================================================


def test_spot_pool_engine_omitted_weight_args_returns_count_path_unchanged():
    # Graceful degradation: kompletne pominiecie argumentow wagowych
    # (wszystkie `None`, domyslnie) -> silnik zachowuje sie DOKLADNIE tak,
    # jakby ta faza nie istniala - identycznie jak istniejace testy wyzej
    # (`test_spot_pool_engine_cold_start_matches_single_window_formula`
    # itd.), pisane PRZED ta faza i celowo nietuszone.
    engine = SpotPoolEngine(ScoringConfig())
    composite = engine.update(good_buyers=3, good_sellers=1, bad_buyers=1, bad_sellers=3)
    good_ratio = 3 / 4
    bad_ratio = 1 / 4
    expected_counts_only = 1.5 * (good_ratio - 0.5) - 1.5 * (bad_ratio - 0.5)
    assert composite == pytest.approx(expected_counts_only)


def test_spot_pool_engine_explicit_zero_weights_differ_from_omitted_weights():
    # Subtelna, ale zamierzona roznica: pominiecie argumentow (None) znaczy
    # "wywolujacy nie zna tej fazy" -> tor liczba-portfeli bez zmian.
    # Podanie jawnego 0.0/0.0/0.0/0.0 znaczy "wywolujacy ZNA ta faze, w tym
    # oknie po prostu nie bylo zadnej wazonej aktywnosci" -> tor wazony
    # AKTYWUJE sie (neutralne 0.5/0.5 -> composite_weighted=0.0), wiec
    # finalny wynik to POLOWA (blend 50/50 z zerem), nie to samo co
    # pominiecie.
    omitted = SpotPoolEngine(ScoringConfig())
    composite_omitted = omitted.update(good_buyers=3, good_sellers=1, bad_buyers=1, bad_sellers=3)

    explicit_zero = SpotPoolEngine(ScoringConfig())
    composite_explicit = explicit_zero.update(
        good_buyers=3,
        good_sellers=1,
        bad_buyers=1,
        bad_sellers=3,
        good_buy_weight=0.0,
        good_sell_weight=0.0,
        bad_buy_weight=0.0,
        bad_sell_weight=0.0,
    )
    assert composite_explicit == pytest.approx(composite_omitted * 0.5)
    assert composite_explicit != pytest.approx(composite_omitted)


def test_spot_pool_engine_blends_50_50_with_volume_weighted_path():
    # Liczby dobrane tak, zeby tor "liczba portfeli" i tor "wazony
    # wolumenem" dawaly WYRAZNIE rozne wyniki - dowod, ze finalny composite
    # to faktycznie srednia obu, nie przypadkowa zgodnosc.
    engine = SpotPoolEngine(ScoringConfig())
    composite = engine.update(
        good_buyers=3,
        good_sellers=1,
        bad_buyers=1,
        bad_sellers=3,
        good_buy_weight=100.0,
        good_sell_weight=900.0,  # ratio_w_good = 0.1 (odwrotnie niz liczba portfeli: 0.75)
        bad_buy_weight=800.0,
        bad_sell_weight=200.0,  # ratio_w_bad = 0.8 (odwrotnie niz liczba portfeli: 0.25)
    )
    good_ratio_counts = 3 / 4
    bad_ratio_counts = 1 / 4
    composite_counts = 1.5 * (good_ratio_counts - 0.5) - 1.5 * (bad_ratio_counts - 0.5)
    good_ratio_w = 100.0 / 1000.0
    bad_ratio_w = 800.0 / 1000.0
    composite_weighted = 1.5 * (good_ratio_w - 0.5) - 1.5 * (bad_ratio_w - 0.5)
    expected = 0.5 * composite_counts + 0.5 * composite_weighted
    assert composite == pytest.approx(expected)
    assert composite == pytest.approx(-0.15)
    # I dla kontrastu - ani sam tor liczba-portfeli, ani sam tor wazony nie
    # daje finalnego wyniku (blend faktycznie cos zmienia w obie strony).
    assert composite != pytest.approx(composite_counts)
    assert composite != pytest.approx(composite_weighted)
    # Faza "diagnostyka wazenia wolumenem w UI" (2026-09-12) - te same dwie
    # posrednie wartosci musza byc tez odczytywalne z instancji PO wywolaniu
    # `update()`, zeby `run_incremental.py` mogl je dolozyc do rekordu
    # swiecy dla front-endu (karta Wallets), bez duplikowania logiki
    # liczenia poza ta klasa.
    assert engine.last_composite_counts == pytest.approx(composite_counts)
    assert engine.last_composite_weighted == pytest.approx(composite_weighted)


def test_spot_pool_engine_last_composite_weighted_is_none_when_track_not_activated():
    # Gdy wywolujacy w ogole nie poda argumentow wagowych (patrz
    # `test_spot_pool_engine_omitted_weight_args_returns_count_path_unchanged`
    # wyzej), `last_composite_weighted` musi byc `None` - front-end (karta
    # Wallets) uzywa dokladnie tego pola do schowania linii diagnostycznej
    # na historii sprzed Fazy "wazenie wolumenem SPOT".
    engine = SpotPoolEngine(ScoringConfig())
    engine.update(good_buyers=3, good_sellers=1, bad_buyers=1, bad_sellers=3)
    assert engine.last_composite_weighted is None
    assert engine.last_composite_counts is not None


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
