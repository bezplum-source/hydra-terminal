"""Etap 6 planu "trend": stablecoiny (DefiLlama) - parsowanie, historia, cechy na świecy."""

from hydra_signals.data_sources import defillama_stables as dl
from live import stables as sb

DAY = 86_400
D0 = 1_789_000_000 // DAY * DAY  # północ UTC


def test_parse_chart_handles_string_dates_dicts_and_garbage():
    raw = [
        {"date": str(D0), "totalCirculatingUSD": {"peggedUSD": 100.0, "peggedEUR": 5.0}},
        {"date": D0 + DAY, "totalCirculatingUSD": {"peggedUSD": 110.0}},
        {"date": "x", "totalCirculatingUSD": {"peggedUSD": 1.0}},
        {"date": str(D0 + 2 * DAY), "totalCirculatingUSD": {}},  # zero -> pominięte
        "śmieć",
    ]
    assert dl.parse_chart(raw) == [[D0, 105.0], [D0 + DAY, 110.0]]
    assert dl.parse_chart({"error": "x"}) == []


def test_fetch_total_uses_injected_get():
    seen = []

    def get(url):
        seen.append(url)
        return [{"date": str(D0), "totalCirculatingUSD": {"peggedUSD": 7.0}}]

    assert dl.fetch_total(get=get) == [[D0, 7.0]]
    assert seen == [dl.CHART_URL]


def test_update_history_merges_overwrites_last_day_and_never_raises():
    h = sb.empty_history()
    now_ms = (D0 + 3 * DAY) * 1000
    c = sb.update_history(h, now_ms, fetch=lambda: [[D0, 1.0], [D0 + DAY, 2.0]])
    assert c["new"] == 2 and h["total"] == [[D0, 1.0], [D0 + DAY, 2.0]]
    c = sb.update_history(h, now_ms, fetch=lambda: [[D0 + DAY, 2.5], [D0 + 2 * DAY, 3.0]])
    assert c["new"] == 1 and h["total"][-2:] == [[D0 + DAY, 2.5], [D0 + 2 * DAY, 3.0]]

    def boom():
        raise OSError("llama down")

    c = sb.update_history(h, now_ms, fetch=boom)
    assert c["errors"] and len(h["total"]) == 3  # dane zostają


def _series(values):
    return sb.StablesSeries([[D0 + i * DAY, v] for i, v in enumerate(values)])


def test_features_use_only_complete_days_and_seven_day_change():
    s = _series([100.0 + i for i in range(10)])  # dni 0..9, rośnie o 1 dziennie
    # w środku dnia 9 znany jest tylko dzień 8 (pełny); zmiana 7 dni = 108 - 101
    f = sb.features_at(s, D0 + 9 * DAY + 3600, "LONG")
    assert f["stablesSupplyUsd"] == 108.0
    assert f["stablesChange7dUsd"] == 7.0 and abs(f["stablesChange7dPct"] - 7 / 101 * 100) < 1e-3
    assert f["signalStables"] == "LONG" and f["signalTrendStables"] == "LONG"
    # trend spadkowy przy napływie -> poza rynkiem
    assert sb.features_at(s, D0 + 9 * DAY + 3600, "SHORT")["signalTrendStables"] == "HOLD"
    # za mało historii na 7 dni -> tylko podaż
    f = sb.features_at(s, D0 + 3 * DAY, "LONG")
    assert f["stablesSupplyUsd"] == 102.0 and f["signalStables"] is None and f["signalTrendStables"] is None
    # przed pierwszym pełnym dniem i przy nieaktualnych danych -> nic
    assert sb.features_at(s, D0 + 3600, "LONG") == sb.empty_features()
    assert sb.features_at(s, D0 + 30 * DAY, "LONG") == sb.empty_features()
    assert sb.features_at(None, D0, "LONG") == sb.empty_features()


def test_outflow_gives_short_and_confirms_downtrend():
    s = _series([200.0 - i for i in range(10)])
    f = sb.features_at(s, D0 + 9 * DAY, "SHORT")
    assert f["signalStables"] == "SHORT" and f["signalTrendStables"] == "SHORT"
    assert sb.trend_stables_signal("HOLD", -1.0) == "HOLD"
    assert sb.trend_stables_signal("LONG", None) is None
