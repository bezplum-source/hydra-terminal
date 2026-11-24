"""Ocena rynku z reguł - dla raportów w archiwum strony, gdy nie ma oceny pisanej
przez Claude'a (Jakub 24.11.2026: „wolę nie zakładać konta z api”, potem
„Tak” na propozycję lepszych reguł).

Zasada: prognoza ma wynikać z KILKU wskaźników naraz, a nie z samego głównego
sygnału - inaczej kafel trafności na stronie pokazywałby po prostu to samo co
punkt odniesienia („kierunek głównego sygnału”).

Każdy czynnik daje głos z wagą (+ = za wzrostem, − = za spadkiem). Wagi i progi
ustalone Z GÓRY (24.11.2026), bez dopasowywania do historii - zasada projektu.
Dwa horyzonty liczone osobno, bo działają w nich inne siły:

- 7 dni: trend (średnia 14 dni i jej nachylenie, zmiana 30 dni, średnie 50/100
  dni), głosowanie strategii, przepływy (stablecoiny, portfele z wyczuciem),
  rynek opcji (skew) - plus słabszy głos „rozciągnięcia” (RSI, odległość od
  średniej), bo wyprzedany rynek w trendzie spadkowym częściej odbija;
- 24 h: rozciągnięcie i ustawienie rynku kontraktów mają większą wagę (krótkie
  odbicia przy wyprzedaniu i skrajnym fundingu), trend - mniejszą; dochodzą
  agresorzy z ostatniej doby.

Wynik = suma głosów / suma wag dostępnych czynników (−1…+1). |wynik| < 0,2 →
„bok”; inaczej kierunek. Pewność: |wynik| ≥ 0,55 → „umiarkowana”, ≥ 0,75 →
„wysoka”, poza tym „niska”; skok oczekiwanej zmienności (DVOL > 1,1 × średnia)
obniża pewność o stopień.
"""

from __future__ import annotations

from live import market_analysis as ma

FLAT_SCORE = 0.2
CONF_LEVELS = ((0.75, "wysoka"), (0.55, "umiarkowana"))


def _sgn(v, dead=0.0):
    if v is None:
        return None
    return 1 if v > dead else -1 if v < -dead else 0


def _p(v, nd=1, plus=True):
    if v is None:
        return "–"
    s = f"{abs(v):.{nd}f}".replace(".", ",")
    sign = "+" if v > 0 and plus else "−" if v < 0 else ""
    return sign + s + "%"


def _usd(v):
    return "–" if v is None else f"{v:,.0f}".replace(",", " ") + " $"


def factors(st: dict) -> list[dict]:
    """Lista czynników: {key, label, dir (+1/−1/0), w7, w24, note}. Czynnik bez
    danych jest pomijany (nie liczy się do sumy wag)."""
    e = st.get("eth") or {}
    ch = e.get("change_pct") or {}
    sig = st.get("signal") or {}
    der = st.get("derivs") or {}
    op = st.get("options") or {}
    sb = st.get("stables") or {}
    wl = st.get("wallets") or {}
    out: list[dict] = []

    def add(key, label, d, w7, w24, note):
        if d is not None:
            out.append({"key": key, "label": label, "dir": d, "w7": w7, "w24": w24, "note": note})

    d14 = e.get("dist_sma14d_pct")
    add("sma14", "cena względem średniej 14 dni", _sgn(d14), 1.0, 0.5,
        f"cena {_p(d14, plus=False).lstrip('−')} {'nad' if (d14 or 0) >= 0 else 'pod'} średnią 14 dni" if d14 is not None else "")
    sl = e.get("sma14d_slope_7d_pct")
    add("slope", "kierunek średniej 14 dni", _sgn(sl, 0.5), 1.0, 0.25,
        f"średnia 14 dni {'rośnie' if (sl or 0) > 0 else 'opada'} ({_p(sl)} w tydzień)" if sl is not None else "")
    c30 = ch.get("30d")
    add("chg30", "zmiana 30 dni", _sgn(c30, 3.0), 1.0, 0.25, f"ETH {_p(c30)} w 30 dni" if c30 is not None else "")
    d50, d100 = e.get("dist_sma50d_pct"), e.get("dist_sma100d_pct")
    if d50 is not None and d100 is not None:
        lt = 1 if d50 > 0 and d100 > 0 else -1 if d50 < 0 and d100 < 0 else 0
        add("longterm", "średnie 50 i 100 dni", lt, 0.5, 0.0,
            f"cena {'nad' if lt > 0 else 'pod' if lt < 0 else 'między'} średnimi 50 i 100 dni")
    v = (sig.get("votes") or {}).get("teraz")
    if v and (v["LONG"] + v["SHORT"]):
        bal = (v["LONG"] - v["SHORT"]) / (v["LONG"] + v["SHORT"] + v["HOLD"])
        add("vote", "głosowanie strategii", _sgn(bal, 0.15), 1.0, 0.5,
            f"głosowanie {v['SHORT']} SHORT / {v['LONG']} LONG / {v['HOLD']} neutr.")
    rsi = e.get("rsi14d")
    if rsi is not None:
        add("rsi", "RSI 14 dni (rozciągnięcie)", 1 if rsi < 30 else -1 if rsi > 70 else 0, 0.5, 1.0,
            f"RSI 14 dni {rsi:.0f}" + (" — wyprzedanie" if rsi < 30 else " — wykupienie" if rsi > 70 else ""))
    if d14 is not None and abs(d14) >= 12:
        add("stretch", "odległość od średniej", -_sgn(d14), 0.25, 0.75,
            f"ruch mocno rozciągnięty ({abs(d14):.0f}% od średniej 14 dni)")
    f24 = (der.get("funding_apr_pct") or {}).get("sr_24h")
    if f24 is not None:
        add("funding", "funding (ustawienie kontraktów)", 1 if f24 < -10 else -1 if f24 > 30 else 0, 0.25, 0.75,
            f"funding {_p(f24)} rocznie (24 h)" + (" — przewaga shortów, paliwo na odbicie" if f24 < -10
                                                     else " — przegrzane longi" if f24 > 30 else ""))
    tk = der.get("taker_imbalance_24h_now")
    add("taker", "agresorzy 24 h", _sgn(tk, 0.05), 0.0, 0.5,
        f"agresorzy 24 h: {'przewaga kupujących' if (tk or 0) > 0.05 else 'przewaga sprzedających' if (tk or 0) < -0.05 else 'równowaga'}"
        if tk is not None else "")
    s7 = sb.get("change_7d_pct")
    add("stables", "stablecoiny 7 dni", _sgn(s7, 0.5), 0.75, 0.25,
        f"stablecoiny {_p(s7, 2)} w 7 dni" + (" — napływ" if (s7 or 0) > 0.5 else " — odpływ" if (s7 or 0) < -0.5 else "")
        if s7 is not None else "")
    sk = wl.get("skilled_net7d_eth")
    add("skilled", "portfele z wyczuciem", _sgn(sk), 0.75, 0.25,
        f"portfele z wyczuciem {'kupują' if (sk or 0) > 0 else 'sprzedają'} netto "
        f"({f'{abs(sk or 0) / 1000:.1f}'.replace('.', ',')} tys. ETH w 7 dni)"
        if sk is not None else "")
    skew = op.get("skew_now")
    if skew is not None:
        add("skew", "skew opcji", -1 if skew > 5 else 1 if skew < -2 else 0, 0.5, 0.25,
            f"skew opcji {skew:.1f}".replace(".", ",") + (" — drogie zabezpieczenia przed spadkiem" if skew > 5
                                                          else " — rynek opcji gra na wzrost" if skew < -2 else ""))
    return out


def score(fs: list[dict], horizon: str) -> float | None:
    w = "w7" if horizon == "7d" else "w24"
    tot = sum(f[w] for f in fs if f[w] > 0)
    if tot <= 0:
        return None
    return sum(f["dir"] * f[w] for f in fs) / tot


def direction(s: float | None) -> str:
    if s is None or abs(s) < FLAT_SCORE:
        return ma.FLAT
    return ma.UP if s > 0 else ma.DOWN


def confidence(s: float | None, st: dict) -> str:
    a = abs(s or 0)
    level = next((name for thr, name in CONF_LEVELS if a >= thr), "niska")
    op = st.get("options") or {}
    if op.get("dvol_now") and op.get("dvol_sma14d") and op["dvol_now"] > 1.1 * op["dvol_sma14d"]:
        level = {"wysoka": "umiarkowana", "umiarkowana": "niska"}.get(level, "niska")
    return level


def stage(st: dict) -> str:
    e = st.get("eth") or {}
    ch = e.get("change_pct") or {}
    d14, sl, rsi = e.get("dist_sma14d_pct"), e.get("sma14d_slope_7d_pct"), e.get("rsi14d")
    d50, d100 = e.get("dist_sma50d_pct"), e.get("dist_sma100d_pct")
    mode = (st.get("signal") or {}).get("market_mode")
    if d50 is not None and d100 is not None and d50 < 0 and d100 < 0:
        base = "bessa"
    elif d50 is not None and d100 is not None and d50 > 0 and d100 > 0:
        base = "hossa"
    else:
        base = "rynek przejściowy"
    down = d14 is not None and sl is not None and d14 < 0 and sl < 0
    up = d14 is not None and sl is not None and d14 > 0 and sl > 0
    c24, c3 = ch.get("24h"), ch.get("3d")
    if down and ((c24 or 0) > 3 or (c3 or 0) > 5):
        mod = "odbicie"
    elif up and ((c24 or 0) < -3 or (c3 or 0) < -5):
        mod = "korekta"
    elif rsi is not None and rsi < 30:
        mod = "wyprzedanie"
    elif rsi is not None and rsi > 70:
        mod = "wykupienie"
    elif mode == "RANGE" or not (down or up):
        mod = "konsolidacja"
    else:
        mod = "trend spadkowy" if down else "trend wzrostowy"
    return f"{base} · {mod}"


def _since_previous(st: dict, prev: dict | None) -> str:
    if not prev:
        return ""
    parts = []
    p0, p1 = prev.get("price"), (st.get("eth") or {}).get("now")
    if p0 and p1:
        parts.append(f"cena {_p(ma.pct(p0, p1))}")
    sig = st.get("signal") or {}
    if prev.get("signal") and sig.get("now") and prev["signal"] != sig["now"]:
        parts.append(f"główny sygnał zmienił się z {prev['signal']} na {sig['now']}")
    v0, v1 = prev.get("votes") or {}, (sig.get("votes") or {}).get("teraz") or {}
    if v0 and v1 and (v0.get("SHORT"), v0.get("LONG")) != (v1.get("SHORT"), v1.get("LONG")):
        parts.append(f"głosy {v0.get('SHORT')}/{v0.get('LONG')} → {v1.get('SHORT')}/{v1.get('LONG')} (SHORT/LONG)")
    if prev.get("stage") and prev["stage"] != stage(st):
        parts.append(f"etap: było „{prev['stage']}”")
    return ("Od poprzedniego raportu: " + ", ".join(parts) + ".") if parts else ""


def analyze(st: dict, mode: str = "daily", previous: dict | None = None) -> dict:
    """Pełna ocena: etap, tytuł, tekst (2-3 akapity), prognozy, pewność, czynniki."""
    fs = factors(st)
    s7, s24 = score(fs, "7d"), score(fs, "24h")
    f7, f24 = direction(s7), direction(s24)
    conf = confidence(s7, st)
    stg = stage(st)
    e = st.get("eth") or {}
    ch = e.get("change_pct") or {}
    pros = [f for f in sorted(fs, key=lambda f: -f["w7"]) if f["dir"] > 0 and f["note"]]
    cons = [f for f in sorted(fs, key=lambda f: -f["w7"]) if f["dir"] < 0 and f["note"]]

    p1 = (f"ETH {_usd(e.get('now'))}: {_p(ch.get('24h'))} w dobę, {_p(ch.get('7d'))} w tydzień, "
          f"{_p(ch.get('30d'))} w 30 dni, {_p(ch.get('od_poczatku_hydry'))} od startu Hydry. ")
    d14, sl, d100, rsi = e.get("dist_sma14d_pct"), e.get("sma14d_slope_7d_pct"), e.get("dist_sma100d_pct"), e.get("rsi14d")
    if d14 is not None:
        p1 += (f"Cena jest {_p(abs(d14), plus=False)} {'pod' if d14 < 0 else 'nad'} średnią 14 dni"
               + (f", która {'opada' if sl < 0 else 'rośnie'} ({_p(sl)} w tydzień)" if sl is not None else "")
               + (f", i {_p(abs(d100), plus=False)} {'pod' if d100 < 0 else 'nad'} średnią 100 dni" if d100 is not None else "")
               + ". ")
    if rsi is not None:
        p1 += f"RSI 14 dni {rsi:.0f}" + (" — rynek wyprzedany." if rsi < 30 else " — rynek wykupiony." if rsi > 70 else ".")
    since = _since_previous(st, previous)
    if since:
        p1 += " " + since
    p2 = ""
    if mode == "weekly":
        sig = st.get("signal") or {}
        race = {r.get("name"): r for r in st.get("race_live") or []}
        vote = next((r for r in st.get("race_live") or [] if r.get("name", "").startswith("GŁOSOWANIE")), None)
        eth_r = race.get("ETH kup i trzymaj")
        p2 = (f"W tygodniu główny sygnał zmieniał się {sig.get('changes_7d', 0)} razy"
              + (f"; wyścig na żywo: głosowanie {_p(vote['result'])} od startu ({_p(vote['change'])} w tym tygodniu)" if vote else "")
              + (f", ETH kup i trzymaj {_p(eth_r['result'])}" if eth_r else "") + ".")
    p3 = ("Za wzrostem: " + "; ".join(f["note"] for f in pros[:3]) + ". " if pros else "Za wzrostem: brak wyraźnych czynników. ")
    p3 += ("Za spadkiem: " + "; ".join(f["note"] for f in cons[:3]) + "." if cons else "Za spadkiem: brak wyraźnych czynników.")
    sma = None
    if d14 is not None and e.get("now"):
        sma = e["now"] / (1 + d14 / 100)
    lvl = ""
    if sma:
        lvl = (f" Obraz zmieniłby powrót nad średnią 14 dni (~{_usd(sma)})." if (d14 or 0) < 0
               else f" Obraz zmieniłby spadek pod średnią 14 dni (~{_usd(sma)}).")
    p4 = (f"Prognoza z reguł: 24 h — {f24}, 7 dni — {f7} (pewność {conf}; bilans czynników "
          f"{_score_txt(s24)} / {_score_txt(s7)} w skali −1…+1).{lvl}")
    text = "\n".join(x for x in (p1.strip(), p2, p3, p4) if x)
    n_up, n_dn = len([f for f in fs if f["dir"] > 0]), len([f for f in fs if f["dir"] < 0])
    title = (f"{n_dn} czynników za spadkiem, {n_up} za wzrostem — prognoza 7 dni: {f7}" if n_dn >= n_up
             else f"{n_up} czynników za wzrostem, {n_dn} za spadkiem — prognoza 7 dni: {f7}")
    sc = st.get("forecast_score") or {}
    s24s, s7s = sc.get("24h") or {}, sc.get("7d") or {}
    if s24s.get("rozliczone"):
        track = (f"Prognozy 24 h: {s24s['trafione']}/{s24s['rozliczone']} trafione (kierunek sygnału: "
                 f"{s24s.get('punkt_odniesienia_sygnal_trafnosc_pct')}%)"
                 + (f" · 7 dni: {s7s['trafione']}/{s7s['rozliczone']} (sygnał: {s7s.get('punkt_odniesienia_sygnal_trafnosc_pct')}%)"
                    if s7s.get("rozliczone") else " · 7 dni: jeszcze nic nierozliczone") + ".")
    else:
        track = "Prognozy będą rozliczane po 24 h i po 7 dniach."
    return {
        "stage": stg, "title": title, "text": text, "f24": f24, "f7": f7, "conf": conf, "track": track,
        "score24": None if s24 is None else round(s24, 2), "score7": None if s7 is None else round(s7, 2),
        "factors": [{"label": f["label"], "dir": f["dir"], "note": f["note"]} for f in fs if f["note"]],
    }


def _score_txt(s):
    return "–" if s is None else (("+" if s > 0 else "−" if s < 0 else "") + f"{abs(s):.2f}".replace(".", ","))


def backtest(raw: dict, start: float, end: float, step: float = 86400.0) -> dict | None:
    """Te same reguły odegrane wstecz raz na dobę (od `start` do `end` minus
    doba): trafność 24 h i 7 dni obok punktów odniesienia - kierunku ówczesnego
    głównego sygnału i „zawsze bok”. Tylko dane znane w danej chwili
    (`analysis_stats` filtruje po czasie). Zasada Jakuba: liczyć od początku
    Hydry."""
    eth = ma.Series(((raw.get("market") or {}).get("coins") or {}).get("ETH"))
    candles = [c for c in raw.get("candles") or [] if c.get("ts") is not None]
    rows = []
    t = start
    j = 0
    sig = "HOLD"
    while t <= end - 86400:
        while j < len(candles) and candles[j]["ts"] <= t:
            sig = candles[j].get("signal") or "HOLD"
            j += 1
        if eth.at(t) is not None:
            a = analyze(ma.analysis_stats(raw, t), "daily")
            rows.append({"ts": int(t), "price": eth.at(t), "24h": a["f24"], "7d": a["f7"], "conf": a["conf"],
                         "signal": sig})
        t += step
    if not rows:
        return None
    sc = ma.score_forecasts(rows, eth, end)
    flat = ma.score_forecasts([{**r, "24h": ma.FLAT, "7d": ma.FLAT} for r in rows], eth, end)
    return {
        "days": len(rows),
        "from_ts": rows[0]["ts"],
        "24h": sc["24h"],
        "7d": sc["7d"],
        "flat_pct": {"24h": flat["24h"]["trafnosc_pct"], "7d": flat["7d"]["trafnosc_pct"]},
    }
