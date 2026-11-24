"""Raport dzienny / tygodniowy do archiwum `data/reports.json` (GitHub Actions).

Prośba Jakuba z 24.11.2026: zakładka na stronie z raportami dostępnymi
historycznie (krótkie wpisy do rozwinięcia, osobno DZIENNE i TYGODNIOWE).
Wybrał wariant: raport powstaje w GitHub Actions (`.github/workflows/reports.yml`,
codziennie ~19:40 czasu polskiego, w niedzielę wersja tygodniowa), ocenę rynku
pisze Claude przez API Anthropic, a wpis trafia do repo i na stronę
(`site/reports.html`, budowaną przez `live/build_site.py`).

Klucz API: sekret repo `ANTHROPIC_API_KEY`, który Jakub dodaje sam (Settings ->
Secrets and variables -> Actions). Bez klucza albo przy błędzie API raport i tak
powstaje - z oceną złożoną z reguł (`rule_analysis`), oznaczoną na stronie jako
„reguły”. Klucz nigdy nie jest wypisywany do logów.

Model: zmienna `HYDRA_REPORT_MODEL`, a gdy jej brak - najnowszy model „sonnet”
z listy `GET /v1/models` (żeby nie wpisywać na sztywno nazwy, która się
zestarzeje), a gdy i to zawiedzie - najnowszy z listy.

Prognozy (24 h / 7 dni) są rozliczane z archiwum (`market_analysis.score_forecasts`)
obok punktu odniesienia „kierunek głównego sygnału”.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from live import market_analysis as ma  # noqa: E402
from live import report_card as rc  # noqa: E402
from live import state as st  # noqa: E402

API = "https://api.anthropic.com/v1"
API_VERSION = "2023-06-01"
MAX_ENTRIES = 1500
DEDUP_S = 6 * 3600  # drugi raport tego samego typu w ciągu 6 h jest pomijany
DIRS = (ma.UP, ma.DOWN, ma.FLAT)
CONFS = ("niska", "umiarkowana", "wysoka")

SYSTEM = """Jesteś analitykiem rynku w papierowym (nie handlującym prawdziwymi pieniędzmi) systemie sygnałów ETH „Hydra” prowadzonym przez Jakuba. Piszesz po polsku, zwięźle, konkretnie, z liczbami. To nie jest porada inwestycyjna i nie udzielasz porad - oceniasz rynek i podajesz prognozę kierunku z uczciwie zaznaczoną niepewnością.

Dostajesz statystyki z wielu okresów (24 h, 3/7/14/30/90 dni, od początku Hydry 23.08.2026): zmiany ETH i BTC, odległość od średnich 14/50/100 dni i nachylenie średniej 14 dni, RSI 14 dni, zakres 30 dni, zmienność, ETH vs BTC, głosowanie strategii (główny sygnał = przewaga głosów LONG/SHORT 14 strategii) teraz i 1/3/7 dni temu, funding i open interest z Hyperliquid, wymuszone zamknięcia, agresorów, podaż stablecoinów, DVOL i skew opcji z Deribit, portfele z wyczuciem, wyścig strategii na żywo, proste podpowiedzi regułowe (`hints`) oraz trafność dotychczasowych prognoz (`forecast_score`) obok punktu odniesienia „kierunek głównego sygnału”. Możesz też dostać kilka poprzednich raportów.

Zasady:
- opieraj się na liczbach z danych, porównuj krótkie i dłuższe okresy, powiedz co się zmieniło;
- nie powtarzaj ślepo głosowania - wolno się z nim nie zgodzić, z uzasadnieniem;
- „bok” = spodziewana zmiana ceny w granicach ±1% (24 h) / ±3% (7 dni);
- pewność „wysoka” tylko przy zgodnych sygnałach z wielu źródeł i okresów;
- jeśli trafność prognoz jest gorsza niż punkt odniesienia, napisz to wprost w polu "track";
- wskaż poziom ceny, który zmieniłby obraz (np. powrót nad średnią 14 dni).

Odpowiedz WYŁĄCZNIE jednym obiektem JSON (bez komentarza, bez bloków kodu):
{"stage": "2-4 słowa etapu rynku, np. bessa · wyprzedanie", "title": "jedno krótkie zdanie-nagłówek (do 90 znaków)", "text": "...", "f24": "wzrost|spadek|bok", "f7": "wzrost|spadek|bok", "conf": "niska|umiarkowana|wysoka", "track": "jedno zdanie o trafności prognoz"}"""

LIMITS = {"daily": "Pole text: MAKS. 4 krótkie zdania (~70 słów).",
          "weekly": "To raport TYGODNIOWY: podsumuj cały tydzień (jak zmienił się etap rynku, kontrakty, opcje, stablecoiny, portfele, jak radził sobie główny sygnał) i daj prognozę na najbliższy tydzień. Pole text: MAKS. 6 krótkich zdań (~110 słów)."}

PostFn = Callable[[str, dict | None, dict], Any]


def _http(url: str, body: dict | None, headers: dict, timeout: float = 60.0) -> Any:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _headers(key: str) -> dict:
    return {"x-api-key": key, "anthropic-version": API_VERSION, "content-type": "application/json"}


def pick_model(key: str, http: PostFn = _http) -> str:
    env = os.environ.get("HYDRA_REPORT_MODEL")
    if env:
        return env
    data = (http(API + "/models?limit=100", None, _headers(key)) or {}).get("data") or []
    models = sorted(data, key=lambda m: m.get("created_at") or "", reverse=True)
    for m in models:
        if "sonnet" in (m.get("id") or ""):
            return m["id"]
    if models:
        return models[0]["id"]
    raise RuntimeError("lista modeli jest pusta")


def parse_analysis(text: str) -> dict:
    """Wyciąga obiekt JSON z odpowiedzi i sprawdza pola."""
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        raise ValueError("brak JSON w odpowiedzi")
    a = json.loads(m.group(0))
    out = {k: str(a.get(k, "")).strip() for k in ("stage", "title", "text", "track")}
    for k in ("f24", "f7"):
        v = str(a.get(k, "")).strip().lower()
        if v not in DIRS:
            raise ValueError(f"niepoprawne {k}: {v!r}")
        out[k] = v
    c = str(a.get("conf", "")).strip().lower()
    out["conf"] = c if c in CONFS else "niska"
    if not out["text"]:
        raise ValueError("pusty tekst")
    return out


def call_claude(stats: dict, mode: str, previous: list[dict], key: str, http: PostFn = _http) -> tuple[dict, str]:
    model = pick_model(key, http)
    prev = [{k: e.get(k) for k in ("time", "type", "stage", "text", "f24", "f7", "conf", "price")} for e in previous[-3:]]
    user = (
        f"Typ raportu: {'TYGODNIOWY' if mode == 'weekly' else 'DZIENNY'}. {LIMITS[mode]}\n\n"
        f"<dane>\n{json.dumps(stats, ensure_ascii=False)}\n</dane>\n\n"
        f"<poprzednie_raporty>\n{json.dumps(prev, ensure_ascii=False)}\n</poprzednie_raporty>"
    )
    body = {"model": model, "max_tokens": 1200, "system": SYSTEM, "messages": [{"role": "user", "content": user}]}
    resp = http(API + "/messages", body, _headers(key))
    text = "".join(b.get("text", "") for b in (resp or {}).get("content") or [] if b.get("type") == "text")
    return parse_analysis(text), model


def rule_analysis(stats: dict, mode: str) -> dict:
    """Ocena bez AI (brak klucza albo błąd API): etap i prognoza z prostych reguł."""
    e = stats.get("eth") or {}
    sig = (stats.get("signal") or {}).get("now")
    d14, slope, rsi = e.get("dist_sma14d_pct"), e.get("sma14d_slope_7d_pct"), e.get("rsi14d")
    if d14 is not None and slope is not None and d14 < 0 and slope < 0:
        stage = "trend spadkowy"
    elif d14 is not None and slope is not None and d14 > 0 and slope > 0:
        stage = "trend wzrostowy"
    else:
        stage = "bez wyraźnego trendu"
    if rsi is not None and rsi < 30:
        stage += " · wyprzedanie"
    elif rsi is not None and rsi > 70:
        stage += " · wykupienie"
    ch = e.get("change_pct") or {}
    parts = [f"ETH: {rc.fmt_pct(ch.get('24h'))} w 24 h, {rc.fmt_pct(ch.get('7d'))} w 7 dni, {rc.fmt_pct(ch.get('30d'))} w 30 dni."]
    parts += [h[0].upper() + h[1:] + "." for h in (stats.get("hints") or [])[:3]]
    f7 = {"LONG": ma.UP, "SHORT": ma.DOWN}.get(sig, ma.FLAT)
    sc = (stats.get("forecast_score") or {}).get("24h") or {}
    track = (f"Prognozy 24 h: {sc['trafione']}/{sc['rozliczone']} trafione." if sc.get("rozliczone")
             else "Brak rozliczonych prognoz.")
    return {"stage": stage, "title": f"Ocena automatyczna (reguły): {stage}", "text": " ".join(parts),
            "f24": ma.FLAT, "f7": f7, "conf": "niska", "track": track}


def build_entry(s: dict, stats: dict, analysis: dict, mode: str, source: str, now: float) -> dict:
    loc = rc.local(now)
    sig = s.get("signal") or {}
    price = (stats.get("eth") or {}).get("now") or (s.get("price") or {}).get("value")
    e = stats.get("eth") or {}
    ch = e.get("change_pct") or {}
    der, op, sb, wl = (stats.get(k) or {} for k in ("derivs", "options", "stables", "wallets"))
    rows = (s.get("race") or {}).get("rows") or []
    race = {r["key"]: round(r["result"], 2) for r in rows if r["key"] in ("vote", "eth", "pullback", "trend14")}
    best = max((r for r in rows if r["key"] not in ("vote", "eth")), key=lambda r: r["result"], default=None)
    return {
        "id": f"{loc.strftime('%Y-%m-%d')}-{mode}",
        "type": mode,
        "ts": int(now),
        "time": loc.strftime("%d.%m.%Y %H:%M"),
        "price": price,
        "signal": sig.get("value"),
        "signalSince": sig.get("since"),
        "signalEntry": sig.get("entryPrice"),
        "signalPnl": ma._r(sig.get("pnlPct")),
        "votes": (s.get("votes") or {}).get("now"),
        **{k: analysis.get(k) for k in ("stage", "title", "text", "f24", "f7", "conf", "track")},
        "source": source,
        "alarms": s.get("alarms") or [],
        "stats": {
            "chg24h": ch.get("24h"), "chg7d": ch.get("7d"), "chg30d": ch.get("30d"),
            "distSma14": e.get("dist_sma14d_pct"), "rsi14": e.get("rsi14d"),
            "fundingApr24h": (der.get("funding_apr_pct") or {}).get("sr_24h"),
            "oiChg24h": (der.get("oi_change_pct") or {}).get("24h"),
            "stables7d": sb.get("change_7d_pct"), "dvol": op.get("dvol_now"), "dvolSma": op.get("dvol_sma14d"),
            "skew": op.get("skew_now"), "skilled7d": wl.get("skilled_net7d_eth"),
        },
        "race": race,
        "raceBest": {"name": best["name"], "result": round(best["result"], 2)} if best else None,
    }


def resolve_mode(mode: str, now: float) -> str:
    if mode in ("daily", "weekly"):
        return mode
    return "weekly" if rc.local(now).isoweekday() == 7 else "daily"


def main(argv: list[str] | None = None, *, http: PostFn = _http) -> int:
    ap = argparse.ArgumentParser(description="Raport do archiwum data/reports.json")
    ap.add_argument("--mode", choices=("auto", "daily", "weekly"), default="auto")
    ap.add_argument("--now", type=float, default=None)
    ap.add_argument("--force", action="store_true", help="dopisz mimo raportu tego typu z ostatnich 6 h")
    args = ap.parse_args(argv)
    now = args.now or dt.datetime.now(dt.timezone.utc).timestamp()
    mode = resolve_mode(args.mode, now)
    reports = st.load_reports()
    last_same = next((r for r in reversed(reports) if r.get("type") == mode), None)
    if last_same and now - last_same.get("ts", 0) < DEDUP_S and not args.force:
        print(f"Raport {mode} z {last_same.get('time')} jest młodszy niż 6 h - pomijam.")
        return 0
    raw = rc.load(ROOT)
    s = rc.collect(raw, now, mode)
    stats = ma.analysis_stats(raw, now)
    eth = ma.Series(((raw.get("market") or {}).get("coins") or {}).get("ETH"))
    stats["forecast_score"] = ma.score_forecasts(ma.rows_from_reports(reports), eth, now)
    stats["race_live"] = [{k: r[k] for k in ("name", "result", "change", "position", "trades")}
                          for r in (s.get("race") or {}).get("rows", [])]
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    source = "reguły"
    analysis = None
    if key:
        try:
            analysis, model = call_claude(stats, mode, reports, key, http)
            source = f"Claude ({model})"
        except (urllib.error.URLError, OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
            code = getattr(exc, "code", "")
            print(f"UWAGA: API Claude nie odpowiedziało poprawnie ({type(exc).__name__} {code}) - ocena z reguł.")
    else:
        print("Brak sekretu ANTHROPIC_API_KEY - ocena z reguł.")
    if analysis is None:
        analysis = rule_analysis(stats, mode)
    entry = build_entry(s, stats, analysis, mode, source, now)
    reports = [r for r in reports if r.get("id") != entry["id"]] + [entry]
    st.save_reports(reports[-MAX_ENTRIES:])
    print(f"Raport {mode} ({source}): {entry['stage']} · 24 h {entry['f24']} · 7 dni {entry['f7']} ({entry['conf']}).")
    print(entry["text"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
