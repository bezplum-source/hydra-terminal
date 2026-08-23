"""Generuje `site/index.html` z historii świec (`data/candles_history.json`)
i szablonu `live/template.html` — ten sam design co artefakt "Hydra
Terminal" na claude.ai (patrz projekt Claude, dokument
`hydrav2-frontend.md`, sekcja "Design system v2").

Osobny, mały moduł — celowo odseparowany od silnika sygnału
(`hydra_signals`), zgodnie z pkt. 4 sekcji "Co dalej" w głównym README.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

MAX_DISPLAY_CANDLES = 500

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = Path(__file__).resolve().parent / "template.html"
SITE_DIR = ROOT / "site"


def _build_streaks(candles: list[dict]) -> list[dict]:
    """Grupuje kolejne świece o tym samym sygnale w okresy (streaks),
    najnowszy pierwszy — identyczna logika jak w `build_real_data_v2.py`
    użytym do ręcznego zbudowania pierwszych wersji dashboardu."""
    streaks: list[dict] = []
    i = 0
    while i < len(candles):
        j = i
        sig = candles[i]["signal"]
        while j + 1 < len(candles) and candles[j + 1]["signal"] == sig:
            j += 1
        start, end = candles[i], candles[j]
        pct = (
            (end["price"] - start["price"]) / start["price"] * 100
            if start["price"]
            else 0.0
        )
        streaks.append(
            {
                "signal": sig,
                "startBlock": start["block"],
                "startPrice": start["price"],
                "endBlock": end["block"],
                "endPrice": end["price"],
                "pct": round(pct, 2),
                "startTime": start["time"],
                "endTime": end["time"],
            }
        )
        i = j + 1
    streaks.reverse()
    return streaks


def build_site(candles_history: list[dict]) -> None:
    display_candles = candles_history[-MAX_DISPLAY_CANDLES:]
    streaks = _build_streaks(display_candles) if display_candles else []

    data = {"candles": display_candles, "streaks": streaks}
    data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    if "__DATA_JSON__" not in template:
        raise RuntimeError("Szablon nie zawiera placeholdera __DATA_JSON__")
    html = template.replace("__DATA_JSON__", data_json)

    marker = "</style>"
    idx = html.find(marker)
    if idx == -1:
        raise RuntimeError("Szablon nie zawiera znacznika </style> — nie da się złożyć pełnego dokumentu HTML")
    head_tail = html[: idx + len(marker)]
    body_part = html[idx + len(marker):]

    full_html = (
        "<!doctype html>\n<html lang=\"pl\">\n<head>\n"
        "<meta charset=\"UTF-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
        + head_tail
        + "\n</head>\n<body>\n"
        + body_part
        + "\n</body>\n</html>\n"
    )

    SITE_DIR.mkdir(parents=True, exist_ok=True)
    (SITE_DIR / "index.html").write_text(full_html, encoding="utf-8")


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    from live import state as st

    build_site(st.load_candles_history())
    print(f"site/index.html zaktualizowany ({len(st.load_candles_history())} świec w historii).")
