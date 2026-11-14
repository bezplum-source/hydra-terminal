"""Minimal JSON-over-HTTPS GET with retries (etapy 6 i 7 planu "trend").

Shared by the key-less public sources added in those steps (DefiLlama
stablecoins, Deribit). Raises after the last retry - callers wrap it and turn
any failure into a warning, never into a failed pipeline run.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

GetFn = Callable[[str], Any]
USER_AGENT = "hydra-terminal/1.0 (+https://github.com/bezplum-source/hydra-terminal)"


def get_json(url: str, *, timeout: float = 25.0, retries: int = 3, base_delay: float = 1.5) -> Any:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(base_delay * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def with_query(url: str, params: dict) -> str:
    return url + "?" + urllib.parse.urlencode(params)
