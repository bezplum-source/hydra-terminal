"""Testy `live/lake.py` (Faza "jezioro danych", Etap B: R2 + Parquet) — bez
zadnej prawdziwej sieci ani prawdziwego bucketa, tylko:
1. Bez sekretow R2 (albo bez transakcji do zapisania) - czyste, ciche
   pominiecie, zero wywolan do "R2".
2. Z sekretami (klient R2 podmieniony na fake'a przechwytujacego wywolania)
   - poprawny klucz partycji, poprawna zawartosc Parquet, Enumy zamienione
   na zwykle stringi.
3. Kazdy blad podczas wysylki jest zlapany WEWNATRZ `upload_trades()` -
   nigdy nie wypływa na zewnatrz jako wyjatek.
"""

from __future__ import annotations

import io

import pandas as pd
import pytest

from hydra_signals.models import Side, Trade
from live import lake


def _sample_trades():
    return [
        Trade(wallet="good0", block=100, side=Side.BUY, price_usd=2000.0, size_eth=1.5),
        Trade(wallet="bad0", block=105, side=Side.SELL, price_usd=2010.0, size_eth=0.5),
    ]


class FakeR2Client:
    def __init__(self, raise_on_put: Exception | None = None):
        self.put_calls: list[dict] = []
        self._raise_on_put = raise_on_put

    def put_object(self, Bucket: str, Key: str, Body: bytes):  # noqa: N803 - dopasowane do boto3
        if self._raise_on_put is not None:
            raise self._raise_on_put
        self.put_calls.append({"Bucket": Bucket, "Key": Key, "Body": Body})


def test_upload_trades_skips_cleanly_without_r2_secrets(monkeypatch):
    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)

    result = lake.upload_trades("uniswap_mainnet", "2026-09-15", "123", _sample_trades())

    assert result is None


def test_upload_trades_skips_when_no_new_trades(monkeypatch):
    # Nawet z kompletnymi sekretami - pusta lista nie ma czego archiwizowac,
    # wiec funkcja nie powinna nawet probowac zbudowac klienta R2.
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")

    def fail_if_called():
        raise AssertionError("nie powinno probowac budowac klienta R2 dla pustej listy transakcji")

    monkeypatch.setattr(lake, "_r2_client", fail_if_called)

    result = lake.upload_trades("uniswap_mainnet", "2026-09-15", "123", [])

    assert result is None


def test_upload_trades_writes_parquet_with_expected_partition_key(monkeypatch):
    fake_client = FakeR2Client()
    monkeypatch.setattr(lake, "_r2_client", lambda: fake_client)

    trades = _sample_trades()
    result = lake.upload_trades("base", "2026-09-15", "run42", trades)

    assert result == {
        "source": "base",
        "key": "trades/source=base/date=2026-09-15/part-run42.parquet",
        "rows": 2,
    }
    assert len(fake_client.put_calls) == 1
    call = fake_client.put_calls[0]
    assert call["Bucket"] == lake.BUCKET
    assert call["Key"] == "trades/source=base/date=2026-09-15/part-run42.parquet"

    # Body musi byc PRAWDZIWYM, czytelnym Parquet - nie tylko "jakimis bajtami".
    df = pd.read_parquet(io.BytesIO(call["Body"]))
    assert len(df) == 2
    assert set(df["wallet"]) == {"good0", "bad0"}
    # Side (Enum) musi wyladowac jako zwykly string "BUY"/"SELL", nie jako
    # obiekt Enum/jego repr - inaczej Parquet/pyarrow by sie na tym wywrocil
    # albo zapisal cos nieczytelnego dla kogokolwiek poza Pythonem.
    assert set(df["side"]) == {"BUY", "SELL"}


def test_upload_trades_respects_custom_bucket_name(monkeypatch):
    # BUCKET jest stala modulowa, czytana z HYDRA_R2_BUCKET RAZ przy imporcie
    # (dokladnie ta sama konwencja co np. BASE_MAX_NEW_BLOCKS_PER_RUN w
    # run_incremental.py) - w testach podmieniamy wiec samo `lake.BUCKET`,
    # nie zmienna srodowiskowa (ktora nie zostalaby juz ponownie odczytana).
    monkeypatch.setattr(lake, "BUCKET", "moje-wlasne-jezioro")
    fake_client = FakeR2Client()
    monkeypatch.setattr(lake, "_r2_client", lambda: fake_client)

    lake.upload_trades("hyperliquid", "2026-09-15", "run1", _sample_trades())

    assert fake_client.put_calls[0]["Bucket"] == "moje-wlasne-jezioro"


def test_upload_trades_catches_upload_error_and_returns_none(monkeypatch):
    fake_client = FakeR2Client(raise_on_put=ConnectionError("symulowany blad sieciowy R2"))
    monkeypatch.setattr(lake, "_r2_client", lambda: fake_client)

    # Nie moze wyleciec wyjatkiem - to funkcja wywolywana na KRYTYCZNEJ
    # sciezce mainnetu w run_incremental.py, ktora nie ma wlasnego
    # try/except (w przeciwienstwie do sekcji Base).
    result = lake.upload_trades("uniswap_mainnet", "2026-09-15", "run1", _sample_trades())

    assert result is None


def test_upload_trades_catches_r2_client_construction_error_and_returns_none(monkeypatch):
    # NAPRAWA (2026-09-17): `_r2_client()` sama (np. `import boto3`, albo
    # `boto3.client(...)` odrzucajace zle poswiadczenia od razu) wczesniej
    # byla wywolywana PRZED blokiem try/except tej funkcji - blad w tym
    # miejscu wylecialby NIEZLAPANY i ubilby caly `run_incremental.py`, mimo
    # ze ta funkcja jest wywolywana na krytycznej sciezce mainnetu (patrz
    # test wyzej). Ten test odtwarza dokladnie ten scenariusz.
    def raise_on_construct():
        raise RuntimeError("symulowany blad budowy klienta R2 (np. zle poswiadczenia)")

    monkeypatch.setattr(lake, "_r2_client", raise_on_construct)

    result = lake.upload_trades("uniswap_mainnet", "2026-09-15", "run1", _sample_trades())

    assert result is None
