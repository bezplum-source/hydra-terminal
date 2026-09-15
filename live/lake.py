"""Faza "jezioro danych" (Etap B planu z 2026-09-15, w odpowiedzi na
przeglad infrastruktury zaproponowany przez kolege uzytkownika - patrz
`hydra-lake-plan.html`) - archiwizuje surowe transakcje zebrane w KAZDYM
uruchomieniu `run_incremental.py` do Cloudflare R2 (magazyn obiektowy
kompatybilny z API S3), w formacie Parquet, partycjonowane wedlug zrodla
i daty:

    trades/source=uniswap_mainnet/date=2026-09-15/part-<run_id>.parquet
    trades/source=base/date=2026-09-15/part-<run_id>.parquet
    trades/source=hyperliquid/date=2026-09-15/part-<run_id>.parquet

PO CO: `trade_buffer.csv`/`base_trade_buffer.csv`/bufor Hyperliquid sa
ROLLING - przycinane do okna klasyfikacji (patrz `run_incremental.py`).
Dzis nic nie trzyma surowej historii transakcji trwale - jesli kiedys
zechcemy zrobic prawdziwy backtest dluzszy niz biezace okno lookback,
dzisiejszych danych, ktorych nie zarchiwizujemy, juz nie bedzie (trzeba by
je od nowa sciagac z RPC). Ten modul archiwizuje dokladnie DELTE nowych
transakcji z kazdego uruchomienia, PRZED przycieciem bufora - nic sie nie
dubluje miedzy uruchomieniami, nic nie ginie po przycieciu.

BEZPIECZENSTWO/IZOLACJA: brak sekretow R2 (`R2_ACCOUNT_ID`/
`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`) - dokladnie tak jak brak
`ALCHEMY_BASE_RPC_URL` dla Base - oznacza czyste, ciche pominiecie tego
kroku, reszta pipeline'u dziala bez zmian. Kazdy blad (siec, R2 padniety,
zle poswiadczenia) jest zlapany WEWNATRZ `upload_trades()` - funkcja NIGDY
nie rzuca wyjatku na zewnatrz, zawsze zwraca `None` przy niepowodzeniu.
To wazne, bo w przeciwienstwie do sekcji Base (juz owinietej w try/except
w run_incremental.py), wywolanie dla mainnetu siedzi na KRYTYCZNEJ sciezce -
awaria jeziora danych nie moze wywrocic liczenia sygnalu.
"""

from __future__ import annotations

import io
import os
from dataclasses import asdict, is_dataclass
from typing import Any

# Nazwa bucketa NIE jest sekretem (nie ma w niej nic wrazliwego) - zwykla
# zmienna srodowiskowa z sensownym domyslnym, tej samej konwencji co
# HYDRA_BASE_MAX_NEW_BLOCKS_PER_RUN i podobne w run_incremental.py. Mozna
# ja ustawic wprost w update.yml (env:), bez zakladania GitHub Secret.
BUCKET = os.environ.get("HYDRA_R2_BUCKET", "hydra-terminal-lake")


def log(msg: str) -> None:
    print(f"[hydra] Jezioro danych: {msg}", flush=True)


def _trade_to_record(trade: Any) -> dict:
    """Zamienia jeden Trade/HyperliquidTrade (frozen dataclass) na plaski
    slownik gotowy do zapisu w Parquet - Enumy (np. `Side.BUY`) na ich
    wartosc string, zeby pyarrow/pandas nie musialy nic zgadywac."""
    record = asdict(trade) if is_dataclass(trade) else dict(trade)
    for key, value in record.items():
        if hasattr(value, "value") and not isinstance(value, (int, float, str, bool)):
            record[key] = value.value
    return record


def _r2_client():
    """Zwraca klienta S3-kompatybilnego wskazujacego na R2, albo `None`
    gdy ktorykolwiek z trzech sekretow nie jest ustawiony - wywolujacy
    (`upload_trades` nizej) traktuje `None` jako "pomin ten krok", nie
    jako blad."""
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key_id = os.environ.get("R2_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    if not (account_id and access_key_id and secret_access_key):
        return None

    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )


def upload_trades(source: str, run_date: str, run_id: str, trades: list) -> dict | None:
    """Archiwizuje `trades` (liste Trade/HyperliquidTrade z TEGO uruchomienia,
    PRZED przycieciem rolling bufora) jako jeden plik Parquet w R2, pod
    kluczem `trades/source=<source>/date=<run_date>/part-<run_id>.parquet`.

    Zwraca `{"key": ..., "rows": ...}` przy udanym zapisie (do wpisania w
    manifest, patrz `run_incremental.py`), albo `None` gdy nie ma czego
    zapisywac (`trades` puste), brak sekretow R2, albo zapis sie nie udal -
    w KAZDYM z tych przypadkow reszta pipeline'u dziala dalej bez zmian."""
    if not trades:
        return None

    client = _r2_client()
    if client is None:
        log("brak sekretow R2 (R2_ACCOUNT_ID/R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY) - pomijam archiwizacje.")
        return None

    try:
        import pandas as pd

        records = [_trade_to_record(t) for t in trades]
        buf = io.BytesIO()
        pd.DataFrame.from_records(records).to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)

        key = f"trades/source={source}/date={run_date}/part-{run_id}.parquet"
        client.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    except Exception as exc:  # noqa: BLE001
        log(f"BLAD wysylki do R2 ({exc!r}) - pomijam ten krok w tym uruchomieniu.")
        return None

    log(f"zapisano {len(trades)} transakcji ({source}) do {key}")
    return {"source": source, "key": key, "rows": len(trades)}
