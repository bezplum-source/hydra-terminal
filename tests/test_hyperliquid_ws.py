"""Testy Fazy H0 (pozyskiwanie danych z Hyperliquid) - bez zadnego live
polaczenia sieciowego. Uzywamy podstawionych (fake) polaczen WS, zeby
zweryfikowac logike parsowania i petle nasluchu/reconnectu (ten sam
wzorzec co fake transport HTTP w `test_onchain_rpc.py`)."""

from __future__ import annotations

import json

import pytest

from hydra_signals.data_sources import hyperliquid_ws as hl_ws


# ---------- fake'i do testowania listen() bez sieci ----------


class _FakeWs:
    """Symuluje jedno polaczenie WS: `send()` zapamietuje wyslane payloady,
    iteracja (`async for message in ws`) oddaje kolejne surowe (juz
    zserializowane do stringa) wiadomosci, a potem konczy sie (symulacja
    zamkniecia polaczenia przez serwer/siec)."""

    def __init__(self, raw_messages: list[str]):
        self.raw_messages = raw_messages
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for m in self.raw_messages:
            yield m


class _RaisingWs(_FakeWs):
    """Jak _FakeWs, ale po oddaniu wiadomosci rzuca wyjatek w trakcie
    iteracji - symuluje zerwanie polaczenia w polowie (nie czyste zamkniecie)."""

    async def _gen(self):
        for m in self.raw_messages:
            yield m
        raise ConnectionError("symulowane zerwanie polaczenia")


class _FakeConnections:
    """Symuluje JEDNO wywolanie `connect_factory()` (czyli jeden obiekt
    `websockets.connect(url)`) uzywane jako `async for ws in
    connect_factory()` - oddaje kolejne polaczenia z podanej listy, potem
    konczy iteracje (StopAsyncIteration), tak jakby wiecej reconnectow nie
    bylo dostepnych w ramach TEGO obiektu polaczen."""

    def __init__(self, ws_sequence: list):
        self._seq = iter(ws_sequence)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._seq)
        except StopIteration:
            raise StopAsyncIteration


class _RaisingConnections:
    """Symuluje `connect_factory()`, ktorego SAMO nawiazanie polaczenia
    (czyli `__anext__` na obiekcie polaczen, ZANIM dostaniemy jakiekolwiek
    `ws`) rzuca wyjatek - dokladnie to, co zaobserwowano w zywym
    smoke-tescie w piaskownicy: `websockets.connect(...)` rzucil
    `InvalidProxyStatus` (HTTP 403 od proxy) zanim w ogole doszlo do
    pierwszej wiadomosci."""

    def __init__(self, exc: Exception):
        self._exc = exc

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise self._exc


async def _instant_sleep(_seconds: float) -> None:
    """Podstawiane zamiast `asyncio.sleep` w testach retry - zeby testy nie
    czekaly naprawde na `retry_delay_seconds`."""
    return None


def _trades_envelope(items: list[dict]) -> dict:
    return {"channel": "trades", "data": items}


def _sample_item(**overrides) -> dict:
    item = {
        "coin": "ETH",
        "side": "B",
        "px": "2455.42",
        "sz": "1.5",
        "hash": "0xabc123",
        "time": 1735689600000,
        "tid": 987654321,
        "users": ["0xBUYER", "0xSELLER"],
    }
    item.update(overrides)
    return item


# ---------- parse_trades_message ----------


def test_parse_trades_message_happy_path():
    raw = _trades_envelope([_sample_item()])
    trades = hl_ws.parse_trades_message(raw)
    assert len(trades) == 1
    t = trades[0]
    assert t.coin == "ETH"
    assert t.aggressor_side == hl_ws.AggressorSide.BUY
    assert t.price_usd == 2455.42
    assert t.size_eth == 1.5
    assert t.buyer == "0xBUYER"
    assert t.seller == "0xSELLER"
    assert t.ts_ms == 1735689600000
    assert t.tid == 987654321
    assert t.tx_hash == "0xabc123"


def test_parse_trades_message_ignores_other_channels():
    assert hl_ws.parse_trades_message({"channel": "subscriptionResponse", "data": {}}) == []
    assert hl_ws.parse_trades_message({"channel": "pong"}) == []
    assert hl_ws.parse_trades_message("not even a dict") == []
    assert hl_ws.parse_trades_message(None) == []


def test_parse_trades_message_filters_out_other_coins_defensively():
    # Nie powinno sie zdarzyc (subskrybujemy tylko ETH), ale parser nie
    # ufa slepo ksztaltowi danych z zewnatrz.
    raw = _trades_envelope([_sample_item(coin="BTC")])
    assert hl_ws.parse_trades_message(raw) == []


def test_parse_trades_message_skips_malformed_item_but_keeps_rest_of_batch():
    good = _sample_item()
    malformed = _sample_item(px="not-a-number")
    missing_users = {**_sample_item(), "users": ["0xONLYONE"]}
    raw = _trades_envelope([malformed, good, missing_users])
    trades = hl_ws.parse_trades_message(raw)
    assert len(trades) == 1
    assert trades[0].buyer == "0xBUYER"


def test_parse_trades_message_multiple_trades_in_one_batch():
    raw = _trades_envelope([_sample_item(tid=1), _sample_item(tid=2, side="A")])
    trades = hl_ws.parse_trades_message(raw)
    assert len(trades) == 2
    assert trades[1].aggressor_side == hl_ws.AggressorSide.SELL


# ---------- trade_to_json_record / json_record_to_trade ----------


def test_trade_json_record_roundtrip():
    original = hl_ws.parse_trades_message(_trades_envelope([_sample_item()]))[0]
    record = hl_ws.trade_to_json_record(original)
    # musi byc JSON-serializowalne (to trafia do pliku JSONL)
    reloaded = hl_ws.json_record_to_trade(json.loads(json.dumps(record)))
    assert reloaded == original


# ---------- prune_trade_records ----------


def test_prune_trade_records_keeps_recent_drops_old():
    now_ms = 1_000_000_000_000
    hour_ms = 3600 * 1000
    records = [
        {"ts_ms": now_ms - 1 * hour_ms},  # 1h temu - zostaje
        {"ts_ms": now_ms - 47 * hour_ms},  # 47h temu - zostaje (lookback=48h)
        {"ts_ms": now_ms - 49 * hour_ms},  # 49h temu - odrzucone
    ]
    kept = hl_ws.prune_trade_records(records, now_ms=now_ms, lookback_hours=48.0)
    assert len(kept) == 2


def test_prune_trade_records_drops_records_without_valid_timestamp():
    records = [{"ts_ms": "not-an-int"}, {"no_ts_field": True}, {"ts_ms": 500}]
    kept = hl_ws.prune_trade_records(records, now_ms=1000, lookback_hours=1.0)
    assert kept == [{"ts_ms": 500}]


# ---------- listen() ----------


async def test_listen_calls_on_trades_for_each_batch_with_trades():
    msg1 = json.dumps(_trades_envelope([_sample_item(tid=1)]))
    msg2 = json.dumps({"channel": "subscriptionResponse"})  # brak transakcji - nie wola on_trades
    msg3 = json.dumps(_trades_envelope([_sample_item(tid=2), _sample_item(tid=3)]))

    ws = _FakeWs([msg1, msg2, msg3])

    # UWAGA: poniewaz `listen()` teraz samo retry'uje NA ZEWNATRZ (nowe
    # `connect_factory()` przy kazdej probie - patrz komentarz w
    # implementacji), samo naturalne wyczerpanie sie polaczenia (bez
    # wyjatku) NIE konczy funkcji - to jest zamierzone (tak dziala tez
    # prawdziwy `websockets.connect()`: reconnectuje w kolko, az minie
    # zewnetrzny deadline). Dlatego now_fn tutaj rosnie z kazdym
    # wywolaniem i duration_seconds jest dobrane tak, by deadline minal
    # TUZ PO przetworzeniu wszystkich trzech wiadomosci z jedynego
    # polaczenia, zanim dojdzie do drugiej proby connect_factory()
    # (ktora powtorzylaby te same wiadomosci i falszywie podbila wynik).
    call_count = {"n": 0}

    def fake_now():
        call_count["n"] += 1
        return call_count["n"] * 1.0

    received_batches: list[list] = []
    await hl_ws.listen(
        received_batches.append,
        duration_seconds=5.5,
        connect_factory=lambda: _FakeConnections([ws]),
        now_fn=fake_now,
    )

    assert len(received_batches) == 2
    assert len(received_batches[0]) == 1
    assert len(received_batches[1]) == 2
    # subskrypcja faktycznie wyslana na poczatku polaczenia
    assert len(ws.sent) == 1
    assert json.loads(ws.sent[0]) == hl_ws.build_subscribe_message("ETH")


async def test_listen_stops_at_deadline_even_mid_stream():
    many_messages = [json.dumps(_trades_envelope([_sample_item(tid=i)])) for i in range(100)]
    ws = _FakeWs(many_messages)

    # now_fn zwraca rosnacy czas przy kazdym wywolaniu - po kilku
    # wiadomosciach "uplynie" deadline.
    call_count = {"n": 0}

    def fake_now():
        call_count["n"] += 1
        return call_count["n"] * 1.0

    received_batches: list[list] = []
    await hl_ws.listen(
        received_batches.append,
        duration_seconds=3.0,  # ~3 "wywolania" fake_now przed deadline'em
        connect_factory=lambda: _FakeConnections([ws]),
        now_fn=fake_now,
    )

    # nie przetworzylismy wszystkich 100 wiadomosci - deadline przerwal wczesniej
    assert 0 < len(received_batches) < 100


async def test_listen_reconnects_after_dropped_connection():
    msg_before_drop = json.dumps(_trades_envelope([_sample_item(tid=1)]))
    msg_after_reconnect = json.dumps(_trades_envelope([_sample_item(tid=2)]))

    ws1 = _RaisingWs([msg_before_drop])
    ws2 = _FakeWs([msg_after_reconnect])
    # Wspoldzielony iterator - kazde wywolanie connect_factory() zwraca NOWY
    # obiekt _FakeConnections, ale ciagnie z TEGO SAMEGO miejsca w sekwencji
    # (dokladnie tak, jak nowe `websockets.connect(url)` po nieudanej
    # probie dalej dostaje "kolejne" polaczenie z serwera, a nie od nowa
    # pierwsze). To odzwierciedla nasza wlasna petle retry w listen(),
    # ktora przy kazdej probie tworzy nowy obiekt polaczen.
    shared_sequence = iter([ws1, ws2])

    # now_fn rosnie z kazdym wywolaniem - duration_seconds dobrane tak, by
    # deadline minal TUZ PO odebraniu wiadomosci z drugiego (odzyskanego)
    # polaczenia, a PRZED trzecia proba connect_factory() (ktora inaczej
    # probowalaby w kolko, bo shared_sequence jest juz wyczerpana - patrz
    # test `test_listen_stops_cleanly_when_no_more_connections_available`
    # dla tej samej mechaniki).
    call_count = {"n": 0}

    def fake_now():
        call_count["n"] += 1
        return call_count["n"] * 1.0

    received_batches: list[list] = []
    await hl_ws.listen(
        received_batches.append,
        duration_seconds=7.5,
        connect_factory=lambda: _FakeConnections(shared_sequence),
        now_fn=fake_now,
        retry_delay_seconds=0.0,
        sleep_fn=_instant_sleep,
    )

    # transakcja z PRZED zerwania i transakcja PO reconnect obie zebrane
    assert len(received_batches) == 2
    assert received_batches[0][0].tid == 1
    assert received_batches[1][0].tid == 2
    # subskrypcja wyslana ponownie po reconnect (na nowym polaczeniu)
    assert len(ws2.sent) == 1


async def test_listen_stops_cleanly_when_no_more_connections_available():
    # _FakeConnections konczy sie po jednym polaczeniu (StopAsyncIteration,
    # BEZ wyjatku) - zewnetrzna petla `while now_fn() < deadline` wtedy
    # normalnie probuje jeszcze raz przez connect_factory(); poniewaz
    # kazde nowe wywolanie fabryki tez zwraca "pusta juz" sekwencje, test
    # weryfikuje, ze i tak NIE wpadamy w nieskonczona petle w praktyce -
    # symulujemy to poprzez now_fn, ktore po pierwszym batchu "przekracza"
    # deadline, wiec petla `while` konczy sie sama.
    ws = _FakeWs([json.dumps(_trades_envelope([_sample_item()]))])

    call_count = {"n": 0}

    def fake_now():
        call_count["n"] += 1
        # Pozwala dokonczyc obsluge jedynej wiadomosci (4 wywolania now_fn:
        # deadline, warunek while, warunek po polaczeniu, warunek po
        # wiadomosci), a deadline "mija" dopiero przy kolejnej probie
        # zewnetrznej petli `while`.
        return 0.0 if call_count["n"] <= 4 else 100.0

    received_batches: list[list] = []
    await hl_ws.listen(
        received_batches.append,
        duration_seconds=1.0,
        connect_factory=lambda: _FakeConnections([ws]),
        now_fn=fake_now,
        retry_delay_seconds=0.0,
        sleep_fn=_instant_sleep,
    )
    assert len(received_batches) == 1


async def test_listen_retries_with_fresh_connection_when_connect_itself_raises():
    """Reprodukuje dokladnie to, co zobaczylismy w zywym smoke-tescie w
    piaskownicy: SAMO nawiazanie polaczenia (nie odbior wiadomosci) rzuca
    wyjatek (tam: `InvalidProxyStatus` / HTTP 403 od proxy). `listen()` NIE
    powinno sie wtedy wywalic, tylko sprobowac ponownie z NOWYM obiektem
    polaczen (`connect_factory()` wywolane od nowa), az do deadline'u."""

    calls = {"n": 0}

    def flaky_factory():
        calls["n"] += 1
        if calls["n"] < 3:
            return _RaisingConnections(ConnectionRefusedError("403 Forbidden (symulacja proxy)"))
        # Trzecia proba wreszcie sie udaje.
        return _FakeConnections([_FakeWs([json.dumps(_trades_envelope([_sample_item(tid=99)]))])])

    # now_fn rosnie z kazdym wywolaniem (1.0, 2.0, 3.0, ...) - duration_seconds
    # dobrane tak, zeby deadline minal DOKLADNIE po odebraniu jedynej
    # wiadomosci z trzeciej (udanej) proby polaczenia, a nie wczesniej.
    call_count = {"n": 0}

    def fake_now():
        call_count["n"] += 1
        return call_count["n"] * 1.0

    received_batches: list[list] = []
    await hl_ws.listen(
        received_batches.append,
        duration_seconds=7.5,
        connect_factory=flaky_factory,
        now_fn=fake_now,
        retry_delay_seconds=0.0,
        sleep_fn=_instant_sleep,
    )

    assert calls["n"] == 3
    assert len(received_batches) == 1
    assert received_batches[0][0].tid == 99


async def test_listen_gives_up_at_deadline_when_connect_keeps_failing():
    """Jesli polaczenie NIGDY sie nie udaje (trwala blokada, np. proxy
    zawsze zwraca 403 - dokladnie przypadek tej piaskownicy), `listen()`
    ma czysto zakonczyc dzialanie po uplywie `duration_seconds`, zamiast
    probowac w nieskonczonosc albo propagowac wyjatek na zewnatrz."""

    def always_raising_factory():
        return _RaisingConnections(ConnectionRefusedError("403 Forbidden (symulacja proxy)"))

    call_count = {"n": 0}

    def fake_now():
        call_count["n"] += 1
        # Po kilku probach "uplywa" deadline.
        return 0.0 if call_count["n"] < 4 else 100.0

    received_batches: list[list] = []
    # Nie powinno rzucic wyjatku ani sie zawiesic.
    await hl_ws.listen(
        received_batches.append,
        duration_seconds=1.0,
        connect_factory=always_raising_factory,
        now_fn=fake_now,
        retry_delay_seconds=0.0,
        sleep_fn=_instant_sleep,
    )

    assert received_batches == []
