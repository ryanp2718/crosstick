"""Tests for the Coinbase Advanced Trade ingester.

Wire contract verified live (see plan): channel `l2_data` with snapshot/update
events carrying absolute `new_quantity`; per-connection `sequence_num`; side
labels `bid`/`offer`; `market_trades` with BUY/SELL; `heartbeats`/`subscriptions`
control frames. Unit tests drive parse_message/process_event directly; the two
integration tests drive the full WS loop via FakeExchangeServer.
"""
from __future__ import annotations

import asyncio
import json
from datetime import UTC
from decimal import Decimal

import pytest

from common.models import BookDelta, BookSnapshot, Side, Trade, decode
from ingest.base_ingester import ResyncRequired, SymbolState
from ingest.book import BookInvariantError
from ingest.coinbase import (
    CoinbaseIngester,
    _l2_side,
    _rfc3339_to_ns,
    _trade_side,
)
from ingest.tests.test_base_ingester import (  # noqa: F401  (fake_server is a fixture)
    FakeProducer,
    fake_server,
)

TS = "2023-02-09T20:32:50.714964855Z"


# ─── helpers ────────────────────────────────────────────────────────────────


class RecordingProducer(FakeProducer):
    """FakeProducer that also captures key/headers kwargs."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, bytes, dict[str, object]]] = []

    async def send(self, topic: str, value: bytes, **kw: object) -> asyncio.Future:
        fut = await super().send(topic, value)
        self.calls.append((topic, value, kw))
        return fut

    async def send_and_wait(self, topic: str, value: bytes, **kw: object) -> None:
        await super().send_and_wait(topic, value)
        self.calls.append((topic, value, kw))


def _l2_frame(seq, *, etype, product="BTC-USD", bids=(), offers=(), ts=TS):
    updates = [{"side": "bid", "price_level": p, "new_quantity": q} for p, q in bids]
    updates += [{"side": "offer", "price_level": p, "new_quantity": q} for p, q in offers]
    return json.dumps({
        "channel": "l2_data",
        "client_id": "",
        "timestamp": ts,
        "sequence_num": seq,
        "events": [{"type": etype, "product_id": product, "updates": updates}],
    }).encode()


def _trades_frame(seq, *, product="BTC-USD", trades=(), ts=TS):
    items = [
        {"trade_id": tid, "product_id": product, "price": px,
         "size": sz, "side": sd, "time": ts}
        for tid, px, sz, sd in trades
    ]
    return json.dumps({
        "channel": "market_trades",
        "timestamp": ts,
        "sequence_num": seq,
        "events": [{"type": "update", "trades": items}],
    }).encode()


def _heartbeat_frame(seq, ts=TS):
    return json.dumps({
        "channel": "heartbeats", "timestamp": ts, "sequence_num": seq,
        "events": [{"current_time": ts, "heartbeat_counter": "1"}],
    }).encode()


def _subs_frame(seq, ts=TS):
    return json.dumps({
        "channel": "subscriptions", "timestamp": ts, "sequence_num": seq,
        "events": [{"subscriptions": {"level2": ["BTC-USD"]}}],
    }).encode()


def _ing(symbols=("BTC-USD",)):
    return CoinbaseIngester(
        producer=RecordingProducer(),
        symbols=list(symbols),
        ws_url="ws://unused",
        stale_timeout=None,
    )


def _msg_count(exchange, channel):
    from common.metrics import messages_received
    return messages_received.labels(exchange=exchange, channel=channel)._value.get()


def make_coinbase(server, symbols=("BTC-USD",), **over):
    kw = dict(
        subscribe_rate=100.0, subscribe_capacity=100.0, queue_maxsize=200,
        backoff_base=0.001, backoff_cap=0.01,
        ping_interval=None, ping_timeout=None, stale_timeout=None,
    )
    kw.update(over)
    return CoinbaseIngester(
        producer=RecordingProducer(), symbols=list(symbols), ws_url=server.url, **kw
    )


# ─── unit: helpers ──────────────────────────────────────────────────────────


def test_rfc3339_to_ns():
    from datetime import datetime
    base = int(datetime(2023, 2, 9, 20, 32, 50, tzinfo=UTC).timestamp()) * 1_000_000_000
    assert _rfc3339_to_ns("2023-02-09T20:32:50.714964855Z") == base + 714964855
    assert _rfc3339_to_ns("2023-02-09T20:32:50.714964Z") == base + 714964000
    assert _rfc3339_to_ns("2023-02-09T20:32:50Z") == base
    assert _rfc3339_to_ns("1970-01-01T00:00:00Z") == 0
    assert _rfc3339_to_ns("") == 0


def test_l2_side_mapping():
    assert _l2_side("bid") is Side.BID
    assert _l2_side("offer") is Side.ASK
    assert _l2_side("ask") is Side.ASK
    with pytest.raises(ResyncRequired):
        _l2_side("sell")


def test_trade_side_mapping():
    assert _trade_side("BUY") is Side.BID
    assert _trade_side("SELL") is Side.ASK
    with pytest.raises(ResyncRequired):
        _trade_side("offer")


# ─── unit: parsing ──────────────────────────────────────────────────────────


def test_build_subscribe_messages():
    ing = _ing(symbols=("BTC-USD", "ETH-USD"))
    msgs = ing.build_subscribe_messages()
    assert len(msgs) == 3
    decoded = [json.loads(m) for m in msgs]
    channels = {d["channel"] for d in decoded}
    assert channels == {"level2", "market_trades", "heartbeats"}
    for d in decoded:
        assert d["type"] == "subscribe"
        assert d["product_ids"] == ["BTC-USD", "ETH-USD"]
        assert "channels" not in d  # singular `channel`, not legacy plural


def test_parse_snapshot():
    ing = _ing()
    evs = ing.parse_message(
        _l2_frame(0, etype="snapshot", bids=[("100", "1")], offers=[("101", "2")]),
        local_recv_ts_ns=999,
    )
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "snapshot"
    assert ev.symbol == "BTC-USD"
    assert ev.sequence == 0
    assert ev.exchange_ts_ns == _rfc3339_to_ns(TS)
    assert ev.local_recv_ts_ns == 999


def test_parse_update_preserves_zero_and_offer():
    ing = _ing()
    ing.parse_message(_l2_frame(0, etype="snapshot", bids=[("100", "1")]), 1)  # baseline
    evs = ing.parse_message(
        _l2_frame(1, etype="update", bids=[("100", "0")], offers=[("101", "5")]), 1
    )
    assert len(evs) == 1
    assert evs[0].kind == "delta"
    updates = evs[0].payload.updates
    quantities = {(u.side, u.price_level): u.new_quantity for u in updates}
    assert quantities[("bid", "100")] == "0"      # removal preserved
    assert quantities[("offer", "101")] == "5"


def test_parse_trades():
    ing = _ing()
    evs = ing.parse_message(
        _trades_frame(0, trades=[("t1", "100", "0.5", "BUY"), ("t2", "101", "0.3", "SELL")]),
        local_recv_ts_ns=5,
    )
    assert len(evs) == 2
    assert all(e.kind == "trade" for e in evs)
    assert {e.payload.trade_id for e in evs} == {"t1", "t2"}
    assert evs[0].payload.side == "BUY"
    assert evs[0].exchange_ts_ns == _rfc3339_to_ns(TS)


def test_parse_heartbeat_and_subscriptions_return_empty():
    ing = _ing()
    assert ing.parse_message(_subs_frame(0), 1) == []
    assert ing.parse_message(_heartbeat_frame(1), 1) == []


def test_connection_seq_baseline_advance_reorder_gap():
    ing = _ing()
    reorder0 = _msg_count("coinbase", "seq_reorder")

    # baseline accepts first seq and returns the event
    evs = ing.parse_message(_l2_frame(5, etype="snapshot", bids=[("100", "1")]), 1)
    assert len(evs) == 1 and evs[0].kind == "snapshot"

    # contiguous advances
    evs = ing.parse_message(_l2_frame(6, etype="update", bids=[("99", "2")]), 1)
    assert len(evs) == 1 and evs[0].kind == "delta"

    # reorder/duplicate (< expected) is ignored + metric ticks
    evs = ing.parse_message(_l2_frame(6, etype="update", bids=[("99", "3")]), 1)
    assert evs == []
    assert _msg_count("coinbase", "seq_reorder") == reorder0 + 1

    # forward gap (> expected) yields a single error sentinel
    evs = ing.parse_message(_l2_frame(100, etype="update", bids=[("98", "1")]), 1)
    assert len(evs) == 1
    assert evs[0].kind == "error"
    assert evs[0].symbol == "BTC-USD"


# ─── unit: process_event ────────────────────────────────────────────────────


async def test_process_snapshot_goes_live_and_emits():
    ing = _ing()
    ctx = ing.contexts["BTC-USD"]
    ev = ing.parse_message(
        _l2_frame(0, etype="snapshot", bids=[("100", "1")], offers=[("101", "2")]), 7
    )[0]
    await ing.process_event(ctx, ev)

    assert ctx.state is SymbolState.LIVE
    assert ctx.book.best_bid() == (Decimal("100"), Decimal("1"))
    assert ctx.book.best_ask() == (Decimal("101"), Decimal("2"))

    topic, value, kw = ing.producer.calls[-1]
    assert topic == "md.book.coinbase.BTC-USD.snapshots"
    assert kw["key"] == b"coinbase:BTC-USD"
    assert ("exchange_ts_ns", str(_rfc3339_to_ns(TS)).encode()) in kw["headers"]
    msg = decode(value)
    assert isinstance(msg, BookSnapshot)
    assert msg.sequence == 0
    assert [(lvl.price, lvl.size) for lvl in msg.bids] == [("100", "1")]
    assert [(lvl.price, lvl.size) for lvl in msg.asks] == [("101", "2")]


async def test_process_delta_updates_and_removes():
    ing = _ing()
    ctx = ing.contexts["BTC-USD"]
    snap = ing.parse_message(
        _l2_frame(0, etype="snapshot", bids=[("100", "1")], offers=[("101", "2")]), 1
    )[0]
    await ing.process_event(ctx, snap)

    delta = ing.parse_message(
        _l2_frame(1, etype="update", bids=[("100", "0"), ("99", "3")], offers=[("101", "5")]), 1
    )[0]
    await ing.process_event(ctx, delta)

    assert ctx.book.best_bid() == (Decimal("99"), Decimal("3"))  # 100 removed
    assert ctx.book.best_ask() == (Decimal("101"), Decimal("5"))  # qty updated
    topic, value, _ = ing.producer.calls[-1]
    assert topic == "md.book.coinbase.BTC-USD.deltas"
    assert isinstance(decode(value), BookDelta)


async def test_process_delta_before_live_resyncs():
    ing = _ing()
    ctx = ing.contexts["BTC-USD"]
    delta = ing.parse_message(_l2_frame(0, etype="update", bids=[("100", "1")]), 1)[0]
    assert ctx.state is SymbolState.BOOTSTRAP
    with pytest.raises(ResyncRequired):
        await ing.process_event(ctx, delta)


async def test_process_crossed_delta_raises_invariant():
    ing = _ing()
    ctx = ing.contexts["BTC-USD"]
    snap = ing.parse_message(
        _l2_frame(0, etype="snapshot", bids=[("100", "1")], offers=[("101", "2")]), 1
    )[0]
    await ing.process_event(ctx, snap)
    # offer at 99 crosses the bid at 100
    crossed = ing.parse_message(_l2_frame(1, etype="update", offers=[("99", "1")]), 1)[0]
    with pytest.raises(BookInvariantError):
        await ing.process_event(ctx, crossed)


async def test_emitted_book_messages_carry_connection_epoch():
    """Snapshot + delta from one connection share that connection's epoch - what
    lets the gateway tell this connection's deltas from a prior (reset-counter)
    one's."""
    ing = _ing()
    ing._reset_contexts()  # assigns a fresh connection epoch
    epoch = ing._epoch
    assert epoch != 0
    ctx = ing.contexts["BTC-USD"]
    snap = ing.parse_message(
        _l2_frame(0, etype="snapshot", bids=[("100", "1")], offers=[("101", "2")]), 1
    )[0]
    await ing.process_event(ctx, snap)
    delta = ing.parse_message(_l2_frame(1, etype="update", bids=[("99", "3")]), 1)[0]
    await ing.process_event(ctx, delta)

    book_msgs = [decode(v) for t, v, _ in ing.producer.calls if t.startswith("md.book.")]
    assert len(book_msgs) == 2
    assert all(m.epoch == epoch for m in book_msgs)


async def test_process_trade_emits_regardless_of_state():
    ing = _ing()
    ctx = ing.contexts["BTC-USD"]
    assert ctx.state is SymbolState.BOOTSTRAP  # no snapshot yet
    ev = ing.parse_message(_trades_frame(0, trades=[("t1", "100", "0.5", "SELL")]), 3)[0]
    await ing.process_event(ctx, ev)

    topic, value, _ = ing.producer.calls[-1]
    assert topic == "md.trades.coinbase.BTC-USD"
    msg = decode(value)
    assert isinstance(msg, Trade)
    assert msg.trade_id == "t1"
    assert msg.side is Side.ASK  # SELL -> ASK


# ─── integration: full WS loop ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_integration_snapshot_updates_trade(fake_server):  # noqa: F811
    fake_server.scripted = [
        _subs_frame(0).decode(),
        _l2_frame(1, etype="snapshot", bids=[("100", "1")], offers=[("101", "2")]).decode(),
        _l2_frame(2, etype="update", bids=[("99", "3")]).decode(),
        _l2_frame(3, etype="update", offers=[("101", "0")]).decode(),  # remove ask
        _trades_frame(4, trades=[("t1", "100", "0.5", "BUY")]).decode(),
    ]
    ing = make_coinbase(fake_server)
    run_task = asyncio.create_task(ing.run())
    for _ in range(200):
        if len(ing.producer.calls) >= 4:
            break
        await asyncio.sleep(0.02)

    # Capture while the connection is still live; run()'s teardown marks STALE.
    ctx = ing.contexts["BTC-USD"]
    live_state = ctx.state
    best_bid, best_ask = ctx.book.best_bid(), ctx.book.best_ask()
    topics = [t for t, _, _ in ing.producer.calls]

    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert live_state is SymbolState.LIVE
    assert "md.book.coinbase.BTC-USD.snapshots" in topics
    assert "md.book.coinbase.BTC-USD.deltas" in topics
    assert "md.trades.coinbase.BTC-USD" in topics
    assert best_ask is None  # ask at 101 removed by the seq-3 delta
    assert best_bid == (Decimal("100"), Decimal("1"))


@pytest.mark.asyncio
async def test_integration_seq_gap_reconnects(fake_server):  # noqa: F811
    fake_server.scripted = [
        _subs_frame(0).decode(),
        _l2_frame(1, etype="snapshot", bids=[("100", "1")], offers=[("101", "2")]).decode(),
        _l2_frame(99, etype="update", bids=[("99", "3")]).decode(),  # forward gap
    ]
    ing = make_coinbase(fake_server)
    run_task = asyncio.create_task(ing.run())
    for _ in range(200):
        if fake_server.connections >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert fake_server.connections >= 2, "connection-seq gap should force reconnect"
