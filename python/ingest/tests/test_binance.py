"""Tests for the Binance Spot ingester.

Wire contract verified against current Binance docs (see plan): diff-depth events
(e="depthUpdate", U/u update ids, b/a price-qty string arrays), trade events
(e="trade", m=buyer-is-maker), REST /api/v3/depth snapshot (lastUpdateId, bids,
asks). Unit tests drive parse_message / process_event / the U/u sync directly;
integration tests drive the full WS loop via FakeExchangeServer with an injected
(no-network) REST snapshot.
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from common.models import BookDelta, BookLevel, BookSnapshot, Side, Trade, decode
from ingest.base_ingester import ResyncRequired, SymbolState
from ingest.binance import _FETCH_FAILED, BinanceIngester
from ingest.book import BookInvariantError
from ingest.tests.test_base_ingester import (  # noqa: F401  (fake_server is a fixture)
    FakeProducer,
    fake_server,
)

# ─── helpers ─────────────────────────────────────────────────────────────────


class RecordingProducer(FakeProducer):
    """FakeProducer that also captures key/headers kwargs."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, bytes, dict[str, object]]] = []

    async def send_and_wait(self, topic: str, value: bytes, **kw: object) -> None:
        await super().send_and_wait(topic, value)
        self.calls.append((topic, value, kw))


def _depth_frame(s, U, u, *, bids=(), asks=(), E=1):
    return json.dumps({
        "e": "depthUpdate", "E": E, "s": s, "U": U, "u": u,
        "b": [[p, q] for p, q in bids], "a": [[p, q] for p, q in asks],
    }).encode()


def _trade_frame(s, *, tid, p, q, m, T=1):
    return json.dumps({
        "e": "trade", "E": T, "s": s, "t": tid, "p": p, "q": q, "T": T, "m": m,
    }).encode()


def _ack_frame(i=1):
    return json.dumps({"result": None, "id": i}).encode()


def _ing(symbols=("BTCUSDT",)):
    return BinanceIngester(
        producer=RecordingProducer(), symbols=list(symbols),
        ws_url="ws://unused", stale_timeout=None,
    )


def _wire(levels):
    return [BookLevel(price=p, size=s) for p, s in levels]


def _inject_snapshot(ing, symbol, last_id, bids, asks):
    """Mimic a completed REST fetch: BUFFERING + a pending snapshot for the applier."""
    ing.contexts[symbol].set_state(SymbolState.BUFFERING, reason="test")
    ing._snapshots[symbol] = (last_id, _wire(bids), _wire(asks))


def make_binance(server, symbols=("BTCUSDT",), snapshots=None, fail=False, delay=0.0, **over):
    snaps = snapshots or {}

    class _Fake(BinanceIngester):
        async def _rest_snapshot(self, symbol):
            if delay:
                await asyncio.sleep(delay)
            if fail:
                raise RuntimeError("rest down")
            last_id, bids, asks = snaps[symbol]
            return last_id, _wire(bids), _wire(asks)

    kw = dict(
        subscribe_rate=100.0, subscribe_capacity=100.0, queue_maxsize=500,
        backoff_base=0.001, backoff_cap=0.01,
        ping_interval=None, ping_timeout=None, stale_timeout=None,
    )
    kw.update(over)
    return _Fake(producer=RecordingProducer(), symbols=list(symbols), ws_url=server.url, **kw)


# ─── unit: subscribe + parse ──────────────────────────────────────────────────


def test_build_subscribe_messages():
    msgs = _ing(symbols=("BTCUSDT", "ETHUSDT")).build_subscribe_messages()
    assert len(msgs) == 1
    d = json.loads(msgs[0])
    assert d["method"] == "SUBSCRIBE"
    assert d["params"] == [
        "btcusdt@depth@100ms", "btcusdt@trade",
        "ethusdt@depth@100ms", "ethusdt@trade",
    ]


def test_parse_depth():
    ev = _ing().parse_message(
        _depth_frame("BTCUSDT", 10, 15, bids=[("100", "1")], asks=[("101", "2")], E=123), 999
    )
    assert len(ev) == 1
    assert ev[0].kind == "delta"
    assert ev[0].symbol == "BTCUSDT"
    assert ev[0].payload.U == 10 and ev[0].payload.u == 15
    assert ev[0].exchange_ts_ns == 123_000_000
    assert ev[0].local_recv_ts_ns == 999


def test_parse_trade():
    ev = _ing().parse_message(_trade_frame("BTCUSDT", tid=7, p="100", q="0.5", m=True, T=5), 1)
    assert len(ev) == 1 and ev[0].kind == "trade"
    assert ev[0].payload.t == 7
    assert ev[0].exchange_ts_ns == 5_000_000


def test_parse_ack_and_unknown_ignored():
    ing = _ing()
    assert ing.parse_message(_ack_frame(), 1) == []
    assert ing.parse_message(json.dumps({"e": "kline", "s": "BTCUSDT"}).encode(), 1) == []


# ─── unit: trade emission ─────────────────────────────────────────────────────


@pytest.mark.parametrize("m,expected", [(False, Side.BID), (True, Side.ASK)])
async def test_trade_emits_with_side(m, expected):
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    ev = ing.parse_message(_trade_frame("BTCUSDT", tid=42, p="100", q="0.5", m=m), 3)[0]
    await ing.process_event(ctx, ev)
    topic, value, _ = ing.producer.calls[-1]
    assert topic == "md.trades.binance.BTCUSDT"
    msg = decode(value)
    assert isinstance(msg, Trade)
    assert msg.trade_id == "42"
    assert msg.side is expected


# ─── unit: snapshot apply + buffer drain ──────────────────────────────────────


async def test_snapshot_applies_buffer_drains_and_goes_live():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])

    # Two deltas that arrived before the snapshot landed.
    d1 = ing.parse_message(_depth_frame("BTCUSDT", 101, 102, bids=[("101", "1")]), 1)[0]
    d2 = ing.parse_message(
        _depth_frame("BTCUSDT", 103, 104, asks=[("110", "0"), ("109", "2")]), 1
    )[0]
    ctx.buffer_append(d1)
    ctx.buffer_append(d2)

    # The delta whose arrival flushes the snapshot + buffer.
    d3 = ing.parse_message(_depth_frame("BTCUSDT", 105, 106, bids=[("100", "0")]), 1)[0]
    await ing.process_event(ctx, d3)

    assert ctx.state is SymbolState.LIVE
    assert ctx.last_seq == 106
    assert ctx.book.best_bid() == (Decimal("101"), Decimal("1"))
    assert ctx.book.best_ask() == (Decimal("109"), Decimal("2"))

    topics = [t for t, _, _ in ing.producer.calls]
    assert topics == [
        "md.book.binance.BTCUSDT.snapshots",
        "md.book.binance.BTCUSDT.deltas",
        "md.book.binance.BTCUSDT.deltas",
        "md.book.binance.BTCUSDT.deltas",
    ]
    assert isinstance(decode(ing.producer.calls[0][1]), BookSnapshot)
    assert isinstance(decode(ing.producer.calls[1][1]), BookDelta)


# ─── unit: U/u gap detection ──────────────────────────────────────────────────


async def test_sync_drops_stale_and_detects_forward_gap():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    ctx.set_state(SymbolState.LIVE)
    ctx.last_seq = 104
    ing._snap_last_id["BTCUSDT"] = 100  # not the first delta anymore

    n0 = len(ing.producer.calls)
    stale = ing.parse_message(_depth_frame("BTCUSDT", 90, 104, bids=[("100", "9")]), 1)[0]
    await ing.process_event(ctx, stale)  # u <= last_seq -> dropped, no emit
    assert len(ing.producer.calls) == n0
    assert ctx.last_seq == 104

    contiguous = ing.parse_message(_depth_frame("BTCUSDT", 105, 106, bids=[("100", "1")]), 1)[0]
    await ing.process_event(ctx, contiguous)
    assert ctx.last_seq == 106

    gap = ing.parse_message(_depth_frame("BTCUSDT", 108, 109, bids=[("100", "2")]), 1)[0]
    with pytest.raises(ResyncRequired):
        await ing.process_event(ctx, gap)


async def test_first_delta_must_straddle_last_update_id():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])
    # First delta starts well past lastUpdateId+1 -> snapshot too old -> resync.
    far = ing.parse_message(_depth_frame("BTCUSDT", 200, 205, bids=[("100", "1")]), 1)[0]
    with pytest.raises(ResyncRequired):
        await ing.process_event(ctx, far)


async def test_delta_while_buffering_without_snapshot_buffers():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    ctx.set_state(SymbolState.BUFFERING)
    d = ing.parse_message(_depth_frame("BTCUSDT", 101, 102, bids=[("100", "1")]), 1)[0]
    await ing.process_event(ctx, d)
    assert len(ctx.buffered) == 1
    assert ctx.state is SymbolState.BUFFERING
    assert ing.producer.calls == []


async def test_fetch_failed_sentinel_resyncs():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    ctx.set_state(SymbolState.BUFFERING)
    ing._snapshots["BTCUSDT"] = _FETCH_FAILED
    d = ing.parse_message(_depth_frame("BTCUSDT", 101, 102, bids=[("100", "1")]), 1)[0]
    with pytest.raises(ResyncRequired):
        await ing.process_event(ctx, d)


async def test_crossed_delta_raises_invariant():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])
    # First delta sets an ask below the resting bid -> crossed book.
    crossed = ing.parse_message(_depth_frame("BTCUSDT", 101, 102, asks=[("99", "1")]), 1)[0]
    with pytest.raises(BookInvariantError):
        await ing.process_event(ctx, crossed)


async def test_reset_contexts_clears_snapshot_state_and_bumps_generation():
    ing = _ing()
    ing._snapshots["BTCUSDT"] = (1, [], [])
    ing._snap_last_id["BTCUSDT"] = 1
    dummy = asyncio.create_task(asyncio.sleep(10))
    ing._snapshot_tasks["BTCUSDT"] = dummy
    gen0 = ing._generation

    ing._reset_contexts()

    assert ing._snapshots == {}
    assert ing._snap_last_id == {}
    assert ing._snapshot_tasks == {}
    assert ing._generation == gen0 + 1
    with pytest.raises(asyncio.CancelledError):
        await dummy


# ─── integration: full WS loop (REST injected, no network) ────────────────────


@pytest.mark.asyncio
async def test_integration_snapshot_deltas_trade(fake_server):  # noqa: F811
    fake_server.scripted = [
        _ack_frame().decode(),
        _depth_frame("BTCUSDT", 101, 102, bids=[("100", "9")]).decode(),
        _depth_frame("BTCUSDT", 103, 104, asks=[("110", "0"), ("109", "2")]).decode(),
        _trade_frame("BTCUSDT", tid=1, p="105", q="0.5", m=False).decode(),
    ]
    ing = make_binance(
        fake_server,
        snapshots={"BTCUSDT": (100, [("100", "5")], [("110", "5")])},
    )
    run_task = asyncio.create_task(ing.run())
    for _ in range(300):
        if len(ing.producer.calls) >= 4:
            break
        await asyncio.sleep(0.02)

    ctx = ing.contexts["BTCUSDT"]
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
    assert "md.book.binance.BTCUSDT.snapshots" in topics
    assert "md.book.binance.BTCUSDT.deltas" in topics
    assert "md.trades.binance.BTCUSDT" in topics
    assert best_bid == (Decimal("100"), Decimal("9"))   # qty updated by seq 101-102
    assert best_ask == (Decimal("109"), Decimal("2"))   # 110 removed, 109 added by seq 103-104


@pytest.mark.asyncio
async def test_integration_rest_failure_reconnects(fake_server):  # noqa: F811
    fake_server.scripted = [
        _ack_frame().decode(),
        _depth_frame("BTCUSDT", 101, 102, bids=[("100", "1")]).decode(),
    ]
    ing = make_binance(fake_server, snapshots={"BTCUSDT": (100, [], [])}, fail=True)
    run_task = asyncio.create_task(ing.run())
    for _ in range(300):
        if fake_server.connections >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass
    assert fake_server.connections >= 2, "REST snapshot failure should force reconnect"


@pytest.mark.asyncio
async def test_integration_update_id_gap_reconnects(fake_server):  # noqa: F811
    fake_server.scripted = [
        _ack_frame().decode(),
        # First (and only) delta starts far past lastUpdateId+1 -> snapshot stale.
        _depth_frame("BTCUSDT", 500, 510, bids=[("100", "1")]).decode(),
    ]
    ing = make_binance(
        fake_server, snapshots={"BTCUSDT": (100, [("100", "5")], [("110", "5")])}
    )
    run_task = asyncio.create_task(ing.run())
    for _ in range(300):
        if fake_server.connections >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass
    assert fake_server.connections >= 2, "update-id gap should force reconnect"
