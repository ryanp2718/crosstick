"""Tests for the Binance USDⓈ-M futures ingester (slices 1-3).

Wire contract verified against current Binance derivatives docs (see
docs/DESIGN_perp_capture.md): combined-stream envelope {"stream","data"},
forceOrder with nested `o`, markPriceUpdate flat fields, streams routed by
type across TWO connections (depth on /public/stream, the rest on
/market/stream — one connection never delivers both), all streams in the
query (no live SUBSCRIBE), futures diff-depth (U/u/pu, snapshot overlap
sync), aggTrade-only tape, REST openInterest.
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from common.kafka_io import header_value
from common.models import (
    BookDelta,
    BookLevel,
    BookSnapshot,
    Liquidation,
    MarkPrice,
    OpenInterest,
    Side,
    Trade,
    decode,
)
from ingest.base_ingester import ResyncRequired, SymbolState
from ingest.binance_futures import (
    _FETCH_FAILED,
    BinanceFuturesIngester,
    _OpenInterestResp,
    stream_names,
)
from ingest.tests.test_binance import RecordingProducer

# ─── helpers ─────────────────────────────────────────────────────────────────


def _ing(symbols=("BTCUSDT",), mode="depth"):
    return BinanceFuturesIngester(
        producer=RecordingProducer(), symbols=list(symbols), mode=mode,
        stale_timeout=None, oi_poll_interval_s=None,
    )


def _force_order_frame(s, *, S="SELL", q="0.014", p="9910", ap="9910",
                       X="FILLED", z="0.014", T=1568014460893, E=1568014460893):
    return json.dumps({
        "stream": f"{s.lower()}@forceOrder",
        "data": {
            "e": "forceOrder", "E": E,
            "o": {"s": s, "S": S, "o": "LIMIT", "f": "IOC", "q": q, "p": p,
                  "ap": ap, "X": X, "l": z, "z": z, "T": T},
        },
    }).encode()


def _mark_price_frame(s, *, p="11794.15", i="11784.62", P="11784.25",
                      r="0.00038167", T=1562306400000, E=1562305380000):
    return json.dumps({
        "stream": f"{s.lower()}@markPrice@1s",
        "data": {"e": "markPriceUpdate", "E": E, "s": s, "p": p, "i": i,
                 "P": P, "r": r, "T": T, "st": 1},
    }).encode()


def _depth_frame(s, U, u, pu, *, bids=(), asks=(), E=1):
    return json.dumps({
        "stream": f"{s.lower()}@depth@100ms",
        "data": {"e": "depthUpdate", "E": E, "T": E, "s": s,
                 "U": U, "u": u, "pu": pu,
                 "b": [[p, q] for p, q in bids], "a": [[p, q] for p, q in asks]},
    }).encode()


def _agg_trade_frame(s, *, a=26129, p="0.01633102", q="4.70443515", m=True,
                     T=123456785, E=123456789):
    return json.dumps({
        "stream": f"{s.lower()}@aggTrade",
        "data": {"e": "aggTrade", "E": E, "s": s, "a": a, "p": p, "q": q,
                 "f": 27781, "l": 27781, "T": T, "m": m},
    }).encode()


def _wire(levels):
    return [BookLevel(price=p, size=s) for p, s in levels]


def _inject_snapshot(ing, symbol, last_id, bids, asks):
    """Mimic a completed REST fetch: BUFFERING + a pending snapshot for the applier."""
    ing.contexts[symbol].set_state(SymbolState.BUFFERING, reason="test")
    ing._snapshots[symbol] = (last_id, _wire(bids), _wire(asks))


def _delta(ing, s, U, u, pu, **kw):
    return ing.parse_message(_depth_frame(s, U, u, pu, **kw), 1)[0]


# ─── unit: routed URLs + subscribe ───────────────────────────────────────────


def test_market_mode_url_no_live_subscribe():
    ing = _ing(symbols=("BTCUSDT", "ETHUSDT"), mode="market")
    assert ing.ws_url == (
        "wss://fstream.binance.com/market/stream?streams="
        "btcusdt@forceOrder/btcusdt@markPrice@1s/btcusdt@aggTrade/"
        "ethusdt@forceOrder/ethusdt@markPrice@1s/ethusdt@aggTrade"
    )
    assert ing.build_subscribe_messages() == []


def test_depth_mode_url_routes_to_public():
    """Depth belongs to Binance's /public endpoint; a /market connection
    silently never delivers it (root cause of the 2026-06 silent book outage)."""
    ing = _ing(symbols=("BTCUSDT", "ETHUSDT"), mode="depth")
    assert ing.ws_url == (
        "wss://fstream.binance.com/public/stream?streams="
        "btcusdt@depth@100ms/ethusdt@depth@100ms"
    )
    assert ing.build_subscribe_messages() == []


def test_stream_names_split_by_mode():
    assert stream_names(["BTCUSDT"], "market") == [
        "btcusdt@forceOrder", "btcusdt@markPrice@1s", "btcusdt@aggTrade",
    ]
    assert stream_names(["BTCUSDT"], "depth") == ["btcusdt@depth@100ms"]


def test_market_mode_is_bookless_and_does_not_own_status():
    ing = _ing(mode="market")
    assert ing.heartbeat_s is None  # depth instance owns md.status
    assert ing.snapshot_interval_s is None  # nothing to re-emit

    depth = _ing(mode="depth")
    assert depth.heartbeat_s is not None
    assert depth.snapshot_interval_s is not None


@pytest.mark.asyncio
async def test_market_mode_bootstrap_is_stateless_noop():
    ing = _ing(mode="market")
    await ing.bootstrap("BTCUSDT")
    assert ing.contexts["BTCUSDT"].state is SymbolState.BOOTSTRAP
    assert ing._snapshot_tasks == {}


def test_market_mode_disconnect_does_not_clobber_book_state():
    """Both instances share (exchange, symbol) metric labels; a market-side
    reconnect must not mark the depth instance's healthy book STALE."""
    ing = _ing(mode="market")
    ing._mark_all_stale("connection_lost")
    assert ing.contexts["BTCUSDT"].state is SymbolState.BOOTSTRAP


@pytest.mark.asyncio
async def test_bootstrap_flips_buffering_and_fetches():
    class _Fake(BinanceFuturesIngester):
        async def _rest_snapshot(self, symbol):
            return 100, _wire([("100", "5")]), _wire([("110", "5")])

    ing = _Fake(producer=RecordingProducer(), symbols=["BTCUSDT"], mode="depth",
                stale_timeout=None, oi_poll_interval_s=None)
    await ing.bootstrap("BTCUSDT")
    assert ing.contexts["BTCUSDT"].state is SymbolState.BUFFERING
    await ing._snapshot_tasks["BTCUSDT"]
    assert ing._snapshots["BTCUSDT"][0] == 100


# ─── unit: parse ─────────────────────────────────────────────────────────────


def test_parse_force_order():
    evs = _ing(mode="market").parse_message(_force_order_frame("BTCUSDT", T=1568014460893), 999)
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "liquidation"
    assert ev.symbol == "BTCUSDT"
    assert ev.exchange_ts_ns == 1568014460893 * 1_000_000
    assert ev.local_recv_ts_ns == 999


def test_parse_mark_price():
    evs = _ing(mode="market").parse_message(_mark_price_frame("BTCUSDT", E=1562305380000), 999)
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "mark_price"
    assert ev.symbol == "BTCUSDT"
    assert ev.exchange_ts_ns == 1562305380000 * 1_000_000


def test_parse_depth():
    evs = _ing().parse_message(
        _depth_frame("BTCUSDT", 95, 105, 94, bids=[("100", "1")], E=123), 999
    )
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "delta"
    assert ev.symbol == "BTCUSDT"
    assert (ev.payload.U, ev.payload.u, ev.payload.pu) == (95, 105, 94)
    assert ev.exchange_ts_ns == 123_000_000
    assert ev.local_recv_ts_ns == 999


def test_parse_agg_trade():
    evs = _ing(mode="market").parse_message(_agg_trade_frame("BTCUSDT", a=7, T=5), 999)
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "trade"
    assert ev.payload.a == 7
    assert ev.exchange_ts_ns == 5_000_000


def test_parse_ignores_non_envelope_and_unknown_events():
    ing = _ing()
    assert ing.parse_message(b'{"result":null,"id":1}', 1) == []
    unknown = json.dumps(
        {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker"}}
    ).encode()
    assert ing.parse_message(unknown, 1) == []


# ─── unit: liquidation / mark price emission (state-independent) ─────────────


@pytest.mark.asyncio
async def test_liquidation_emitted_with_conventions():
    ing = _ing(mode="market")
    [ev] = ing.parse_message(_force_order_frame("BTCUSDT", S="SELL"), 999)
    await ing.process_event(ing.contexts["BTCUSDT"], ev)

    [(topic, value, kw)] = ing.producer.calls
    assert topic == "md.liquidations.binance-futures.BTCUSDT"
    assert kw["key"] == b"binance-futures:BTCUSDT"
    assert header_value(kw["headers"], "local_recv_ts_ns") == b"999"

    msg = decode(value)
    assert isinstance(msg, Liquidation)
    assert msg.exchange == "binance-futures"
    assert msg.symbol == "BTCUSDT"
    assert msg.side is Side.ASK  # SELL forced order = long liquidated
    assert msg.price == "9910"
    assert msg.avg_price == "9910"
    assert msg.orig_size == "0.014"
    assert msg.filled_size == "0.014"
    assert msg.status == "FILLED"
    assert msg.exchange_ts_ns == 1568014460893 * 1_000_000
    assert msg.local_ts_ns == 999


@pytest.mark.asyncio
async def test_liquidation_buy_side_maps_to_bid():
    ing = _ing(mode="market")
    [ev] = ing.parse_message(_force_order_frame("BTCUSDT", S="BUY"), 999)
    await ing.process_event(ing.contexts["BTCUSDT"], ev)
    msg = decode(ing.producer.calls[0][1])
    assert msg.side is Side.BID  # BUY forced order = short liquidated


@pytest.mark.asyncio
async def test_mark_price_emitted_with_conventions():
    ing = _ing(mode="market")
    [ev] = ing.parse_message(_mark_price_frame("BTCUSDT"), 999)
    await ing.process_event(ing.contexts["BTCUSDT"], ev)

    [(topic, value, kw)] = ing.producer.calls
    assert topic == "md.markprice.binance-futures.BTCUSDT"
    assert kw["key"] == b"binance-futures:BTCUSDT"

    msg = decode(value)
    assert isinstance(msg, MarkPrice)
    assert msg.mark_price == "11794.15"
    assert msg.index_price == "11784.62"
    assert msg.est_settle_price == "11784.25"
    assert msg.funding_rate == "0.00038167"
    assert msg.next_funding_ts_ns == 1562306400000 * 1_000_000
    assert msg.exchange_ts_ns == 1562305380000 * 1_000_000
    assert msg.local_ts_ns == 999


# ─── unit: aggTrade emission ─────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("m,expected", [(False, Side.BID), (True, Side.ASK)])
async def test_agg_trade_emits_with_side(m, expected):
    ing = _ing(mode="market")
    [ev] = ing.parse_message(_agg_trade_frame("BTCUSDT", a=42, m=m), 3)
    await ing.process_event(ing.contexts["BTCUSDT"], ev)
    topic, value, kw = ing.producer.calls[-1]
    assert topic == "md.trades.binance-futures.BTCUSDT"
    assert kw["key"] == b"binance-futures:BTCUSDT"
    msg = decode(value)
    assert isinstance(msg, Trade)
    assert msg.trade_id == "42"
    assert msg.price == "0.01633102"
    assert msg.size == "4.70443515"
    assert msg.side is expected
    assert msg.exchange_ts_ns == 123456785 * 1_000_000


# ─── unit: futures depth sync (overlap + pu chain — NOT spot's rules) ────────


@pytest.mark.asyncio
async def test_snapshot_applies_buffer_drains_and_goes_live():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])

    # Buffered while the snapshot was in flight: the first STRADDLES
    # lastUpdateId (U <= 100 <= u), the second chains pu == prev u.
    d1 = _delta(ing, "BTCUSDT", 95, 105, 94, bids=[("101", "1")])
    d2 = _delta(ing, "BTCUSDT", 106, 110, 105, asks=[("110", "0"), ("109", "2")])
    ctx.buffer_append(d1)
    ctx.buffer_append(d2)

    # The delta whose arrival flushes the snapshot + buffer.
    d3 = _delta(ing, "BTCUSDT", 111, 115, 110, bids=[("100", "0")])
    await ing.process_event(ctx, d3)

    assert ctx.state is SymbolState.LIVE
    assert ctx.last_seq == 115
    assert ctx.book.best_bid() == (Decimal("101"), Decimal("1"))
    assert ctx.book.best_ask() == (Decimal("109"), Decimal("2"))

    topics = [t for t, _, _ in ing.producer.calls]
    assert topics == [
        "md.book.binance-futures.BTCUSDT.snapshots",
        "md.book.binance-futures.BTCUSDT.deltas",
        "md.book.binance-futures.BTCUSDT.deltas",
        "md.book.binance-futures.BTCUSDT.deltas",
    ]
    assert isinstance(decode(ing.producer.calls[0][1]), BookSnapshot)
    assert isinstance(decode(ing.producer.calls[1][1]), BookDelta)


@pytest.mark.asyncio
async def test_pre_snapshot_events_dropped():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])
    # u < lastUpdateId (strict): entirely covered by the snapshot, dropped.
    stale = _delta(ing, "BTCUSDT", 80, 90, 79, bids=[("90", "9")])
    ctx.buffer_append(stale)
    first = _delta(ing, "BTCUSDT", 95, 105, 90, bids=[("101", "1")])
    await ing.process_event(ctx, first)

    assert ctx.state is SymbolState.LIVE
    assert ctx.last_seq == 105
    deltas = [t for t, _, _ in ing.producer.calls if t.endswith(".deltas")]
    assert len(deltas) == 1  # the stale one emitted nothing
    assert ctx.book.best_bid() == (Decimal("101"), Decimal("1"))


@pytest.mark.asyncio
async def test_first_delta_must_straddle_last_update_id():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])
    # U = lastUpdateId+1 would be a VALID first delta on spot; futures requires
    # overlap (U <= lastUpdateId <= u), so exact contiguity means a gap.
    far = _delta(ing, "BTCUSDT", 101, 105, 99, bids=[("100", "1")])
    with pytest.raises(ResyncRequired):
        await ing.process_event(ctx, far)


@pytest.mark.asyncio
async def test_pu_gap_resyncs():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])
    await ing.process_event(ctx, _delta(ing, "BTCUSDT", 95, 105, 94, bids=[("101", "1")]))
    assert ctx.last_seq == 105

    ok = _delta(ing, "BTCUSDT", 106, 110, 105, bids=[("101", "2")])
    await ing.process_event(ctx, ok)
    assert ctx.last_seq == 110

    gap = _delta(ing, "BTCUSDT", 112, 118, 111, bids=[("101", "3")])  # pu != 110
    with pytest.raises(ResyncRequired):
        await ing.process_event(ctx, gap)


@pytest.mark.asyncio
async def test_first_delta_ending_at_last_update_id_then_pu_chain():
    """Regression for the synced flag: a first delta with u == lastUpdateId
    leaves last_seq unchanged, so 'first delta' must be tracked explicitly —
    the NEXT event chains via pu and must not be re-checked for straddle."""
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    _inject_snapshot(ing, "BTCUSDT", 100, bids=[("100", "5")], asks=[("110", "5")])
    edge = _delta(ing, "BTCUSDT", 98, 100, 97, bids=[("100", "6")])
    await ing.process_event(ctx, edge)
    assert ctx.last_seq == 100
    # Everything in `edge` is already in the snapshot: no delta emitted, book
    # untouched (the snapshot's 100->5 stands, not the event's 100->6).
    assert [t for t, _, _ in ing.producer.calls if t.endswith(".deltas")] == []
    assert ctx.book.best_bid() == (Decimal("100"), Decimal("5"))

    nxt = _delta(ing, "BTCUSDT", 101, 103, 100, bids=[("101", "1")])
    await ing.process_event(ctx, nxt)  # would raise if treated as "first"
    assert ctx.last_seq == 103
    assert ctx.book.best_bid() == (Decimal("101"), Decimal("1"))


@pytest.mark.asyncio
async def test_delta_while_buffering_without_snapshot_buffers():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    ctx.set_state(SymbolState.BUFFERING)
    await ing.process_event(ctx, _delta(ing, "BTCUSDT", 95, 105, 94, bids=[("100", "1")]))
    assert len(ctx.buffered) == 1
    assert ctx.state is SymbolState.BUFFERING
    assert ing.producer.calls == []


@pytest.mark.asyncio
async def test_fetch_failed_sentinel_resyncs():
    ing = _ing()
    ctx = ing.contexts["BTCUSDT"]
    ctx.set_state(SymbolState.BUFFERING)
    ing._snapshots["BTCUSDT"] = _FETCH_FAILED
    d = _delta(ing, "BTCUSDT", 95, 105, 94, bids=[("100", "1")])
    with pytest.raises(ResyncRequired):
        await ing.process_event(ctx, d)


@pytest.mark.asyncio
async def test_reset_contexts_clears_sync_state_and_bumps_generation():
    ing = _ing()
    ing._snapshots["BTCUSDT"] = (1, [], [])
    ing._synced.add("BTCUSDT")
    dummy = asyncio.create_task(asyncio.sleep(10))
    ing._snapshot_tasks["BTCUSDT"] = dummy
    gen0 = ing._generation

    ing._reset_contexts()

    assert ing._snapshots == {}
    assert ing._synced == set()
    assert ing._snapshot_tasks == {}
    assert ing._generation == gen0 + 1
    with pytest.raises(asyncio.CancelledError):
        await dummy


# ─── unit: open-interest poll (slice 2) ──────────────────────────────────────


def _oi_ing(responses):
    class _Fake(BinanceFuturesIngester):
        async def _fetch_open_interest(self, symbol):
            value = responses[symbol]
            if isinstance(value, Exception):
                raise value
            return value

    return _Fake(producer=RecordingProducer(), symbols=list(responses), mode="market",
                 stale_timeout=None, oi_poll_interval_s=None)


@pytest.mark.asyncio
async def test_open_interest_emitted_with_conventions():
    ing = _oi_ing({
        "BTCUSDT": _OpenInterestResp(
            openInterest="10659.509", symbol="BTCUSDT", time=1589437530011
        )
    })
    await ing._poll_open_interest()

    [(topic, value, kw)] = ing.producer.calls
    assert topic == "md.openinterest.binance-futures.BTCUSDT"
    assert kw["key"] == b"binance-futures:BTCUSDT"

    msg = decode(value)
    assert isinstance(msg, OpenInterest)
    assert msg.exchange == "binance-futures"
    assert msg.symbol == "BTCUSDT"
    assert msg.open_interest == "10659.509"
    assert msg.exchange_ts_ns == 1589437530011 * 1_000_000
    assert msg.local_ts_ns > 0
    assert header_value(kw["headers"], "local_recv_ts_ns") == str(msg.local_ts_ns).encode()
    assert header_value(kw["headers"], "exchange_ts_ns") == str(msg.exchange_ts_ns).encode()


@pytest.mark.asyncio
async def test_open_interest_poll_survives_per_symbol_failure():
    ing = _oi_ing({
        "BTCUSDT": RuntimeError("rest down"),
        "ETHUSDT": _OpenInterestResp(openInterest="7", symbol="ETHUSDT", time=5),
    })
    await ing._poll_open_interest()  # must not raise
    [(topic, _, _)] = ing.producer.calls
    assert topic == "md.openinterest.binance-futures.ETHUSDT"
