"""Tests for the Kraken v2 ingester (offline; no network).

The checksum is the linchpin: test_snapshot_real_vector feeds a frame captured
live from wss://ws.kraken.com/v2 and asserts our book reproduces Kraken's own
CRC32 - that gates the Decimal-preserves-trailing-zeros assumption end to end.
The synthetic tests build internally-consistent checksums to exercise the state
machine (go-live, gap/mismatch resync, zero-qty removal, trade side mapping).
"""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal

import pytest

from common.models import Side, decode
from ingest.base_ingester import ResyncRequired, SymbolState
from ingest.book import OrderBook, kraken_checksum
from ingest.kraken import KrakenIngester
from ingest.tests.test_base_ingester import FakeProducer, fake_server  # noqa: F401

# A real BTC/USD depth-10 snapshot captured from wss://ws.kraken.com/v2.
# checksum=1494999917 is Kraken's own - the worked-example regression vector.
REAL_SNAPSHOT = (
    b'{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD",'
    b'"bids":[{"price":75620.2,"qty":0.01733846},{"price":75619.5,"qty":0.00132241},'
    b'{"price":75615.9,"qty":0.00132200},{"price":75615.7,"qty":0.00005100},'
    b'{"price":75614.6,"qty":0.00105184},{"price":75614.5,"qty":0.03143545},'
    b'{"price":75614.4,"qty":1.32191273},{"price":75612.2,"qty":0.00264507},'
    b'{"price":75608.4,"qty":0.35542926},{"price":75604.8,"qty":0.01000000}],'
    b'"asks":[{"price":75623.1,"qty":0.73947363},{"price":75623.2,"qty":0.09475141},'
    b'{"price":75623.8,"qty":0.03112375},{"price":75623.9,"qty":0.06612853},'
    b'{"price":75624.4,"qty":0.00736362},{"price":75624.5,"qty":0.00026220},'
    b'{"price":75626.0,"qty":0.03051283},{"price":75630.0,"qty":0.00013202},'
    b'{"price":75630.1,"qty":0.15487980},{"price":75630.2,"qty":0.19734180}],'
    b'"checksum":1494999917,"timestamp":"2026-05-27T02:18:39.697069Z"}]}'
)


# ─── frame builders ──────────────────────────────────────────────────────────


def _lvls(xs: list[tuple[str, str]]) -> str:
    # Emit price/qty as raw JSON number literals so exact digits (incl. trailing
    # zeros) survive - json.dumps(float) would drop them and break the checksum.
    return ",".join(f'{{"price":{p},"qty":{q}}}' for p, q in xs)


def _book_frame(
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    checksum: int,
    *,
    symbol: str = "BTC/USD",
    typ: str = "snapshot",
    ts: str = "2026-05-27T02:18:39.697069Z",
) -> bytes:
    return (
        f'{{"channel":"book","type":"{typ}","data":[{{"symbol":"{symbol}",'
        f'"bids":[{_lvls(bids)}],"asks":[{_lvls(asks)}],'
        f'"checksum":{checksum},"timestamp":"{ts}"}}]}}'
    ).encode()


def _trade_frame(
    side: str, price: str, qty: str, *, symbol: str = "BTC/USD", trade_id: int = 1
) -> bytes:
    return (
        f'{{"channel":"trade","type":"update","data":[{{"symbol":"{symbol}",'
        f'"side":"{side}","price":{price},"qty":{qty},"trade_id":{trade_id},'
        f'"timestamp":"2026-05-27T02:18:39.7Z"}}]}}'
    ).encode()


def _checksum(bids: list[tuple[str, str]], asks: list[tuple[str, str]]) -> int:
    """Checksum of a full book: asks low->high, bids high->low, top 10 each."""
    a = sorted(asks, key=lambda x: float(x[0]))[:10]
    b = sorted(bids, key=lambda x: float(x[0]), reverse=True)[:10]
    return kraken_checksum(a, b)


def _ing(**kw: object) -> KrakenIngester:
    kw.setdefault("stale_timeout", None)  # no watchdog in direct-call unit tests
    return KrakenIngester(producer=FakeProducer(), symbols=["BTC/USD"], **kw)


async def _feed(ing: KrakenIngester, raw: bytes) -> None:
    for ev in ing.parse_message(raw, time.time_ns()):
        await ing.process_event(ing.contexts[ev.symbol], ev)


# ─── subscribe / parse ───────────────────────────────────────────────────────


def test_subscribe_messages_book_and_trade() -> None:
    msgs = [json.loads(m) for m in _ing().build_subscribe_messages()]
    book = next(m for m in msgs if m["params"]["channel"] == "book")
    trade = next(m for m in msgs if m["params"]["channel"] == "trade")
    assert book["method"] == "subscribe"
    assert book["params"]["symbol"] == ["BTC/USD"]
    assert book["params"]["depth"] == 10
    assert book["params"]["snapshot"] is True
    assert trade["params"]["symbol"] == ["BTC/USD"]


@pytest.mark.parametrize(
    "raw",
    [
        b'{"method":"subscribe","success":true,"time_in":"t","time_out":"t"}',
        b'{"channel":"heartbeat"}',
        b'{"channel":"status","type":"update","data":[]}',
    ],
)
def test_control_frames_ignored(raw: bytes) -> None:
    assert _ing().parse_message(raw, 0) == []


def test_parse_classifies_book_and_trade() -> None:
    ing = _ing()
    snap = ing.parse_message(REAL_SNAPSHOT, 123)
    assert len(snap) == 1 and snap[0].kind == "snapshot" and snap[0].symbol == "BTC/USD"
    upd = ing.parse_message(_book_frame([("1", "1")], [("2", "1")], 0, typ="update"), 0)
    assert upd[0].kind == "delta"
    tr = ing.parse_message(_trade_frame("buy", "75000.0", "0.5"), 0)
    assert tr[0].kind == "trade"


# ─── checksum / go-live ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_snapshot_real_vector_goes_live() -> None:
    """Worked example: our book must reproduce Kraken's own CRC32 (1494999917).
    Reaching LIVE means _verify_checksum passed against the real value."""
    ing = _ing()
    await _feed(ing, REAL_SNAPSHOT)
    ctx = ing.contexts["BTC/USD"]
    assert ctx.state is SymbolState.LIVE
    assert ctx.book.best_bid()[0] == Decimal("75620.2")
    assert ctx.book.best_ask()[0] == Decimal("75623.1")
    # Snapshot emitted on the snapshots topic and round-trips.
    topic, payload = ing.producer.sent[0]
    assert topic.endswith("BTC-USD.snapshots")
    snap = decode(payload)
    assert snap.bids[0].price == "75620.2"


@pytest.mark.asyncio
async def test_snapshot_bad_checksum_resyncs() -> None:
    ing = _ing()
    bad = _book_frame([("100.0", "1.0")], [("101.0", "1.0")], 12345)
    with pytest.raises(ResyncRequired, match="crc mismatch"):
        await _feed(ing, bad)


@pytest.mark.asyncio
async def test_delta_before_snapshot_resyncs() -> None:
    ing = _ing()
    upd = _book_frame([("100.0", "1.0")], [("101.0", "1.0")], 0, typ="update")
    with pytest.raises(ResyncRequired, match="before snapshot"):
        await _feed(ing, upd)


@pytest.mark.asyncio
async def test_delta_applies_with_synthetic_counter() -> None:
    ing = _ing()
    bids = [("100.0", "1.0"), ("99.0", "2.0")]
    asks = [("101.0", "1.5"), ("102.0", "2.5")]
    await _feed(ing, _book_frame(bids, asks, _checksum(bids, asks)))
    ctx = ing.contexts["BTC/USD"]
    assert ctx.state is SymbolState.LIVE and ctx.last_seq == 0

    # Update best bid qty; checksum over the resulting full book.
    new_bids = [("100.0", "3.0"), ("99.0", "2.0")]
    upd = _book_frame([("100.0", "3.0")], [], _checksum(new_bids, asks), typ="update")
    await _feed(ing, upd)
    assert ctx.state is SymbolState.LIVE
    assert ctx.last_seq == 1  # synthetic monotonic counter advanced
    assert ctx.book.best_bid() == (Decimal("100.0"), Decimal("3.0"))


@pytest.mark.asyncio
async def test_delta_zero_qty_removes_level() -> None:
    ing = _ing()
    bids = [("100.0", "1.0"), ("99.0", "2.0")]
    asks = [("101.0", "1.5")]
    await _feed(ing, _book_frame(bids, asks, _checksum(bids, asks)))
    ctx = ing.contexts["BTC/USD"]
    # Remove the 99.0 bid.
    remaining = [("100.0", "1.0")]
    upd = _book_frame([("99.0", "0")], [], _checksum(remaining, asks), typ="update")
    await _feed(ing, upd)
    assert ctx.book.depth(Side.BID) == 1
    assert ctx.state is SymbolState.LIVE


@pytest.mark.asyncio
async def test_delta_bad_checksum_resyncs() -> None:
    ing = _ing()
    bids = [("100.0", "1.0")]
    asks = [("101.0", "1.5")]
    await _feed(ing, _book_frame(bids, asks, _checksum(bids, asks)))
    upd = _book_frame([("100.0", "9.9")], [], 999, typ="update")  # wrong checksum
    with pytest.raises(ResyncRequired, match="crc mismatch"):
        await _feed(ing, upd)


@pytest.mark.asyncio
async def test_published_log_reconstructs_book() -> None:
    """The contract that justifies emitting size=0 deletes for evicted levels:
    replaying the published snapshot + deltas (verbatim, NO trim) must reproduce
    the ingester's depth-limited book. This is what would have caught the live
    divergence offline."""
    ing = _ing(depth=2)
    # depth=2 so a single insert forces an eviction Kraken would never delete.
    await _feed(ing, _book_frame(
        [("100.0", "1.0"), ("99.0", "1.0")], [("101.0", "1.0"), ("102.0", "1.0")],
        _checksum([("100.0", "1.0"), ("99.0", "1.0")], [("101.0", "1.0"), ("102.0", "1.0")]),
    ))
    # Insert a better bid -> 99.0 falls out of the window (Kraken sends only the insert).
    await _feed(ing, _book_frame(
        [("100.5", "2.0")], [],
        _checksum([("100.5", "2.0"), ("100.0", "1.0")], [("101.0", "1.0"), ("102.0", "1.0")]),
        typ="update",
    ))
    # Plain in-window update, no eviction.
    await _feed(ing, _book_frame(
        [], [("101.0", "3.0")],
        _checksum([("100.5", "2.0"), ("100.0", "1.0")], [("101.0", "3.0"), ("102.0", "1.0")]),
        typ="update",
    ))

    # Rebuild a fresh book from the published log alone - crucially WITHOUT trim,
    # the way a generic downstream consumer would.
    rebuilt = OrderBook("kraken", "BTC/USD")
    for topic, payload in ing.producer.sent:
        msg = decode(payload)
        bids = [(Decimal(lvl.price), Decimal(lvl.size)) for lvl in msg.bids]
        asks = [(Decimal(lvl.price), Decimal(lvl.size)) for lvl in msg.asks]
        if topic.endswith(".snapshots"):
            rebuilt.apply_snapshot(msg.sequence, bids, asks)
        else:
            rebuilt.apply_delta(msg.sequence, bids, asks)

    live = ing.contexts["BTC/USD"].book
    assert rebuilt.top_n(Side.BID, 10) == live.top_n(Side.BID, 10)
    assert rebuilt.top_n(Side.ASK, 10) == live.top_n(Side.ASK, 10)
    # The deletes kept the rebuilt book bounded without any depth knowledge.
    assert rebuilt.depth(Side.BID) == 2 and rebuilt.depth(Side.ASK) == 2


# ─── trades ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("side,expected", [("buy", Side.BID), ("sell", Side.ASK)])
async def test_trade_side_mapping(side: str, expected: Side) -> None:
    ing = _ing()
    await _feed(ing, _trade_frame(side, "75000.0", "0.5", trade_id=42))
    topic, payload = ing.producer.sent[0]
    assert topic.endswith("md.trades.kraken.BTC-USD")
    t = decode(payload)
    assert t.side is expected and t.trade_id == "42" and t.price == "75000.0"


@pytest.mark.asyncio
async def test_unknown_trade_side_resyncs() -> None:
    ing = _ing()
    with pytest.raises(ResyncRequired, match="unexpected trade side"):
        await _feed(ing, _trade_frame("hold", "1.0", "1.0"))


# ─── integration (FakeExchangeServer) ────────────────────────────────────────


def _make_live(server, **kw: object) -> KrakenIngester:
    return KrakenIngester(
        producer=FakeProducer(), symbols=["BTC/USD"], ws_url=server.url,
        subscribe_rate=100.0, subscribe_capacity=100.0, queue_maxsize=200,
        backoff_base=0.001, backoff_cap=0.01, ping_interval=None, ping_timeout=None,
        stale_timeout=None, **kw,
    )


@pytest.mark.asyncio
async def test_integration_snapshot_update_trade(fake_server):  # noqa: F811
    bids = [("100.0", "1.0"), ("99.0", "2.0")]
    asks = [("101.0", "1.5"), ("102.0", "2.5")]
    new_bids = [("100.0", "3.0"), ("99.0", "2.0")]
    fake_server.scripted = [
        _book_frame(bids, asks, _checksum(bids, asks)).decode(),
        _book_frame([("100.0", "3.0")], [], _checksum(new_bids, asks), typ="update").decode(),
        _trade_frame("buy", "100.5", "0.25").decode(),
    ]
    ing = _make_live(fake_server)
    run_task = asyncio.create_task(ing.run())
    for _ in range(100):
        if len(ing.producer.sent) >= 3:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    topics = [t for t, _ in ing.producer.sent]
    assert any(t.endswith("BTC-USD.snapshots") for t in topics)
    # A delta is only emitted in LIVE state, so this proves it went live.
    assert any(t.endswith("BTC-USD.deltas") for t in topics)
    assert any(t == "md.trades.kraken.BTC-USD" for t in topics)


@pytest.mark.asyncio
async def test_integration_checksum_mismatch_reconnects(fake_server):  # noqa: F811
    bids = [("100.0", "1.0")]
    asks = [("101.0", "1.5")]
    fake_server.scripted = [
        _book_frame(bids, asks, _checksum(bids, asks)).decode(),
        _book_frame([("100.0", "9.9")], [], 999, typ="update").decode(),  # bad crc
    ]
    ing = _make_live(fake_server)
    run_task = asyncio.create_task(ing.run())
    for _ in range(150):
        if fake_server.connections >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert fake_server.connections >= 2, "crc mismatch should force a reconnect"
