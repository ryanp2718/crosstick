"""Tests for the Binance USDⓈ-M futures ingester (slice 1: forceOrder + markPrice).

Wire contract verified against current Binance derivatives docs (see
docs/DESIGN_perp_capture.md): combined-stream envelope {"stream","data"},
forceOrder with nested `o`, markPriceUpdate flat fields, routed /market/stream
URL with all streams in the query (no live SUBSCRIBE).
"""
from __future__ import annotations

import json

import pytest

from common.kafka_io import header_value
from common.models import Liquidation, MarkPrice, Side, decode
from ingest.base_ingester import SymbolState
from ingest.binance_futures import BinanceFuturesIngester, stream_names
from ingest.tests.test_binance import RecordingProducer

# ─── helpers ─────────────────────────────────────────────────────────────────


def _ing(symbols=("BTCUSDT",)):
    return BinanceFuturesIngester(
        producer=RecordingProducer(), symbols=list(symbols), stale_timeout=None,
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


# ─── unit: URL + subscribe ───────────────────────────────────────────────────


def test_streams_in_connection_url_no_live_subscribe():
    ing = _ing(symbols=("BTCUSDT", "ETHUSDT"))
    assert ing.ws_url == (
        "wss://fstream.binance.com/market/stream?streams="
        "btcusdt@forceOrder/btcusdt@markPrice@1s/"
        "ethusdt@forceOrder/ethusdt@markPrice@1s"
    )
    assert ing.build_subscribe_messages() == []


def test_stream_names_pairs_per_symbol():
    assert stream_names(["BTCUSDT"]) == ["btcusdt@forceOrder", "btcusdt@markPrice@1s"]


@pytest.mark.asyncio
async def test_bootstrap_goes_straight_to_live():
    ing = _ing()
    await ing.bootstrap("BTCUSDT")
    assert ing.contexts["BTCUSDT"].state is SymbolState.LIVE


# ─── unit: parse ─────────────────────────────────────────────────────────────


def test_parse_force_order():
    evs = _ing().parse_message(_force_order_frame("BTCUSDT", T=1568014460893), 999)
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "liquidation"
    assert ev.symbol == "BTCUSDT"
    assert ev.exchange_ts_ns == 1568014460893 * 1_000_000
    assert ev.local_recv_ts_ns == 999


def test_parse_mark_price():
    evs = _ing().parse_message(_mark_price_frame("BTCUSDT", E=1562305380000), 999)
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "mark_price"
    assert ev.symbol == "BTCUSDT"
    assert ev.exchange_ts_ns == 1562305380000 * 1_000_000


def test_parse_ignores_non_envelope_and_unknown_events():
    ing = _ing()
    assert ing.parse_message(b'{"result":null,"id":1}', 1) == []
    unknown = json.dumps(
        {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker"}}
    ).encode()
    assert ing.parse_message(unknown, 1) == []


# ─── unit: process_event → emitted messages ──────────────────────────────────


@pytest.mark.asyncio
async def test_liquidation_emitted_with_conventions():
    ing = _ing()
    await ing.bootstrap("BTCUSDT")
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
    ing = _ing()
    await ing.bootstrap("BTCUSDT")
    [ev] = ing.parse_message(_force_order_frame("BTCUSDT", S="BUY"), 999)
    await ing.process_event(ing.contexts["BTCUSDT"], ev)
    msg = decode(ing.producer.calls[0][1])
    assert msg.side is Side.BID  # BUY forced order = short liquidated


@pytest.mark.asyncio
async def test_mark_price_emitted_with_conventions():
    ing = _ing()
    await ing.bootstrap("BTCUSDT")
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
