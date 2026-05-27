"""Wire-format types shared across ingest, analytics, and gateway.

Prices and sizes are strings on the wire (no float drift, no Decimal-in-JSON pain).
Convert to Decimal at the boundary where math is needed.
"""
from __future__ import annotations

import enum

import msgspec


class Side(enum.StrEnum):
    BID = "bid"
    ASK = "ask"


class BookLevel(msgspec.Struct, frozen=True, array_like=True):
    """Decodes from a JSON array [price, size] — NOT a dict.

    Matches Binance's wire format (string [price, qty] arrays), so the Binance
    driver decodes levels straight into this. Coinbase sends objects
    {price_level, new_quantity} and Kraken v2 sends objects {price, qty} as
    JSON numbers — those drivers must decode by key, not reuse BookLevel.
    """

    price: str
    size: str


class BookSnapshot(msgspec.Struct, tag="snap", tag_field="t", frozen=True):
    exchange: str
    symbol: str
    sequence: int
    bids: list[BookLevel]
    asks: list[BookLevel]
    exchange_ts_ns: int
    local_ts_ns: int


class BookDelta(msgspec.Struct, tag="delta", tag_field="t", frozen=True):
    exchange: str
    symbol: str
    sequence: int
    bids: list[BookLevel]
    asks: list[BookLevel]
    exchange_ts_ns: int
    local_ts_ns: int


class Trade(msgspec.Struct, tag="trade", tag_field="t", frozen=True):
    exchange: str
    symbol: str
    trade_id: str
    price: str
    size: str
    side: Side
    exchange_ts_ns: int
    local_ts_ns: int


class BBO(msgspec.Struct, tag="bbo", tag_field="t", frozen=True):
    exchange: str
    symbol: str
    bid_px: str
    bid_sz: str
    ask_px: str
    ask_sz: str
    exchange_ts_ns: int
    local_ts_ns: int


class Spread(msgspec.Struct, tag="spread", tag_field="t", frozen=True):
    symbol: str
    bid_exchange: str
    bid_px: str
    ask_exchange: str
    ask_px: str
    spread: str
    local_ts_ns: int


# ── Warehouse / batch types (NOT published by streaming ingesters) ─────────
# VWAP is computed by dbt after the fact, not emitted on any Kafka topic.
# Kept here for materializer pipelines that may need to decode archived data.


class VWAP(msgspec.Struct, tag="vwap", tag_field="t", frozen=True):
    exchange: str
    symbol: str
    window_sec: int
    vwap: str
    volume: str
    local_ts_ns: int


_ENCODER = msgspec.json.Encoder()


def encode(msg: msgspec.Struct) -> bytes:
    return _ENCODER.encode(msg)


# Streaming decoder: covers only the types that ingesters and the gateway
# actually produce.  VWAP is excluded — it is never on a live topic.
_STREAMING_TYPES = BookSnapshot | BookDelta | Trade | BBO | Spread
_DECODER = msgspec.json.Decoder(_STREAMING_TYPES)


def decode(buf: bytes) -> _STREAMING_TYPES:
    return _DECODER.decode(buf)
