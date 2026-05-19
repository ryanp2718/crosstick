"""Wire-format types shared across ingest, analytics, and gateway.

Prices and sizes are strings on the wire (no float drift, no Decimal-in-JSON pain).
Convert to Decimal at the boundary where math is needed.
"""
from __future__ import annotations

import enum

import msgspec


class Side(str, enum.Enum):
    BID = "bid"
    ASK = "ask"


class BookLevel(msgspec.Struct, frozen=True, array_like=True):
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


_DECODER_TYPES = BookSnapshot | BookDelta | Trade | BBO | Spread | VWAP
_DECODER = msgspec.json.Decoder(_DECODER_TYPES)


def decode(buf: bytes) -> _DECODER_TYPES:
    return _DECODER.decode(buf)
