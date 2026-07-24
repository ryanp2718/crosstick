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
    """Decodes from a JSON array [price, size] - NOT a dict.

    Matches Binance's wire format (string [price, qty] arrays), so the Binance
    driver decodes levels straight into this. Coinbase sends objects
    {price_level, new_quantity} and Kraken v2 sends objects {price, qty} as
    JSON numbers - those drivers must decode by key, not reuse BookLevel.
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
    # Per-WS-connection generation. coinbase/kraken reset `sequence` on each
    # reconnect, so sequence alone can't tell a prior connection's deltas from
    # the current one's - the gateway keys book reconstruction on (epoch,
    # sequence) to avoid replaying a stale epoch onto a fresh snapshot. Defaults
    # to 0 so pre-epoch captures decode (compared by equality only - never time).
    epoch: int = 0


class BookDelta(msgspec.Struct, tag="delta", tag_field="t", frozen=True):
    exchange: str
    symbol: str
    sequence: int
    bids: list[BookLevel]
    asks: list[BookLevel]
    exchange_ts_ns: int
    local_ts_ns: int
    epoch: int = 0  # see BookSnapshot.epoch


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


class Liquidation(msgspec.Struct, tag="liq", tag_field="t", frozen=True):
    """A forced (liquidation) order from a derivatives venue.

    SAMPLED, not a tape: Binance pushes only the largest liquidation per
    symbol per 1000ms (see DESIGN_perp_capture.md). `side` is the side of the
    forced order itself - ASK means a long position was liquidated."""

    exchange: str
    symbol: str
    side: Side
    price: str
    avg_price: str
    orig_size: str
    filled_size: str
    status: str  # exchange order status, e.g. "FILLED"
    exchange_ts_ns: int
    local_ts_ns: int


class MarkPrice(msgspec.Struct, tag="mark", tag_field="t", frozen=True):
    """Derivatives mark/index/funding tick (Binance markPriceUpdate)."""

    exchange: str
    symbol: str
    mark_price: str
    index_price: str
    est_settle_price: str
    funding_rate: str
    next_funding_ts_ns: int
    exchange_ts_ns: int
    local_ts_ns: int


class OpenInterest(msgspec.Struct, tag="oi", tag_field="t", frozen=True):
    """Total open interest for a derivatives instrument.

    REST-polled - Binance has no OI stream - so the cadence is the ingester's
    poll interval, not an exchange event clock. `exchange_ts_ns` is the
    venue's response timestamp. (DESIGN_perp_capture.md slice 2.)"""

    exchange: str
    symbol: str
    open_interest: str
    exchange_ts_ns: int
    local_ts_ns: int


class Status(msgspec.Struct, tag="status", tag_field="t", frozen=True):
    """Per-exchange venue health: connection-state liveness, not quote freshness.
    'up' is a periodic heartbeat while streaming; 'down' is sent on graceful
    shutdown. The gateway evicts a venue's NBBO legs on 'down' or missed
    heartbeats (which covers a crash/kill that emits no 'down')."""

    exchange: str
    state: str  # "up" | "down"
    ts_ns: int


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
# actually produce.  VWAP is excluded - it is never on a live topic.
_STREAMING_TYPES = (
    BookSnapshot | BookDelta | Trade | BBO | Spread | Status | Liquidation | MarkPrice
    | OpenInterest
)
_DECODER = msgspec.json.Decoder(_STREAMING_TYPES)


def decode(buf: bytes) -> _STREAMING_TYPES:
    return _DECODER.decode(buf)
