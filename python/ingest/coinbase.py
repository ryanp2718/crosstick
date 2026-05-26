"""Coinbase Advanced Trade ingester.

Feed: wss://advanced-trade-ws.coinbase.com (public, no auth).

Wire contract (verified live, see plan):
  - Frames are envelopes: {channel, sequence_num, timestamp, events:[...]}.
  - `sequence_num` is a single PER-CONNECTION monotonic counter stamped on every
    envelope across all channels/products. A gap means we may have missed data
    for any symbol on the socket → resync the whole connection.
  - `l2_data` events: type `snapshot` (full book) then `update`. Each update level
    is {side, price_level, new_quantity} where new_quantity is ABSOLUTE size
    (0 = remove). Sides are `bid` / `offer`.
  - `market_trades` events carry a `trades` list with BUY/SELL taker side.
  - `heartbeats` / `subscriptions` are control frames (no book/trade data) but
    still advance `sequence_num`.

Because the counter is per-connection (not per-symbol), gap detection lives on the
ingester, not in SymbolContext.last_seq. parse_message runs in the single applier
task in strict frame order, so tracking _expected_seq there is safe.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal

import msgspec

from common.kafka_io import book_delta_topic, book_snapshot_topic, latency_headers, trade_topic
from common.metrics import messages_received
from common.models import BookDelta, BookLevel, BookSnapshot, Side, Trade, encode
from ingest.base_ingester import (
    BaseIngester,
    ParsedEvent,
    ResyncRequired,
    SymbolContext,
    SymbolState,
)

log = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://advanced-trade-ws.coinbase.com"
DEFAULT_SYMBOLS = ["BTC-USD", "ETH-USD"]
_CHANNELS = ("level2", "market_trades", "heartbeats")

# Side maps. _l2_side: `ask` kept as a defensive fallback though the live feed
# uses `offer`. Unknown labels resync rather than silently mis-book.
_L2_SIDE = {"bid": Side.BID, "offer": Side.ASK, "ask": Side.ASK}
_TRADE_SIDE = {"BUY": Side.BID, "SELL": Side.ASK}


# ─── wire structs (msgspec, two-stage decode) ───────────────────────────────


class _Update(msgspec.Struct):
    side: str
    price_level: str
    new_quantity: str


class _L2Event(msgspec.Struct):
    type: str
    product_id: str
    updates: list[_Update]


class _TradeItem(msgspec.Struct):
    trade_id: str
    product_id: str
    price: str
    size: str
    side: str
    time: str = ""


class _TradeEvent(msgspec.Struct):
    trades: list[_TradeItem]


class _Envelope(msgspec.Struct):
    channel: str
    sequence_num: int
    timestamp: str = ""
    events: list[msgspec.Raw] = msgspec.field(default_factory=list)


_ENV_DEC = msgspec.json.Decoder(_Envelope)
_L2_DEC = msgspec.json.Decoder(_L2Event)
_TRADE_DEC = msgspec.json.Decoder(_TradeEvent)


# ─── helpers ─────────────────────────────────────────────────────────────────


def _l2_side(s: str) -> Side:
    try:
        return _L2_SIDE[s]
    except KeyError:
        raise ResyncRequired(f"unexpected l2 side {s!r}") from None


def _trade_side(s: str) -> Side:
    try:
        return _TRADE_SIDE[s]
    except KeyError:
        raise ResyncRequired(f"unexpected trade side {s!r}") from None


def _rfc3339_to_ns(ts: str) -> int:
    """Parse a UTC RFC3339 timestamp ('...Z') to epoch nanoseconds.

    datetime only resolves microseconds, so the fractional part (up to 9 digits)
    is taken from the string directly. Coinbase always emits 'Z' UTC times.
    """
    if not ts:
        return 0
    body = ts[:-1] if ts.endswith("Z") else ts
    if "." in body:
        head, frac = body.split(".", 1)
        frac_ns = int(frac[:9].ljust(9, "0"))
    else:
        head, frac_ns = body, 0
    dt = datetime.fromisoformat(head).replace(tzinfo=UTC)
    return int(dt.timestamp()) * 1_000_000_000 + frac_ns


# ─── ingester ────────────────────────────────────────────────────────────────


class CoinbaseIngester(BaseIngester):
    def __init__(
        self,
        *,
        producer,
        symbols: list[str] | None = None,
        ws_url: str = DEFAULT_WS_URL,
        **kw: object,
    ) -> None:
        # ~5 MiB full-depth snapshot needs headroom over the 4 MiB base default;
        # heartbeats arrive ~1/s so a 15s data-staleness timeout is safe.
        kw.setdefault("ws_max_size", 2**24)
        kw.setdefault("stale_timeout", 15.0)
        super().__init__(
            exchange="coinbase",
            symbols=symbols if symbols is not None else list(DEFAULT_SYMBOLS),
            ws_url=ws_url,
            producer=producer,
            **kw,
        )
        self._expected_seq: int | None = None

    # ─── hooks ───────────────────────────────────────────────────────────────

    async def bootstrap(self, symbol: str) -> None:
        """No-op: the snapshot arrives in-band on the level2 channel and
        process_event() flips the symbol LIVE when it lands."""
        return

    def build_subscribe_messages(self) -> list[str]:
        return [
            json.dumps({"type": "subscribe", "product_ids": self.symbols, "channel": ch})
            for ch in _CHANNELS
        ]

    def parse_message(self, raw: bytes, local_recv_ts_ns: int) -> list[ParsedEvent]:
        env = _ENV_DEC.decode(raw)
        seq = env.sequence_num

        # Per-connection gap detection (every channel advances the counter).
        if self._expected_seq is None:
            self._expected_seq = seq + 1
        elif seq == self._expected_seq:
            self._expected_seq += 1
        elif seq < self._expected_seq:
            messages_received.labels(exchange=self.exchange, channel="seq_reorder").inc()
            log.debug("seq reorder/dup: got %d expected %d", seq, self._expected_seq)
            return []
        else:
            log.warning("connection seq gap: got %d expected %d", seq, self._expected_seq)
            return [ParsedEvent(
                symbol=self.symbols[0], kind="error", sequence=seq,
                local_recv_ts_ns=local_recv_ts_ns,
            )]

        if env.channel == "l2_data":
            ts_ns = _rfc3339_to_ns(env.timestamp)
            out: list[ParsedEvent] = []
            for raw_ev in env.events:
                ev = _L2_DEC.decode(raw_ev)
                out.append(ParsedEvent(
                    symbol=ev.product_id,
                    kind="snapshot" if ev.type == "snapshot" else "delta",
                    sequence=seq, payload=ev, raw_bytes=len(raw),
                    exchange_ts_ns=ts_ns, local_recv_ts_ns=local_recv_ts_ns,
                ))
            return out

        if env.channel == "market_trades":
            out = []
            for raw_ev in env.events:
                ev = _TRADE_DEC.decode(raw_ev)
                for t in ev.trades:
                    out.append(ParsedEvent(
                        symbol=t.product_id, kind="trade", sequence=seq, payload=t,
                        raw_bytes=len(raw), exchange_ts_ns=_rfc3339_to_ns(t.time),
                        local_recv_ts_ns=local_recv_ts_ns,
                    ))
            return out

        return []  # heartbeats, subscriptions, unknown control frames

    async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        if event.kind == "error":
            raise ResyncRequired(f"connection sequence gap at {event.sequence}")

        if event.kind == "snapshot":
            bids, asks, wbids, wasks = _split_levels(event.payload.updates)
            ctx.book.apply_snapshot(event.sequence, bids, asks)
            ctx.set_state(SymbolState.LIVE, reason="snapshot")
            await self._emit(
                book_snapshot_topic(self.exchange, ctx.symbol),
                BookSnapshot(
                    exchange=self.exchange, symbol=ctx.symbol, sequence=event.sequence,
                    bids=wbids, asks=wasks,
                    exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
            return

        if event.kind == "delta":
            if ctx.state is not SymbolState.LIVE:
                raise ResyncRequired("delta before snapshot")
            bids, asks, wbids, wasks = _split_levels(event.payload.updates)
            ctx.book.apply_delta(event.sequence, bids, asks)
            await self._emit(
                book_delta_topic(self.exchange, ctx.symbol),
                BookDelta(
                    exchange=self.exchange, symbol=ctx.symbol, sequence=event.sequence,
                    bids=wbids, asks=wasks,
                    exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
            return

        if event.kind == "trade":
            t = event.payload
            await self._emit(
                trade_topic(self.exchange, ctx.symbol),
                Trade(
                    exchange=self.exchange, symbol=ctx.symbol, trade_id=t.trade_id,
                    price=t.price, size=t.size, side=_trade_side(t.side),
                    exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )

    # ─── internals ─────────────────────────────────────────────────────────

    def _reset_contexts(self) -> None:
        super()._reset_contexts()
        self._expected_seq = None

    async def _emit(self, topic: str, msg, symbol: str, event: ParsedEvent) -> None:
        await self.producer.send_and_wait(
            topic, encode(msg),
            key=f"{self.exchange}:{symbol}".encode(),
            headers=latency_headers(event.local_recv_ts_ns, event.exchange_ts_ns),
        )


def _split_levels(
    updates: list[_Update],
) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]],
           list[BookLevel], list[BookLevel]]:
    """One pass over updates → (decimal bids, decimal asks, wire bids, wire asks).

    Decimal tuples feed OrderBook; BookLevel wire rows feed the Kafka payload.
    """
    bids: list[tuple[Decimal, Decimal]] = []
    asks: list[tuple[Decimal, Decimal]] = []
    wbids: list[BookLevel] = []
    wasks: list[BookLevel] = []
    for u in updates:
        level = (Decimal(u.price_level), Decimal(u.new_quantity))
        wire = BookLevel(price=u.price_level, size=u.new_quantity)
        if _l2_side(u.side) is Side.BID:
            bids.append(level)
            wbids.append(wire)
        else:
            asks.append(level)
            wasks.append(wire)
    return bids, asks, wbids, wasks
