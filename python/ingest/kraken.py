"""Kraken Spot v2 market-data ingester.

Feed: wss://ws.kraken.com/v2 (public, no auth). Snapshot is in-band (no REST).

Wire contract (verified against current Kraken v2 docs, see plan):
  - Subscribe with {"method":"subscribe","params":{"channel":"book","symbol":[...],
    "depth":10,"snapshot":true}} and a separate {"channel":"trade",...}. The ack
    {"method":"subscribe","success":true,...} carries no `channel` and is ignored,
    as are {"channel":"heartbeat"} and {"channel":"status"} control frames.
  - Book message: {"channel":"book","type":"snapshot"|"update","data":[{symbol,
    bids:[{price,qty}],asks:[{price,qty}],checksum,timestamp}]}. price/qty are JSON
    numbers; qty 0 removes the level. There is NO per-message sequence number —
    integrity is verified by the CRC32 `checksum` over the top-10 book after each
    apply. A mismatch means we missed an update -> resync.
  - Trade message: {"channel":"trade","type":...,"data":[{symbol,side,price,qty,
    trade_id,timestamp}]}. `side` is the taker direction "buy"/"sell" -> BID/ASK.

Decimal, not float: the checksum strips the decimal point and leading zeros but
KEEPS trailing zeros ("0.00100000" -> "100000"), so the exact wire digits matter.
msgspec decodes the JSON number straight into Decimal preserving those digits, and
str(Decimal) reproduces the original token — feeding kraken_checksum() the book's
own stored values therefore reconstructs Kraken's checksum input exactly.

Kraken carries no sequence number, so OrderBook's monotonic-sequence guard is fed a
synthetic per-connection counter (snapshot = 0, each update += 1).
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal

import msgspec

from common.kafka_io import book_delta_topic, book_snapshot_topic, trade_topic
from common.models import BookDelta, BookLevel, BookSnapshot, Side, Trade
from ingest.base_ingester import (
    BaseIngester,
    ParsedEvent,
    ResyncRequired,
    SymbolContext,
    SymbolState,
)
from ingest.book import kraken_checksum

log = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://ws.kraken.com/v2"
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD"]
DEFAULT_DEPTH = 10

# Taker direction -> aggressed side (matches coinbase/binance convention).
_TRADE_SIDE = {"buy": Side.BID, "sell": Side.ASK}


# ─── wire structs (msgspec, two-stage decode) ───────────────────────────────


class _Disc(msgspec.Struct):
    """Discriminator: data channels carry `channel`; controls vary."""
    channel: str = ""
    type: str = ""


class _Level(msgspec.Struct):
    price: Decimal
    qty: Decimal


class _BookData(msgspec.Struct):
    symbol: str
    checksum: int = 0
    timestamp: str = ""
    bids: list[_Level] = msgspec.field(default_factory=list)
    asks: list[_Level] = msgspec.field(default_factory=list)


class _BookMsg(msgspec.Struct):
    data: list[_BookData] = msgspec.field(default_factory=list)


class _TradeData(msgspec.Struct):
    symbol: str
    side: str
    price: Decimal
    qty: Decimal
    trade_id: int = 0
    timestamp: str = ""


class _TradeMsg(msgspec.Struct):
    data: list[_TradeData] = msgspec.field(default_factory=list)


_DISC_DEC = msgspec.json.Decoder(_Disc)
_BOOK_DEC = msgspec.json.Decoder(_BookMsg)
_TRADE_DEC = msgspec.json.Decoder(_TradeMsg)


# ─── helpers ─────────────────────────────────────────────────────────────────


def _trade_side(s: str) -> Side:
    try:
        return _TRADE_SIDE[s]
    except KeyError:
        raise ResyncRequired(f"unexpected trade side {s!r}") from None


def _rfc3339_to_ns(ts: str) -> int:
    """Parse a UTC RFC3339 timestamp ('...Z') to epoch nanoseconds.

    datetime only resolves microseconds, so the fractional part (up to 9 digits)
    is taken from the string directly. Kraken always emits 'Z' UTC times.
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


def _book_levels(levels: list[_Level]) -> list[tuple[Decimal, Decimal]]:
    return [(lvl.price, lvl.qty) for lvl in levels]


def _wire_levels(levels: list[_Level]) -> list[BookLevel]:
    # str(Decimal) reproduces the exact wire digits (trailing zeros kept).
    return [BookLevel(price=str(lvl.price), size=str(lvl.qty)) for lvl in levels]


def _deletes(prices: list[Decimal]) -> list[BookLevel]:
    # size "0" is the cross-exchange remove convention; relays a depth eviction
    # that Kraken itself never sends, so the published delta stays reconstructable.
    return [BookLevel(price=str(px), size="0") for px in prices]


# ─── ingester ────────────────────────────────────────────────────────────────


class KrakenIngester(BaseIngester):
    def __init__(
        self,
        *,
        producer,
        symbols: list[str] | None = None,
        ws_url: str = DEFAULT_WS_URL,
        depth: int = DEFAULT_DEPTH,
        **kw: object,
    ) -> None:
        # Heartbeats arrive ~1/s so a silent-feed watchdog is safe.
        kw.setdefault("stale_timeout", 15.0)
        super().__init__(
            exchange="kraken",
            symbols=symbols if symbols is not None else list(DEFAULT_SYMBOLS),
            ws_url=ws_url,
            producer=producer,
            **kw,
        )
        self._depth = depth

    # ─── hooks ───────────────────────────────────────────────────────────────

    async def bootstrap(self, symbol: str) -> None:
        """No-op: the snapshot arrives in-band on the book channel and
        process_event() flips the symbol LIVE when it lands."""
        return

    def build_subscribe_messages(self) -> list[str]:
        return [
            json.dumps({
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": self.symbols,
                    "depth": self._depth,
                    "snapshot": True,
                },
            }),
            json.dumps({
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": self.symbols, "snapshot": False},
            }),
        ]

    def parse_message(self, raw: bytes, local_recv_ts_ns: int) -> list[ParsedEvent]:
        disc = _DISC_DEC.decode(raw)
        if disc.channel == "book":
            kind = "snapshot" if disc.type == "snapshot" else "delta"
            out: list[ParsedEvent] = []
            for d in _BOOK_DEC.decode(raw).data:
                out.append(ParsedEvent(
                    symbol=d.symbol, kind=kind, payload=d, raw_bytes=len(raw),
                    exchange_ts_ns=_rfc3339_to_ns(d.timestamp),
                    local_recv_ts_ns=local_recv_ts_ns,
                ))
            return out
        if disc.channel == "trade":
            out = []
            for t in _TRADE_DEC.decode(raw).data:
                out.append(ParsedEvent(
                    symbol=t.symbol, kind="trade", payload=t, raw_bytes=len(raw),
                    exchange_ts_ns=_rfc3339_to_ns(t.timestamp),
                    local_recv_ts_ns=local_recv_ts_ns,
                ))
            return out
        return []  # subscribe ack / heartbeat / status

    async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        if event.kind == "trade":
            t = event.payload
            await self._emit(
                trade_topic(self.exchange, ctx.symbol),
                Trade(
                    exchange=self.exchange, symbol=ctx.symbol, trade_id=str(t.trade_id),
                    price=str(t.price), size=str(t.qty), side=_trade_side(t.side),
                    exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
            return

        if event.kind == "snapshot":
            d = event.payload
            ctx.book.apply_snapshot(0, _book_levels(d.bids), _book_levels(d.asks))
            ctx.book.trim(self._depth)
            ctx.last_seq = 0
            self._verify_checksum(ctx, d.checksum)
            ctx.set_state(SymbolState.LIVE, reason="snapshot")
            await self._emit(
                book_snapshot_topic(self.exchange, ctx.symbol),
                BookSnapshot(
                    exchange=self.exchange, symbol=ctx.symbol, sequence=0,
                    bids=_wire_levels(d.bids), asks=_wire_levels(d.asks),
                    exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
            return

        # event.kind == "delta"
        if ctx.state is not SymbolState.LIVE:
            raise ResyncRequired("delta before snapshot")
        d = event.payload
        seq = ctx.last_seq + 1
        ctx.book.apply_delta(seq, _book_levels(d.bids), _book_levels(d.asks))
        # Kraken drops levels that fall out of the depth window WITHOUT sending a
        # delete, so trim locally and relay an explicit size=0 delete for each
        # evicted level — that keeps "snapshot + deltas -> book" reconstructable
        # downstream, matching the coinbase/binance delta contract.
        evicted_bids, evicted_asks = ctx.book.trim(self._depth)
        self._verify_checksum(ctx, d.checksum)
        ctx.last_seq = seq
        await self._emit(
            book_delta_topic(self.exchange, ctx.symbol),
            BookDelta(
                exchange=self.exchange, symbol=ctx.symbol, sequence=seq,
                bids=_wire_levels(d.bids) + _deletes(evicted_bids),
                asks=_wire_levels(d.asks) + _deletes(evicted_asks),
                exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
            ),
            ctx.symbol, event,
        )

    # ─── internals ─────────────────────────────────────────────────────────

    def _verify_checksum(self, ctx: SymbolContext, expected: int) -> None:
        """CRC32 over the top-10 asks (low->high) then bids (high->low). A
        mismatch means a dropped/reordered update — the book is wrong, resync."""
        asks = [(str(px), str(sz)) for px, sz in ctx.book.top_n(Side.ASK, 10)]
        bids = [(str(px), str(sz)) for px, sz in ctx.book.top_n(Side.BID, 10)]
        got = kraken_checksum(asks, bids)
        if got != expected:
            raise ResyncRequired(f"crc mismatch: got {got} expected {expected}")
