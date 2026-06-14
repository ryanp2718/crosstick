"""Binance Spot market-data ingester.

Feed: wss://stream.binance.com:9443/ws (public, no auth) + REST depth snapshot.

Wire contract (verified against current Binance docs, see plan):
  - Subscribe with {"method":"SUBSCRIBE","params":[...],"id":1}; stream names are
    lowercase ("btcusdt@depth@100ms", "btcusdt@trade"). The ack {"result":null,
    "id":1} carries no `e` field and is ignored.
  - Diff-depth event: e="depthUpdate", E (ms), s (UPPER symbol), U (first update
    id), u (final update id), b/a = [price, qty] string arrays (qty "0" = remove).
  - Trade event: e="trade", t (id), p, q, T (ms), m (buyer-is-maker). Taker side:
    m=false -> taker bought -> BID; m=true -> taker sold -> ASK.
  - REST snapshot GET /api/v3/depth?symbol=...&limit=... -> {lastUpdateId, bids, asks}.

Local-book sync (the reason BaseIngester has BUFFERING / buffer_append):
  1. bootstrap() flips the symbol BUFFERING and kicks off the REST fetch in the
     background, returning immediately (the connect sequence must not block — the
     reader/applier only start once every bootstrap returns).
  2. WS deltas that arrive before the snapshot lands are buffered (bounded; an
     overflow resyncs). Trades are emitted immediately regardless of book state.
  3. When the snapshot lands the applier applies it, drains the buffer, and goes
     LIVE. Each delta is gap-checked by update id: drop u <= lastUpdateId, the
     first applied delta must straddle lastUpdateId+1 (U <= lastUpdateId+1 <= u),
     and thereafter every delta's U must equal the previous u + 1. Any gap resyncs.

The REST fetch runs off the applier but the snapshot is *applied* on the applier
(single writer to the book). A per-connection generation guards against a fetch
from a torn-down connection writing into the next one.
"""
from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal

import httpx
import msgspec

from common.kafka_io import book_delta_topic, book_snapshot_topic, trade_topic
from common.metrics import book_resyncs
from common.models import BookDelta, BookLevel, BookSnapshot, Side, Trade
from ingest.base_ingester import (
    BaseIngester,
    ParsedEvent,
    ResyncRequired,
    SymbolContext,
    SymbolState,
)

log = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://stream.binance.com:9443/ws"
DEFAULT_REST_BASE = "https://api.binance.com"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_DEPTH_LIMIT = 1000

# Sentinel stored in self._snapshots when the REST fetch failed: the applier
# resyncs on the next delta instead of waiting for the buffer to overflow.
_FETCH_FAILED = object()


# ─── wire structs (msgspec) ──────────────────────────────────────────────────


class _Disc(msgspec.Struct):
    """Discriminator: every market frame carries `e`; control acks do not."""
    e: str = ""


class _DepthEvent(msgspec.Struct):
    s: str
    U: int
    u: int
    b: list[BookLevel] = msgspec.field(default_factory=list)
    a: list[BookLevel] = msgspec.field(default_factory=list)
    E: int = 0


class _TradeEvent(msgspec.Struct):
    s: str
    t: int
    p: str
    q: str
    m: bool
    T: int = 0


class _DepthSnapshot(msgspec.Struct):
    lastUpdateId: int
    bids: list[BookLevel] = msgspec.field(default_factory=list)
    asks: list[BookLevel] = msgspec.field(default_factory=list)


_DISC_DEC = msgspec.json.Decoder(_Disc)
_DEPTH_DEC = msgspec.json.Decoder(_DepthEvent)
_TRADE_DEC = msgspec.json.Decoder(_TradeEvent)
_SNAP_DEC = msgspec.json.Decoder(_DepthSnapshot)


# ─── helpers ─────────────────────────────────────────────────────────────────


def _levels_to_decimal(levels: list[BookLevel]) -> list[tuple[Decimal, Decimal]]:
    return [(Decimal(lvl.price), Decimal(lvl.size)) for lvl in levels]


# ─── ingester ────────────────────────────────────────────────────────────────


class BinanceIngester(BaseIngester):
    def __init__(
        self,
        *,
        producer,
        symbols: list[str] | None = None,
        ws_url: str = DEFAULT_WS_URL,
        rest_base: str = DEFAULT_REST_BASE,
        depth_limit: int = DEFAULT_DEPTH_LIMIT,
        **kw: object,
    ) -> None:
        super().__init__(
            exchange="binance",
            symbols=symbols if symbols is not None else list(DEFAULT_SYMBOLS),
            ws_url=ws_url,
            producer=producer,
            **kw,
        )
        self._rest_base = rest_base.rstrip("/")
        self._depth_limit = depth_limit
        self._client: httpx.AsyncClient | None = None
        # Per-connection REST-snapshot handoff (background fetch -> applier).
        self._snapshots: dict[str, object] = {}
        self._snapshot_tasks: dict[str, asyncio.Task[None]] = {}
        self._snap_last_id: dict[str, int] = {}
        # Bumped on every reconnect; a fetch tagged with a stale generation
        # discards its result instead of poisoning the fresh connection.
        self._generation = 0

    # ─── hooks ───────────────────────────────────────────────────────────────

    async def bootstrap(self, symbol: str) -> None:
        """Flip BUFFERING and fetch the REST snapshot in the background.

        Returns immediately: bootstrap runs inside the connect sequence before
        the reader/applier start, so it must not block on the REST round-trip
        (gotcha #10). The snapshot is applied later, on the applier."""
        self.contexts[symbol].set_state(SymbolState.BUFFERING, reason="awaiting REST snapshot")
        self._snapshot_tasks[symbol] = asyncio.create_task(
            self._fetch_snapshot(symbol), name=f"binance-snap-{symbol}"
        )

    def build_subscribe_messages(self) -> list[str]:
        params: list[str] = []
        for sym in self.symbols:
            low = sym.lower()
            params.append(f"{low}@depth@100ms")
            params.append(f"{low}@trade")
        return [json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1})]

    def parse_message(self, raw: bytes, local_recv_ts_ns: int) -> list[ParsedEvent]:
        kind = _DISC_DEC.decode(raw).e
        if kind == "depthUpdate":
            ev = _DEPTH_DEC.decode(raw)
            return [ParsedEvent(
                symbol=ev.s, kind="delta", sequence=ev.u, payload=ev,
                raw_bytes=len(raw), exchange_ts_ns=ev.E * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        if kind == "trade":
            t = _TRADE_DEC.decode(raw)
            return [ParsedEvent(
                symbol=t.s, kind="trade", payload=t,
                raw_bytes=len(raw), exchange_ts_ns=t.T * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        return []  # subscribe ack / unknown control frame

    async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        if event.kind == "trade":
            t = event.payload
            await self._emit(
                trade_topic(self.exchange, ctx.symbol),
                Trade(
                    exchange=self.exchange, symbol=ctx.symbol, trade_id=str(t.t),
                    price=t.p, size=t.q, side=Side.ASK if t.m else Side.BID,
                    exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
            return

        # event.kind == "delta"
        if ctx.state is SymbolState.BUFFERING:
            snap = self._snapshots.get(ctx.symbol)
            if snap is None:
                ctx.buffer_append(event)  # raises ResyncRequired at MAX_BUFFER_DELTAS
                return
            if snap is _FETCH_FAILED:
                raise ResyncRequired("REST snapshot fetch failed")
            await self._apply_snapshot_and_drain(ctx, snap, event)
            return

        if ctx.state is SymbolState.LIVE:
            await self._apply_delta_synced(ctx, event)
            return

        raise ResyncRequired(f"delta in state {ctx.state}")

    # ─── internals ─────────────────────────────────────────────────────────

    async def _apply_snapshot_and_drain(
        self, ctx: SymbolContext, snap: object, event: ParsedEvent
    ) -> None:
        last_id, wbids, wasks = snap  # type: ignore[misc]
        ctx.book.apply_snapshot(last_id, _levels_to_decimal(wbids), _levels_to_decimal(wasks))
        ctx.set_state(SymbolState.LIVE, reason="rest snapshot")
        ctx.last_seq = last_id
        self._snap_last_id[ctx.symbol] = last_id
        await self._emit(
            book_snapshot_topic(self.exchange, ctx.symbol),
            BookSnapshot(
                exchange=self.exchange, symbol=ctx.symbol, sequence=last_id,
                bids=wbids, asks=wasks,
                exchange_ts_ns=0, local_ts_ns=event.local_recv_ts_ns,
                epoch=self._epoch,
            ),
            ctx.symbol, event,
        )
        # Replay everything buffered while the snapshot was in flight, then the
        # delta that triggered the snapshot application.
        buffered = list(ctx.buffered)
        ctx.buffered.clear()
        self._snapshots.pop(ctx.symbol, None)
        for be in buffered:
            await self._apply_delta_synced(ctx, be)
        await self._apply_delta_synced(ctx, event)

    async def _apply_delta_synced(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        ev = event.payload
        if ev.u <= ctx.last_seq:
            return  # stale/duplicate; already covered by the snapshot or a prior delta
        is_first = ctx.last_seq == self._snap_last_id.get(ctx.symbol)
        if is_first:
            # First delta after the snapshot must straddle lastUpdateId+1.
            if ev.U > ctx.last_seq + 1:
                raise ResyncRequired(
                    f"snapshot stale: first delta U={ev.U} > lastUpdateId+1={ctx.last_seq + 1}"
                )
        elif ev.U != ctx.last_seq + 1:
            raise ResyncRequired(f"update-id gap: U={ev.U} expected {ctx.last_seq + 1}")
        ctx.book.apply_delta(ev.u, _levels_to_decimal(ev.b), _levels_to_decimal(ev.a))
        await self._emit(
            book_delta_topic(self.exchange, ctx.symbol),
            BookDelta(
                exchange=self.exchange, symbol=ctx.symbol, sequence=ev.u,
                bids=ev.b, asks=ev.a,
                exchange_ts_ns=event.exchange_ts_ns, local_ts_ns=event.local_recv_ts_ns,
                epoch=self._epoch,
            ),
            ctx.symbol, event,
        )
        ctx.last_seq = ev.u

    async def _fetch_snapshot(self, symbol: str) -> None:
        gen = self._generation
        try:
            value: object = await self._rest_snapshot(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("REST snapshot failed for %s/%s: %s", self.exchange, symbol, e)
            book_resyncs.labels(exchange=self.exchange, reason="rest_snapshot_error").inc()
            value = _FETCH_FAILED
        # Discard if this connection was torn down while the fetch was in flight.
        if gen == self._generation:
            self._snapshots[symbol] = value

    async def _rest_snapshot(
        self, symbol: str
    ) -> tuple[int, list[BookLevel], list[BookLevel]]:
        client = await self._get_client()
        resp = await client.get(
            f"{self._rest_base}/api/v3/depth",
            params={"symbol": symbol, "limit": self._depth_limit},
        )
        resp.raise_for_status()
        snap = _SNAP_DEC.decode(resp.content)
        return snap.lastUpdateId, snap.bids, snap.asks

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        return self._client

    def _reset_contexts(self) -> None:
        super()._reset_contexts()
        self._generation += 1
        for task in self._snapshot_tasks.values():
            task.cancel()
        self._snapshot_tasks.clear()
        self._snapshots.clear()
        self._snap_last_id.clear()

    async def shutdown(self) -> None:
        await super().shutdown()
        tasks = list(self._snapshot_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._client is not None:
            await self._client.aclose()
            self._client = None
