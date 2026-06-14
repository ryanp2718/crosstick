"""Binance USDⓈ-M perpetual-futures ingester — liquidations, mark/funding,
open interest, and the perp L2 book + trade tape.

Feed: wss://fstream.binance.com routed combined streams (no auth) plus REST
on fapi.binance.com for depth snapshots and open interest.
Design + verified wire contract: docs/DESIGN_perp_capture.md.

Binance ROUTES futures WS streams by type (mandatory since the 2026-04-23
legacy-URL sunset): depth lives on /public, aggTrade/markPrice/forceOrder on
/market, and a connection to one path silently never delivers the other
path's streams. So this driver runs as TWO instances of the same class
(main.py builds both into one process), selected by ``mode``:

  - mode="market": /market/stream — forceOrder + markPrice + aggTrade, plus
    the slice-2 open-interest REST poll rider. Book-less and stateless.
  - mode="depth":  /public/stream — depth@100ms + the REST-snapshot/BUFFERING
    book machinery. This instance owns md.status.binance-futures: venue
    status exists to evict stale book legs from NBBO, and the book lives
    here. The market connection's liveness is observable independently via
    the markPrice@1s cadence (md_messages_received_total).

Slice 1 (forceOrder + markPrice) is stateless: both streams are venue-computed
snapshots, so a reconnect (Binance force-closes at 24h) costs only the gap
window.

Slice 3 (depth + aggTrade) brings the REST-snapshot/BUFFERING machinery — but
the FUTURES depth sync differs from spot in every load-bearing rule, which is
why none of binance.py's sync code is reused:

  - continuity: each event's `pu` must equal the previous event's `u`
    (spot: `U == prev_u + 1`);
  - sync point: the first applied event must OVERLAP the snapshot,
    `U <= lastUpdateId <= u` — futures events straddle the snapshot id,
    they do not resume at lastUpdateId+1;
  - pre-snapshot drop: `u < lastUpdateId`, strictly (spot: `u <= lastUpdateId`).

Slice 2 (open interest) is a REST poll rider: there is no OI stream, so a
background loop polls /fapi/v1/openInterest per symbol and emits
md.openinterest.* — independent of the WS connection state.

Wire notes:
  - All streams are known at construction, so each instance connects to its
    routed /<path>/stream?streams=... URL and never sends live SUBSCRIBE
    messages (build_subscribe_messages returns []).
  - Combined-stream frames are enveloped {"stream": ..., "data": {...}}.
  - forceOrder is SAMPLED: largest liquidation per symbol per 1000ms only.
  - futures has only @aggTrade — there is no raw per-fill trade stream.
  - markPrice@1s ticks every second per symbol, which makes the staleness
    watchdog meaningful even when no liquidations occur.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from decimal import Decimal
from typing import Literal

import httpx
import msgspec

from common.kafka_io import (
    book_delta_topic,
    book_snapshot_topic,
    liquidation_topic,
    markprice_topic,
    openinterest_topic,
    trade_topic,
)
from common.metrics import book_resyncs
from common.models import (
    BookDelta,
    BookLevel,
    BookSnapshot,
    Liquidation,
    MarkPrice,
    OpenInterest,
    Side,
    Trade,
)
from ingest.base_ingester import (
    BaseIngester,
    ParsedEvent,
    ResyncRequired,
    SymbolContext,
    SymbolState,
)

log = logging.getLogger(__name__)

DEFAULT_WS_HOST = "wss://fstream.binance.com"
DEFAULT_REST_BASE = "https://fapi.binance.com"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_DEPTH_LIMIT = 1000
# market: markPrice@1s guarantees ~1 frame/s/symbol when healthy;
# depth: depth@100ms is ~10 frames/s/symbol — both well inside 10s.
DEFAULT_STALE_TIMEOUT = 10.0
DEFAULT_OI_POLL_INTERVAL = 10.0  # finer than the 5m REST-backfill grain, weight-trivial

Mode = Literal["market", "depth"]
_MODE_PATH: dict[str, str] = {"market": "market", "depth": "public"}

# Sentinel stored in self._snapshots when the REST fetch failed: the applier
# resyncs on the next delta instead of waiting for the buffer to overflow.
_FETCH_FAILED = object()


def stream_names(symbols: list[str], mode: Mode) -> list[str]:
    if mode == "depth":
        return [f"{sym.lower()}@depth@100ms" for sym in symbols]
    return [name for sym in symbols for name in
            (f"{sym.lower()}@forceOrder", f"{sym.lower()}@markPrice@1s",
             f"{sym.lower()}@aggTrade")]


# ─── wire structs (msgspec) ──────────────────────────────────────────────────


class _Envelope(msgspec.Struct):
    """Combined-stream frame; `data` stays raw until discriminated by `e`."""
    stream: str = ""
    data: msgspec.Raw = msgspec.Raw(b"")


class _Disc(msgspec.Struct):
    e: str = ""


class _ForceOrder(msgspec.Struct):
    """Inner `o` object of a forceOrder event."""
    s: str
    S: str
    q: str
    p: str
    ap: str
    X: str
    z: str
    T: int = 0


class _ForceOrderEvent(msgspec.Struct):
    o: _ForceOrder
    E: int = 0


class _MarkPriceEvent(msgspec.Struct):
    s: str
    p: str
    i: str
    r: str
    T: int = 0
    P: str = ""
    E: int = 0


class _DepthEvent(msgspec.Struct):
    s: str
    U: int
    u: int
    pu: int
    b: list[BookLevel] = msgspec.field(default_factory=list)
    a: list[BookLevel] = msgspec.field(default_factory=list)
    E: int = 0


class _AggTradeEvent(msgspec.Struct):
    s: str
    a: int
    p: str
    q: str
    m: bool
    T: int = 0


class _DepthSnapshot(msgspec.Struct):
    lastUpdateId: int
    bids: list[BookLevel] = msgspec.field(default_factory=list)
    asks: list[BookLevel] = msgspec.field(default_factory=list)


class _OpenInterestResp(msgspec.Struct):
    openInterest: str
    symbol: str
    time: int = 0


_ENV_DEC = msgspec.json.Decoder(_Envelope)
_DISC_DEC = msgspec.json.Decoder(_Disc)
_FORCE_DEC = msgspec.json.Decoder(_ForceOrderEvent)
_MARK_DEC = msgspec.json.Decoder(_MarkPriceEvent)
_DEPTH_DEC = msgspec.json.Decoder(_DepthEvent)
_AGG_DEC = msgspec.json.Decoder(_AggTradeEvent)
_SNAP_DEC = msgspec.json.Decoder(_DepthSnapshot)
_OI_DEC = msgspec.json.Decoder(_OpenInterestResp)


def _levels_to_decimal(levels: list[BookLevel]) -> list[tuple[Decimal, Decimal]]:
    return [(Decimal(lvl.price), Decimal(lvl.size)) for lvl in levels]


# ─── ingester ────────────────────────────────────────────────────────────────


class BinanceFuturesIngester(BaseIngester):
    def __init__(
        self,
        *,
        producer,
        mode: Mode,
        symbols: list[str] | None = None,
        ws_host: str = DEFAULT_WS_HOST,
        rest_base: str = DEFAULT_REST_BASE,
        depth_limit: int = DEFAULT_DEPTH_LIMIT,
        stale_timeout: float | None = DEFAULT_STALE_TIMEOUT,
        oi_poll_interval_s: float | None = DEFAULT_OI_POLL_INTERVAL,
        **kw: object,
    ) -> None:
        symbols = symbols if symbols is not None else list(DEFAULT_SYMBOLS)
        self._mode: Mode = mode
        if mode == "market":
            # Book-less: no periodic re-snapshot, and no md.status heartbeats —
            # the depth instance owns venue status (see module docstring).
            kw.setdefault("snapshot_interval_s", None)
            kw.setdefault("heartbeat_s", None)
        super().__init__(
            exchange="binance-futures",
            symbols=symbols,
            ws_url=(f"{ws_host}/{_MODE_PATH[mode]}/stream"
                    f"?streams={'/'.join(stream_names(symbols, mode))}"),
            producer=producer,
            stale_timeout=stale_timeout,
            **kw,
        )
        self._rest_base = rest_base.rstrip("/")
        self._depth_limit = depth_limit
        self._oi_poll_interval_s = oi_poll_interval_s
        self._client: httpx.AsyncClient | None = None
        # Per-connection REST-snapshot handoff (background fetch -> applier).
        self._snapshots: dict[str, object] = {}
        self._snapshot_tasks: dict[str, asyncio.Task[None]] = {}
        # Symbols past their first applied delta: from then on continuity is
        # the pu chain. A flag, not spot's last_seq==snap_id trick — a first
        # event ending exactly at lastUpdateId leaves last_seq unchanged.
        self._synced: set[str] = set()
        # Bumped on every reconnect; a fetch tagged with a stale generation
        # discards its result instead of poisoning the fresh connection.
        self._generation = 0

    # ─── lifecycle ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        if self._mode != "market":
            await super().run()
            return
        poller = asyncio.create_task(self._oi_poll_loop(), name="binance-futures-oi")
        try:
            await super().run()
        finally:
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller

    # ─── hooks ───────────────────────────────────────────────────────────────

    async def bootstrap(self, symbol: str) -> None:
        """Depth mode: flip BUFFERING and fetch the REST snapshot in the
        background. Market mode: no-op — its streams are stateless.

        Returns immediately: bootstrap runs inside the connect sequence before
        the reader/applier start, so it must not block on the REST round-trip.
        The snapshot is applied later, on the applier."""
        if self._mode == "market":
            return
        self.contexts[symbol].set_state(SymbolState.BUFFERING, reason="awaiting REST snapshot")
        self._snapshot_tasks[symbol] = asyncio.create_task(
            self._fetch_snapshot(symbol), name=f"binance-futures-snap-{symbol}"
        )

    def build_subscribe_messages(self) -> list[str]:
        return []  # streams are in the connection URL

    def parse_message(self, raw: bytes, local_recv_ts_ns: int) -> list[ParsedEvent]:
        env = _ENV_DEC.decode(raw)
        if not env.stream:
            return []  # not a combined-stream frame (control/unknown)
        data = bytes(env.data)
        kind = _DISC_DEC.decode(data).e
        if kind == "depthUpdate":
            d = _DEPTH_DEC.decode(data)
            return [ParsedEvent(
                symbol=d.s, kind="delta", sequence=d.u, payload=d,
                raw_bytes=len(raw), exchange_ts_ns=d.E * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        if kind == "aggTrade":
            t = _AGG_DEC.decode(data)
            return [ParsedEvent(
                symbol=t.s, kind="trade", payload=t,
                raw_bytes=len(raw), exchange_ts_ns=t.T * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        if kind == "forceOrder":
            ev = _FORCE_DEC.decode(data)
            return [ParsedEvent(
                symbol=ev.o.s, kind="liquidation", payload=ev.o,
                raw_bytes=len(raw), exchange_ts_ns=ev.o.T * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        if kind == "markPriceUpdate":
            m = _MARK_DEC.decode(data)
            return [ParsedEvent(
                symbol=m.s, kind="mark_price", payload=m,
                raw_bytes=len(raw), exchange_ts_ns=m.E * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        return []

    async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        if event.kind == "liquidation":
            o = event.payload
            await self._emit(
                liquidation_topic(self.exchange, ctx.symbol),
                Liquidation(
                    exchange=self.exchange, symbol=ctx.symbol,
                    side=Side.ASK if o.S == "SELL" else Side.BID,
                    price=o.p, avg_price=o.ap, orig_size=o.q, filled_size=o.z,
                    status=o.X,
                    exchange_ts_ns=event.exchange_ts_ns,
                    local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
            return
        if event.kind == "mark_price":
            m = event.payload
            await self._emit(
                markprice_topic(self.exchange, ctx.symbol),
                MarkPrice(
                    exchange=self.exchange, symbol=ctx.symbol,
                    mark_price=m.p, index_price=m.i, est_settle_price=m.P,
                    funding_rate=m.r, next_funding_ts_ns=m.T * 1_000_000,
                    exchange_ts_ns=event.exchange_ts_ns,
                    local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
            return
        if event.kind == "trade":
            t = event.payload
            await self._emit(
                trade_topic(self.exchange, ctx.symbol),
                Trade(
                    exchange=self.exchange, symbol=ctx.symbol, trade_id=str(t.a),
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
        """Futures depth sync — overlap + pu chain, NOT spot's +1 update ids."""
        ev = event.payload
        if ctx.symbol not in self._synced:
            # First event after the REST snapshot: drop anything that ended
            # before it; what remains must straddle lastUpdateId.
            if ev.u < ctx.last_seq:
                return  # entirely covered by the snapshot
            if not (ev.U <= ctx.last_seq <= ev.u):
                raise ResyncRequired(
                    f"snapshot stale: first delta [U={ev.U}, u={ev.u}] does not "
                    f"straddle lastUpdateId={ctx.last_seq}"
                )
            self._synced.add(ctx.symbol)
            if ev.u == ctx.last_seq:
                # Sync point established, but every update in this event is
                # already in the snapshot — applying would violate the book's
                # monotonic-sequence invariant for nothing.
                return
        elif ev.pu != ctx.last_seq:
            raise ResyncRequired(f"depth continuity broken: pu={ev.pu} != prev u={ctx.last_seq}")
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
            f"{self._rest_base}/fapi/v1/depth",
            params={"symbol": symbol, "limit": self._depth_limit},
        )
        resp.raise_for_status()
        snap = _SNAP_DEC.decode(resp.content)
        return snap.lastUpdateId, snap.bids, snap.asks

    # ─── open interest (slice 2: REST poll, no stream exists) ───────────────

    async def _oi_poll_loop(self) -> None:
        """Emit md.openinterest.* every oi_poll_interval_s per symbol.

        Independent of the WS connection: OI is a REST resource, so a depth
        reconnect shouldn't gap it. Per-symbol failures log and skip — the
        poll must survive transient REST errors for the process lifetime."""
        if self._oi_poll_interval_s is None:
            return
        while not self._shutdown.is_set():
            await asyncio.sleep(self._oi_poll_interval_s)
            await self._poll_open_interest()

    async def _poll_open_interest(self) -> None:
        for symbol in self.symbols:
            try:
                resp = await self._fetch_open_interest(symbol)
                local_ts_ns = time.time_ns()
                event = ParsedEvent(
                    symbol=symbol, kind="open_interest",
                    exchange_ts_ns=resp.time * 1_000_000,
                    local_recv_ts_ns=local_ts_ns,
                )
                await self._emit(
                    openinterest_topic(self.exchange, symbol),
                    OpenInterest(
                        exchange=self.exchange, symbol=symbol,
                        open_interest=resp.openInterest,
                        exchange_ts_ns=event.exchange_ts_ns,
                        local_ts_ns=local_ts_ns,
                    ),
                    symbol, event,
                )
            except Exception as e:
                log.warning("open-interest poll failed for %s/%s: %s",
                            self.exchange, symbol, e)

    async def _fetch_open_interest(self, symbol: str) -> _OpenInterestResp:
        client = await self._get_client()
        resp = await client.get(
            f"{self._rest_base}/fapi/v1/openInterest", params={"symbol": symbol}
        )
        resp.raise_for_status()
        return _OI_DEC.decode(resp.content)

    # ─── plumbing ────────────────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        return self._client

    def _mark_all_stale(self, reason: str) -> None:
        if self._mode == "market":
            # No book state to invalidate — and both instances share the
            # (exchange, symbol) metric labels, so a market-connection blip
            # must not overwrite the depth instance's md_book_state.
            return
        super()._mark_all_stale(reason)

    def _reset_contexts(self) -> None:
        super()._reset_contexts()
        self._generation += 1
        for task in self._snapshot_tasks.values():
            task.cancel()
        self._snapshot_tasks.clear()
        self._snapshots.clear()
        self._synced.clear()

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
