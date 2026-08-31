"""Abstract base ingester: WS lifecycle, backoff, token bucket, bounded queue,
per-symbol state machine.

Subclasses (binance.py, coinbase.py, kraken.py) implement four hooks:
    bootstrap(symbol)           - REST snapshot + warm the local book to LIVE
    build_subscribe_messages()  - exchange-specific subscribe payloads
    parse_message(raw, ts_ns)   - decode WS frame to ParsedEvent[]
    process_event(event)        - apply event to book state; emit to Kafka

The base class owns:
    - the WS connect loop with full-jitter exponential backoff
    - the token bucket on outbound subscribe messages
    - the bounded asyncio.Queue between WS reader and the applier
    - per-symbol state: BOOTSTRAP / BUFFERING / LIVE / STALE
    - graceful shutdown
    - Prometheus metric emission

On `asyncio.QueueFull` we close the WS and let the connect-loop resync - see
`docs/anti-patterns.md`: dropping deltas corrupts the book permanently.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import Literal

import websockets
import websockets.exceptions
from aiokafka import AIOKafkaProducer

from common.backoff import FullJitterBackoff
from common.kafka_io import book_snapshot_topic, latency_headers, status_topic
from common.metrics import (
    book_invariant_violations,
    book_resyncs,
    book_state,
    bytes_received,
    exchange_latency,
    messages_received,
    queue_depth,
    recv_clock_backward_steps,
    recv_clock_worst_step_ms,
    ws_reconnects,
)
from common.models import BookLevel, BookSnapshot, Side, Status, encode
from common.ratelimit import AsyncTokenBucket
from ingest.book import BookInvariantError, OrderBook

log = logging.getLogger(__name__)

EventKind = Literal[
    "trade",
    "snapshot",
    "delta",
    "liquidation",
    "mark_price",
    "open_interest",
    "heartbeat",
    "subscribed",
    "error",
    "other",
]

# Maximum number of WS deltas to buffer while a REST snapshot is in-flight
# (Binance pattern).  If this limit is exceeded before the snapshot arrives,
# buffer_append() raises ResyncRequired so the connection is torn down and
# a fresh snapshot is fetched.
MAX_BUFFER_DELTAS: int = 500


class SymbolState(enum.StrEnum):
    BOOTSTRAP = "bootstrap"  # initial; awaiting snapshot (REST or in-band)
    BUFFERING = "buffering"  # WS up; deltas buffered while REST snapshot in flight (Binance)
    LIVE = "live"  # snapshot applied; deltas applying normally
    STALE = "stale"  # gap/CRC mismatch detected; awaiting resync


_STATE_TO_METRIC = {
    SymbolState.BOOTSTRAP: 0,
    SymbolState.BUFFERING: 0,
    SymbolState.LIVE: 1,
    SymbolState.STALE: 2,
}


@dataclass
class SymbolContext:
    exchange: str
    symbol: str
    book: OrderBook
    state: SymbolState = SymbolState.BOOTSTRAP
    last_seq: int = -1
    # Binance pattern: WS deltas that arrive before the REST snapshot completes.
    # Stored as a bounded deque; call buffer_append() to append with overflow detection.
    buffered: deque[ParsedEvent] = field(default_factory=deque)

    def set_state(self, new: SymbolState, *, reason: str = "") -> None:
        if new is self.state:
            return
        log.info(
            "symbol_state %s/%s: %s -> %s (%s)", self.exchange, self.symbol, self.state, new, reason
        )
        self.state = new
        book_state.labels(exchange=self.exchange, symbol=self.symbol).set(_STATE_TO_METRIC[new])

    def buffer_append(self, event: ParsedEvent) -> None:
        """Append a delta to the pre-snapshot buffer.

        Raises ResyncRequired if more than MAX_BUFFER_DELTAS accumulate before
        the snapshot arrives - a stalled bootstrap that keeps buffering is safer
        to restart than to replay a truncated sequence.
        """
        if len(self.buffered) >= MAX_BUFFER_DELTAS:
            raise ResyncRequired(
                f"buffer overflow: >{MAX_BUFFER_DELTAS} deltas arrived before bootstrap"
            )
        self.buffered.append(event)


@dataclass
class ParsedEvent:
    """Normalized exchange event after parsing the raw WS frame."""

    symbol: str
    kind: EventKind
    sequence: int | None = None  # for sequence-validated kinds
    payload: object | None = None  # exchange-specific structured payload
    raw_bytes: int = 0
    exchange_ts_ns: int = 0
    local_recv_ts_ns: int = 0


class ResyncRequired(Exception):
    """Raised by process_event() to signal the base to close + resync this symbol.

    The base catches this, marks the symbol STALE, and aborts the connection
    so the connect-loop resnapshots.
    """


class BaseIngester(ABC):
    """Abstract base for per-exchange ingesters.

    Lifecycle:
        ingester = ConcreteIngester(producer=..., symbols=[...])
        await ingester.run()      # forever; respects shutdown event
        await ingester.shutdown() # graceful

    The run() loop:
        1. wait for jittered backoff (no-op first time)
        2. connect WS
        3. await bootstrap(symbol) for each symbol (sets LIVE or BUFFERING)
        4. send subscribe messages through the token bucket
        5. read frames into the bounded queue; applier drains the queue
        6. on any disconnect/error → close, mark all symbols STALE, loop
    """

    def __init__(
        self,
        *,
        exchange: str,
        symbols: list[str],
        ws_url: str,
        producer: AIOKafkaProducer,
        subscribe_rate: float = 5.0,
        subscribe_capacity: float = 5.0,
        queue_maxsize: int = 1000,
        backoff_base: float = 0.5,
        backoff_cap: float = 30.0,
        ping_interval: float | None = 20.0,
        ping_timeout: float | None = 10.0,
        ws_max_size: int = 2**22,
        stale_timeout: float | None = None,
        heartbeat_s: float | None = 2.0,
        snapshot_interval_s: float | None = 300.0,
    ) -> None:
        self.exchange = exchange
        self.symbols = list(symbols)
        self.ws_url = ws_url
        self.producer = producer
        self.subscribe_bucket = AsyncTokenBucket(subscribe_rate, subscribe_capacity)
        self.backoff = FullJitterBackoff(backoff_base, backoff_cap)
        self.queue_maxsize = queue_maxsize
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        # Per-frame WS receive cap. Coinbase's full-depth L2 snapshot is ~5 MiB,
        # so the 4 MiB default is too small for it - drivers raise this as needed.
        self.ws_max_size = ws_max_size
        # Data-staleness watchdog: a socket can stay alive (heartbeats, pings)
        # while market data goes silent. When set, reconnect if no frame arrives
        # within this many seconds. None disables it (relies on ping/pong only).
        self.stale_timeout = stale_timeout
        self._last_msg_monotonic: float = 0.0
        # Venue-health heartbeat: emit 'up' on md.status.<exchange> every
        # heartbeat_s while streaming so the gateway can tell "alive" from a
        # crashed ingester (which sends no graceful 'down'). None disables all
        # status emission - for venues split across several connections
        # (binance-futures), exactly one instance owns md.status.<exchange>.
        # See common.models.Status.
        self.heartbeat_s = heartbeat_s
        # Periodic re-snapshot: without it a snapshot exists only at
        # bootstrap/resync, so any consumer warming up from the log would have
        # to replay an unbounded delta tail. Re-emitting the local book every
        # snapshot_interval_s bounds that tail to one interval - the keystone
        # of the gateway's warm restart and of bounded replay seeks.
        # None disables (book-less connections, e.g. the binance-futures
        # market-streams instance, have nothing to re-emit).
        self.snapshot_interval_s = snapshot_interval_s
        self._connected = False
        # Per-connection generation stamped on every BookSnapshot/BookDelta this
        # connection emits, so the gateway can tell a prior connection's deltas
        # (whose per-connection sequence counter may have reset) from the current
        # one's. Reset to a fresh value on each connect in _reset_contexts.
        self._epoch = 0

        # Per-symbol context.
        self.contexts: dict[str, SymbolContext] = {
            sym: SymbolContext(exchange=exchange, symbol=sym, book=OrderBook(exchange, sym))
            for sym in self.symbols
        }
        # Initialize state metric.
        for ctx in self.contexts.values():
            book_state.labels(exchange=exchange, symbol=ctx.symbol).set(0)

        # Live recv-clock canary (see _observe_recv_clock). Instance-level so a
        # backward step across a reconnect gap is still caught; pre-init the series
        # at 0 so the "should be 0" panel/alert always has a baseline.
        self._prev_recv_ns: int | None = None
        self._worst_recv_step_ms: float = 0.0
        recv_clock_backward_steps.labels(exchange=exchange)
        recv_clock_worst_step_ms.labels(exchange=exchange).set(0.0)

        self._shutdown = asyncio.Event()
        # New queue per connection; allocated in _connect_and_stream.
        self._queue: asyncio.Queue[tuple[bytes, int] | None] | None = None
        # Per-connection delivery-failure signal. The producer's done-callback
        # sets it; _connect_and_stream awaits it alongside reader/applier so a
        # delivery failure tears the connection down (resync) instead of
        # silently advancing past a gap. Reallocated per connection.
        self._produce_failed: asyncio.Event | None = None

    # ─── abstract hooks ────────────────────────────────────────────────────

    @abstractmethod
    async def bootstrap(self, symbol: str) -> None:
        """Populate the local book for `symbol`.

        For REST-bootstrap exchanges (Binance): fetch the REST snapshot, apply
        any buffered deltas with sequence > snapshot.lastUpdateId, set LIVE.

        For in-band-snapshot exchanges (Coinbase/Kraken): can be a no-op; the
        snapshot frame will arrive over WS and process_event() will apply it.
        """

    @abstractmethod
    def build_subscribe_messages(self) -> list[str]:
        """Return one or more JSON-encoded subscribe payloads to send post-connect."""

    @abstractmethod
    def parse_message(self, raw: bytes, local_recv_ts_ns: int) -> list[ParsedEvent]:
        """Decode a raw WS frame into zero or more ParsedEvent.

        Should not raise on protocol-level "I don't care" messages (acks,
        heartbeats); return an empty list or a heartbeat event.
        """

    @abstractmethod
    async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        """Apply event to the symbol's book/state and emit to Kafka if needed.

        Raise ResyncRequired to trigger a full reconnect+resnapshot of this
        connection (and all its symbols).
        """

    # ─── lifecycle ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main loop. Returns when shutdown() is called and any in-flight
        connection cleanly closes."""
        log.info("ingester start: exchange=%s symbols=%s", self.exchange, self.symbols)
        heartbeat = asyncio.create_task(self._heartbeat_loop(), name="ingest-heartbeat")
        snapshotter = asyncio.create_task(self._snapshot_loop(), name="ingest-snapshotter")
        try:
            while not self._shutdown.is_set():
                try:
                    await self._connect_and_stream()
                except asyncio.CancelledError:
                    raise
                except websockets.exceptions.ConnectionClosed as e:
                    log.warning("ws closed: %s", e)
                    ws_reconnects.labels(exchange=self.exchange, reason="closed").inc()
                except Exception as e:
                    log.exception("ws loop error: %s", e)
                    ws_reconnects.labels(exchange=self.exchange, reason="error").inc()
                finally:
                    self._mark_all_stale("connection_lost")

                if self._shutdown.is_set():
                    break
                delay = await self.backoff.sleep()
                log.info("reconnect after %.2fs (attempt %d)", delay, self.backoff.attempt)
        finally:
            heartbeat.cancel()
            snapshotter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            with contextlib.suppress(asyncio.CancelledError):
                await snapshotter
            # Best-effort graceful 'down' so the gateway evicts us immediately
            # instead of waiting out its liveness timeout.
            if self.heartbeat_s is not None:
                await self._send_status("down")

    async def shutdown(self) -> None:
        log.info("ingester shutdown requested")
        self._shutdown.set()

    # ─── internals ─────────────────────────────────────────────────────────

    async def _send_status(self, state: str) -> None:
        """Publish a venue-health status to md.status.<exchange>, keyed by
        exchange. send_and_wait (not pipelined send) because these are rare and
        we want the 'down' delivered before shutdown completes. Best-effort: a
        failure here must not crash the ingester."""
        try:
            await self.producer.send_and_wait(
                status_topic(self.exchange),
                encode(Status(exchange=self.exchange, state=state, ts_ns=time.time_ns())),
                key=self.exchange.encode(),
            )
        except Exception as e:
            log.warning("status %s send failed: %s", state, e)

    async def _heartbeat_loop(self) -> None:
        """Emit an 'up' heartbeat every heartbeat_s while connected. Pauses
        (no send) during reconnect/backoff so a missed beat means truly down."""
        if self.heartbeat_s is None:
            return
        while not self._shutdown.is_set():
            await asyncio.sleep(self.heartbeat_s)
            if self._connected:
                await self._send_status("up")

    async def _snapshot_loop(self) -> None:
        """Re-emit each LIVE symbol's local book to its snapshot topic every
        snapshot_interval_s (see __init__ - bounds the delta tail a warm
        consumer must replay). Skips while disconnected: a STALE book is the
        previous connection's state and must not be republished as current."""
        if self.snapshot_interval_s is None:
            return
        while not self._shutdown.is_set():
            await asyncio.sleep(self.snapshot_interval_s)
            if not self._connected:
                continue
            for ctx in self.contexts.values():
                if ctx.state is not SymbolState.LIVE or ctx.book.sequence < 0:
                    continue
                try:
                    await self._emit_book_snapshot(ctx)
                except Exception as e:
                    # Best-effort, like the heartbeat: delivery failures already
                    # trip _produce_failed; a sync send error must not kill the
                    # loop for the process lifetime.
                    log.warning("periodic snapshot emit failed for %s: %s", ctx.symbol, e)

    async def _emit_book_snapshot(self, ctx: SymbolContext) -> None:
        """Serialize the full local book as a BookSnapshot at its current
        sequence - identical shape to a bootstrap snapshot, so consumers can't
        tell (and needn't care) which kind they warmed from."""
        now_ns = time.time_ns()
        book = ctx.book
        snap = BookSnapshot(
            exchange=self.exchange,
            symbol=ctx.symbol,
            sequence=book.sequence,
            bids=[
                BookLevel(price=str(px), size=str(sz))
                for px, sz in book.top_n(Side.BID, book.depth(Side.BID))
            ],
            asks=[
                BookLevel(price=str(px), size=str(sz))
                for px, sz in book.top_n(Side.ASK, book.depth(Side.ASK))
            ],
            exchange_ts_ns=0,  # locally generated, no exchange event behind it
            local_ts_ns=now_ns,
            epoch=self._epoch,
        )
        event = ParsedEvent(
            symbol=ctx.symbol,
            kind="snapshot",
            exchange_ts_ns=0,
            local_recv_ts_ns=now_ns,
        )
        await self._emit(book_snapshot_topic(self.exchange, ctx.symbol), snap, ctx.symbol, event)

    def _mark_all_stale(self, reason: str) -> None:
        counter = book_resyncs.labels(exchange=self.exchange, reason=reason)
        for ctx in self.contexts.values():
            if ctx.state is SymbolState.LIVE:
                counter.inc()
            if ctx.state is not SymbolState.STALE:
                ctx.set_state(SymbolState.STALE, reason=reason)

    def _reset_contexts(self) -> None:
        """Clear per-symbol mutable state before each fresh connection.

        Prevents stale buffered deltas, old last_seq values, and dirty book
        state from a prior connection cycle from poisoning the next bootstrap.
        """
        # New connection generation, monotonic: silver ORDERS book records by
        # epoch, so a backwards wall-clock step must never lower it.
        self._epoch = max(self._epoch + 1, time.time_ns())
        for ctx in self.contexts.values():
            ctx.buffered.clear()
            ctx.last_seq = -1
            ctx.book.clear()
            ctx.set_state(SymbolState.BOOTSTRAP, reason="reconnect")

    async def _emit(self, topic: str, msg: object, symbol: str, event: ParsedEvent) -> None:
        """Produce a market-data message via pipelined send().

        ``send_and_wait`` would await the broker ack per message, capping
        throughput at 1/RTT and defeating the producer's linger_ms batching.
        ``send`` returns a delivery future immediately so the background sender
        can batch and pipeline in-flight requests. ``enable_idempotence=True``
        (see make_producer) keeps per-partition order and dedups retries, so
        we keep exactly-once-into-broker.

        Delivery failures arrive asynchronously on the returned future; the
        done-callback (``_on_produce_done``) trips ``_produce_failed`` so the
        connection tears down → reconnect → resnapshot, preserving the
        no-silent-gap invariant.
        """
        fut = await self.producer.send(
            topic,
            encode(msg),
            key=f"{self.exchange}:{symbol}".encode(),
            headers=latency_headers(event.local_recv_ts_ns, event.exchange_ts_ns),
        )
        # Capture the per-connection event at attach time. self._produce_failed
        # is reallocated each cycle in _connect_and_stream; a bound-method
        # callback would re-read it at fire time, so a stale failure from a
        # torn-down connection could trip the *new* connection's event and
        # cause a spurious reconnect+resync.
        produce_failed = self._produce_failed
        fut.add_done_callback(lambda f: self._on_produce_done(f, produce_failed))

    def _on_produce_done(self, fut: asyncio.Future, produce_failed: asyncio.Event | None) -> None:
        """Trip the bound-at-attach-time ``produce_failed`` event on delivery
        failure so the connection resyncs instead of advancing past a gap.
        Calling ``fut.exception()`` also marks the exception as retrieved,
        suppressing asyncio's unhandled-exception warning."""
        exc = fut.exception()
        if exc is None:
            return
        log.error("kafka produce failed; forcing resync: %s", exc)
        book_resyncs.labels(exchange=self.exchange, reason="produce_failed").inc()
        if produce_failed is not None:
            produce_failed.set()

    async def _connect_and_stream(self) -> None:
        self._reset_contexts()
        self._queue = asyncio.Queue(maxsize=self.queue_maxsize)
        # Fresh per-connection delivery-failure signal - see _emit().
        self._produce_failed = asyncio.Event()
        log.info("connecting %s -> %s", self.exchange, self.ws_url)
        async with websockets.connect(
            self.ws_url,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
            max_size=self.ws_max_size,
            close_timeout=1.0,
        ) as ws:
            self.backoff.reset()
            await self._send_subscribes(ws)

            # Bootstrap symbols in parallel. return_exceptions=True ensures every
            # coroutine settles before the first exception propagates; without it,
            # a fast-failing symbol orphans the slower peers as detached Tasks.
            results = await asyncio.gather(
                *(self._bootstrap_safe(sym) for sym in self.symbols),
                return_exceptions=True,
            )
            failures = [r for r in results if isinstance(r, BaseException)]
            if failures:
                for exc in failures[1:]:
                    log.error("additional bootstrap failure (suppressed): %s", exc)
                raise failures[0]

            # Streaming now: announce liveness immediately; the heartbeat loop
            # keeps it fresh. Cleared in the finally below on any teardown.
            self._connected = True
            if self.heartbeat_s is not None:
                await self._send_status("up")

            reader_task = asyncio.create_task(self._reader(ws), name="ingest-reader")
            applier_task = asyncio.create_task(self._applier(), name="ingest-applier")
            shutdown_task = asyncio.create_task(self._shutdown.wait(), name="ingest-shutdown-watch")
            produce_failed_task = asyncio.create_task(
                self._produce_failed.wait(), name="ingest-produce-failed"
            )
            tasks = {reader_task, applier_task, produce_failed_task}
            if self.stale_timeout is not None:
                # Seed the clock now so the watchdog doesn't fire before any frame
                # has had a chance to arrive on a fresh connection.
                self._last_msg_monotonic = time.monotonic()
                tasks.add(asyncio.create_task(self._staleness_watchdog(), name="ingest-watchdog"))
            try:
                # Wait until any of: reader/applier exits (normal or resync),
                # OR the shutdown event fires. Whichever happens, we cancel
                # the others so the connection winds down promptly.
                # This avoids the deadlock where the applier blocks on a
                # sentinel the reader couldn't enqueue (QueueFull).
                done, pending = await asyncio.wait(
                    tasks | {shutdown_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                # Propagate the first real exception from reader/applier,
                # ignoring CancelledError and the shutdown-watch result.
                for t in done:
                    if t is shutdown_task or t is produce_failed_task:
                        # Event-completion tasks: they signal "wind down now",
                        # not a failure to propagate. The resync metric is
                        # already incremented by _on_produce_done.
                        continue
                    exc = t.exception()
                    if exc is not None and not isinstance(exc, asyncio.CancelledError):
                        raise exc
            finally:
                self._connected = False
                for t in tasks | {shutdown_task}:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*(tasks | {shutdown_task}), return_exceptions=True)

    async def _bootstrap_safe(self, symbol: str) -> None:
        try:
            await self.bootstrap(symbol)
        except Exception as e:
            log.exception("bootstrap failed for %s/%s: %s", self.exchange, symbol, e)
            book_resyncs.labels(exchange=self.exchange, reason="bootstrap_error").inc()
            raise

    async def _send_subscribes(self, ws: websockets.WebSocketClientProtocol) -> None:
        msgs = self.build_subscribe_messages()
        for msg in msgs:
            await self.subscribe_bucket.acquire(1.0)
            await ws.send(msg)
            log.debug("subscribe sent: %s", msg[:200])

    async def _staleness_watchdog(self) -> None:
        """Reconnect if no WS frame arrives within `stale_timeout` seconds.

        Returns (rather than raising) on trip: returning trips the
        wait(FIRST_COMPLETED) in _connect_and_stream, which cancels the
        reader/applier and lets run() reconnect - same wind-down path as a
        clean reader exit. ping/pong catches dead sockets; this catches a live
        socket whose data has gone silent.
        """
        assert self.stale_timeout is not None
        interval = max(0.05, self.stale_timeout / 2)
        while True:
            await asyncio.sleep(interval)
            idle = time.monotonic() - self._last_msg_monotonic
            if idle > self.stale_timeout:
                log.warning(
                    "no WS frame for %.2fs (> %.2fs stale_timeout); reconnecting",
                    idle,
                    self.stale_timeout,
                )
                ws_reconnects.labels(exchange=self.exchange, reason="stale").inc()
                return

    def _observe_recv_clock(self, recv_ns: int) -> None:
        """Live host-clock canary: count backward steps between consecutive WS-frame
        recv timestamps. One process clock read in arrival order, so a step back is a
        real wall-clock regression - none of the fold/epoch/snapshot reordering the
        silver `clock_monotonic` check must exclude, so a healthy host reads exactly 0.
        """
        prev = self._prev_recv_ns
        self._prev_recv_ns = recv_ns
        if prev is None or recv_ns >= prev:
            return
        recv_clock_backward_steps.labels(exchange=self.exchange).inc()
        step_ms = (prev - recv_ns) / 1e6
        if step_ms > self._worst_recv_step_ms:
            self._worst_recv_step_ms = step_ms
            recv_clock_worst_step_ms.labels(exchange=self.exchange).set(step_ms)

    async def _reader(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Pulls frames off the WS as fast as possible; pushes to bounded queue.

        QueueFull means the applier is behind - close the WS to force a
        full reconnect + resync. Dropping frames silently is forbidden:
        a missed `del price` delta leaves a phantom level forever.
        """
        assert self._queue is not None
        try:
            async for raw in ws:
                local_recv_ts_ns = time.time_ns()
                self._last_msg_monotonic = time.monotonic()
                self._observe_recv_clock(local_recv_ts_ns)
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                bytes_received.labels(exchange=self.exchange).inc(len(raw))
                queue_depth.labels(component="ingest", name=self.exchange).set(self._queue.qsize())
                try:
                    self._queue.put_nowait((raw, local_recv_ts_ns))
                except asyncio.QueueFull:
                    log.error("queue full (applier behind); aborting connection to force resync")
                    book_resyncs.labels(exchange=self.exchange, reason="queue_full").inc()
                    # Don't await ws.close() here - it would block on the
                    # server's close response. The `async with ws:` exit
                    # will close with our configured close_timeout. Return
                    # immediately so the wait(FIRST_COMPLETED) in
                    # _connect_and_stream can cancel the applier.
                    return
        finally:
            # Signal applier to drain and exit.
            assert self._queue is not None
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

    async def _applier(self) -> None:
        """Drains the queue: parse → dispatch to process_event → handle errors."""
        assert self._queue is not None
        while True:
            item = await self._queue.get()
            if item is None:
                return
            raw, ts = item
            try:
                events = self.parse_message(raw, ts)
            except Exception as e:
                log.exception("parse error: %s", e)
                messages_received.labels(exchange=self.exchange, channel="parse_error").inc()
                continue

            for ev in events:
                messages_received.labels(exchange=self.exchange, channel=ev.kind).inc()
                if ev.exchange_ts_ns > 0:
                    exchange_latency.labels(exchange=self.exchange).observe(
                        max(0.0, (ts - ev.exchange_ts_ns) / 1e9)
                    )
                ctx = self.contexts.get(ev.symbol)
                if ctx is None:
                    # Unsolicited symbol - possible if exchange sends extras.
                    continue
                try:
                    await self.process_event(ctx, ev)
                except ResyncRequired as r:
                    log.warning(
                        "resync required for %s/%s: %s",
                        self.exchange,
                        ev.symbol,
                        r,
                    )
                    ctx.set_state(SymbolState.STALE, reason=str(r))
                    book_resyncs.labels(exchange=self.exchange, reason="event_handler").inc()
                    # Abort the whole connection - simplest correct behaviour.
                    raise
                except BookInvariantError as e:
                    log.error(
                        "invariant violation %s/%s: %s",
                        self.exchange,
                        ev.symbol,
                        e,
                    )
                    book_invariant_violations.labels(
                        exchange=self.exchange, symbol=ev.symbol, kind=e.kind
                    ).inc()
                    ctx.book.clear()  # book is partially-applied; clear before marking STALE
                    ctx.set_state(SymbolState.STALE, reason="invariant_violation")
                    book_resyncs.labels(exchange=self.exchange, reason="invariant").inc()
                    raise ResyncRequired(str(e)) from e
