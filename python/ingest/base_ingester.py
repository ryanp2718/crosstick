"""Abstract base ingester: WS lifecycle, backoff, token bucket, bounded queue,
per-symbol state machine.

Subclasses (binance.py, coinbase.py, kraken.py) implement four hooks:
    bootstrap(symbol)           — REST snapshot + warm the local book to LIVE
    build_subscribe_messages()  — exchange-specific subscribe payloads
    parse_message(raw, ts_ns)   — decode WS frame to ParsedEvent[]
    process_event(event)        — apply event to book state; emit to Kafka

The base class owns:
    - the WS connect loop with full-jitter exponential backoff
    - the token bucket on outbound subscribe messages
    - the bounded asyncio.Queue between WS reader and the applier
    - per-symbol state: BOOTSTRAP / BUFFERING / LIVE / STALE
    - graceful shutdown
    - Prometheus metric emission

On `asyncio.QueueFull` we close the WS and let the connect-loop resync — see
`docs/anti-patterns.md`: dropping deltas corrupts the book permanently.
"""
from __future__ import annotations

import asyncio
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
from common.metrics import (
    book_resyncs,
    book_state,
    bytes_received,
    exchange_latency,
    messages_received,
    queue_depth,
    ws_reconnects,
)
from common.ratelimit import AsyncTokenBucket
from ingest.book import BookInvariantError, OrderBook

log = logging.getLogger(__name__)

EventKind = Literal["trade", "snapshot", "delta", "heartbeat", "subscribed", "error", "other"]

# Maximum number of WS deltas to buffer while a REST snapshot is in-flight
# (Binance pattern).  If this limit is exceeded before the snapshot arrives,
# buffer_append() raises ResyncRequired so the connection is torn down and
# a fresh snapshot is fetched.
MAX_BUFFER_DELTAS: int = 500


class SymbolState(enum.StrEnum):
    BOOTSTRAP = "bootstrap"  # initial; awaiting snapshot (REST or in-band)
    BUFFERING = "buffering"  # WS up; deltas buffered while REST snapshot in flight (Binance)
    LIVE = "live"            # snapshot applied; deltas applying normally
    STALE = "stale"          # gap/CRC mismatch detected; awaiting resync


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
        log.info("symbol_state %s/%s: %s -> %s (%s)",
                 self.exchange, self.symbol, self.state, new, reason)
        self.state = new
        book_state.labels(exchange=self.exchange, symbol=self.symbol).set(
            _STATE_TO_METRIC[new]
        )

    def buffer_append(self, event: ParsedEvent) -> None:
        """Append a delta to the pre-snapshot buffer.

        Raises ResyncRequired if more than MAX_BUFFER_DELTAS accumulate before
        the snapshot arrives — a stalled bootstrap that keeps buffering is safer
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
    sequence: int | None = None             # for sequence-validated kinds
    payload: object | None = None           # exchange-specific structured payload
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

        # Per-symbol context.
        self.contexts: dict[str, SymbolContext] = {
            sym: SymbolContext(exchange=exchange, symbol=sym, book=OrderBook(exchange, sym))
            for sym in self.symbols
        }
        # Initialize state metric.
        for ctx in self.contexts.values():
            book_state.labels(exchange=exchange, symbol=ctx.symbol).set(0)

        self._shutdown = asyncio.Event()
        # New queue per connection; allocated in _connect_and_stream.
        self._queue: asyncio.Queue[tuple[bytes, int]] | None = None

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

    async def shutdown(self) -> None:
        log.info("ingester shutdown requested")
        self._shutdown.set()

    # ─── internals ─────────────────────────────────────────────────────────

    def _mark_all_stale(self, reason: str) -> None:
        for ctx in self.contexts.values():
            if ctx.state is SymbolState.LIVE:
                book_resyncs.labels(exchange=self.exchange, reason=reason).inc()
            if ctx.state is not SymbolState.STALE:
                ctx.set_state(SymbolState.STALE, reason=reason)

    def _reset_contexts(self) -> None:
        """Clear per-symbol mutable state before each fresh connection.

        Prevents stale buffered deltas, old last_seq values, and dirty book
        state from a prior connection cycle from poisoning the next bootstrap.
        """
        for ctx in self.contexts.values():
            ctx.buffered.clear()
            ctx.last_seq = -1
            ctx.book.clear()
            ctx.set_state(SymbolState.BOOTSTRAP, reason="reconnect")

    async def _connect_and_stream(self) -> None:
        self._reset_contexts()
        self._queue = asyncio.Queue(maxsize=self.queue_maxsize)
        log.info("connecting %s -> %s", self.exchange, self.ws_url)
        async with websockets.connect(
            self.ws_url,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
            max_size=2**22,  # 4 MiB; large enough for full depth snapshots
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
            for r in results:
                if isinstance(r, BaseException):
                    raise r

            reader_task = asyncio.create_task(self._reader(ws), name="ingest-reader")
            applier_task = asyncio.create_task(self._applier(), name="ingest-applier")
            shutdown_task = asyncio.create_task(
                self._shutdown.wait(), name="ingest-shutdown-watch"
            )
            tasks = {reader_task, applier_task}
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
                    if t is shutdown_task:
                        continue
                    exc = t.exception()
                    if exc is not None and not isinstance(exc, asyncio.CancelledError):
                        raise exc
            finally:
                for t in tasks | {shutdown_task}:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(
                    *(tasks | {shutdown_task}), return_exceptions=True
                )

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

    async def _reader(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Pulls frames off the WS as fast as possible; pushes to bounded queue.

        QueueFull means the applier is behind — close the WS to force a
        full reconnect + resync. Dropping frames silently is forbidden:
        a missed `del price` delta leaves a phantom level forever.
        """
        assert self._queue is not None
        try:
            async for raw in ws:
                local_recv_ts_ns = time.time_ns()
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                bytes_received.labels(exchange=self.exchange).inc(len(raw))
                queue_depth.labels(component="ingest", name=self.exchange).set(
                    self._queue.qsize()
                )
                try:
                    self._queue.put_nowait((raw, local_recv_ts_ns))
                except asyncio.QueueFull:
                    log.error(
                        "queue full (applier behind); aborting connection to force resync"
                    )
                    book_resyncs.labels(
                        exchange=self.exchange, reason="queue_full"
                    ).inc()
                    # Don't await ws.close() here — it would block on the
                    # server's close response. The `async with ws:` exit
                    # will close with our configured close_timeout. Return
                    # immediately so the wait(FIRST_COMPLETED) in
                    # _connect_and_stream can cancel the applier.
                    return
        finally:
            # Signal applier to drain and exit.
            assert self._queue is not None
            try:
                self._queue.put_nowait((b"", 0))  # sentinel
            except asyncio.QueueFull:
                pass

    async def _applier(self) -> None:
        """Drains the queue: parse → dispatch to process_event → handle errors."""
        assert self._queue is not None
        while True:
            raw, ts = await self._queue.get()
            if raw == b"" and ts == 0:
                return  # sentinel from reader
            try:
                events = self.parse_message(raw, ts)
            except Exception as e:
                log.exception("parse error: %s", e)
                messages_received.labels(
                    exchange=self.exchange, channel="parse_error"
                ).inc()
                continue

            for ev in events:
                messages_received.labels(
                    exchange=self.exchange, channel=ev.kind
                ).inc()
                if ev.exchange_ts_ns > 0:
                    exchange_latency.labels(exchange=self.exchange).observe(
                        max(0.0, (ts - ev.exchange_ts_ns) / 1e9)
                    )
                ctx = self.contexts.get(ev.symbol)
                if ctx is None:
                    # Unsolicited symbol — possible if exchange sends extras.
                    continue
                try:
                    await self.process_event(ctx, ev)
                except ResyncRequired as r:
                    log.warning(
                        "resync required for %s/%s: %s",
                        self.exchange, ev.symbol, r,
                    )
                    ctx.set_state(SymbolState.STALE, reason=str(r))
                    book_resyncs.labels(
                        exchange=self.exchange, reason="event_handler"
                    ).inc()
                    # Abort the whole connection — simplest correct behaviour.
                    raise
                except BookInvariantError as e:
                    log.error(
                        "invariant violation %s/%s: %s",
                        self.exchange, ev.symbol, e,
                    )
                    ctx.book.clear()  # book is partially-applied; clear before marking STALE
                    ctx.set_state(SymbolState.STALE, reason="invariant_violation")
                    book_resyncs.labels(
                        exchange=self.exchange, reason="invariant"
                    ).inc()
                    raise ResyncRequired(str(e)) from e
