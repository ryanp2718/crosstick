"""Integration tests for BaseIngester using a real local WS server (no network).

A `FakeExchangeServer` accepts WS connections, captures subscribe messages,
and emits scripted messages. A `FakeIngester` subclasses BaseIngester with
trivial parse/process logic. Tests exercise the state machine, the bounded
queue, the token bucket, and the backoff loop.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import AsyncIterator

import pytest
import websockets
import websockets.exceptions
import websockets.server

from ingest.base_ingester import (
    BaseIngester,
    ParsedEvent,
    ResyncRequired,
    SymbolContext,
    SymbolState,
)


# ─── fakes ────────────────────────────────────────────────────────────────


class FakeProducer:
    """Minimal AIOKafkaProducer stand-in. Captures send_and_wait() calls."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def send_and_wait(self, topic: str, value: bytes, **kw: object) -> None:
        self.sent.append((topic, value))


class FakeExchangeServer:
    """Tiny WS server. Records connections, subscribes, and serves scripted msgs."""

    def __init__(self, scripted: list[str | bytes] | None = None) -> None:
        self.scripted: list[str | bytes] = list(scripted or [])
        self.subscribes: list[str] = []
        self.connections: int = 0
        self.send_delay: float = 0.0
        self._stop = asyncio.Event()
        self._port: int | None = None

    @property
    def url(self) -> str:
        assert self._port is not None
        return f"ws://127.0.0.1:{self._port}"

    async def __aenter__(self) -> "FakeExchangeServer":
        async def handler(
            ws: websockets.server.WebSocketServerProtocol,
        ) -> None:
            self.connections += 1
            try:
                # Read up to N subscribe messages (don't block forever).
                read_task = asyncio.create_task(self._read_subscribes(ws))
                emit_task = asyncio.create_task(self._emit_scripted(ws))
                done, pending = await asyncio.wait(
                    {read_task, emit_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
            except websockets.exceptions.ConnectionClosed:
                pass

        self._server = await websockets.serve(handler, "127.0.0.1", 0)
        sock = next(iter(self._server.sockets))
        self._port = sock.getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _read_subscribes(self, ws: websockets.server.WebSocketServerProtocol) -> None:
        async for msg in ws:
            self.subscribes.append(msg if isinstance(msg, str) else msg.decode())

    async def _emit_scripted(self, ws: websockets.server.WebSocketServerProtocol) -> None:
        for m in self.scripted:
            if self.send_delay:
                await asyncio.sleep(self.send_delay)
            await ws.send(m)
        # Keep open until client disconnects (so test reader can complete).
        await asyncio.Future()


class FakeIngester(BaseIngester):
    """Minimal concrete ingester. parse_message() turns each frame into one
    trade event for symbol 'X' so tests can assert dispatch behaviour.
    """

    def __init__(self, *args: object, **kw: object) -> None:
        super().__init__(*args, **kw)
        self.bootstraps: list[str] = []
        self.processed: list[ParsedEvent] = []
        # Optional injected behaviour per event for tests.
        self.process_hook = None

    async def bootstrap(self, symbol: str) -> None:
        self.bootstraps.append(symbol)
        ctx = self.contexts[symbol]
        ctx.set_state(SymbolState.LIVE, reason="fake-bootstrap")

    def build_subscribe_messages(self) -> list[str]:
        return [json.dumps({"subscribe": self.symbols})]

    def parse_message(self, raw: bytes, local_recv_ts_ns: int) -> list[ParsedEvent]:
        # Each frame is JSON: {"sym": "X", "seq": N, "kind": "trade"|...}
        d = json.loads(raw.decode())
        return [
            ParsedEvent(
                symbol=d.get("sym", self.symbols[0]),
                kind=d.get("kind", "trade"),
                sequence=d.get("seq"),
                raw_bytes=len(raw),
                exchange_ts_ns=d.get("ts_ns", local_recv_ts_ns - 1_000_000),
                local_recv_ts_ns=local_recv_ts_ns,
            )
        ]

    async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        self.processed.append(event)
        if self.process_hook is not None:
            await self.process_hook(ctx, event)


# ─── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
async def fake_server() -> AsyncIterator[FakeExchangeServer]:
    async with FakeExchangeServer() as srv:
        yield srv


def make_ingester(server: FakeExchangeServer, **overrides: object) -> FakeIngester:
    defaults: dict[str, object] = dict(
        exchange="fake",
        symbols=["X"],
        ws_url=server.url,
        producer=FakeProducer(),
        subscribe_rate=100.0,
        subscribe_capacity=100.0,
        queue_maxsize=100,
        backoff_base=0.001,
        backoff_cap=0.01,
        ping_interval=None,
        ping_timeout=None,
    )
    defaults.update(overrides)
    return FakeIngester(**defaults)


# ─── tests ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_subscribes_sent_and_messages_processed(fake_server: FakeExchangeServer) -> None:
    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": 1, "kind": "trade"}),
        json.dumps({"sym": "X", "seq": 2, "kind": "trade"}),
        json.dumps({"sym": "X", "seq": 3, "kind": "trade"}),
    ]
    ing = make_ingester(fake_server)
    run_task = asyncio.create_task(ing.run())
    # Wait for events to land.
    for _ in range(50):
        if len(ing.processed) >= 3:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert ing.bootstraps == ["X"]
    assert len(fake_server.subscribes) == 1
    assert json.loads(fake_server.subscribes[0]) == {"subscribe": ["X"]}
    assert len(ing.processed) >= 3
    assert all(e.symbol == "X" for e in ing.processed)


@pytest.mark.asyncio
async def test_state_starts_bootstrap_then_goes_live(fake_server: FakeExchangeServer) -> None:
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]
    ing = make_ingester(fake_server)
    assert ing.contexts["X"].state is SymbolState.BOOTSTRAP
    run_task = asyncio.create_task(ing.run())
    for _ in range(50):
        if ing.contexts["X"].state is SymbolState.LIVE:
            break
        await asyncio.sleep(0.02)
    assert ing.contexts["X"].state is SymbolState.LIVE
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_resync_required_marks_stale_and_reconnects(
    fake_server: FakeExchangeServer,
) -> None:
    """A ResyncRequired in process_event() should mark the symbol STALE and
    trigger reconnect (visible: bootstraps gets called twice)."""
    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": 1, "kind": "trade"}),
        json.dumps({"sym": "X", "seq": 99, "kind": "trade"}),  # triggers resync
    ]
    ing = make_ingester(fake_server)
    triggered = {"count": 0}

    async def hook(ctx: SymbolContext, event: ParsedEvent) -> None:
        if event.sequence and event.sequence > 50:
            triggered["count"] += 1
            raise ResyncRequired("simulated gap")

    ing.process_hook = hook
    run_task = asyncio.create_task(ing.run())
    # Wait for re-bootstrap (proof we reconnected).
    for _ in range(100):
        if len(ing.bootstraps) >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert triggered["count"] >= 1
    assert len(ing.bootstraps) >= 2  # reconnect re-bootstrapped


@pytest.mark.asyncio
async def test_token_bucket_throttles_subscribes(fake_server: FakeExchangeServer) -> None:
    """With rate=10/s and 5 subscribe messages, total send time ≥ ~400ms."""
    # Use a subclass that emits 5 subscribe messages.
    class ManyMsgs(FakeIngester):
        def build_subscribe_messages(self) -> list[str]:
            return [json.dumps({"i": i}) for i in range(5)]

    fake_server.scripted = []  # nothing to read after subscribe
    ing = ManyMsgs(
        exchange="fake",
        symbols=["X"],
        ws_url=fake_server.url,
        producer=FakeProducer(),
        subscribe_rate=10.0,
        subscribe_capacity=1.0,  # only 1 token initial, others must wait
        queue_maxsize=10,
        backoff_base=0.001,
        backoff_cap=0.01,
        ping_interval=None,
        ping_timeout=None,
    )

    t0 = time.monotonic()
    run_task = asyncio.create_task(ing.run())
    for _ in range(100):
        if len(fake_server.subscribes) >= 5:
            break
        await asyncio.sleep(0.02)
    elapsed = time.monotonic() - t0
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass
    assert len(fake_server.subscribes) == 5
    # 5 msgs - 1 initial token = 4 waits at 10/s = 400ms minimum.
    assert elapsed >= 0.35, f"too fast: {elapsed:.3f}s (token bucket bypassed?)"


@pytest.mark.asyncio
async def test_shutdown_causes_clean_exit_without_cancel(
    fake_server: FakeExchangeServer,
) -> None:
    """Calling shutdown() alone should make run() return within reasonable time
    (no need to cancel the task externally)."""
    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": i, "kind": "trade"}) for i in range(3)
    ]
    ing = make_ingester(fake_server)
    run_task = asyncio.create_task(ing.run())
    # Let some messages flow through.
    for _ in range(50):
        if len(ing.processed) >= 1:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    # Now run_task should complete on its own (no cancel needed).
    try:
        await asyncio.wait_for(run_task, timeout=3.0)
    except asyncio.TimeoutError:
        run_task.cancel()
        raise AssertionError("run() did not exit within 3s of shutdown()")
    assert run_task.done()
    assert run_task.exception() is None


@pytest.mark.asyncio
async def test_queue_full_aborts_connection_and_increments_resync(
    fake_server: FakeExchangeServer,
) -> None:
    """If applier is blocked, queue fills, reader aborts the connection."""
    # Emit far more messages than queue capacity; block the applier so it can't drain.
    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": i, "kind": "trade"}) for i in range(200)
    ]
    ing = make_ingester(fake_server, queue_maxsize=5)
    blocker = asyncio.Event()

    async def block(ctx: SymbolContext, event: ParsedEvent) -> None:
        await blocker.wait()

    ing.process_hook = block

    from common.metrics import book_resyncs

    # Snapshot the counter before; we'll check it ticks for "queue_full".
    initial = _resync_count("fake", "queue_full")

    run_task = asyncio.create_task(ing.run())

    # Wait long enough for queue to fill and reader to abort.
    for _ in range(150):
        if _resync_count("fake", "queue_full") > initial:
            break
        await asyncio.sleep(0.02)

    # Release the blocker so the applier finishes and the reconnect can complete.
    blocker.set()

    # Wait for the reconnect to land before we shut down.
    for _ in range(200):
        if fake_server.connections >= 2:
            break
        await asyncio.sleep(0.02)

    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert _resync_count("fake", "queue_full") > initial, (
        "expected queue_full resync metric to increment"
    )
    assert fake_server.connections >= 2, "expected reconnect after queue_full disconnect"


def _resync_count(exchange: str, reason: str) -> float:
    from common.metrics import book_resyncs
    return book_resyncs.labels(exchange=exchange, reason=reason)._value.get()
