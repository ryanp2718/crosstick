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
from collections.abc import AsyncIterator

import pytest
import websockets
import websockets.exceptions
import websockets.server

from common.metrics import book_resyncs
from ingest.base_ingester import (
    BaseIngester,
    ParsedEvent,
    ResyncRequired,
    SymbolContext,
    SymbolState,
)

# ─── fakes ────────────────────────────────────────────────────────────────


class FakeProducer:
    """Minimal AIOKafkaProducer stand-in. Captures send() (and send_and_wait())
    calls; `fail_next` primes the next produce future to fail, exercising the
    delivery-error → resync path."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self.started = False
        self.fail_next: BaseException | None = None

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def send(self, topic: str, value: bytes, **kw: object) -> asyncio.Future:
        self.sent.append((topic, value))
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            fut.set_exception(exc)
        else:
            fut.set_result(None)
        return fut

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

    async def __aenter__(self) -> FakeExchangeServer:
        async def handler(
            ws: websockets.server.WebSocketServerProtocol,
        ) -> None:
            self.connections += 1
            try:
                # Read up to N subscribe messages (don't block forever).
                read_task = asyncio.create_task(self._read_subscribes(ws))
                emit_task = asyncio.create_task(self._emit_scripted(ws))
                _, pending = await asyncio.wait(
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
    except TimeoutError:
        run_task.cancel()
        raise AssertionError("run() did not exit within 3s of shutdown()") from None
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


def _status_msgs(producer: FakeProducer) -> list[tuple[str, dict]]:
    """Decode the md.status.* messages captured by the fake producer."""
    out: list[tuple[str, dict]] = []
    for topic, value in producer.sent:
        if topic.startswith("md.status."):
            out.append((topic, json.loads(value)))
    return out


@pytest.mark.asyncio
async def test_heartbeat_emits_up_while_connected(fake_server: FakeExchangeServer) -> None:
    """A connected ingester emits periodic 'up' heartbeats on md.status.<exchange>
    keyed by exchange. This is the liveness signal the gateway uses to evict a
    venue whose ingester died (no graceful 'down')."""
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]
    ing = make_ingester(fake_server, heartbeat_s=0.05)
    run_task = asyncio.create_task(ing.run())
    for _ in range(100):
        if len([m for _, m in _status_msgs(ing.producer) if m["state"] == "up"]) >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)

    ups = _status_msgs(ing.producer)
    up_only = [m for _, m in ups if m["state"] == "up"]
    assert len(up_only) >= 2, "expected repeated 'up' heartbeats while connected"
    assert all(t == "md.status.fake" for t, _ in ups), "status topic must be md.status.<exchange>"
    assert all(m["exchange"] == "fake" for _, m in ups)


@pytest.mark.asyncio
async def test_heartbeat_none_emits_no_status_at_all(
    fake_server: FakeExchangeServer,
) -> None:
    """heartbeat_s=None disables every status emission (up, heartbeat, down):
    for venues split across several connections (binance-futures), exactly one
    instance owns md.status.<exchange> and the others must stay silent."""
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]
    ing = make_ingester(fake_server, heartbeat_s=None)
    run_task = asyncio.create_task(ing.run())
    for _ in range(100):
        if len(ing.processed) >= 1:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)

    assert _status_msgs(ing.producer) == []


@pytest.mark.asyncio
async def test_graceful_shutdown_emits_down(fake_server: FakeExchangeServer) -> None:
    """A graceful shutdown emits a final 'down' so the gateway evicts the venue
    immediately (without waiting for the liveness timeout)."""
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]
    ing = make_ingester(fake_server, heartbeat_s=10.0)  # large → isolate the down
    run_task = asyncio.create_task(ing.run())
    for _ in range(100):
        if any(m["state"] == "up" for _, m in _status_msgs(ing.producer)):
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)

    downs = [m for _, m in _status_msgs(ing.producer) if m["state"] == "down"]
    assert len(downs) >= 1, "graceful shutdown must emit a 'down' status"
    assert downs[-1]["exchange"] == "fake"


def _resync_count(exchange: str, reason: str) -> float:
    from common.metrics import book_resyncs
    return book_resyncs.labels(exchange=exchange, reason=reason)._value.get()


def _ws_reconnect_count(exchange: str, reason: str) -> float:
    from common.metrics import ws_reconnects
    return ws_reconnects.labels(exchange=exchange, reason=reason)._value.get()


# ─── base-class changes: ws_max_size + staleness watchdog (TDD) ─────────────


@pytest.mark.asyncio
async def test_ws_max_size_override_accepts_large_frame(
    fake_server: FakeExchangeServer,
) -> None:
    """A frame larger than the 4 MiB default must be accepted when ws_max_size is
    raised. Coinbase's BTC-USD snapshot is ~5 MiB; the default would close the
    connection with 1009. Fails if base hardcodes max_size or ignores the param."""
    big = "x" * (5 * 1024 * 1024)  # > 2**22 (4 MiB) default
    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": 1, "kind": "trade", "pad": big})
    ]
    ing = make_ingester(fake_server, ws_max_size=2**24)  # 16 MiB
    run_task = asyncio.create_task(ing.run())
    for _ in range(150):
        if len(ing.processed) >= 1:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert len(ing.processed) >= 1, (
        "a >4 MiB frame should be accepted when ws_max_size is raised"
    )


@pytest.mark.asyncio
async def test_staleness_watchdog_reconnects_on_silent_feed(
    fake_server: FakeExchangeServer,
) -> None:
    """A live socket with no data frames should trip the staleness watchdog and
    force a reconnect, tagged ws_reconnects{reason=stale}."""
    fake_server.scripted = []  # socket stays open, no frames ever
    ing = make_ingester(fake_server, stale_timeout=0.2)
    initial = _ws_reconnect_count("fake", "stale")

    run_task = asyncio.create_task(ing.run())
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

    assert fake_server.connections >= 2, (
        "staleness watchdog should force reconnect on a silent feed"
    )
    assert _ws_reconnect_count("fake", "stale") > initial, (
        "expected ws_reconnects{reason=stale} to increment"
    )


@pytest.mark.asyncio
async def test_staleness_watchdog_no_false_reconnect_when_active(
    fake_server: FakeExchangeServer,
) -> None:
    """The watchdog must NOT reconnect while frames keep arriving faster than
    stale_timeout, even past the timeout window."""
    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": i, "kind": "trade"}) for i in range(60)
    ]
    fake_server.send_delay = 0.02  # ~50/s, well under stale_timeout
    ing = make_ingester(fake_server, stale_timeout=0.3)
    run_task = asyncio.create_task(ing.run())
    await asyncio.sleep(0.7)  # > stale_timeout, but frames stream the whole time
    conns = fake_server.connections
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert conns == 1, (
        f"watchdog falsely reconnected an active feed (connections={conns})"
    )


# ─── new tests (TDD: written before fixes) ────────────────────────────────


@pytest.mark.asyncio
async def test_reconnect_clears_buffered_and_last_seq(
    fake_server: FakeExchangeServer,
) -> None:
    """After reconnect, ctx.buffered must be empty and ctx.last_seq must be -1
    at the start of each bootstrap call.  Fails without _reset_contexts()."""
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]

    bootstrap_entry_states: list[tuple[int, int]] = []

    class CapturingIngester(FakeIngester):
        async def bootstrap(self, symbol: str) -> None:
            ctx = self.contexts[symbol]
            bootstrap_entry_states.append((len(ctx.buffered), ctx.last_seq))
            # Pollute state (simulates what a real Binance driver does).
            ctx.buffered.append(ParsedEvent(symbol=symbol, kind="delta", sequence=1))
            ctx.last_seq = 999
            ctx.set_state(SymbolState.LIVE, reason="fake-bootstrap")

        async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
            await super().process_event(ctx, event)
            # Force a reconnect so we can observe the second bootstrap call.
            raise ResyncRequired("trigger reconnect for test")

    ing = CapturingIngester(
        exchange="fake",
        symbols=["X"],
        ws_url=fake_server.url,
        producer=FakeProducer(),
        subscribe_rate=100.0,
        subscribe_capacity=100.0,
        queue_maxsize=100,
        backoff_base=0.001,
        backoff_cap=0.01,
        ping_interval=None,
        ping_timeout=None,
    )
    run_task = asyncio.create_task(ing.run())
    for _ in range(150):
        if len(bootstrap_entry_states) >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert len(bootstrap_entry_states) >= 2, "need ≥2 bootstrap calls to test reset"
    buf0, seq0 = bootstrap_entry_states[0]
    assert buf0 == 0 and seq0 == -1, f"first bootstrap: dirty state buf={buf0} seq={seq0}"
    buf1, seq1 = bootstrap_entry_states[1]
    assert buf1 == 0 and seq1 == -1, (
        f"second bootstrap: stale state buf={buf1} seq={seq1} "
        "— _reset_contexts() is missing"
    )


@pytest.mark.asyncio
async def test_gather_bootstrap_collects_all_coroutines(
    fake_server: FakeExchangeServer,
) -> None:
    """With return_exceptions=True, all bootstrap coroutines settle per cycle.
    Orphan tasks from a fast-failing symbol would inflate X's call count."""
    call_counts: dict[str, int] = {}

    class AsymmetricIngester(FakeIngester):
        async def bootstrap(self, symbol: str) -> None:
            if symbol == "Y":
                # Y increments immediately then fails.
                call_counts["Y"] = call_counts.get("Y", 0) + 1
                raise RuntimeError("Y always fails immediately")
            # X increments only at completion (after the sleep) so orphaned tasks
            # that haven't finished by shutdown time are NOT counted.
            await asyncio.sleep(0.02)
            call_counts["X"] = call_counts.get("X", 0) + 1
            self.contexts[symbol].set_state(SymbolState.LIVE, reason="ok")

    ing = AsymmetricIngester(
        exchange="fake",
        symbols=["X", "Y"],
        ws_url=fake_server.url,
        producer=FakeProducer(),
        subscribe_rate=100.0,
        subscribe_capacity=100.0,
        queue_maxsize=100,
        backoff_base=0.001,
        backoff_cap=0.01,
        ping_interval=None,
        ping_timeout=None,
    )
    run_task = asyncio.create_task(ing.run())
    await asyncio.sleep(0.4)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    x_count = call_counts.get("X", 0)
    y_count = call_counts.get("Y", 0)
    assert x_count > 0 and y_count > 0, "both symbols must have been bootstrapped"
    # With return_exceptions=True: gather awaits all coroutines per cycle, so X
    # completes in every cycle that Y completes. The final cycle may be cut short
    # by shutdown while X's 20ms sleep is in-flight, so y_count can exceed x_count
    # by at most 1. X must never exceed Y (that would indicate orphaned gather tasks
    # from return_exceptions=False running outside the gather's control).
    assert x_count <= y_count, (
        f"X bootstrapped {x_count} times vs Y {y_count} times — "
        "return_exceptions=False would orphan X tasks, inflating its count"
    )
    assert x_count >= y_count - 1, (
        f"X bootstrapped {x_count} times vs Y {y_count} times — "
        "X fell more than 1 cycle behind Y (unexpected)"
    )


@pytest.mark.asyncio
async def test_buffer_overflow_triggers_resync(
    fake_server: FakeExchangeServer,
) -> None:
    """buffer_append raises ResyncRequired once MAX_BUFFER_DELTAS is exceeded."""
    from ingest.base_ingester import MAX_BUFFER_DELTAS

    n_msgs = MAX_BUFFER_DELTAS + 10
    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": i, "kind": "delta"}) for i in range(n_msgs)
    ]

    class BufferingIngester(FakeIngester):
        async def bootstrap(self, symbol: str) -> None:
            # Binance-style: set BUFFERING immediately and return so the
            # reader/applier start while the REST snapshot is "in flight".
            self.contexts[symbol].set_state(SymbolState.BUFFERING, reason="test")

        async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
            if ctx.state is SymbolState.BUFFERING:
                ctx.buffer_append(event)  # raises ResyncRequired at MAX_BUFFER_DELTAS
            else:
                await super().process_event(ctx, event)

    ing = BufferingIngester(
        exchange="fake",
        symbols=["X"],
        ws_url=fake_server.url,
        producer=FakeProducer(),
        subscribe_rate=100.0,
        subscribe_capacity=100.0,
        queue_maxsize=2_000,
        backoff_base=0.001,
        backoff_cap=0.01,
        ping_interval=None,
        ping_timeout=None,
    )
    run_task = asyncio.create_task(ing.run())
    for _ in range(150):
        if fake_server.connections >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert fake_server.connections >= 2, (
        "expected reconnect after buffer_append overflow — "
        "buffer_append() must raise ResyncRequired"
    )


# ─── base _emit: pipelined send + resync-on-delivery-failure ──────────────


def _make_ingester_for_emit() -> FakeIngester:
    """A FakeIngester bound to a FakeProducer for direct _emit() unit tests."""
    return FakeIngester(
        exchange="emit-test", symbols=["X"], ws_url="ws://unused",
        producer=FakeProducer(),
    )


def _trade_msg() -> object:
    from common.models import Side, Trade
    return Trade(
        exchange="emit-test", symbol="X", trade_id="t1",
        price="100.0", size="1.0", side=Side.BID,
        exchange_ts_ns=1_000_000_000, local_ts_ns=2_000_000_000,
    )


def _trade_event() -> ParsedEvent:
    return ParsedEvent(
        symbol="X", kind="trade", sequence=1, raw_bytes=0,
        exchange_ts_ns=1_000_000_000, local_recv_ts_ns=2_000_000_000,
    )


async def test_base_emit_uses_send_not_send_and_wait() -> None:
    """The base _emit pipelines via producer.send() (not send_and_wait), so
    linger_ms batching and idempotent in-flight requests can actually engage."""
    ing = _make_ingester_for_emit()
    ing._produce_failed = asyncio.Event()
    await ing._emit("md.trades.emit-test.X", _trade_msg(), "X", _trade_event())
    producer = ing.producer  # type: ignore[assignment]
    assert len(producer.sent) == 1
    assert producer.sent[0][0] == "md.trades.emit-test.X"
    assert not ing._produce_failed.is_set()


async def test_base_emit_resyncs_on_delivery_failure() -> None:
    """A failed delivery future trips _produce_failed (so _connect_and_stream
    tears the connection down → reconnect → resnapshot) and increments
    book_resyncs(reason="produce_failed"). Preserves the no-silent-gap
    invariant: never advance the published log past a delivery hole."""
    before = book_resyncs.labels(
        exchange="emit-test", reason="produce_failed"
    )._value.get()

    ing = _make_ingester_for_emit()
    ing._produce_failed = asyncio.Event()
    ing.producer.fail_next = RuntimeError("broker boom")  # type: ignore[attr-defined]

    await ing._emit("md.trades.emit-test.X", _trade_msg(), "X", _trade_event())
    # done-callback runs on the next loop tick via call_soon.
    await asyncio.sleep(0)
    assert ing._produce_failed.is_set()

    after = book_resyncs.labels(
        exchange="emit-test", reason="produce_failed"
    )._value.get()
    assert after == before + 1


async def test_base_emit_success_does_not_trip_resync() -> None:
    ing = _make_ingester_for_emit()
    ing._produce_failed = asyncio.Event()
    await ing._emit("md.trades.emit-test.X", _trade_msg(), "X", _trade_event())
    await asyncio.sleep(0)
    assert not ing._produce_failed.is_set()


async def test_base_emit_callback_does_not_cross_connections() -> None:
    """A delivery-failure callback from a prior connection must NOT trip the
    *new* connection's _produce_failed event after a reconnect. Without per-
    attach binding, the bound-method callback reads self._produce_failed at
    fire time → stale callback signals the live connection → spurious resync."""
    ing = _make_ingester_for_emit()

    # Connection A: allocate event, issue a send whose future we control.
    conn_a_event = asyncio.Event()
    ing._produce_failed = conn_a_event
    pending: asyncio.Future = asyncio.get_running_loop().create_future()

    class StagedProducer:
        def __init__(self) -> None:
            self.sent: list[tuple[str, bytes]] = []

        async def send(self, topic: str, value: bytes, **kw: object) -> asyncio.Future:
            self.sent.append((topic, value))
            return pending

    ing.producer = StagedProducer()  # type: ignore[assignment]
    await ing._emit("md.trades.emit-test.X", _trade_msg(), "X", _trade_event())

    # Simulate _connect_and_stream allocating a fresh event for connection B.
    conn_b_event = asyncio.Event()
    ing._produce_failed = conn_b_event
    assert conn_a_event is not conn_b_event

    # Connection A's stale send now fails (broker noticed the closed conn).
    pending.set_exception(RuntimeError("connection A: broker boom"))
    await asyncio.sleep(0)

    assert not conn_b_event.is_set(), (
        "stale callback from prior connection tripped the new connection's "
        "_produce_failed — would cause an unnecessary reconnect+resync"
    )


# ─── periodic snapshot re-emission (bounds the D2 warm-start delta tail) ───


def _book_snaps(producer: FakeProducer) -> list[dict]:
    return [
        json.loads(value)
        for topic, value in producer.sent
        if topic == "md.book.fake.X.snapshots"
    ]


class BookedIngester(FakeIngester):
    """FakeIngester whose bootstrap also warms a real local book."""

    async def bootstrap(self, symbol: str) -> None:
        from decimal import Decimal as D
        await super().bootstrap(symbol)
        self.contexts[symbol].book.apply_snapshot(
            7,
            [(D("100.5"), D("2")), (D("100.0"), D("1"))],
            [(D("101.0"), D("3"))],
        )


@pytest.mark.asyncio
async def test_periodic_snapshot_reemits_live_book(
    fake_server: FakeExchangeServer,
) -> None:
    """While LIVE, the full local book is re-published to the snapshot topic
    every snapshot_interval_s, at the book's current sequence."""
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]
    ing = BookedIngester(
        exchange="fake", symbols=["X"], ws_url=fake_server.url,
        producer=FakeProducer(), subscribe_rate=100.0, subscribe_capacity=100.0,
        queue_maxsize=100, backoff_base=0.001, backoff_cap=0.01,
        ping_interval=None, ping_timeout=None,
        snapshot_interval_s=0.05,
    )
    run_task = asyncio.create_task(ing.run())
    for _ in range(150):
        if len(_book_snaps(ing.producer)) >= 2:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)

    snaps = _book_snaps(ing.producer)
    assert len(snaps) >= 2, "expected repeated periodic snapshots while LIVE"
    s = snaps[0]
    assert s["t"] == "snap"
    assert s["sequence"] == 7
    # Full book, decimal strings preserved; bids best-first, asks best-first.
    assert s["bids"] == [["100.5", "2"], ["100.0", "1"]]
    assert s["asks"] == [["101.0", "3"]]
    assert s["exchange_ts_ns"] == 0  # locally generated, no exchange event
    assert s["local_ts_ns"] > 0


@pytest.mark.asyncio
async def test_periodic_snapshot_skips_unwarmed_book(
    fake_server: FakeExchangeServer,
) -> None:
    """A LIVE symbol whose book is empty (sequence < 0 — trade-only ingester)
    must not emit snapshots: an empty snap would wipe consumers' books."""
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]
    ing = make_ingester(fake_server, snapshot_interval_s=0.03)
    run_task = asyncio.create_task(ing.run())
    await asyncio.sleep(0.25)
    await ing.shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)
    assert _book_snaps(ing.producer) == []


async def test_reset_contexts_sets_fresh_epoch_stamped_on_snapshot() -> None:
    """_reset_contexts() assigns a fresh per-connection epoch (overwriting any
    prior value) and the periodic re-snapshot stamps the live book with it."""
    from decimal import Decimal as D

    ing = BookedIngester(
        exchange="fake", symbols=["X"], ws_url="ws://unused",
        producer=FakeProducer(), ping_interval=None, ping_timeout=None,
    )
    ing._epoch = 12345  # sentinel a fresh connection must replace
    ing._reset_contexts()
    assert ing._epoch != 12345 and ing._epoch != 0

    ctx = ing.contexts["X"]
    ctx.book.apply_snapshot(7, [(D("100"), D("1"))], [(D("101"), D("2"))])
    ctx.set_state(SymbolState.LIVE, reason="test")
    await ing._emit_book_snapshot(ctx)

    snaps = _book_snaps(ing.producer)
    assert snaps and snaps[-1]["epoch"] == ing._epoch


@pytest.mark.asyncio
async def test_periodic_snapshot_none_disables(
    fake_server: FakeExchangeServer,
) -> None:
    fake_server.scripted = [json.dumps({"sym": "X", "seq": 1, "kind": "trade"})]
    ing = BookedIngester(
        exchange="fake", symbols=["X"], ws_url=fake_server.url,
        producer=FakeProducer(), subscribe_rate=100.0, subscribe_capacity=100.0,
        queue_maxsize=100, backoff_base=0.001, backoff_cap=0.01,
        ping_interval=None, ping_timeout=None,
        snapshot_interval_s=None,
    )
    run_task = asyncio.create_task(ing.run())
    await asyncio.sleep(0.25)
    await ing.shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)
    assert _book_snaps(ing.producer) == []


# ─── book invariant metric wiring ─────────────────────────────────────────


def _invariant_count(exchange: str, symbol: str, kind: str) -> float:
    from common.metrics import book_invariant_violations
    return book_invariant_violations.labels(
        exchange=exchange, symbol=symbol, kind=kind
    )._value.get()


@pytest.mark.asyncio
async def test_book_invariant_increments_metric_with_kind(
    fake_server: FakeExchangeServer,
) -> None:
    """A BookInvariantError from process_event is caught by the applier, which
    increments md_book_invariant_violations_total labeled by the error's kind
    (and still resyncs)."""
    from ingest.book import BookInvariantError

    fake_server.scripted = [
        json.dumps({"sym": "X", "seq": 1, "kind": "trade"}),
        json.dumps({"sym": "X", "seq": 2, "kind": "trade"}),  # trips the invariant
    ]
    ing = make_ingester(fake_server)
    before = _invariant_count("fake", "X", "crossed_after_delta")

    async def hook(ctx: SymbolContext, event: ParsedEvent) -> None:
        if event.sequence and event.sequence >= 2:
            raise BookInvariantError("forced cross", kind="crossed_after_delta")

    ing.process_hook = hook
    run_task = asyncio.create_task(ing.run())
    for _ in range(150):
        if _invariant_count("fake", "X", "crossed_after_delta") > before:
            break
        await asyncio.sleep(0.02)
    await ing.shutdown()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert _invariant_count("fake", "X", "crossed_after_delta") > before, (
        "expected md_book_invariant_violations_total to increment with kind"
    )


# ─── live recv-clock canary (md_recv_clock_*) ──────────────────────────────


def _recv_clock(exchange: str) -> tuple[float, float]:
    from common.metrics import recv_clock_backward_steps, recv_clock_worst_step_ms
    steps = recv_clock_backward_steps.labels(exchange=exchange)._value.get()
    worst = recv_clock_worst_step_ms.labels(exchange=exchange)._value.get()
    return steps, worst


def _clock_ingester(exchange: str) -> FakeIngester:
    return FakeIngester(
        exchange=exchange, symbols=["X"], ws_url="ws://unused", producer=FakeProducer(),
    )


def test_recv_clock_monotonic_counts_no_backward_steps() -> None:
    """A strictly increasing recv clock (the healthy host) records zero steps."""
    ing = _clock_ingester("clk-mono")
    before, _ = _recv_clock("clk-mono")
    for ns in (100, 200, 300, 400):
        ing._observe_recv_clock(ns)
    steps, worst = _recv_clock("clk-mono")
    assert steps == before
    assert worst == 0.0


def test_recv_clock_counts_backward_step_and_tracks_worst() -> None:
    """Each backward step increments the counter; the worst-step gauge is a
    high-water mark, unchanged by a smaller later step."""
    ing = _clock_ingester("clk-back")
    before, _ = _recv_clock("clk-back")
    ing._observe_recv_clock(1_000_000_000)
    ing._observe_recv_clock(950_000_000)   # back 50 ms
    ing._observe_recv_clock(960_000_000)   # forward again — no step
    ing._observe_recv_clock(900_000_000)   # back 60 ms — new worst
    ing._observe_recv_clock(880_000_000)   # back 20 ms — worse count, smaller step
    steps, worst = _recv_clock("clk-back")
    assert steps == before + 3
    assert worst == pytest.approx(60.0)


def test_recv_clock_persists_across_reconnect_gap() -> None:
    """_prev_recv_ns is instance-level (not reset per connection), so a step
    between the last frame of one connection and the first of the next is caught."""
    ing = _clock_ingester("clk-recon")
    before, _ = _recv_clock("clk-recon")
    ing._observe_recv_clock(5_000_000_000)  # last frame of connection A
    ing._observe_recv_clock(4_000_000_000)  # first frame of connection B, clock behind
    steps, _ = _recv_clock("clk-recon")
    assert steps == before + 1
