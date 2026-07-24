"""Prometheus metrics + a tiny aiohttp-free HTTP server exposing /metrics."""

from __future__ import annotations

import asyncio
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

REGISTRY = CollectorRegistry()

messages_received = Counter(
    "md_messages_received_total",
    "Messages received from exchange WS",
    ["exchange", "channel"],
    registry=REGISTRY,
)
bytes_received = Counter(
    "md_bytes_received_total",
    "Raw bytes received from exchange WS",
    ["exchange"],
    registry=REGISTRY,
)
ws_reconnects = Counter(
    "md_ws_reconnects_total",
    "WS reconnection events",
    ["exchange", "reason"],
    registry=REGISTRY,
)
book_resyncs = Counter(
    "md_book_resyncs_total",
    "Order book full resyncs",
    ["exchange", "reason"],
    registry=REGISTRY,
)
book_invariant_violations = Counter(
    "md_book_invariant_violations_total",
    "best_bid >= best_ask or other invariant failures",
    ["exchange", "symbol", "kind"],
    registry=REGISTRY,
)
exchange_latency = Histogram(
    "md_exchange_latency_seconds",
    "Exchange-reported ts to local recv ts",
    ["exchange"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    registry=REGISTRY,
)
book_state = Gauge(
    "md_book_state",
    "0=BOOTSTRAP, 1=LIVE, 2=STALE",
    ["exchange", "symbol"],
    registry=REGISTRY,
)
queue_depth = Gauge(
    "md_queue_depth",
    "Bounded asyncio queue depth",
    ["component", "name"],
    registry=REGISTRY,
)
# Live host capture-clock canary: backward steps in time.time_ns() between
# consecutive WS frames (the per-build gold_dq_clock_* comes from the scorecard).
recv_clock_backward_steps = Counter(
    "md_recv_clock_backward_steps_total",
    "Backward steps in the host capture clock between consecutive WS frames",
    ["exchange"],
    registry=REGISTRY,
)
recv_clock_worst_step_ms = Gauge(
    "md_recv_clock_worst_step_ms",
    "Largest backward host-clock step since process start (ms)",
    ["exchange"],
    registry=REGISTRY,
)


def _handler_for(registry: CollectorRegistry) -> type[BaseHTTPRequestHandler]:
    class _MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/metrics":
                body = generate_latest(registry)
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    return _MetricsHandler


def serve_metrics_in_background(
    port: int | None = None, registry: CollectorRegistry = REGISTRY
) -> None:
    """Start a metrics HTTP server on a daemon thread.

    Threading (not asyncio) is intentional - prom-client renders are sync and
    we want metrics to keep responding even if the event loop is busy. `registry`
    defaults to the shared ingest/materializer REGISTRY; the lake-exporter passes
    its own so it serves only its lake-derived metrics.
    """
    if port is None:
        port = int(os.environ.get("METRICS_PORT", "9100"))
    server = ThreadingHTTPServer(("0.0.0.0", port), _handler_for(registry))
    thread = Thread(target=server.serve_forever, name="metrics-http", daemon=True)
    thread.start()


def install_uvloop_if_available() -> None:
    """Best-effort uvloop install. No-op on Windows where uvloop has no wheels."""
    try:
        import uvloop  # type: ignore[import-not-found]
    except ImportError:
        return
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
