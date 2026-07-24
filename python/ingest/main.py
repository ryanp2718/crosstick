"""Ingester entrypoint: ``python -m ingest.main``.

Selects an exchange driver from ``$EXCHANGE``, wires it to a Kafka producer,
starts the Prometheus sidecar, and runs until SIGINT/SIGTERM. Symbols come from
``$SYMBOLS`` (comma-separated) or the driver's defaults; brokers from
``$KAFKA_BROKERS`` (see common.kafka_io). This is the process docker-compose
launches for ingest-binance / ingest-coinbase / ingest-kraken.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal

from common.kafka_io import make_producer
from common.metrics import install_uvloop_if_available, serve_metrics_in_background
from ingest.base_ingester import BaseIngester
from ingest.binance import BinanceIngester
from ingest.binance_futures import BinanceFuturesIngester
from ingest.coinbase import CoinbaseIngester
from ingest.kraken import KrakenIngester

log = logging.getLogger(__name__)

INGESTERS: dict[str, type[BaseIngester]] = {
    "coinbase": CoinbaseIngester,
    "binance": BinanceIngester,
    "binance-futures": BinanceFuturesIngester,
    "kraken": KrakenIngester,
}


def parse_symbols(raw: str | None) -> list[str] | None:
    """Parse comma-separated ``$SYMBOLS``; None/blank means use driver defaults."""
    if not raw:
        return None
    syms = [s.strip() for s in raw.split(",") if s.strip()]
    return syms or None


def build_ingesters(
    exchange: str,
    symbols: list[str] | None,
    producer: object,
) -> list[BaseIngester]:
    """Construct the driver(s) for ``exchange``; ValueError on an unknown name.

    binance-futures returns two instances sharing one producer: Binance routes
    depth and market streams to different WS endpoints, and one connection
    never delivers both (see ingest/binance_futures.py)."""
    if exchange == "binance-futures":
        return [
            BinanceFuturesIngester(producer=producer, symbols=symbols, mode="market"),
            BinanceFuturesIngester(producer=producer, symbols=symbols, mode="depth"),
        ]
    try:
        cls = INGESTERS[exchange]
    except KeyError:
        raise ValueError(
            f"unknown EXCHANGE {exchange!r}; expected one of {sorted(INGESTERS)}"
        ) from None
    return [cls(producer=producer, symbols=symbols)]


async def amain() -> None:
    exchange = os.environ.get("EXCHANGE", "").strip().lower()
    if not exchange:
        raise SystemExit(
            "EXCHANGE env var is required (coinbase|binance|binance-futures|kraken)"
        )
    symbols = parse_symbols(os.environ.get("SYMBOLS"))

    serve_metrics_in_background()  # port from $METRICS_PORT
    producer = await make_producer(client_id=f"ingest-{exchange}")
    # Anything between make_producer and the matching producer.stop() must be
    # inside this try, or a construction failure (unknown EXCHANGE, driver
    # __init__ error) leaks the producer's network connections and background
    # sender task.
    try:
        ingesters = build_ingesters(exchange, symbols, producer)

        shutdown_futs: list[asyncio.Future[None]] = []  # keep refs (RUF006)

        def _request_shutdown() -> None:
            shutdown_futs.extend(
                asyncio.ensure_future(ing.shutdown()) for ing in ingesters
            )

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                # Windows ProactorEventLoop lacks add_signal_handler;
                # KeyboardInterrupt (handled in main) still stops a local dev run.
                log.warning("signal handlers unavailable on this platform; use Ctrl+C")
                break

        # One failed run() fails the process (compose restarts it) - letting
        # half a venue keep running would mask the loss of the other half.
        await asyncio.gather(*(ing.run() for ing in ingesters))
    finally:
        await producer.stop()


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    install_uvloop_if_available()
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
