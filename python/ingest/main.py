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
from ingest.coinbase import CoinbaseIngester
from ingest.kraken import KrakenIngester

log = logging.getLogger(__name__)

INGESTERS: dict[str, type[BaseIngester]] = {
    "coinbase": CoinbaseIngester,
    "binance": BinanceIngester,
    "kraken": KrakenIngester,
}


def parse_symbols(raw: str | None) -> list[str] | None:
    """Parse comma-separated ``$SYMBOLS``; None/blank means use driver defaults."""
    if not raw:
        return None
    syms = [s.strip() for s in raw.split(",") if s.strip()]
    return syms or None


def build_ingester(
    exchange: str,
    symbols: list[str] | None,
    producer: object,
) -> BaseIngester:
    """Construct the driver for ``exchange``; ValueError on an unknown name."""
    try:
        cls = INGESTERS[exchange]
    except KeyError:
        raise ValueError(
            f"unknown EXCHANGE {exchange!r}; expected one of {sorted(INGESTERS)}"
        ) from None
    return cls(producer=producer, symbols=symbols)


async def amain() -> None:
    exchange = os.environ.get("EXCHANGE", "").strip().lower()
    if not exchange:
        raise SystemExit("EXCHANGE env var is required (coinbase|binance|kraken)")
    symbols = parse_symbols(os.environ.get("SYMBOLS"))

    serve_metrics_in_background()  # port from $METRICS_PORT
    producer = await make_producer(client_id=f"ingest-{exchange}")
    ingester = build_ingester(exchange, symbols, producer)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(ingester.shutdown()))
        except NotImplementedError:
            # Windows ProactorEventLoop lacks add_signal_handler; KeyboardInterrupt
            # (handled in main) still stops a local dev run.
            log.warning("signal handlers unavailable on this platform; use Ctrl+C")
            break

    try:
        await ingester.run()
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
