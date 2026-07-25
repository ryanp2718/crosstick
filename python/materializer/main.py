"""Materializer entrypoint: ``python -m materializer.main``.

Consumes every ``md.*`` topic (pattern subscription, parallel to the gateway)
into bronze Parquet on the lake. Config via env, matching docker-compose:
``KAFKA_BROKERS``, ``S3_ENDPOINT``/``S3_ACCESS_KEY``/``S3_SECRET_KEY``/
``S3_BUCKET``, ``INSTRUMENTS_FILE``, ``FLUSH_BYTES``, ``FLUSH_INTERVAL_SEC``,
``METRICS_PORT``.

The consumer group is the durable "how far has bronze got" cursor:
``auto_offset_reset=earliest`` means a first deployment drains everything the
log still retains (the 30-day retention window is the data-loss horizon -
see docs/data-contracts.md "Retention").
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from aiokafka import AIOKafkaConsumer

from common.kafka_io import brokers_from_env
from common.lake import filesystem_from_env, instruments_path_from_env
from common.metrics import install_uvloop_if_available, serve_metrics_in_background
from materializer.bronze import CanonicalMap
from materializer.service import Materializer

log = logging.getLogger(__name__)

GROUP_ID = "materializer"


async def amain() -> None:
    serve_metrics_in_background()  # port from $METRICS_PORT
    canonical_map = CanonicalMap.from_yaml(instruments_path_from_env())
    consumer = AIOKafkaConsumer(
        bootstrap_servers=brokers_from_env(),
        group_id=GROUP_ID,
        client_id="materializer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    materializer = Materializer(
        consumer,
        filesystem_from_env(),
        os.environ.get("S3_BUCKET", "lake"),
        canonical_map,
        flush_bytes=int(os.environ.get("FLUSH_BYTES", str(16 * 1024 * 1024))),
        flush_interval_sec=float(os.environ.get("FLUSH_INTERVAL_SEC", "900")),
    )
    consumer.subscribe(pattern=r"^md\.", listener=materializer.rebalance_listener())
    await consumer.start()
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, materializer.shutdown)
            except NotImplementedError:
                # Windows ProactorEventLoop; Ctrl+C in main still stops a dev run.
                log.warning("signal handlers unavailable on this platform; use Ctrl+C")
                break
        await materializer.run()
    finally:
        await consumer.stop()


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    install_uvloop_if_available()
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
