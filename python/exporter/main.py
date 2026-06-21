"""Lake-exporter entrypoint: ``python -m exporter.main``.

Reads the MinIO/S3 lake and publishes the gold DQ scorecard, basis, and per-layer
freshness as Prometheus metrics. Long-running + pull-based (Prometheus scrapes
``/metrics``) so it fits the existing observability stack; cloud cutover is an env
change via ``filesystem_from_env()``. Config (env): ``S3_*``, ``METRICS_PORT``
(9120), ``REFRESH_SEC`` (60), ``LAKE_BUCKET``/``SILVER_BUCKET``/``GOLD_BUCKET``.
"""
from __future__ import annotations

import logging
import os
import time

from prometheus_client import CollectorRegistry

from common.lake import filesystem_from_env
from common.metrics import serve_metrics_in_background
from exporter.service import LakeCollector
from exporter.snapshot import build_families

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    fs = filesystem_from_env()
    lake = os.environ.get("LAKE_BUCKET", "lake")
    silver = os.environ.get("SILVER_BUCKET", "silver")
    gold = os.environ.get("GOLD_BUCKET", "gold")
    refresh_sec = float(os.environ.get("REFRESH_SEC", "60"))
    port = int(os.environ.get("METRICS_PORT", "9120"))

    collector = LakeCollector(lambda: build_families(fs, lake, silver, gold, time.time()))
    registry = CollectorRegistry()
    registry.register(collector)

    collector.refresh()  # prime before serving so the first scrape has data
    serve_metrics_in_background(port=port, registry=registry)
    log.info("lake-exporter serving /metrics on :%d (refresh %.0fs)", port, refresh_sec)
    while True:
        time.sleep(refresh_sec)
        collector.refresh()


if __name__ == "__main__":
    main()
