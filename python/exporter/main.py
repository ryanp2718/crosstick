"""Lake-exporter entrypoint: ``python -m exporter.main``.

Reads the lake and publishes the gold DQ scorecard, basis, and per-layer freshness
as Prometheus metrics. Long-running + pull-based (Prometheus scrapes ``/metrics``)
so it fits the existing observability stack. Base ``S3_*`` addresses the derived
layers (silver/gold); ``LAKE_S3_*`` optionally points bronze reads at a distinct
endpoint, falling back to ``S3_*`` when unset (see bronze_filesystem_from_env), so
cloud cutover is an env change. Config (env): ``S3_*`` + ``LAKE_S3_*``,
``METRICS_PORT`` (9120), ``REFRESH_SEC`` (60), ``AUDIT_INTERVAL_SEC`` (86400, the
slow marker-vs-reality cross-check), ``LAKE_BUCKET``/``SILVER_BUCKET``/``GOLD_BUCKET``.
"""
from __future__ import annotations

import logging
import os
import time

from prometheus_client import CollectorRegistry

from common.lake import bronze_filesystem_from_env, filesystem_from_env
from common.metrics import serve_metrics_in_background
from exporter.service import LakeCollector
from exporter.snapshot import audit_families, build_families

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    bronze_fs = bronze_filesystem_from_env()
    derived_fs = filesystem_from_env()
    lake = os.environ.get("LAKE_BUCKET", "lake")
    silver = os.environ.get("SILVER_BUCKET", "silver")
    gold = os.environ.get("GOLD_BUCKET", "gold")
    refresh_sec = float(os.environ.get("REFRESH_SEC", "60"))
    audit_sec = float(os.environ.get("AUDIT_INTERVAL_SEC", "86400"))
    port = int(os.environ.get("METRICS_PORT", "9120"))

    collector = LakeCollector(
        lambda: build_families(bronze_fs, derived_fs, lake, silver, gold, time.time()),
        audit=lambda: audit_families(derived_fs, silver, gold),
    )
    registry = CollectorRegistry()
    registry.register(collector)

    collector.refresh()  # prime before serving so the first scrape has data
    collector.run_audit()  # prime the daily marker-vs-reality skew once at startup
    serve_metrics_in_background(port=port, registry=registry)
    log.info(
        "lake-exporter serving /metrics on :%d (refresh %.0fs, audit %.0fs)",
        port, refresh_sec, audit_sec,
    )
    next_audit = time.time() + audit_sec
    while True:
        time.sleep(refresh_sec)
        collector.refresh()
        if time.time() >= next_audit:
            collector.run_audit()
            next_audit = time.time() + audit_sec


if __name__ == "__main__":
    main()
