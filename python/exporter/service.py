"""Lake-exporter collector: serve cached lake-derived metrics, refresh in a loop.

A `prometheus_client` custom Collector returns metric families from a snapshot
cached by a background refresh, so a Prometheus scrape is always fast and never
blocks on S3 - and each scrape reflects one consistent snapshot (no stale label
sets, the failure mode of mutating shared Gauges in place). A failed refresh
(transient MinIO blip) keeps the last good snapshot and is itself observable via
`lake_exporter_refresh_errors_total`.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric

log = logging.getLogger(__name__)


class LakeCollector:
    """Wraps a snapshot builder; `refresh()` rebuilds the cache, `collect()` serves it.

    An optional `audit` builder runs on a slower cadence (the caller drives it) for the
    once-daily marker-vs-reality cross-check; its families are cached separately and
    merged into every scrape, so a scrape never blocks on the full LIST walk."""

    def __init__(
        self, build: Callable[[], list[Metric]],
        audit: Callable[[], list[Metric]] | None = None,
    ) -> None:
        self._build = build
        self._audit = audit
        self._families: list[Metric] = []
        self._audit_families: list[Metric] = []
        self._errors = 0.0
        self._last_success = 0.0

    def refresh(self) -> None:
        try:
            self._families = self._build()
            self._last_success = time.time()
        except Exception:  # transient S3/MinIO blip - keep the last good snapshot
            self._errors += 1
            log.exception("lake-exporter refresh failed; serving last good snapshot")

    def run_audit(self) -> None:
        if self._audit is None:
            return
        try:
            self._audit_families = self._audit()
        except Exception:  # keep the last good audit snapshot
            self._errors += 1
            log.exception("lake-exporter audit failed; serving last good audit snapshot")

    def collect(self) -> Iterator[Metric]:
        yield from self._families
        yield from self._audit_families
        errs = CounterMetricFamily(
            "lake_exporter_refresh_errors",
            "Lake snapshot refreshes that raised (serving the last good snapshot)",
        )
        errs.add_metric([], self._errors)
        yield errs
        last = GaugeMetricFamily(
            "lake_exporter_last_success_seconds",
            "Unix time of the last successful lake snapshot refresh",
        )
        last.add_metric([], self._last_success)
        yield last
