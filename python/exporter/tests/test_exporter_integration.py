"""Lake-exporter over S3 (MinIO): the read/serve path the unit tests don't cover.

Seeds a gold scorecard + basis_summary + a bronze object on an ephemeral MinIO,
then builds the snapshot over S3 and renders it through the real collector +
registry exactly as a Prometheus scrape would. Requires Docker; run with:

    uv run python -m pytest -m integration
"""
from __future__ import annotations

import uuid

import pyarrow as pa
import pytest
from prometheus_client import CollectorRegistry, generate_latest
from pyarrow import fs as pafs

from common.lake import partition_key, write_object
from exporter.service import LakeCollector
from exporter.snapshot import build_families
from exporter.tests.test_snapshot import BASIS_ROWS, DATE, SCORECARD_ROWS, _index
from gold.basis import BASIS_SUMMARY_SCHEMA
from gold.scorecard import scorecard_table

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def minio():
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = (
        DockerContainer("minio/minio:latest")
        .with_env("MINIO_ROOT_USER", "minio")
        .with_env("MINIO_ROOT_PASSWORD", "minio12345")
        .with_exposed_ports(9000)
        .with_command("server /data")
    )
    with container:
        wait_for_logs(container, "API:", timeout=60)
        yield container


@pytest.fixture
def fs(minio) -> pafs.S3FileSystem:
    host = minio.get_container_host_ip()
    port = minio.get_exposed_port(9000)
    return pafs.S3FileSystem(
        access_key="minio", secret_key="minio12345",
        endpoint_override=f"http://{host}:{port}", scheme="http",
        region="us-east-1", allow_bucket_creation=True,
    )


def test_exporter_over_s3(fs: pafs.S3FileSystem) -> None:
    suffix = uuid.uuid4().hex[:8]
    lake, silver, gold = (f"{name}-{suffix}" for name in ("lake", "silver", "gold"))
    for bucket in (lake, silver, gold):
        fs.create_dir(bucket)

    write_object(fs, lake, f"book_deltas/exchange=kraken/symbol=BTC-USD/date={DATE}/part.parquet",
                 pa.table({"x": [1]}))
    write_object(fs, gold, partition_key("scorecard", date=DATE), scorecard_table(SCORECARD_ROWS))
    write_object(fs, gold, partition_key("basis_summary", date=DATE),
                 pa.Table.from_pylist(BASIS_ROWS, schema=BASIS_SUMMARY_SCHEMA))

    import time
    idx = _index(build_families(fs, lake, silver, gold, now_s=time.time()))
    assert idx[("gold_dq_violations", (("check", "clock_monotonic"), ("exchange", "kraken"),
                                       ("symbol", "BTC-USD")))] == 5
    assert idx[("gold_basis_bps", (("base", "BTC"), ("stat", "mean")))] == -4.1
    assert idx[("lake_freshness_seconds", (("dataset", "book_deltas"), ("layer", "bronze")))] > 0

    # full serve path: register the collector, refresh, render like a scrape.
    collector = LakeCollector(lambda: build_families(fs, lake, silver, gold, time.time()))
    registry = CollectorRegistry()
    registry.register(collector)
    collector.refresh()
    body = generate_latest(registry).decode()
    for name in ("gold_dq_violations", "gold_basis_bps", "lake_freshness_seconds",
                 "lake_exporter_last_success_seconds", "lake_exporter_refresh_errors_total"):
        assert name in body
