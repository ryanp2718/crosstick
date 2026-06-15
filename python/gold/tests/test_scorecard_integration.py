"""Phase 2 — scorecard-over-S3 integration: bronze -> silver -> gold on MinIO.

Seeds the golden corpus as bronze Parquet on an ephemeral MinIO (the same layout
the materializer writes), then runs the real silver and gold entrypoints against
it and asserts the planted incidents land in the gold scorecard. This is the only
test that exercises the S3 read/write path in common.lake (the unit tests use a
LocalFileSystem); the transform/aggregation logic itself is covered without Docker
by test_golden_pipeline.py.

Requires Docker; excluded from the default suite. Run with:

    uv run python -m pytest -m integration
"""
from __future__ import annotations

import json
import uuid
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pyarrow import fs as pafs

from analytics.corpus import CorpusRecord
from analytics.tests.golden import build_golden_records
from common.lake import read_dataset
from gold.main import build_basis_for_date, build_for_date
from materializer.bronze import (
    CanonicalMap,
    object_key,
    parse_topic,
    record_date,
    records_to_table,
)
from silver.dq import build_silver
from silver.main import read_bronze_records, write_silver

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"


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
        access_key="minio",
        secret_key="minio12345",
        endpoint_override=f"http://{host}:{port}",
        scheme="http",
        region="us-east-1",
        allow_bucket_creation=True,
    )


def _seed_bronze(
    fs: pafs.S3FileSystem, bucket: str, records: list[CorpusRecord], canonical: CanonicalMap
) -> None:
    by_topic: dict[str, list[CorpusRecord]] = defaultdict(list)
    for r in records:
        by_topic[r.topic].append(r)
    for topic, recs in by_topic.items():
        recs.sort(key=lambda r: r.offset)
        meta = parse_topic(topic)
        date = record_date(recs[0].timestamp_ms)
        key = object_key(meta, canonical, 0, recs[0].offset, date)
        pq.write_table(records_to_table(recs), f"{bucket}/{key}", filesystem=fs)


def test_scorecard_pipeline_over_s3(fs: pafs.S3FileSystem) -> None:
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    records = build_golden_records()
    date = record_date(records[0].timestamp_ms)

    suffix = uuid.uuid4().hex[:8]
    lake, silver, gold = (f"{name}-{suffix}" for name in ("lake", "silver", "gold"))
    for bucket in (lake, silver, gold):
        fs.create_dir(bucket)

    _seed_bronze(fs, lake, records, canonical)

    # silver: read bronze back over S3, transform, write the DQ facts.
    facts = build_silver(read_bronze_records(fs, lake, date), canonical)
    write_silver(fs, silver, facts)
    assert read_dataset(fs, silver, "book_quality", date) is not None

    # gold: aggregate silver over S3 into the scorecard.
    rows = build_for_date(fs, silver, date)
    sc = {(r["check"], r["exchange"], r["canonical_symbol"]): r for r in rows}
    assert sc[("sequence_gap", "kraken", "BTC-USD")]["n_violations"] == 1
    assert sc[("sequence_gap", "binance-futures", "BTC-USDT-PERP")]["n_violations"] == 0
    crossed = sc[("book_invariant", "binance", "BTC-USDT")]
    assert crossed["n_violations"] == 1
    assert json.loads(crossed["detail"])["crossed_after_delta"] == 1

    # gold: the basis mart over the same S3 silver (quotes -> nbbo -> basis).
    assert read_dataset(fs, silver, "nbbo", date) is not None
    basis, summary = build_basis_for_date(fs, silver, date, canonical)
    btc = [r for r in basis if r["base"] == "BTC"]
    assert btc and all(r["basis_abs"] == Decimal("5") for r in btc)
    assert {s["base"] for s in summary} == {"BTC"}
