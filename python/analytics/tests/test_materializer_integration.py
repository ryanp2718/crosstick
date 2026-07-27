"""Phase 1 - materializer-in-the-loop integration.

Replays the golden corpus into an ephemeral Redpanda, runs the real
materializer against an ephemeral MinIO, and asserts:

  * bronze == corpus, verbatim and exactly once - every md.* record lands in
    a Parquet object under its canonical-resolved partition path, and reading
    the lake back reproduces the corpus records losslessly (bronze is
    corpus-shaped by design);
  * crash-recovery exactly-once: simulate "PUT landed, commit lost" by
    rewinding the group's committed offset for the busiest topic and
    restarting - the rerun rewrites the *same* start-offset keys, so the
    row count is unchanged (the bronze exactly-once property test).

Requires Docker; excluded from the default suite. Run with:

    uv run python -m pytest -m integration
"""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from aiokafka import AIOKafkaConsumer
from pyarrow import fs as pafs

from analytics.corpus import CorpusRecord, read_corpus
from analytics.kafka_admin import create_single_partition_topics
from analytics.replay import replay_corpus
from analytics.tests.kafka_admin import seed_group_offsets
from common.kafka_io import brokers_from_env, make_producer
from materializer.bronze import CanonicalMap, table_to_records
from materializer.service import Materializer

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"
GROUP_ID = "materializer"


@pytest.fixture(scope="module")
def redpanda():
    from testcontainers.kafka import RedpandaContainer

    with RedpandaContainer() as container:
        yield container


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
def brokers(redpanda, monkeypatch: pytest.MonkeyPatch) -> str:
    bootstrap = redpanda.get_bootstrap_server()
    monkeypatch.setenv("KAFKA_BROKERS", bootstrap)
    return bootstrap


@pytest.fixture
def lake_fs(minio) -> pafs.S3FileSystem:
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


async def _start_materializer(fs: pafs.S3FileSystem, bucket: str):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=brokers_from_env(),
        group_id=GROUP_ID,
        client_id="materializer-test",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    materializer = Materializer(
        consumer,
        fs,
        bucket,
        CanonicalMap.from_yaml(INSTRUMENTS_FILE),
        # Small chunks force multiple objects per topic; short age sweeps tails.
        flush_bytes=512,
        flush_interval_sec=0.4,
    )
    consumer.subscribe(pattern=r"^md\.", listener=materializer.rebalance_listener())
    await consumer.start()
    return consumer, materializer


async def _run_until_flushed(materializer: Materializer, expected: int, deadline_sec=30.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_sec
    while materializer.records_flushed < expected and loop.time() < deadline:
        await materializer.poll_once(timeout_ms=200)
    return materializer.records_flushed


def _read_bronze(fs: pafs.S3FileSystem, bucket: str) -> list[CorpusRecord]:
    out: list[CorpusRecord] = []
    for info in fs.get_file_info(pafs.FileSelector(bucket, recursive=True)):
        if info.type == pafs.FileType.File and info.path.endswith(".parquet"):
            out.extend(table_to_records(pq.read_table(info.path, filesystem=fs)))
    return out


def _by_topic(records: list[CorpusRecord]) -> dict[str, list[CorpusRecord]]:
    grouped: dict[str, list[CorpusRecord]] = defaultdict(list)
    for r in records:
        grouped[r.topic].append(r)
    return {t: sorted(rs, key=lambda r: r.offset) for t, rs in grouped.items()}


@pytest.mark.asyncio
async def test_bronze_equals_corpus_and_survives_lost_commit(
    brokers: str, lake_fs: pafs.S3FileSystem, golden_corpus_path: Path
) -> None:
    records = list(read_corpus(golden_corpus_path))
    topics = sorted({r.topic for r in records})
    await create_single_partition_topics(topics)

    producer = await make_producer(client_id="phase1-replay")
    try:
        await replay_corpus(producer, records)
    finally:
        await producer.stop()

    bucket = f"lake-{uuid.uuid4().hex[:8]}"
    lake_fs.create_dir(bucket)

    consumer, materializer = await _start_materializer(lake_fs, bucket)
    try:
        flushed = await _run_until_flushed(materializer, len(records))
    finally:
        await consumer.stop()
    assert flushed == len(records)

    # ── bronze == corpus: lossless, exactly once, corpus-shaped ──────────────
    bronze = _read_bronze(lake_fs, bucket)
    assert len(bronze) == len(records)
    assert _by_topic(bronze) == _by_topic(records)

    # Canonical resolution + per-dataset partition schemes show up in the paths.
    paths = [
        info.path
        for info in lake_fs.get_file_info(pafs.FileSelector(bucket, recursive=True))
        if info.type == pafs.FileType.File
    ]
    assert any("book_deltas/exchange=binance/symbol=BTC-USDT/" in p for p in paths)
    assert any("book_snapshots/exchange=kraken/symbol=BTC-USD/" in p for p in paths)
    assert any(f"{bucket}/status/exchange=" in p for p in paths)
    # Perp datasets resolve to the distinct BTC-USDT-PERP canonical (not spot's
    # BTC-USDT) despite the shared native symbol BTCUSDT.
    assert any("book_deltas/exchange=binance-futures/symbol=BTC-USDT-PERP/" in p for p in paths)
    assert any("mark_price/exchange=binance-futures/symbol=BTC-USDT-PERP/" in p for p in paths)
    assert any("liquidations/exchange=binance-futures/symbol=BTC-USDT-PERP/" in p for p in paths)
    assert any("open_interest/exchange=binance-futures/symbol=BTC-USDT-PERP/" in p for p in paths)

    # ── crash recovery: "PUT landed, commit lost" on the busiest topic ───────
    victim = max(topics, key=lambda t: sum(r.topic == t for r in records))
    n_victim = sum(r.topic == victim for r in records)
    await seed_group_offsets(GROUP_ID, [victim], offset=0)

    consumer2, materializer2 = await _start_materializer(lake_fs, bucket)
    try:
        reflushed = await _run_until_flushed(materializer2, n_victim)
    finally:
        await consumer2.stop()
    assert reflushed == n_victim

    # The rerun rewrote identical start-offset keys: nothing duplicated.
    bronze_after = _read_bronze(lake_fs, bucket)
    assert len(bronze_after) == len(records)
    assert _by_topic(bronze_after) == _by_topic(records)
