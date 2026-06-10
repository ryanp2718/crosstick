"""Component tests for Materializer chunk/commit behaviour.

The Parquet PUT is monkeypatched to a recorder — the real S3 path is covered
by the integration test. Here we pin the cut rules (size-dominant, UTC date
boundary, age), the start-offset key naming, and commit-after-PUT offsets.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from aiokafka.structs import TopicPartition

import materializer.service as service_mod
from materializer.bronze import CanonicalMap
from materializer.service import Materializer

TP = TopicPartition("md.trades.coinbase.BTC-USD", 0)
BASE_MS = 1_700_000_000_000  # 2023-11-14 UTC
DAY_MS = 86_400_000


class FakeConsumer:
    def __init__(self, batches: list[dict] | None = None):
        self.batches = batches or []
        self.commits: list[dict] = []

    async def getmany(self, timeout_ms: int = 1000) -> dict:
        return self.batches.pop(0) if self.batches else {}

    async def commit(self, offsets: dict) -> None:
        self.commits.append(dict(offsets))


def msg(offset: int, *, value: bytes = b"x" * 10, ts: int = BASE_MS) -> SimpleNamespace:
    return SimpleNamespace(
        topic=TP.topic,
        partition=TP.partition,
        offset=offset,
        timestamp=ts,
        key=b"coinbase:BTC-USD",
        value=value,
        headers=[],
    )


@pytest.fixture
def written(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, object]]:
    calls: list[tuple[str, object]] = []

    def fake_write_table(table, path, filesystem=None, **kwargs) -> None:
        calls.append((path, table))

    monkeypatch.setattr(service_mod.pq, "write_table", fake_write_table)
    return calls


def make_materializer(consumer: FakeConsumer, **kwargs) -> Materializer:
    cmap = CanonicalMap({("coinbase", "BTC-USD"): "BTC-USD"})
    return Materializer(consumer, filesystem=None, bucket="lake", canonical_map=cmap, **kwargs)


async def test_size_cut_then_commit_after_put(written) -> None:
    consumer = FakeConsumer([{TP: [msg(0), msg(1), msg(2)]}])
    mat = make_materializer(consumer, flush_bytes=20, flush_interval_sec=3600)
    await mat.poll_once()

    # 10-byte values vs 20-byte threshold → cut after offset 1; offset 2 buffered.
    assert [p for p, _ in written] == [
        "lake/trades/exchange=coinbase/symbol=BTC-USD/date=2023-11-14/000-000000000000.parquet"
    ]
    assert consumer.commits == [{TP: 2}]
    assert mat.records_flushed == 2

    await mat.flush_all()
    assert written[1][0].endswith("000-000000000002.parquet")
    assert consumer.commits == [{TP: 2}, {TP: 3}]
    assert mat.records_flushed == 3


async def test_date_boundary_cut(written) -> None:
    consumer = FakeConsumer([{TP: [msg(0, ts=BASE_MS), msg(1, ts=BASE_MS + DAY_MS)]}])
    mat = make_materializer(consumer, flush_bytes=10**9, flush_interval_sec=3600)
    await mat.poll_once()

    # Crossing UTC midnight flushes the old day before buffering the new one.
    assert len(written) == 1 and "date=2023-11-14" in written[0][0]
    await mat.flush_all()
    assert "date=2023-11-15" in written[1][0]


async def test_age_flush(written) -> None:
    consumer = FakeConsumer([{TP: [msg(0)]}])
    mat = make_materializer(consumer, flush_bytes=10**9, flush_interval_sec=0.0)
    await mat.poll_once()
    assert len(written) == 1
    assert consumer.commits == [{TP: 1}]


async def test_run_flushes_remainder_on_shutdown(written) -> None:
    consumer = FakeConsumer([{TP: [msg(0)]}])
    mat = make_materializer(consumer, flush_bytes=10**9, flush_interval_sec=3600)
    await mat.poll_once()
    assert written == []  # still buffered

    mat.shutdown()
    await mat.run()  # exits immediately, final sweep flushes
    assert len(written) == 1
    assert consumer.commits == [{TP: 1}]
