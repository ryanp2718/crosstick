"""Tests for corpus replay (fake producer, no Docker)."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from analytics.corpus import CorpusRecord, read_corpus, write_corpus
from analytics.replay import replay_corpus


def _records() -> list[CorpusRecord]:
    return [
        CorpusRecord(
            topic="md.trades.coinbase.BTC-USD",
            partition=0,
            offset=0,
            timestamp_ms=1700,
            key=b"coinbase.BTC-USD",
            value=b'{"t":"trade","price":"1"}',
            headers=[("local_recv_ts_ns", b"123"), ("exchange_ts_ns", b"100")],
        ),
        CorpusRecord(
            topic="md.status.kraken",
            partition=0,
            offset=1,
            timestamp_ms=1701,
            key=None,
            value=b'{"t":"status"}',
            headers=[],
        ),
    ]


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[SimpleNamespace] = []

    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.sent.append(
            SimpleNamespace(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
            )
        )


@pytest.mark.asyncio
async def test_replay_forwards_all_fields() -> None:
    records = _records()
    producer = FakeProducer()
    n = await replay_corpus(producer, records)

    assert n == len(records)
    first = producer.sent[0]
    assert first.topic == records[0].topic
    assert first.value == records[0].value
    assert first.key == records[0].key
    assert first.partition == 0  # forced single-partition replay
    assert first.timestamp_ms == records[0].timestamp_ms
    assert first.headers == records[0].headers
    assert producer.sent[1].key is None


@pytest.mark.asyncio
async def test_capture_corpus_replay_preserves_bytes(tmp_path: Path) -> None:
    """corpus round-trip → replay yields byte-identical values, in order."""
    records = _records()
    path = tmp_path / "c.jsonl.gz"
    write_corpus(path, records)

    producer = FakeProducer()
    await replay_corpus(producer, read_corpus(path))

    assert [s.value for s in producer.sent] == [r.value for r in records]
    assert [s.topic for s in producer.sent] == [r.topic for r in records]
