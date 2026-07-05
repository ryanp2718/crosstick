"""Tests for corpus replay (fake producer, no Docker)."""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from analytics.corpus import CorpusRecord, read_corpus, write_corpus
from analytics.replay import pace_delay_sec, parse_args, replay_corpus


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


def test_pace_delay_scales_and_clamps() -> None:
    assert pace_delay_sec(1000, 2000, 0.0, 1.0) == 1.0
    assert pace_delay_sec(1000, 2000, 0.0, 2.0) == 0.5
    assert pace_delay_sec(1000, 2000, 0.75, 1.0) == 0.25  # drift-corrected
    assert pace_delay_sec(1000, 2000, 5.0, 1.0) == 0.0  # behind schedule
    assert pace_delay_sec(1000, 900, 0.0, 1.0) == 0.0  # timestamp jitter


@pytest.mark.asyncio
async def test_paced_replay_sleeps_between_records(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def fake_sleep(sec: float) -> None:
        delays.append(sec)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    records = _records()  # 1ms apart; speed=0.001 schedules a ~1s gap
    producer = FakeProducer()
    n = await replay_corpus(producer, records, speed=0.001)

    assert n == len(records)
    assert len(delays) == 1
    assert 0 < delays[0] <= 1.0
    assert [s.topic for s in producer.sent] == [r.topic for r in records]


def test_parse_args_speed() -> None:
    assert parse_args(["c.jsonl.gz"]).speed is None
    assert parse_args(["--speed", "2", "c.jsonl.gz"]).speed == 2.0
    with pytest.raises(SystemExit):
        parse_args(["--speed", "0", "c.jsonl.gz"])
