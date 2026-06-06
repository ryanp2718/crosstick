"""Integration: replay the golden corpus into an ephemeral Redpanda and read it
back, asserting a faithful round-trip.

This is the foundation of the integration harness — it proves ephemeral infra +
deterministic replay work end-to-end. The gateway-in-the-loop NBBO assertion
(Phase 0b) builds on the same fixtures.

Requires Docker; excluded from the default suite. Run with:

    uv run python -m pytest -m integration
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from analytics.corpus import read_corpus
from analytics.replay import replay_corpus
from analytics.tests.kafka_admin import create_single_partition_topics
from common.kafka_io import make_consumer, make_producer

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def redpanda():
    from testcontainers.kafka import RedpandaContainer

    with RedpandaContainer() as container:
        yield container


@pytest.fixture
def brokers(redpanda, monkeypatch: pytest.MonkeyPatch) -> str:
    bootstrap = redpanda.get_bootstrap_server()
    # brokers_from_env() (used by make_producer/make_consumer) reads this.
    monkeypatch.setenv("KAFKA_BROKERS", bootstrap)
    return bootstrap


async def _consume_all(topics: list[str], expected: int, timeout_sec: float = 30.0) -> list:
    consumer = await make_consumer(
        *topics, group_id="test-consume", auto_offset_reset="earliest"
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec
    out: list = []
    try:
        while len(out) < expected and loop.time() < deadline:
            batches = await consumer.getmany(timeout_ms=1000)
            for msgs in batches.values():
                out.extend(msgs)
    finally:
        await consumer.stop()
    return out


@pytest.mark.asyncio
async def test_replay_roundtrip_is_faithful(brokers: str, golden_corpus_path: Path) -> None:
    records = list(read_corpus(golden_corpus_path))
    topics = sorted({r.topic for r in records})
    await create_single_partition_topics(topics)

    producer = await make_producer(client_id="test-replay")
    try:
        produced = await replay_corpus(producer, records)
    finally:
        await producer.stop()
    assert produced == len(records)

    consumed = await _consume_all(topics, expected=len(records))
    assert len(consumed) == len(records)

    # Per-topic order is guaranteed (single partition); cross-topic order is not.
    for topic in topics:
        expected = [(r.key, r.value, r.headers) for r in records if r.topic == topic]
        got = [
            (m.key, m.value, [(k, v) for k, v in (m.headers or [])])
            for m in consumed
            if m.topic == topic
        ]
        assert got == expected, f"round-trip mismatch on {topic}"

        offsets = [m.offset for m in consumed if m.topic == topic]
        assert offsets == list(range(len(expected))), f"non-deterministic offsets on {topic}"
