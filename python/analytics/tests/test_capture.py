"""Tests for the capture mapping + drain loop (fake consumer, no Docker)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from analytics.capture import record_from_message, run_capture
from analytics.corpus import CorpusWriter, read_corpus


def _msg(
    topic: str,
    offset: int,
    value: bytes,
    *,
    key: bytes | None = b"k",
    headers: list[tuple[str, bytes]] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        topic=topic,
        partition=0,
        offset=offset,
        timestamp=1000 + offset,
        key=key,
        value=value,
        headers=headers,
    )


def test_record_from_message_maps_fields() -> None:
    m = _msg(
        "md.trades.coinbase.BTC-USD",
        7,
        b'{"t":"trade"}',
        key=b"coinbase.BTC-USD",
        headers=[("local_recv_ts_ns", b"123")],
    )
    r = record_from_message(m)
    assert r.topic == "md.trades.coinbase.BTC-USD"
    assert r.partition == 0
    assert r.offset == 7
    assert r.timestamp_ms == 1007
    assert r.key == b"coinbase.BTC-USD"
    assert r.value == b'{"t":"trade"}'
    assert r.headers == [("local_recv_ts_ns", b"123")]


def test_record_from_message_handles_none_headers_and_key() -> None:
    r = record_from_message(_msg("md.status.kraken", 1, b"{}", key=None, headers=None))
    assert r.key is None
    assert r.headers == []


class FakeConsumer:
    """Returns scripted getmany batches, then empty dicts forever."""

    def __init__(self, batches: list[dict[str, list[SimpleNamespace]]]):
        self._batches = list(batches)

    async def getmany(self, timeout_ms: int) -> dict[str, list[SimpleNamespace]]:
        if self._batches:
            return self._batches.pop(0)
        return {}


@pytest.mark.asyncio
async def test_run_capture_stops_at_max_records(tmp_path: Path) -> None:
    batches = [
        {"tp": [_msg("md.trades.x", 0, b"a"), _msg("md.trades.x", 1, b"b")]},
        {"tp": [_msg("md.trades.x", 2, b"c"), _msg("md.trades.x", 3, b"d")]},
    ]
    consumer = FakeConsumer(batches)
    path = tmp_path / "c.jsonl.gz"
    with CorpusWriter(path) as w:
        n = await run_capture(consumer, w, max_records=3)
    assert n == 3
    assert len(list(read_corpus(path))) == 3


@pytest.mark.asyncio
async def test_run_capture_drains_until_duration(tmp_path: Path) -> None:
    batches = [{"tp": [_msg("md.trades.x", 0, b"a"), _msg("md.trades.x", 1, b"b")]}]
    consumer = FakeConsumer(batches)
    path = tmp_path / "c.jsonl.gz"
    with CorpusWriter(path) as w:
        n = await run_capture(consumer, w, duration_sec=0.05)
    assert n == 2
    assert len(list(read_corpus(path))) == 2
