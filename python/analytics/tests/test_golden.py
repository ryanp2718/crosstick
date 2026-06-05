"""Verify the golden corpus is deterministic and its planted events are real."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from analytics.corpus import CorpusRecord, read_corpus, write_corpus
from analytics.tests.golden import build_golden_records
from common.kafka_io import book_delta_topic, book_snapshot_topic, status_topic
from common.models import BookDelta, BookSnapshot, Status, decode
from ingest.book import BookInvariantError, OrderBook


def _levels(levels: list) -> list[tuple[Decimal, Decimal]]:
    return [(Decimal(lv.price), Decimal(lv.size)) for lv in levels]


def test_builder_is_deterministic() -> None:
    assert build_golden_records() == build_golden_records()


def test_all_values_decode(golden_records: list[CorpusRecord]) -> None:
    assert golden_records  # non-empty
    for r in golden_records:
        decode(r.value)  # raises on any malformed value


def test_corpus_roundtrips_through_file(
    golden_records: list[CorpusRecord], tmp_path: Path
) -> None:
    path = tmp_path / "golden.jsonl.gz"
    write_corpus(path, golden_records)
    assert list(read_corpus(path)) == golden_records


def test_planted_sequence_gap(golden_records: list[CorpusRecord]) -> None:
    """Kraken book deltas skip seq 7 (6 → 8)."""
    topic = book_delta_topic("kraken", "BTC/USD")
    seqs = [decode(r.value).sequence for r in golden_records if r.topic == topic]
    assert seqs == [6, 8]
    assert all(isinstance(decode(r.value), BookDelta) for r in golden_records if r.topic == topic)


def test_planted_venue_down(golden_records: list[CorpusRecord]) -> None:
    downs = [
        decode(r.value)
        for r in golden_records
        if r.topic == status_topic("binance") and decode(r.value).state == "down"
    ]
    assert len(downs) == 1
    assert isinstance(downs[0], Status)


def test_planted_crossed_book_raises_on_reconstruction(
    golden_records: list[CorpusRecord],
) -> None:
    """Replaying binance snapshot+deltas through the real OrderBook must cross."""
    snap_topic = book_snapshot_topic("binance", "BTCUSDT")
    delta_topic = book_delta_topic("binance", "BTCUSDT")
    book = OrderBook("binance", "BTCUSDT")

    with pytest.raises(BookInvariantError, match="crossed"):
        for r in golden_records:
            if r.topic == snap_topic:
                m = decode(r.value)
                assert isinstance(m, BookSnapshot)
                book.apply_snapshot(m.sequence, _levels(m.bids), _levels(m.asks))
            elif r.topic == delta_topic:
                m = decode(r.value)
                book.apply_delta(m.sequence, _levels(m.bids), _levels(m.asks))
