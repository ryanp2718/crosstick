"""Unit tests for the bronze projection logic (pure, no infrastructure)."""
from __future__ import annotations

from pathlib import Path

import pytest

from analytics.corpus import CorpusRecord
from materializer.bronze import (
    BRONZE_FORMAT,
    CanonicalMap,
    TopicMeta,
    object_key,
    parse_topic,
    record_date,
    records_to_table,
    table_to_records,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"


@pytest.mark.parametrize(
    ("topic", "expected"),
    [
        ("md.book.binance.BTCUSDT.snapshots", TopicMeta("book_snapshots", "binance", "BTCUSDT")),
        ("md.book.kraken.BTC-USD.deltas", TopicMeta("book_deltas", "kraken", "BTC-USD")),
        ("md.trades.coinbase.BTC-USD", TopicMeta("trades", "coinbase", "BTC-USD")),
        ("md.bbo.binance.BTCUSDT", TopicMeta("bbo", "binance", "BTCUSDT")),
        ("md.status.kraken", TopicMeta("status", "kraken", None)),
        ("md.nbbo.BTC-USDT", TopicMeta("nbbo", None, "BTC-USDT")),
    ],
)
def test_parse_topic(topic: str, expected: TopicMeta) -> None:
    assert parse_topic(topic) == expected


@pytest.mark.parametrize(
    "topic",
    [
        "md.book.binance",  # no symbol, no snapshot/delta suffix
        "md.book.BTCUSDT.snapshots",  # missing exchange segment
        "md.spread.BTC-USD",  # not a contract topic
        "other.topic",
    ],
)
def test_parse_topic_rejects_unknown_shapes(topic: str) -> None:
    with pytest.raises(ValueError):
        parse_topic(topic)


def test_canonical_map_resolves_and_falls_back() -> None:
    cmap = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    assert cmap.resolve("binance", "BTCUSDT") == "BTC-USDT"
    assert cmap.resolve("coinbase", "BTC-USD") == "BTC-USD"
    # Native and normalized spellings resolve identically (kraken BTC/USD).
    assert cmap.resolve("kraken", "BTC/USD") == "BTC-USD"
    assert cmap.resolve("kraken", "BTC-USD") == "BTC-USD"
    # Unmapped → normalized native symbol, never a drop.
    assert cmap.resolve("coinbase", "DOGE-USD") == "DOGE-USD"


def test_object_key_layouts() -> None:
    cmap = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    deltas = parse_topic("md.book.binance.BTCUSDT.deltas")
    assert object_key(deltas, cmap, 0, 42, "2023-11-14") == (
        "book_deltas/exchange=binance/symbol=BTC-USDT/date=2023-11-14/"
        "000-000000000042.parquet"
    )
    status = parse_topic("md.status.kraken")
    assert object_key(status, cmap, 0, 0, "2023-11-14") == (
        "status/exchange=kraken/date=2023-11-14/000-000000000000.parquet"
    )
    nbbo = parse_topic("md.nbbo.BTC-USDT")
    assert object_key(nbbo, cmap, 0, 7, "2023-11-14") == (
        "nbbo/symbol=BTC-USDT/date=2023-11-14/000-000000000007.parquet"
    )


def test_record_date_is_utc() -> None:
    assert record_date(1_700_000_000_000) == "2023-11-14"
    assert record_date(1_700_000_000_000 + 86_400_000) == "2023-11-15"


def _records() -> list[CorpusRecord]:
    return [
        CorpusRecord(
            topic="md.trades.coinbase.BTC-USD",
            partition=0,
            offset=i,
            timestamp_ms=1_700_000_000_000 + i,
            key=b"coinbase:BTC-USD" if i else None,
            value=b'{"t":"trade","i":%d}' % i,
            headers=[("local_recv_ts_ns", b"123"), ("exchange_ts_ns", b"456")] if i else [],
        )
        for i in range(3)
    ]


def test_table_round_trip_is_lossless() -> None:
    records = _records()
    table = records_to_table(records)
    assert table_to_records(table) == records


def test_table_footer_metadata_carries_provenance() -> None:
    table = records_to_table(_records())
    meta = table.schema.metadata
    assert meta[b"crosstick:format"] == BRONZE_FORMAT.encode()
    assert meta[b"crosstick:start_offset"] == b"0"
    assert meta[b"crosstick:end_offset"] == b"2"
    assert meta[b"crosstick:record_count"] == b"3"


def test_empty_table_refused() -> None:
    with pytest.raises(ValueError):
        records_to_table([])
