"""Unit tests for the shared lake I/O helpers (LocalFileSystem, no infra)."""
from __future__ import annotations

import pyarrow as pa
from pyarrow import fs as pafs

from common.lake import partition_key, read_dataset, write_object


def test_partition_key_layouts() -> None:
    assert partition_key(
        "book_quality", exchange="binance", symbol="BTC-USDT", date="2026-06-12"
    ) == "book_quality/exchange=binance/symbol=BTC-USDT/date=2026-06-12/part.parquet"
    assert partition_key("status_events", exchange="kraken", date="2026-06-12") == (
        "status_events/exchange=kraken/date=2026-06-12/part.parquet"
    )
    assert partition_key("scorecard", date="2026-06-12") == (
        "scorecard/date=2026-06-12/part.parquet"
    )


def test_write_then_read_round_trip(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    key = partition_key("book_quality", exchange="binance", symbol="BTC-USDT", date="2026-06-12")
    write_object(fs, bucket, key, table)

    got = read_dataset(fs, bucket, "book_quality", "2026-06-12")
    assert got is not None
    assert got.sort_by("a").to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}


def test_read_missing_dataset_is_none(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    assert read_dataset(fs, tmp_path.as_posix(), "book_quality", "2026-06-12") is None


def test_read_filters_by_date(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    t1 = pa.table({"a": [1]})
    t2 = pa.table({"a": [2]})
    write_object(fs, bucket, partition_key("ds", exchange="e", date="2026-06-12"), t1)
    write_object(fs, bucket, partition_key("ds", exchange="e", date="2026-06-13"), t2)
    got = read_dataset(fs, bucket, "ds", "2026-06-13")
    assert got is not None and got.to_pydict() == {"a": [2]}
