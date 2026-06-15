"""Unit tests for the shared lake I/O helpers (LocalFileSystem, no infra)."""
from __future__ import annotations

import pyarrow as pa
from pyarrow import fs as pafs

from common.lake import (
    PartitionWriter,
    iter_dataset_tables,
    iter_partition_tables,
    list_partitions,
    partition_key,
    read_dataset,
    write_object,
)


def _file_key(dataset: str, *, exchange: str, symbol: str, date: str, name: str) -> str:
    """A bronze-style object key with an explicit offset filename (so a partition
    can hold multiple files, unlike partition_key's fixed part.parquet)."""
    return f"{dataset}/exchange={exchange}/symbol={symbol}/date={date}/{name}"


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


D = "2026-06-12"


def _put_o(fs, bucket: str, key: str, o: int) -> None:
    write_object(fs, bucket, key, pa.table({"o": [o]}))


def _bd(ex: str, sym: str, name: str | None = None) -> str:
    """A book_deltas key — a fixed part.parquet, or an explicit offset filename."""
    if name is None:
        return partition_key("book_deltas", exchange=ex, symbol=sym, date=D)
    return _file_key("book_deltas", exchange=ex, symbol=sym, date=D, name=name)


def test_list_partitions_distinct(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    for ex, sym in [("coinbase", "BTC-USD"), ("kraken", "BTC-USD"), ("binance", "BTC-USDT")]:
        _put_o(fs, bucket, _bd(ex, sym), 1)
    _put_o(fs, bucket, _bd("coinbase", "BTC-USD", "f1.parquet"), 1)  # 2nd file, same partition

    parts = list_partitions(fs, bucket, "book_deltas", D)
    assert sorted((p["exchange"], p["symbol"]) for p in parts) == [
        ("binance", "BTC-USDT"), ("coinbase", "BTC-USD"), ("kraken", "BTC-USD"),
    ]
    # status-style partition (exchange only, no symbol)
    _put_o(fs, bucket, partition_key("status", exchange="kraken", date=D), 1)
    assert list_partitions(fs, bucket, "status", D) == [{"exchange": "kraken"}]


def test_iter_partition_tables_order_and_isolation(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    _put_o(fs, bucket, _bd("coinbase", "BTC-USD", "f0.parquet"), 0)  # sorted name == offset order
    _put_o(fs, bucket, _bd("coinbase", "BTC-USD", "f1.parquet"), 100)
    _put_o(fs, bucket, _bd("coinbase", "BTC-USD2", "f0.parquet"), 9)  # prefix collision

    part = {"exchange": "coinbase", "symbol": "BTC-USD"}
    tables = iter_partition_tables(fs, bucket, "book_deltas", D, part)
    assert [t.to_pydict()["o"][0] for t in tables] == [0, 100]  # ordered, no BTC-USD2 leak


def test_iter_dataset_tables_spans_partitions(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    _put_o(fs, bucket, _bd("coinbase", "BTC-USD"), 1)
    _put_o(fs, bucket, _bd("kraken", "BTC-USD"), 2)
    tabs = iter_dataset_tables(fs, bucket, "book_deltas", D)
    assert sorted(t.to_pydict()["o"][0] for t in tabs) == [1, 2]


def test_partition_writer_round_trip(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    key = partition_key("book_quality", exchange="binance", symbol="BTC-USDT", date="2026-06-12")
    with PartitionWriter(fs, bucket, key, schema) as w:
        w.write_rows([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])  # batch 1 -> row group 1
        w.write_rows([{"a": 3, "b": "z"}])                       # batch 2 -> row group 2
    got = read_dataset(fs, bucket, "book_quality", "2026-06-12")
    assert got is not None
    assert got.sort_by("a").to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}


def test_partition_writer_no_rows_writes_nothing(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    schema = pa.schema([("a", pa.int64())])
    key = partition_key("book_quality", exchange="binance", symbol="BTC-USDT", date="2026-06-12")
    with PartitionWriter(fs, bucket, key, schema) as w:
        w.write_rows([])  # nothing
    assert read_dataset(fs, bucket, "book_quality", "2026-06-12") is None
