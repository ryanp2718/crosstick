"""Unit tests for the shared lake I/O helpers (LocalFileSystem, no infra)."""
from __future__ import annotations

import pyarrow as pa
import pytest
from pyarrow import fs as pafs

from common.lake import (
    PartitionWriter,
    _s3_env,
    bronze_filesystem_from_env,
    filesystem_from_env,
    iter_dataset_tables,
    iter_partition_tables,
    list_partitions,
    partition_key,
    read_dataset,
    read_freshness_markers,
    write_freshness_marker,
    write_freshness_markers,
    write_object,
)

_S3_VARS = ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_REGION", "S3_MAX_ATTEMPTS")


@pytest.fixture
def clean_s3_env(monkeypatch):
    """A monkeypatch with every base + LAKE_-prefixed S3 var cleared, so a test
    starts from the built-in defaults regardless of the shell's environment."""
    for name in _S3_VARS:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv("LAKE_" + name, raising=False)
    return monkeypatch


def test_s3_env_prefix_falls_back_to_base(clean_s3_env) -> None:
    clean_s3_env.setenv("S3_ENDPOINT", "http://minio:9000")
    # No LAKE_S3_ENDPOINT set: the bronze role resolves to the base value.
    assert _s3_env("S3_ENDPOINT", "LAKE_") == "http://minio:9000"
    assert _s3_env("S3_ENDPOINT", "") == "http://minio:9000"


def test_s3_env_prefix_overrides_base(clean_s3_env) -> None:
    clean_s3_env.setenv("S3_ENDPOINT", "https://acct.r2.cloudflarestorage.com")
    clean_s3_env.setenv("LAKE_S3_ENDPOINT", "http://minio:9000")
    assert _s3_env("S3_ENDPOINT", "LAKE_") == "http://minio:9000"  # bronze -> MinIO
    assert _s3_env("S3_ENDPOINT", "") == "https://acct.r2.cloudflarestorage.com"  # derived -> R2


def test_s3_env_default_when_unset(clean_s3_env) -> None:
    assert _s3_env("S3_ACCESS_KEY", "LAKE_") == "minio"


def test_bronze_fs_matches_primary_without_override(clean_s3_env) -> None:
    clean_s3_env.setenv("S3_ENDPOINT", "http://minio:9000")
    assert bronze_filesystem_from_env().equals(filesystem_from_env())


def test_bronze_fs_diverges_with_override(clean_s3_env) -> None:
    clean_s3_env.setenv("S3_ENDPOINT", "https://acct.r2.cloudflarestorage.com")
    clean_s3_env.setenv("S3_REGION", "auto")
    clean_s3_env.setenv("LAKE_S3_ENDPOINT", "http://minio:9000")
    assert not bronze_filesystem_from_env().equals(filesystem_from_env())


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
    """A book_deltas key - a fixed part.parquet, or an explicit offset filename."""
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


def test_freshness_marker_round_trip(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    write_freshness_marker(fs, bucket, "nbbo", date=D, row_count=42, written_at_epoch=1_700.0)

    markers = read_freshness_markers(fs, bucket)
    assert markers == {
        "nbbo": {"dataset": "nbbo", "date": D, "written_at_epoch": 1_700.0, "row_count": 42}
    }


def test_read_freshness_markers_absent_is_empty(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    assert read_freshness_markers(fs, tmp_path.as_posix()) == {}


def test_write_freshness_markers_skips_zero_and_shares_timestamp(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = tmp_path.as_posix()
    # status_events has no rows this run -> no marker; the rest share one write time.
    write_freshness_markers(
        fs, bucket, D, {"quotes": 10, "status_events": 0, "nbbo": 3}, written_at_epoch=900.0
    )
    markers = read_freshness_markers(fs, bucket)
    assert set(markers) == {"quotes", "nbbo"}
    assert markers["quotes"]["row_count"] == 10
    assert {m["written_at_epoch"] for m in markers.values()} == {900.0}
