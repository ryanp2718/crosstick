"""Shared lake I/O for the downstream medallion layers (silver, gold).

The materializer owns the *bronze* contract (object layout + Parquet schema);
this module is the read/write plumbing the layers above bronze share:

  - an S3/MinIO filesystem and the instruments path from env (lifted here so
    silver and gold don't each re-derive the materializer's wiring),
  - Hive-partitioned object listing + reading for one dataset+date,
  - overwrite-keyed writes — a deterministic key per partition+date, so a
    recompute rewrites the identical object (the same idempotency discipline as
    bronze's start-offset keys; see materializer/bronze.object_key).
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs


def filesystem_from_env() -> pafs.S3FileSystem:
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    scheme = "https" if endpoint.startswith("https://") else "http"
    return pafs.S3FileSystem(
        access_key=os.environ.get("S3_ACCESS_KEY", "minio"),
        secret_key=os.environ.get("S3_SECRET_KEY", "minio12345"),
        endpoint_override=endpoint,
        scheme=scheme,
        # MinIO ignores regions but pyarrow requires one to skip discovery.
        region=os.environ.get("S3_REGION", "us-east-1"),
    )


def instruments_path_from_env() -> Path:
    raw = os.environ.get("INSTRUMENTS_FILE")
    if raw:
        return Path(raw)
    # Source-tree default (python/ is the cwd for local runs); compose sets the env.
    return Path(__file__).resolve().parents[2] / "ops" / "instruments.yml"


def partition_key(
    dataset: str,
    *,
    date: str,
    exchange: str | None = None,
    symbol: str | None = None,
    filename: str = "part.parquet",
) -> str:
    """Hive-partitioned object key under a dataset (mirrors the bronze layout).

    A single deterministic file per (dataset, partition values, date): a layer
    aggregates a whole date's bronze into one object per partition, and a
    recompute overwrites it in place.
    """
    parts = [dataset]
    if exchange is not None:
        parts.append(f"exchange={exchange}")
    if symbol is not None:
        parts.append(f"symbol={symbol}")
    parts.append(f"date={date}")
    parts.append(filename)
    return "/".join(parts)


def _list_date_files(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str
) -> list[str]:
    """Sorted parquet object paths for one dataset+date (sorted == offset order:
    objects are named `{partition:03d}-{start_offset:012d}.parquet`, contiguous
    per partition). Paths are returned verbatim (a local FS on Windows may use
    backslashes) for opening; callers normalize when matching on path content.
    """
    selector = pafs.FileSelector(f"{bucket}/{dataset}", recursive=True, allow_not_found=True)
    return sorted(
        info.path
        for info in fs.get_file_info(selector)
        if info.type == pafs.FileType.File
        and info.path.endswith(".parquet")
        and f"/date={date}/" in info.path.replace("\\", "/")
    )


def _read_file(fs: pafs.FileSystem, path: str) -> pa.Table:
    # Read by handle, not pq.read_table(path, filesystem=...), which would infer
    # Hive partition columns from the path — those collide with the exchange/date
    # columns silver/gold already carry as real data.
    with fs.open_input_file(path) as handle:
        return pq.read_table(handle)


def read_dataset(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str
) -> pa.Table | None:
    """Read every Parquet object for one dataset+date into a single table.

    Lists `{bucket}/{dataset}` recursively and keeps the `date={date}` leaves;
    returns None when nothing has been written for that dataset+date yet.
    """
    paths = _list_date_files(fs, bucket, dataset, date)
    if not paths:
        return None
    return pa.concat_tables([_read_file(fs, path) for path in paths])


def list_partitions(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str
) -> list[dict[str, str]]:
    """Distinct partition-value dicts present for one dataset+date.

    Each dict splats into `partition_key(dataset, date=date, **part)`:
    book/quotes/latency -> {"exchange": ..., "symbol": ...}; status -> {"exchange": ...}.
    Derived by segment-splitting the normalized object paths (deterministic order
    by first sighting in sorted-file order).
    """
    seen: dict[tuple, dict[str, str]] = {}
    for path in _list_date_files(fs, bucket, dataset, date):
        part: dict[str, str] = {}
        for seg in path.replace("\\", "/").split("/"):
            if seg.startswith("exchange="):
                part["exchange"] = seg[len("exchange="):]
            elif seg.startswith("symbol="):
                part["symbol"] = seg[len("symbol="):]
        seen.setdefault(tuple(sorted(part.items())), part)
    return list(seen.values())


def iter_partition_tables(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str, part: dict[str, str]
) -> Iterator[pa.Table]:
    """Yield one table per parquet file of a single partition, in sorted (offset)
    order, reading one file at a time (the streaming-read seam — the whole
    partition is never resident). Prefix-matches with a trailing slash so
    `symbol=BTC-USD` does not match `symbol=BTC-USD2`.
    """
    prefix = f"{bucket}/" + partition_key(dataset, date=date, filename="", **part)
    for path in _list_date_files(fs, bucket, dataset, date):
        if path.replace("\\", "/").startswith(prefix):
            yield _read_file(fs, path)


def iter_dataset_tables(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str
) -> Iterator[pa.Table]:
    """Yield one table per parquet file across all partitions of a dataset+date,
    file by file (date-granular streaming read; nothing held across files)."""
    for path in _list_date_files(fs, bucket, dataset, date):
        yield _read_file(fs, path)


def write_object(fs: pafs.FileSystem, bucket: str, key: str, table: pa.Table) -> str:
    """Write `table` to `{bucket}/{key}` (zstd), overwriting any existing object."""
    path = f"{bucket}/{key}"
    # S3 has no real directories (open_output_stream creates the prefix); a local
    # filesystem needs the parent created first. Match the materializer, which
    # writes straight to S3 without create_dir.
    if isinstance(fs, pafs.LocalFileSystem):
        fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
    pq.write_table(table, path, filesystem=fs, compression="zstd")
    return path


class PartitionWriter:
    """Streaming writer for one overwrite-keyed partition object.

    Rows are written in batches to a single `{bucket}/{key}` parquet via
    `pq.ParquetWriter` (one row group per batch), so a partition's output never
    has to be fully resident. The output stream is opened lazily on the first
    non-empty batch — a partition with no rows writes nothing (matching the
    previous group-then-write behavior). Same zstd + overwrite semantics as
    `write_object`, so the idempotency contract holds.
    """

    def __init__(self, fs: pafs.FileSystem, bucket: str, key: str, schema: pa.Schema):
        self._fs = fs
        self._path = f"{bucket}/{key}"
        self._schema = schema
        self._stream: pafs.NativeFile | None = None
        self._writer: pq.ParquetWriter | None = None

    def write_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        self._ensure_open()
        assert self._writer is not None
        self._writer.write_table(pa.Table.from_pylist(rows, schema=self._schema))

    def _ensure_open(self) -> None:
        if self._writer is not None:
            return
        if isinstance(self._fs, pafs.LocalFileSystem):
            self._fs.create_dir(self._path.rsplit("/", 1)[0], recursive=True)
        self._stream = self._fs.open_output_stream(self._path)
        self._writer = pq.ParquetWriter(self._stream, self._schema, compression="zstd")

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            assert self._stream is not None
            self._stream.close()
            self._writer = None
            self._stream = None

    def __enter__(self) -> PartitionWriter:
        return self

    def __exit__(self, *exc: object) -> bool:
        self.close()
        return False
