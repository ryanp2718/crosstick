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


def read_dataset(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str
) -> pa.Table | None:
    """Read every Parquet object for one dataset+date into a single table.

    Lists `{bucket}/{dataset}` recursively and keeps the `date={date}` leaves;
    returns None when nothing has been written for that dataset+date yet.
    """
    base = f"{bucket}/{dataset}"
    selector = pafs.FileSelector(base, recursive=True, allow_not_found=True)
    paths = sorted(
        info.path
        for info in fs.get_file_info(selector)
        if info.type == pafs.FileType.File
        and info.path.endswith(".parquet")
        # normalize separators: a local filesystem on Windows may return backslashes
        and f"/date={date}/" in info.path.replace("\\", "/")
    )
    if not paths:
        return None
    # Read each file by handle, not pq.read_table(path, filesystem=...), which
    # would infer Hive partition columns from the path — those collide with the
    # exchange/date columns silver/gold already carry as real data.
    tables = []
    for path in paths:
        with fs.open_input_file(path) as handle:
            tables.append(pq.read_table(handle))
    return pa.concat_tables(tables)


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
