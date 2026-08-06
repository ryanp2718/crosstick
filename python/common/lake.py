"""Shared lake I/O for the downstream medallion layers (silver, gold).

The materializer owns the *bronze* contract (object layout + Parquet schema);
this module is the read/write plumbing the layers above bronze share:

  - an S3/MinIO filesystem and the instruments path from env (lifted here so
    silver and gold don't each re-derive the materializer's wiring),
  - Hive-partitioned object listing + reading for one dataset+date,
  - overwrite-keyed writes - a deterministic key per partition+date, so a
    recompute rewrites the identical object (the same idempotency discipline as
    bronze's start-offset keys; see materializer/bronze.object_key).
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from common.schemas import FRESHNESS_SCHEMA, table_from_rows

# Base env names + defaults for an S3/MinIO connection. A role prefix (e.g. "LAKE_")
# selects an override group whose vars fall back to these base names when unset, so a
# single-endpoint deploy that sets no prefixed vars behaves identically to the base.
_S3_DEFAULTS = {
    "S3_ENDPOINT": "http://localhost:9000",
    "S3_ACCESS_KEY": "minio",
    "S3_SECRET_KEY": "minio12345",
    "S3_REGION": "us-east-1",  # MinIO ignores it; pyarrow needs one to skip discovery.
    "S3_MAX_ATTEMPTS": "10",
}


def _s3_env(name: str, prefix: str) -> str:
    """Read `{prefix}{name}`, falling back to the un-prefixed base, then the default."""
    default = _S3_DEFAULTS[name]
    if prefix:
        return os.environ.get(prefix + name, os.environ.get(name, default))
    return os.environ.get(name, default)


def _filesystem(prefix: str = "") -> pafs.S3FileSystem:
    endpoint = _s3_env("S3_ENDPOINT", prefix)
    scheme = "https" if endpoint.startswith("https://") else "http"
    return pafs.S3FileSystem(
        access_key=_s3_env("S3_ACCESS_KEY", prefix),
        secret_key=_s3_env("S3_SECRET_KEY", prefix),
        endpoint_override=endpoint,
        scheme=scheme,
        region=_s3_env("S3_REGION", prefix),
        # Batch builds make thousands of calls; ride through transient connection
        # blips (local WSL2/MinIO, and expected on cloud S3) instead of aborting.
        retry_strategy=pafs.AwsStandardS3RetryStrategy(
            max_attempts=int(_s3_env("S3_MAX_ATTEMPTS", prefix))
        ),
    )


def filesystem_from_env() -> pafs.S3FileSystem:
    """Filesystem for the primary endpoint (S3_* env). On cloud this is the derived
    (silver/gold) target; for the materializer it is bronze. Use
    bronze_filesystem_from_env for a process that must read bronze from a *different*
    endpoint than it writes (the split-endpoint case)."""
    return _filesystem()


def bronze_filesystem_from_env() -> pafs.S3FileSystem:
    """Filesystem for the bronze (raw lake) role. Reads LAKE_S3_* with fallback to the
    base S3_*, so a single-endpoint deploy (no LAKE_* set) is byte-for-byte the same as
    filesystem_from_env; the split activates only when LAKE_S3_ENDPOINT points somewhere
    distinct (e.g. bronze stays on MinIO while silver/gold move to R2)."""
    return _filesystem("LAKE_")


def instruments_path_from_env() -> Path:
    raw = os.environ.get("INSTRUMENTS_FILE")
    if raw:
        return Path(raw)
    # Source-tree default (python/ is the cwd for local runs); compose sets the env.
    return Path(__file__).resolve().parents[2] / "ops" / "instruments.yml"


def dq_budget_path_from_env() -> Path:
    raw = os.environ.get("DQ_BUDGET_FILE")
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "ops" / "dq_budgets.yml"


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


def _list_date_files(fs: pafs.FileSystem, bucket: str, dataset: str, date: str) -> list[str]:
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
    # Hive partition columns from the path - those collide with the exchange/date
    # columns silver/gold already carry as real data.
    with fs.open_input_file(path) as handle:
        return pq.read_table(handle)


def read_dataset(fs: pafs.FileSystem, bucket: str, dataset: str, date: str) -> pa.Table | None:
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
                part["exchange"] = seg[len("exchange=") :]
            elif seg.startswith("symbol="):
                part["symbol"] = seg[len("symbol=") :]
        seen.setdefault(tuple(sorted(part.items())), part)
    return list(seen.values())


def latest_date(fs: pafs.FileSystem, bucket: str, dataset: str) -> str | None:
    """The newest `date=` partition present for one dataset, or None if empty.

    Dates are ISO `YYYY-MM-DD` so lexical max == chronological max. Lists the
    dataset prefix once (ListObjects only, no bodies) - used by the lake-exporter
    to find which day's gold rollup to publish.
    """
    selector = pafs.FileSelector(f"{bucket}/{dataset}", recursive=True, allow_not_found=True)
    latest: str | None = None
    for info in fs.get_file_info(selector):
        if info.type != pafs.FileType.File or not info.path.endswith(".parquet"):
            continue
        for seg in info.path.replace("\\", "/").split("/"):
            if seg.startswith("date=") and (latest is None or seg[len("date=") :] > latest):
                latest = seg[len("date=") :]
    return latest


def iter_partition_tables(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str, part: dict[str, str]
) -> Iterator[pa.Table]:
    """Yield one table per parquet file of a single partition, in sorted (offset)
    order, reading one file at a time (the streaming-read seam - the whole
    partition is never resident). Prefix-matches with a trailing slash so
    `symbol=BTC-USD` does not match `symbol=BTC-USD2`.
    """
    prefix = f"{bucket}/" + partition_key(dataset, date=date, filename="", **part)
    for path in _list_date_files(fs, bucket, dataset, date):
        if path.replace("\\", "/").startswith(prefix):
            yield _read_file(fs, path)


def iter_partition_batches(
    fs: pafs.FileSystem,
    bucket: str,
    dataset: str,
    date: str,
    part: dict[str, str],
    batch_rows: int = 50_000,
) -> Iterator[pa.RecordBatch]:
    """Yield RecordBatches of a single partition, reading row groups lazily so the
    whole file is never resident (peak ~one batch). The bounded-memory read seam for
    the NBBO reorder; same trailing-slash prefix match as `iter_partition_tables`.
    """
    prefix = f"{bucket}/" + partition_key(dataset, date=date, filename="", **part)
    for path in _list_date_files(fs, bucket, dataset, date):
        if path.replace("\\", "/").startswith(prefix):
            with fs.open_input_file(path) as handle:
                yield from pq.ParquetFile(handle).iter_batches(batch_size=batch_rows)


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


# ── freshness markers ─────────────────────────────────────────────────────────
# A tiny overwrite-keyed object per dataset under `_freshness/`, stamped with when
# the dataset was last built. It lets the lake-exporter read a derived layer's
# freshness in O(1) (one GET per marker) instead of a full LIST walk whose Class A
# cost grows with venues x securities (the difference between free and metered on
# R2). A once-daily audit still walks the layer to cross-check the markers.
FRESHNESS_PREFIX = "_freshness"


def write_freshness_marker(
    fs: pafs.FileSystem,
    bucket: str,
    dataset: str,
    *,
    date: str,
    row_count: int,
    written_at_epoch: float | None = None,
) -> str:
    """Write the freshness marker for one dataset at `_freshness/<dataset>.parquet`,
    overwriting in place. Records the date built, the wall-clock write time, and how
    many rows landed. Call this LAST, after the dataset's data objects are written,
    so a partial or aborted build can never read back as fresh."""
    written_at = time.time() if written_at_epoch is None else written_at_epoch
    table = pa.table(
        {
            "dataset": [dataset],
            "date": [date],
            "written_at_epoch": [float(written_at)],
            "row_count": [int(row_count)],
        },
        schema=FRESHNESS_SCHEMA,
    )
    return write_object(fs, bucket, f"{FRESHNESS_PREFIX}/{dataset}.parquet", table)


def write_freshness_markers(
    fs: pafs.FileSystem,
    bucket: str,
    date: str,
    counts: dict[str, int],
    written_at_epoch: float | None = None,
) -> None:
    """Write a marker for every dataset that produced rows, sharing one timestamp.
    A zero-row dataset writes no object, so it gets no marker (its freshness stays
    undefined, exactly as the LIST walk reported it)."""
    written_at = time.time() if written_at_epoch is None else written_at_epoch
    for dataset, n in counts.items():
        if n > 0:
            write_freshness_marker(
                fs, bucket, dataset, date=date, row_count=n, written_at_epoch=written_at
            )


def read_freshness_markers(fs: pafs.FileSystem, bucket: str) -> dict[str, dict]:
    """Every freshness marker in a bucket, keyed by dataset. One LIST of the small
    `_freshness/` prefix plus a GET per marker: cost tracks the dataset count, not
    the retained history the date-pruned walk descends."""
    selector = pafs.FileSelector(f"{bucket}/{FRESHNESS_PREFIX}", allow_not_found=True)
    out: dict[str, dict] = {}
    for info in fs.get_file_info(selector):
        if info.type == pafs.FileType.File and info.path.endswith(".parquet"):
            dataset = info.path.replace("\\", "/").rsplit("/", 1)[-1][: -len(".parquet")]
            out[dataset] = _read_file(fs, info.path).to_pylist()[0]
    return out


class PartitionWriter:
    """Streaming writer for one overwrite-keyed partition object.

    Rows are written in batches to a single `{bucket}/{key}` parquet via
    `pq.ParquetWriter` (one row group per batch), so a partition's output never
    has to be fully resident. The output stream is opened lazily on the first
    non-empty batch - a partition with no rows writes nothing (matching the
    previous group-then-write behavior). Same zstd + overwrite semantics as
    `write_object`, so the idempotency contract holds. It is all-or-nothing: leaving the
    `with` via an exception discards the object rather than publishing however many rows
    happened to be flushed. `with` is mandatory, because a caller that closes in a
    `finally` instead gets the old truncating behavior back and reads identically.
    """

    def __init__(self, fs: pafs.FileSystem, bucket: str, key: str, schema: pa.Schema):
        self._fs = fs
        self._path = f"{bucket}/{key}"
        self._schema = schema
        self._stream: pafs.NativeFile | None = None
        self._writer: pq.ParquetWriter | None = None
        self._entered = False

    def write_rows(self, rows: list[dict]) -> None:
        if not self._entered:
            raise RuntimeError(f"PartitionWriter({self._path}) must be used as a context manager")
        if not rows:
            return
        self._ensure_open()
        assert self._writer is not None
        self._writer.write_table(table_from_rows(rows, self._schema))

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

    def discard(self) -> None:
        """Close the stream and delete the object, leaving no partition behind.

        A half-written parquet still closes into a perfectly readable file, so a build
        that dies mid-partition would otherwise leave a short object that nothing
        downstream can distinguish from a complete one.
        """
        self.close()
        try:
            self._fs.delete_file(self._path)
        except FileNotFoundError:  # nothing was ever opened
            pass

    def __enter__(self) -> PartitionWriter:
        self._entered = True
        return self

    def __exit__(self, exc_type: type[BaseException] | None, *_exc: object) -> bool:
        if exc_type is None:
            self.close()
        else:
            self.discard()
        return False
