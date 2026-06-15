"""Silver DQ entrypoint: ``python -m silver.main <date> [<date> ...]``.

For each UTC date, read the bronze datasets the DQ transforms need, compute the
silver fact streams (book_quality, latency, status_events, quotes, nbbo), and
write them to the silver bucket — one overwrite-keyed object per partition, so a
recompute is idempotent.

Env mirrors the materializer: ``S3_ENDPOINT`` / ``S3_ACCESS_KEY`` /
``S3_SECRET_KEY`` / ``INSTRUMENTS_FILE``; plus ``LAKE_BUCKET`` (bronze source,
default ``lake``) and ``SILVER_BUCKET`` (default ``silver``).
"""
from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from collections.abc import Callable

import pyarrow as pa
from pyarrow import fs as pafs

from analytics.corpus import CorpusRecord
from common.lake import (
    PartitionWriter,
    filesystem_from_env,
    instruments_path_from_env,
    iter_partition_tables,
    list_partitions,
    partition_key,
    read_dataset,
    write_object,
)
from materializer.bronze import CanonicalMap, table_to_records
from silver.dq import (
    BOOK_QUALITY_SCHEMA,
    LATENCY_SCHEMA,
    NBBO_SCHEMA,
    QUOTES_SCHEMA,
    STATUS_SCHEMA,
    SilverFacts,
    _build_nbbo,
    _status_transitions,
    book_partition_rows,
    latency_rows,
    to_book_recs,
)

log = logging.getLogger(__name__)

# Bronze datasets read by the DQ transforms (bbo/nbbo are derived — skipped).
SOURCE_DATASETS = (
    "book_snapshots",
    "book_deltas",
    "trades",
    "liquidations",
    "mark_price",
    "open_interest",
    "status",
)
BOOK_DATASETS = ("book_snapshots", "book_deltas")
# Non-book firehose datasets that carry latency headers (book latency is emitted
# during the fold, since it already holds the decoded records).
NONBOOK_LATENCY_DATASETS = ("trades", "liquidations", "mark_price", "open_interest")
# Rows per ParquetWriter row group — bounds the streaming driver's write buffers.
BATCH_ROWS = 50_000


def read_bronze_records(
    fs: pafs.FileSystem, bucket: str, date: str
) -> list[CorpusRecord]:
    """Read a whole day of bronze into memory (the in-memory reference path used
    by build_silver in tests). The batch entrypoint uses build_silver_streaming,
    which never holds a whole day; this stays for small inputs and equivalence."""
    records: list[CorpusRecord] = []
    for dataset in SOURCE_DATASETS:
        table = read_dataset(fs, bucket, dataset, date)
        if table is not None:
            records.extend(table_to_records(table))
    return records


def _write_grouped(
    fs: pafs.FileSystem,
    bucket: str,
    dataset: str,
    rows: list[dict],
    schema: pa.Schema,
    partition_of: Callable[[dict], dict],
) -> None:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    parts: dict[tuple, dict] = {}
    for row in rows:
        part = partition_of(row)
        key = tuple(sorted(part.items()))
        groups[key].append(row)
        parts[key] = part
    for key, group in groups.items():
        table = pa.Table.from_pylist(group, schema=schema)
        path = write_object(fs, bucket, partition_key(dataset, **parts[key]), table)
        log.info("silver PUT %s (%d rows)", path, len(group))


def write_silver(fs: pafs.FileSystem, bucket: str, facts: SilverFacts) -> None:
    _write_grouped(
        fs, bucket, "book_quality", facts.book_quality, BOOK_QUALITY_SCHEMA,
        lambda r: {"exchange": r["exchange"], "symbol": r["canonical_symbol"], "date": r["date"]},
    )
    _write_grouped(
        fs, bucket, "latency", facts.latency, LATENCY_SCHEMA,
        lambda r: {"exchange": r["exchange"], "symbol": r["canonical_symbol"], "date": r["date"]},
    )
    _write_grouped(
        fs, bucket, "status_events", facts.status_events, STATUS_SCHEMA,
        lambda r: {"exchange": r["exchange"], "date": r["date"]},
    )
    _write_grouped(
        fs, bucket, "quotes", facts.quotes, QUOTES_SCHEMA,
        lambda r: {"exchange": r["exchange"], "symbol": r["canonical_symbol"], "date": r["date"]},
    )
    # nbbo is cross-venue: partitioned by canonical symbol only (no exchange).
    _write_grouped(
        fs, bucket, "nbbo", facts.nbbo, NBBO_SCHEMA,
        lambda r: {"symbol": r["canonical_symbol"], "date": r["date"]},
    )


class _Batch:
    """Buffers rows for a PartitionWriter, flushing a row group every BATCH_ROWS."""

    def __init__(self, writer: PartitionWriter):
        self._w = writer
        self._buf: list[dict] = []

    def add(self, row: dict) -> None:
        self._buf.append(row)
        if len(self._buf) >= BATCH_ROWS:
            self._w.write_rows(self._buf)
            self._buf = []

    def flush(self) -> None:
        self._w.write_rows(self._buf)
        self._buf = []


def _iter_records(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str, part: dict
):
    """Stream a partition's records file-by-file (the whole partition is never
    resident — one file's records at a time)."""
    for table in iter_partition_tables(fs, bucket, dataset, date, part):
        yield from table_to_records(table)


def _process_partition(
    fs: pafs.FileSystem, lake_bucket: str, silver_bucket: str, date: str,
    canonical: CanonicalMap, exchange: str, symbol: str, present: set[str],
    counts: dict[str, int],
) -> None:
    """Fold one bronze (exchange, symbol) partition into book_quality + quotes +
    latency, streamed to one object each. Peak memory is one file + one OrderBook
    + a row batch. Bronze is already partitioned by canonical symbol (object_key
    resolves), so the partition symbol is the output canonical; the rows resolve
    the same value, so output partitions match the in-memory build_silver."""
    canon = symbol
    bron = {"exchange": exchange, "symbol": symbol}
    bq_key = partition_key("book_quality", exchange=exchange, symbol=canon, date=date)
    qt_key = partition_key("quotes", exchange=exchange, symbol=canon, date=date)
    lat_key = partition_key("latency", exchange=exchange, symbol=canon, date=date)
    bq = PartitionWriter(fs, silver_bucket, bq_key, BOOK_QUALITY_SCHEMA)
    qt = PartitionWriter(fs, silver_bucket, qt_key, QUOTES_SCHEMA)
    lat = PartitionWriter(fs, silver_bucket, lat_key, LATENCY_SCHEMA)
    bqb, qtb, latb = _Batch(bq), _Batch(qt), _Batch(lat)
    try:
        if present & set(BOOK_DATASETS):
            snap_recs = _iter_records(fs, lake_bucket, "book_snapshots", date, bron)
            delta_recs = _iter_records(fs, lake_bucket, "book_deltas", date, bron)
            snaps = to_book_recs(snap_recs, canonical)
            deltas = to_book_recs(delta_recs, canonical)
            for bq_row, q_row, lat_row in book_partition_rows(snaps, deltas, exchange):
                bqb.add(bq_row)
                counts["book_quality"] += 1
                if q_row is not None:
                    qtb.add(q_row)
                    counts["quotes"] += 1
                if lat_row is not None:
                    latb.add(lat_row)
                    counts["latency"] += 1
        for dataset in NONBOOK_LATENCY_DATASETS:
            if dataset in present:
                recs = _iter_records(fs, lake_bucket, dataset, date, bron)
                for lat_row in latency_rows(recs, canonical):
                    latb.add(lat_row)
                    counts["latency"] += 1
        bqb.flush()
        qtb.flush()
        latb.flush()
    finally:
        bq.close()
        qt.close()
        lat.close()


def _write_rows(
    fs: pafs.FileSystem, bucket: str, key: str, schema: pa.Schema, rows: list[dict]
) -> None:
    """Write an already-collected (small) row list to one object in batches."""
    if not rows:
        return
    with PartitionWriter(fs, bucket, key, schema) as w:
        for i in range(0, len(rows), BATCH_ROWS):
            w.write_rows(rows[i:i + BATCH_ROWS])


def build_silver_streaming(
    fs: pafs.FileSystem, lake_bucket: str, silver_bucket: str, date: str, canonical: CanonicalMap
) -> dict[str, int]:
    """Memory-bounded silver build: one bronze (exchange, symbol) partition at a
    time (book fold + latency), then NBBO per canonical from the persisted quotes.
    Output matches build_silver on clean data; peak memory is a single partition's
    file + OrderBook + a row batch, not the whole day."""
    counts: dict[str, int] = defaultdict(int)

    # Phase 1: per bronze (exchange, native-symbol) partition -> book_quality,
    # quotes, latency. All of an instrument's datasets share one native symbol.
    datasets_by_part: dict[tuple[str, str], set[str]] = defaultdict(set)
    for dataset in (*BOOK_DATASETS, *NONBOOK_LATENCY_DATASETS):
        for part in list_partitions(fs, lake_bucket, dataset, date):
            datasets_by_part[(part["exchange"], part["symbol"])].add(dataset)
    for (exchange, symbol), present in datasets_by_part.items():
        _process_partition(fs, lake_bucket, silver_bucket, date, canonical,
                           exchange, symbol, present, counts)

    # Phase 1c: status per exchange (small), retained for NBBO eviction.
    status_events: list[dict] = []
    for part in list_partitions(fs, lake_bucket, "status", date):
        recs = list(_iter_records(fs, lake_bucket, "status", date, part))
        rows = _status_transitions(part["exchange"], recs)
        key = partition_key("status_events", exchange=part["exchange"], date=date)
        _write_rows(fs, silver_bucket, key, STATUS_SCHEMA, rows)
        status_events.extend(rows)
        counts["status_events"] += len(rows)

    # Phase 2: NBBO per canonical from the persisted quotes (bounded per canonical,
    # the only cross-venue step — it reads back the small top-of-book quotes).
    quotes_by_canon: dict[str, list[dict]] = defaultdict(list)
    for part in list_partitions(fs, silver_bucket, "quotes", date):
        quotes_by_canon[part["symbol"]].append(part)
    for canon, parts in quotes_by_canon.items():
        quotes: list[dict] = []
        for part in parts:
            for table in iter_partition_tables(fs, silver_bucket, "quotes", date, part):
                quotes.extend(table.to_pylist())
        rows = _build_nbbo(quotes, status_events)
        key = partition_key("nbbo", symbol=canon, date=date)
        _write_rows(fs, silver_bucket, key, NBBO_SCHEMA, rows)
        counts["nbbo"] += len(rows)

    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build silver DQ facts from bronze.")
    p.add_argument("dates", nargs="+", help="UTC dates to process, e.g. 2026-06-12")
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    args = parse_args()
    fs = filesystem_from_env()
    lake_bucket = os.environ.get("LAKE_BUCKET", "lake")
    silver_bucket = os.environ.get("SILVER_BUCKET", "silver")
    canonical = CanonicalMap.from_yaml(instruments_path_from_env())
    for date in args.dates:
        counts = build_silver_streaming(fs, lake_bucket, silver_bucket, date, canonical)
        if not counts:
            log.warning("no bronze partitions for %s", date)
            continue
        log.info(
            "silver %s: %d book_quality, %d latency, %d status_events, %d quotes, %d nbbo",
            date, counts["book_quality"], counts["latency"], counts["status_events"],
            counts["quotes"], counts["nbbo"],
        )


if __name__ == "__main__":
    main()
