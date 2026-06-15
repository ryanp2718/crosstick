"""Silver DQ entrypoint: ``python -m silver.main <date> [<date> ...]``.

For each UTC date, read the bronze datasets the DQ transforms need, compute the
three fact streams, and write them to the silver bucket — one overwrite-keyed
object per partition, so a recompute is idempotent.

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
    filesystem_from_env,
    instruments_path_from_env,
    partition_key,
    read_dataset,
    write_object,
)
from materializer.bronze import CanonicalMap, table_to_records
from silver.dq import (
    BOOK_QUALITY_SCHEMA,
    LATENCY_SCHEMA,
    STATUS_SCHEMA,
    SilverFacts,
    build_silver,
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


def read_bronze_records(
    fs: pafs.FileSystem, bucket: str, date: str
) -> list[CorpusRecord]:
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
        records = read_bronze_records(fs, lake_bucket, date)
        if not records:
            log.warning("no bronze records for %s; skipping", date)
            continue
        facts = build_silver(records, canonical)
        write_silver(fs, silver_bucket, facts)
        log.info(
            "silver %s: %d book_quality, %d latency, %d status_events",
            date, len(facts.book_quality), len(facts.latency), len(facts.status_events),
        )


if __name__ == "__main__":
    main()
