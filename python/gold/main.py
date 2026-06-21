"""Gold marts entrypoint: ``python -m gold.main <date> [<date> ...]``.

For each UTC date, read silver and build every gold mart whose inputs exist:
  - scorecard      : the data-quality rollup over book_quality/latency/status.
  - basis/_summary : the stablecoin (USDT/USD) basis from per-canonical nbbo.
Each is written one overwrite-keyed object per date. ``--fail-on-violation``
exits non-zero if any scorecard check has violations (ops/CI use).

Env: ``S3_ENDPOINT`` / keys; ``INSTRUMENTS_FILE``; ``SILVER_BUCKET`` (default
``silver``) and ``GOLD_BUCKET`` (default ``gold``).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Iterator

from pyarrow import fs as pafs

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
from gold.basis import BASIS_SCHEMA, basis_summary_table, iter_basis, summary_row
from gold.scorecard import (
    BookCheckAccumulator,
    LatencyAccumulator,
    _status_checks,
    scorecard_table,
)
from materializer.bronze import CanonicalMap

log = logging.getLogger(__name__)

# Rows per ParquetWriter row group for the streamed basis series (mirrors silver).
BATCH_ROWS = 50_000


def _read_rows(fs: pafs.FileSystem, bucket: str, dataset: str, date: str) -> list[dict]:
    table = read_dataset(fs, bucket, dataset, date)
    return table.to_pylist() if table is not None else []


def build_for_date(fs: pafs.FileSystem, silver_bucket: str, date: str) -> list[dict]:
    """Scorecard for one date, folding silver one partition at a time. Every group
    key nests in a partition, so book_quality/latency never load a whole day (the
    old to_pylist OOM); status_events is tiny and read whole. Output matches the
    in-memory build_scorecard oracle (gold/tests/test_streaming)."""
    rows: list[dict] = []
    for part in list_partitions(fs, silver_bucket, "book_quality", date):
        acc = BookCheckAccumulator(part["exchange"], part["symbol"], date)
        for table in iter_partition_tables(fs, silver_bucket, "book_quality", date, part):
            acc.update(table)
        rows += acc.rows()
    for part in list_partitions(fs, silver_bucket, "latency", date):
        acc = LatencyAccumulator(part["exchange"], part["symbol"], date)
        for table in iter_partition_tables(fs, silver_bucket, "latency", date, part):
            acc.update(table)
        rows += acc.rows()
    rows += _status_checks(_read_rows(fs, silver_bucket, "status_events", date))
    return rows


def _nbbo_stream(
    fs: pafs.FileSystem, silver_bucket: str, date: str, canonical_symbol: str
) -> Iterator[tuple[int, tuple]]:
    """Stream one canonical's NBBO partition as sorted `(ts_ns, (ts_ns, best_bid,
    best_ask))`. NBBO is written ts-ascending by silver (it is merge_latest output),
    so the file is already ordered — the leg feeds straight into the k-way merge. The
    value embeds its own ts so iter_basis can evict a stale (frozen) leg."""
    part = {"symbol": canonical_symbol}
    for table in iter_partition_tables(fs, silver_bucket, "nbbo", date, part):
        ts = table.column("ts_ns").to_pylist()
        bids = table.column("best_bid").to_pylist()
        asks = table.column("best_ask").to_pylist()
        for t, bid, ask in zip(ts, bids, asks, strict=True):
            yield t, (t, bid, ask)


def write_basis_for_date(
    fs: pafs.FileSystem, silver_bucket: str, gold_bucket: str, date: str, canonical: CanonicalMap
) -> int:
    """Stream the basis mart for one date: per base, k-way merge the two legs' sorted
    NBBO partitions (O(frontier) memory) into the event-grain series, written
    incrementally, accumulating the daily summary — no whole-day nbbo read. Returns
    the number of basis observations written."""
    have = {p["symbol"] for p in list_partitions(fs, silver_bucket, "nbbo", date)}
    summaries: list[dict] = []
    n = 0
    writer = PartitionWriter(fs, gold_bucket, partition_key("basis", date=date), BASIS_SCHEMA)
    try:
        for base, usd_c, usdt_c in canonical.pairs_by_base():
            if usd_c not in have or usdt_c not in have:
                continue
            bps: list[float] = []
            ts: list[int] = []
            batch: list[dict] = []
            for row in iter_basis(
                base,
                _nbbo_stream(fs, silver_bucket, date, usd_c),
                _nbbo_stream(fs, silver_bucket, date, usdt_c),
            ):
                batch.append(row)
                bps.append(row["basis_bps"])
                ts.append(row["ts_ns"])
                n += 1
                if len(batch) >= BATCH_ROWS:
                    writer.write_rows(batch)
                    batch = []
            writer.write_rows(batch)
            if bps:
                summaries.append(summary_row(base, date, bps, ts))
    finally:
        writer.close()
    if summaries:
        path = write_object(
            fs, gold_bucket, partition_key("basis_summary", date=date),
            basis_summary_table(summaries),
        )
        log.info("gold PUT %s (%d basis obs, %d base(s))", path, n, len(summaries))
    return n


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build the gold marts from silver.")
    p.add_argument("dates", nargs="+", help="UTC dates to process, e.g. 2026-06-12")
    p.add_argument(
        "--fail-on-violation",
        action="store_true",
        help="exit non-zero if any check reports violations",
    )
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    args = parse_args()
    fs = filesystem_from_env()
    silver_bucket = os.environ.get("SILVER_BUCKET", "silver")
    gold_bucket = os.environ.get("GOLD_BUCKET", "gold")
    canonical = CanonicalMap.from_yaml(instruments_path_from_env())
    total_violations = 0
    for date in args.dates:
        scorecard = build_for_date(fs, silver_bucket, date)
        if scorecard:
            path = write_object(
                fs, gold_bucket, partition_key("scorecard", date=date),
                scorecard_table(scorecard),
            )
            violations = sum(r["n_violations"] for r in scorecard)
            total_violations += violations
            log.info("gold PUT %s (%d checks, %d violations)", path, len(scorecard), violations)
            for r in sorted(scorecard, key=lambda r: (-r["n_violations"], r["check"])):
                if r["n_violations"]:
                    log.warning(
                        "  %s %s/%s: %d violations %s",
                        r["check"], r["exchange"], r["canonical_symbol"] or "-",
                        r["n_violations"], r["detail"] or "",
                    )

        n_basis = write_basis_for_date(fs, silver_bucket, gold_bucket, date, canonical)

        if not scorecard and not n_basis:
            log.warning("no silver facts for %s; skipping", date)
    if args.fail_on_violation and total_violations:
        log.error("scorecard found %d violations", total_violations)
        sys.exit(1)


if __name__ == "__main__":
    main()
