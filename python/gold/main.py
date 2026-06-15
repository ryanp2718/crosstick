"""Gold scorecard entrypoint: ``python -m gold.main <date> [<date> ...]``.

For each UTC date, read the silver DQ facts, roll them up into the scorecard
mart, and write ``gold/scorecard/date=…/`` (one overwrite-keyed object per date).
``--fail-on-violation`` exits non-zero if any check has violations (ops/CI use).

Env: ``S3_ENDPOINT`` / keys; ``SILVER_BUCKET`` (default ``silver``) and
``GOLD_BUCKET`` (default ``gold``).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from pyarrow import fs as pafs

from common.lake import filesystem_from_env, partition_key, read_dataset, write_object
from gold.scorecard import build_scorecard, scorecard_table

log = logging.getLogger(__name__)


def _read_rows(fs: pafs.FileSystem, bucket: str, dataset: str, date: str) -> list[dict]:
    table = read_dataset(fs, bucket, dataset, date)
    return table.to_pylist() if table is not None else []


def build_for_date(fs: pafs.FileSystem, silver_bucket: str, date: str) -> list[dict]:
    return build_scorecard(
        _read_rows(fs, silver_bucket, "book_quality", date),
        _read_rows(fs, silver_bucket, "latency", date),
        _read_rows(fs, silver_bucket, "status_events", date),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build the gold DQ scorecard from silver.")
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
    total_violations = 0
    for date in args.dates:
        rows = build_for_date(fs, silver_bucket, date)
        if not rows:
            log.warning("no silver facts for %s; skipping", date)
            continue
        path = write_object(
            fs, gold_bucket, partition_key("scorecard", date=date), scorecard_table(rows)
        )
        violations = sum(r["n_violations"] for r in rows)
        total_violations += violations
        log.info("gold PUT %s (%d checks, %d violations)", path, len(rows), violations)
        for r in sorted(rows, key=lambda r: (-r["n_violations"], r["check"])):
            if r["n_violations"]:
                log.warning(
                    "  %s %s/%s: %d violations %s",
                    r["check"], r["exchange"], r["canonical_symbol"] or "-",
                    r["n_violations"], r["detail"] or "",
                )
    if args.fail_on_violation and total_violations:
        log.error("scorecard found %d violations", total_violations)
        sys.exit(1)


if __name__ == "__main__":
    main()
