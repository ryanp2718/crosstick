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

from pyarrow import fs as pafs

from common.lake import (
    filesystem_from_env,
    instruments_path_from_env,
    partition_key,
    read_dataset,
    write_object,
)
from gold.basis import basis_summary_table, basis_table, build_basis, build_basis_summary
from gold.scorecard import build_scorecard, scorecard_table
from materializer.bronze import CanonicalMap

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


def build_basis_for_date(
    fs: pafs.FileSystem, silver_bucket: str, date: str, canonical: CanonicalMap
) -> tuple[list[dict], list[dict]]:
    series = build_basis(_read_rows(fs, silver_bucket, "nbbo", date), canonical.pairs_by_base())
    return series, build_basis_summary(series)


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

        basis, summary = build_basis_for_date(fs, silver_bucket, date, canonical)
        if basis:
            write_object(fs, gold_bucket, partition_key("basis", date=date), basis_table(basis))
            path = write_object(
                fs, gold_bucket, partition_key("basis_summary", date=date),
                basis_summary_table(summary),
            )
            log.info("gold PUT %s (%d basis obs, %d base(s))", path, len(basis), len(summary))

        if not scorecard and not basis:
            log.warning("no silver facts for %s; skipping", date)
    if args.fail_on_violation and total_violations:
        log.error("scorecard found %d violations", total_violations)
        sys.exit(1)


if __name__ == "__main__":
    main()
