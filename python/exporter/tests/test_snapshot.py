"""Unit tests for the lake-exporter metric mapping + I/O orchestration.

The pure mappers are tested with plain dicts; `build_families` is exercised over a
LocalFileSystem (real read/write path, no Docker) — the S3 path is in
test_exporter_integration.py.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
from pyarrow import fs as pafs

from common.lake import partition_key, write_object
from exporter.snapshot import (
    basis_families,
    build_families,
    freshness_families,
    scorecard_families,
)
from gold.basis import BASIS_SUMMARY_SCHEMA
from gold.scorecard import scorecard_table

DATE = "2026-06-19"

SCORECARD_ROWS = [
    {"exchange": "kraken", "canonical_symbol": "BTC-USD", "date": DATE, "check": "sequence_gap",
     "n_records": 100, "n_violations": 2, "p50_ms": None, "p95_ms": None, "p99_ms": None,
     "detail": '{"max_gap": 3, "total_missing": 2}'},
    {"exchange": "kraken", "canonical_symbol": "BTC-USD", "date": DATE, "check": "clock_monotonic",
     "n_records": 100, "n_violations": 5, "p50_ms": None, "p95_ms": None, "p99_ms": None,
     "detail": '{"inter_epoch_steps": 1, "worst_lateness_ms": 1234.5}'},
    {"exchange": "kraken", "canonical_symbol": "BTC-USD", "date": DATE, "check": "latency",
     "n_records": 100, "n_violations": 0, "p50_ms": 1.0, "p95_ms": 5.0, "p99_ms": 12.0,
     "detail": None},
    {"exchange": "kraken", "canonical_symbol": None, "date": DATE, "check": "status",
     "n_records": 3, "n_violations": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None,
     "detail": '{"downs": 0}'},
]

BASIS_ROWS = [
    {"base": "BTC", "date": DATE, "n_obs": 1000, "basis_bps_mean": -4.1, "basis_bps_std": 1.2,
     "basis_bps_min": -9.0, "basis_bps_max": 1.0, "coverage_ns": 1},
]


def _index(families) -> dict[tuple, float]:
    """{(metric_name, sorted-label-tuple): value} across all samples."""
    out: dict[tuple, float] = {}
    for fam in families:
        for s in fam.samples:
            out[(s.name, tuple(sorted(s.labels.items())))] = s.value
    return out


def test_scorecard_families_maps_each_check() -> None:
    idx = _index(scorecard_families(SCORECARD_ROWS, DATE))
    k = (("check", "sequence_gap"), ("exchange", "kraken"), ("symbol", "BTC-USD"))
    assert idx[("gold_dq_violations", k)] == 2
    assert idx[("gold_dq_records", (("check", "latency"), ("exchange", "kraken"),
                                    ("symbol", "BTC-USD")))] == 100
    # latency percentiles fan out by quantile; only the latency row carries them.
    assert idx[("gold_dq_latency_ms", (("exchange", "kraken"), ("quantile", "p99"),
                                       ("symbol", "BTC-USD")))] == 12.0
    # clock worst-step parsed out of the detail JSON.
    clock = ("gold_dq_clock_worst_ms", (("exchange", "kraken"), ("symbol", "BTC-USD")))
    assert idx[clock] == 1234.5
    # venue-wide check (canonical_symbol None) -> empty symbol label, not a crash.
    assert idx[("gold_dq_violations", (("check", "status"), ("exchange", "kraken"),
                                       ("symbol", "")))] == 0
    expect = datetime(2026, 6, 19, tzinfo=UTC).timestamp()
    assert idx[("gold_scorecard_date_seconds", ())] == expect


def test_basis_families() -> None:
    idx = _index(basis_families(BASIS_ROWS))
    assert idx[("gold_basis_bps", (("base", "BTC"), ("stat", "mean")))] == -4.1
    assert idx[("gold_basis_bps", (("base", "BTC"), ("stat", "min")))] == -9.0
    assert idx[("gold_basis_obs", (("base", "BTC"),))] == 1000


def test_freshness_families_is_age_since_newest() -> None:
    by_layer = {"bronze": {"book_deltas": 1000.0}, "gold": {"scorecard": 1500.0}}
    idx = _index(freshness_families(by_layer, now_s=2000.0))
    bronze = ("lake_freshness_seconds", (("dataset", "book_deltas"), ("layer", "bronze")))
    gold = ("lake_freshness_seconds", (("dataset", "scorecard"), ("layer", "gold")))
    assert idx[bronze] == 1000.0
    assert idx[gold] == 500.0


def test_build_families_over_local_fs(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    lake, silver, gold = (str(tmp_path / b) for b in ("lake", "silver", "gold"))

    write_object(fs, lake, f"book_deltas/exchange=kraken/symbol=BTC-USD/date={DATE}/part.parquet",
                 pa.table({"x": [1]}))
    write_object(fs, gold, partition_key("scorecard", date=DATE), scorecard_table(SCORECARD_ROWS))
    write_object(fs, gold, partition_key("basis_summary", date=DATE),
                 pa.Table.from_pylist(BASIS_ROWS, schema=BASIS_SUMMARY_SCHEMA))

    idx = _index(build_families(fs, lake, silver, gold, now_s=2_000_000_000.0))
    assert idx[("gold_dq_violations", (("check", "sequence_gap"), ("exchange", "kraken"),
                                       ("symbol", "BTC-USD")))] == 2
    assert ("gold_basis_bps", (("base", "BTC"), ("stat", "mean"))) in idx
    # freshness present for the seeded bronze dataset, and a real positive age.
    assert idx[("lake_freshness_seconds", (("dataset", "book_deltas"), ("layer", "bronze")))] > 0
