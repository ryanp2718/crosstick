"""Gold's per-partition scorecard fold must equal the whole-day build_scorecard.

build_for_date streams silver one partition at a time (bounded memory); this pins
it row-for-row against build_scorecard over the whole day (the simple oracle).
Seeds the golden corpus as bronze on a LocalFileSystem and runs the real silver
streaming driver to produce the silver it reads - no Docker.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from analytics.corpus import CorpusRecord
from analytics.tests.golden import build_golden_records
from common.lake import iter_partition_tables, list_partitions, read_dataset
from gold.main import build_for_date
from gold.scorecard import BookCheckAccumulator, _clock_monotonic_row, build_scorecard
from materializer.bronze import (
    CanonicalMap,
    object_key,
    parse_topic,
    record_date,
    records_to_table,
)
from silver.dq import BOOK_QUALITY_SCHEMA
from silver.main import build_silver_streaming

INSTRUMENTS_FILE = Path(__file__).resolve().parents[3] / "ops" / "instruments.yml"


def _seed_bronze(
    fs: pafs.FileSystem, bucket: str, records: list[CorpusRecord], canonical: CanonicalMap
) -> None:
    by_topic: dict[str, list[CorpusRecord]] = defaultdict(list)
    for r in records:
        by_topic[r.topic].append(r)
    for topic, recs in by_topic.items():
        recs.sort(key=lambda r: r.offset)
        meta = parse_topic(topic)
        date = record_date(recs[0].timestamp_ms)
        key = object_key(meta, canonical, 0, recs[0].offset, date)
        fs.create_dir(f"{bucket}/{key}".rsplit("/", 1)[0], recursive=True)
        pq.write_table(records_to_table(recs), f"{bucket}/{key}", filesystem=fs)


def _silver(tmp_path) -> tuple[pafs.FileSystem, str, str]:
    fs = pafs.LocalFileSystem()
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    records = build_golden_records()
    date = record_date(records[0].timestamp_ms)
    lake = (tmp_path / "lake").as_posix()
    silver = (tmp_path / "silver").as_posix()
    _seed_bronze(fs, lake, records, canonical)
    build_silver_streaming(fs, fs, lake, silver, date, canonical)
    return fs, silver, date


def _oracle(fs: pafs.FileSystem, silver: str, date: str) -> list[dict]:
    def rows(ds: str) -> list[dict]:
        table = read_dataset(fs, silver, ds, date)
        return table.to_pylist() if table is not None else []

    return build_scorecard(rows("book_quality"), rows("latency"), rows("status_events"))


def test_per_partition_matches_build_scorecard(tmp_path) -> None:
    fs, silver, date = _silver(tmp_path)
    stream = sorted(build_for_date(fs, silver, date), key=repr)
    oracle = sorted(_oracle(fs, silver, date), key=repr)
    assert stream  # the golden corpus produces a non-trivial scorecard
    assert stream == oracle


def test_book_accumulator_merges_across_tables(tmp_path) -> None:
    """Splitting a partition across update() calls equals one call - the additive
    merge that makes the columnar fold (and later map-reduce) correct."""
    fs, silver, date = _silver(tmp_path)
    parts = list_partitions(fs, silver, "book_quality", date)
    part = next(
        p
        for p in parts
        if sum(t.num_rows for t in iter_partition_tables(fs, silver, "book_quality", date, p)) > 1
    )
    table = pa.concat_tables(list(iter_partition_tables(fs, silver, "book_quality", date, part)))

    whole = BookCheckAccumulator(part["exchange"], part["symbol"], date)
    whole.update(table)
    split = BookCheckAccumulator(part["exchange"], part["symbol"], date)
    half = table.num_rows // 2
    split.update(table.slice(0, half))
    split.update(table.slice(half))
    assert whole.rows() == split.rows()


def _bq(epoch: int, recv: int, kind: str = "delta") -> dict:
    return {
        "exchange": "kraken",
        "canonical_symbol": "BTC-USD",
        "date": "2026-06-16",
        "kind": kind,
        "offset": 0,
        "sequence": 0,
        "epoch": epoch,
        "exchange_ts_ns": 0,
        "local_ts_ns": recv,
        "local_recv_ts_ns": recv,
        "best_bid": None,
        "best_ask": None,
        "seq_gap": 0,
        "crossed": False,
        "invariant_kind": None,
    }


def test_clock_monotonic_counts_intra_epoch_only() -> None:
    # 200->150 is a same-epoch backward step (host clock); 150->120 crosses an epoch
    # boundary (benign reconnect overlap) and must NOT count as a violation.
    rows = [_bq(10, 100), _bq(10, 200), _bq(10, 150), _bq(20, 120), _bq(20, 300)]
    oracle = _clock_monotonic_row("kraken", "BTC-USD", "2026-06-16", rows)
    assert oracle["check"] == "clock_monotonic"
    assert oracle["n_violations"] == 1
    detail = json.loads(oracle["detail"])
    assert detail["inter_epoch_steps"] == 1
    assert detail["worst_lateness_ms"] == 50 / 1e6

    acc = BookCheckAccumulator("kraken", "BTC-USD", "2026-06-16")
    acc.update(pa.Table.from_pylist(rows, schema=BOOK_QUALITY_SCHEMA))
    streamed = next(r for r in acc.rows() if r["check"] == "clock_monotonic")
    assert streamed == oracle  # streaming accumulator equals the oracle


def test_clock_monotonic_ignores_snapshots() -> None:
    # a periodic snapshot is folded in by sequence but fetched at a different time,
    # so its recv reads as a backward step; it must NOT count as a clock fault (this
    # inflated the count ~2x on clock-clean days). Deltas alone here are monotonic.
    rows = [_bq(10, 100), _bq(10, 200), _bq(10, 150, kind="snapshot"), _bq(10, 300)]
    oracle = _clock_monotonic_row("kraken", "BTC-USD", "2026-06-16", rows)
    assert oracle["n_violations"] == 0
    assert oracle["n_records"] == 3  # the snapshot is excluded from the denominator too

    acc = BookCheckAccumulator("kraken", "BTC-USD", "2026-06-16")
    acc.update(pa.Table.from_pylist(rows, schema=BOOK_QUALITY_SCHEMA))
    streamed = next(r for r in acc.rows() if r["check"] == "clock_monotonic")
    assert streamed == oracle
