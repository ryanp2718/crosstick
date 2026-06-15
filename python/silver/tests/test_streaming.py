"""The streaming silver driver must produce the SAME silver as the in-memory
build_silver. Seeds the golden corpus as bronze on a LocalFileSystem (no Docker)
and asserts every silver dataset matches, row-for-row, between the two paths.

The golden corpus is order-clean, so the streaming 2-way merge of snapshots and
deltas is identical to build_silver's per-partition sort — this pins that
equivalence unconditionally.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from analytics.corpus import CorpusRecord
from analytics.tests.golden import build_golden_records
from common.lake import list_partitions, read_dataset
from materializer.bronze import (
    CanonicalMap,
    object_key,
    parse_topic,
    record_date,
    records_to_table,
)
from silver.dq import build_silver
from silver.main import build_silver_streaming, read_bronze_records, write_silver

INSTRUMENTS_FILE = Path(__file__).resolve().parents[3] / "ops" / "instruments.yml"
SILVER_DATASETS = ("book_quality", "latency", "status_events", "quotes", "nbbo")


def _seed_bronze(
    fs: pafs.FileSystem, bucket: str, records: list[CorpusRecord], canonical: CanonicalMap
) -> None:
    """Write the records as bronze, one object per topic (the materializer layout)."""
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


def _sorted_rows(table: pa.Table | None) -> list | None:
    return None if table is None else sorted(table.to_pylist(), key=repr)


def test_streaming_matches_in_memory(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    records = build_golden_records()
    date = record_date(records[0].timestamp_ms)

    lake = (tmp_path / "lake").as_posix()
    silver_stream = (tmp_path / "silver_stream").as_posix()
    silver_mem = (tmp_path / "silver_mem").as_posix()
    _seed_bronze(fs, lake, records, canonical)

    build_silver_streaming(fs, lake, silver_stream, date, canonical)
    write_silver(fs, silver_mem, build_silver(read_bronze_records(fs, lake, date), canonical))

    for ds in SILVER_DATASETS:
        stream = read_dataset(fs, silver_stream, ds, date)
        mem = read_dataset(fs, silver_mem, ds, date)
        assert (stream is None) == (mem is None), f"{ds}: one side missing"
        assert _sorted_rows(stream) == _sorted_rows(mem), f"{ds}: rows differ"
        # the partition layout must match too (same files written)
        sp = sorted(map(repr, list_partitions(fs, silver_stream, ds, date)))
        mp = sorted(map(repr, list_partitions(fs, silver_mem, ds, date)))
        assert sp == mp, f"{ds}: partitions differ"


def test_streaming_counts_match_facts(tmp_path) -> None:
    """The returned counts equal the in-memory fact-stream lengths."""
    fs = pafs.LocalFileSystem()
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    records = build_golden_records()
    date = record_date(records[0].timestamp_ms)
    lake = (tmp_path / "lake").as_posix()
    _seed_bronze(fs, lake, records, canonical)

    counts = build_silver_streaming(fs, lake, (tmp_path / "silver").as_posix(), date, canonical)
    facts = build_silver(read_bronze_records(fs, lake, date), canonical)
    assert counts["book_quality"] == len(facts.book_quality)
    assert counts["latency"] == len(facts.latency)
    assert counts["quotes"] == len(facts.quotes)
    assert counts["status_events"] == len(facts.status_events)
    assert counts["nbbo"] == len(facts.nbbo)
