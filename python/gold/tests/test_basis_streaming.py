"""Streaming gold basis must equal the in-memory build_basis oracle.

write_basis_for_date k-way merges each base's two NBBO legs straight from their
sorted silver partitions and stream-writes the series (no whole-day nbbo read);
this pins it row-for-row against build_basis over the whole day. Seeds the golden
corpus as bronze on a LocalFileSystem and runs the real silver streaming driver —
no Docker.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from pyarrow import fs as pafs

from analytics.corpus import CorpusRecord
from analytics.tests.golden import build_golden_records
from common.lake import read_dataset
from gold.basis import (
    basis_summary_table,
    basis_table,
    build_basis,
    build_basis_summary,
)
from gold.main import write_basis_for_date
from materializer.bronze import (
    CanonicalMap,
    object_key,
    parse_topic,
    record_date,
    records_to_table,
)
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


def test_streaming_basis_matches_build_basis(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    records = build_golden_records()
    date = record_date(records[0].timestamp_ms)
    lake = (tmp_path / "lake").as_posix()
    silver = (tmp_path / "silver").as_posix()
    gold = (tmp_path / "gold").as_posix()
    _seed_bronze(fs, lake, records, canonical)
    build_silver_streaming(fs, fs, lake, silver, date, canonical)

    counts = write_basis_for_date(fs, silver, gold, date, canonical)
    assert counts["basis"]  # the golden corpus produces a BTC basis series

    # oracle: build_basis over the whole day of nbbo, round-tripped through the
    # schema so both sides carry the same decimal scale before comparison.
    nbbo = read_dataset(fs, silver, "nbbo", date).to_pylist()
    oracle = build_basis(nbbo, canonical.pairs_by_base())
    stream_basis = read_dataset(fs, gold, "basis", date).to_pylist()
    assert sorted(stream_basis, key=repr) == sorted(basis_table(oracle).to_pylist(), key=repr)

    stream_summary = read_dataset(fs, gold, "basis_summary", date).to_pylist()
    oracle_summary = basis_summary_table(build_basis_summary(oracle)).to_pylist()
    assert sorted(stream_summary, key=repr) == sorted(oracle_summary, key=repr)
