"""Bronze projection logic: raw Kafka records → Parquet objects on the lake.

Pure on purpose (no aiokafka / S3 imports): topic parsing, canonical
resolution, object-key derivation, and Arrow table construction are all
unit-testable without infrastructure. The service half (`materializer.service`)
wires this to a consumer and a filesystem.

Layout and schema are the contract documented in docs/data-contracts.md
("Bronze lake"). Rows are verbatim `CorpusRecord`s — the same lossless shape
the capture/replay harness uses — so any bronze slice is also a replayable
corpus.
"""
from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import yaml

from analytics.corpus import CorpusRecord
from common.kafka_io import normalize_symbol

log = logging.getLogger(__name__)

BRONZE_FORMAT = "bronze-v1"


@dataclass(frozen=True)
class TopicMeta:
    """A topic name parsed into its bronze dataset coordinates.

    `symbol` is the topic's normalized symbol (md.nbbo carries the canonical_id
    directly); `exchange` is None only for nbbo (cross-venue).
    """

    dataset: str
    exchange: str | None
    symbol: str | None


def _split_exchange_symbol(rest: str, topic: str) -> tuple[str, str]:
    exchange, _, symbol = rest.partition(".")
    if not exchange or not symbol:
        raise ValueError(f"cannot parse exchange.symbol from topic {topic!r}")
    return exchange, symbol


def parse_topic(topic: str) -> TopicMeta:
    """Map a topic name to its dataset coordinates; ValueError on unknown shape."""
    if topic.startswith("md.book.") and topic.endswith(".snapshots"):
        ex, sym = _split_exchange_symbol(topic[len("md.book.") : -len(".snapshots")], topic)
        return TopicMeta("book_snapshots", ex, sym)
    if topic.startswith("md.book.") and topic.endswith(".deltas"):
        ex, sym = _split_exchange_symbol(topic[len("md.book.") : -len(".deltas")], topic)
        return TopicMeta("book_deltas", ex, sym)
    if topic.startswith("md.trades."):
        ex, sym = _split_exchange_symbol(topic[len("md.trades.") :], topic)
        return TopicMeta("trades", ex, sym)
    if topic.startswith("md.bbo."):
        ex, sym = _split_exchange_symbol(topic[len("md.bbo.") :], topic)
        return TopicMeta("bbo", ex, sym)
    if topic.startswith("md.liquidations."):
        ex, sym = _split_exchange_symbol(topic[len("md.liquidations.") :], topic)
        return TopicMeta("liquidations", ex, sym)
    if topic.startswith("md.markprice."):
        ex, sym = _split_exchange_symbol(topic[len("md.markprice.") :], topic)
        return TopicMeta("mark_price", ex, sym)
    if topic.startswith("md.openinterest."):
        ex, sym = _split_exchange_symbol(topic[len("md.openinterest.") :], topic)
        return TopicMeta("open_interest", ex, sym)
    if topic.startswith("md.status."):
        return TopicMeta("status", topic[len("md.status.") :], None)
    if topic.startswith("md.nbbo."):
        return TopicMeta("nbbo", None, topic[len("md.nbbo.") :])
    raise ValueError(f"topic {topic!r} does not match any md.* contract shape")


class CanonicalMap:
    """(exchange, normalized symbol) → canonical_id from ops/instruments.yml.

    Mirrors the gateway's resolution so the lake partitions by canonical
    instrument, not each venue's native spelling. Unmapped pairs fall back to
    the normalized topic symbol (warn once) — bronze never drops data over
    missing reference data; curated-vs-discovered symbology at 50+ is
    DESIGN_analytics.md open concern #1.
    """

    def __init__(self, mapping: dict[tuple[str, str], str]):
        self._map = mapping
        self._warned: set[tuple[str, str]] = set()

    @classmethod
    def from_yaml(cls, path: str | Path) -> CanonicalMap:
        doc = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        mapping: dict[tuple[str, str], str] = {}
        for canonical_id, spec in (doc.get("instruments") or {}).items():
            for venue in spec.get("venues", []):
                key = (venue["exchange"], normalize_symbol(venue["symbol"]))
                mapping[key] = canonical_id
        return cls(mapping)

    def resolve(self, exchange: str, symbol: str) -> str:
        key = (exchange, normalize_symbol(symbol))
        canonical = self._map.get(key)
        if canonical is None:
            if key not in self._warned:
                self._warned.add(key)
                log.warning("no canonical mapping for %s; partitioning by native symbol", key)
            return key[1]
        return canonical


def record_date(timestamp_ms: int) -> str:
    """UTC date partition value for a Kafka record timestamp."""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).strftime("%Y-%m-%d")


def object_key(
    meta: TopicMeta, canonical: CanonicalMap, partition: int, start_offset: int, date: str
) -> str:
    """Deterministic object key for a chunk.

    Named by partition + START offset only (Kafka Connect S3-sink convention),
    not start-end: the end offset of a chunk is wall-clock-dependent (the age
    flush), but the start is always the committed offset — so a crash-retry
    rewrites the *identical* key and at-least-once becomes exactly-once at the
    file grain. End offset/count live in the Parquet footer metadata. Chunks
    are contiguous per topic-partition, so a sorted listing implies coverage.
    """
    parts = [meta.dataset]
    if meta.exchange is not None:
        parts.append(f"exchange={meta.exchange}")
    if meta.symbol is not None:
        if meta.exchange is None:  # nbbo: the topic symbol IS the canonical_id
            sym = meta.symbol
        else:
            sym = canonical.resolve(meta.exchange, meta.symbol)
        parts.append(f"symbol={sym}")
    parts.append(f"date={date}")
    parts.append(f"{partition:03d}-{start_offset:012d}.parquet")
    return "/".join(parts)


SCHEMA = pa.schema(
    [
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("timestamp_ms", pa.int64()),
        ("key", pa.binary()),
        ("value", pa.binary()),
        ("headers", pa.list_(pa.struct([("key", pa.string()), ("value", pa.binary())]))),
    ]
)


def records_to_table(records: Sequence[CorpusRecord]) -> pa.Table:
    """Verbatim records → Arrow table with provenance footer metadata."""
    if not records:
        raise ValueError("refusing to build an empty bronze table")
    table = pa.Table.from_pydict(
        {
            "topic": [r.topic for r in records],
            "partition": [r.partition for r in records],
            "offset": [r.offset for r in records],
            "timestamp_ms": [r.timestamp_ms for r in records],
            "key": [r.key for r in records],
            "value": [r.value for r in records],
            "headers": [[{"key": k, "value": v} for k, v in r.headers] for r in records],
        },
        schema=SCHEMA,
    )
    return table.replace_schema_metadata(
        {
            "crosstick:format": BRONZE_FORMAT,
            "crosstick:start_offset": str(records[0].offset),
            "crosstick:end_offset": str(records[-1].offset),
            "crosstick:record_count": str(len(records)),
        }
    )


def table_to_records(table: pa.Table) -> list[CorpusRecord]:
    """Inverse of records_to_table — bronze is corpus-shaped, so a bronze slice
    can be read back as replay fodder (and tests assert lossless round-trip)."""
    return [
        CorpusRecord(
            topic=row["topic"],
            partition=row["partition"],
            offset=row["offset"],
            timestamp_ms=row["timestamp_ms"],
            key=row["key"],
            value=row["value"],
            headers=[(h["key"], h["value"]) for h in row["headers"]],
        )
        for row in table.to_pylist()
    ]
