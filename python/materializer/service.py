"""Materializer service: md.* topics → bronze Parquet on the lake.

Consume → buffer per topic-partition → cut a chunk on size (dominant), UTC
date boundary, or age → PUT Parquet → commit offsets for that partition.

Exactly-once at the file grain: commits happen only AFTER the PUT (the
make_consumer manual-commit rationale), and each PUT is awaited before
consumption continues - so at most one PUT is ever un-committed. A crash
therefore re-reads exactly the in-flight chunk, starting at the committed
offset, and rewrites the *identical* start-offset-keyed object (see
bronze.object_key). No downstream dedup needed.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

import pyarrow.fs
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition
from prometheus_client import Counter, Gauge

from analytics.capture import record_from_message
from analytics.corpus import CorpusRecord
from common.metrics import REGISTRY
from materializer.bronze import (
    CanonicalMap,
    object_key,
    parse_topic,
    record_date,
    records_to_table,
)

log = logging.getLogger(__name__)

bronze_records = Counter(
    "bronze_records_total",
    "Records written to bronze Parquet",
    ["dataset"],
    registry=REGISTRY,
)
bronze_objects = Counter(
    "bronze_objects_total",
    "Parquet objects PUT to the lake",
    ["dataset"],
    registry=REGISTRY,
)
bronze_value_bytes = Counter(
    "bronze_value_bytes_total",
    "Uncompressed record-value bytes written to bronze",
    ["dataset"],
    registry=REGISTRY,
)
# Producer-side pipeline-lag gauges (refreshed on a cadence in run()). The
# lake-side freshness alone misses a consumer that is behind-but-flushing (lag
# high, objects still landing) or up-but-stuck (consuming into the buffer but not
# flushing) - these two close that gap.
bronze_consumer_lag = Gauge(
    "bronze_consumer_lag_messages",
    "Unconsumed messages in the log per assigned partition (highwater - position)",
    ["topic", "partition"],
    registry=REGISTRY,
)
bronze_flush_age = Gauge(
    "bronze_flush_age_seconds",
    "Seconds since the last successful bronze flush, per dataset",
    ["dataset"],
    registry=REGISTRY,
)


@dataclass
class _Buffer:
    """Pending records for one topic-partition chunk."""

    date: str  # UTC date of the first record - the chunk's date partition
    first_append: float  # event-loop time; drives the age flush
    value_bytes: int = 0
    records: list[CorpusRecord] = field(default_factory=list)


class _DropBuffersOnRevoke(ConsumerRebalanceListener):
    """On rebalance, drop uncommitted buffers - the next assignee re-reads them
    from the committed offset, and start-offset keys make the rewrite safe."""

    def __init__(self, materializer: Materializer):
        self._mat = materializer

    async def on_partitions_revoked(self, revoked) -> None:
        for tp in revoked:
            if self._mat._buffers.pop(tp, None) is not None:
                log.info("dropped uncommitted buffer for revoked %s", tp)

    async def on_partitions_assigned(self, assigned) -> None:
        pass


class Materializer:
    """Drains a consumer into start-offset-keyed Parquet objects under
    `{bucket}/` on `filesystem`. The consumer must be started, subscribed
    (use `rebalance_listener()`), and have auto-commit off."""

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        filesystem: pyarrow.fs.FileSystem,
        bucket: str,
        canonical_map: CanonicalMap,
        *,
        flush_bytes: int = 16 * 1024 * 1024,
        flush_interval_sec: float = 900.0,
        metrics_interval_sec: float = 15.0,
    ):
        self.consumer = consumer
        self.fs = filesystem
        self.bucket = bucket
        self.canonical_map = canonical_map
        self.flush_bytes = flush_bytes
        self.flush_interval_sec = flush_interval_sec
        self.metrics_interval_sec = metrics_interval_sec
        self.records_flushed = 0
        self._buffers: dict[TopicPartition, _Buffer] = {}
        self._last_flush: dict[str, float] = {}  # dataset -> wall-clock of last PUT
        self._last_metrics = 0.0  # loop time of last gauge refresh
        self._stopping = False

    def rebalance_listener(self) -> ConsumerRebalanceListener:
        return _DropBuffersOnRevoke(self)

    def shutdown(self) -> None:
        self._stopping = True

    async def run(self) -> None:
        """Consume until shutdown(); final sweep flushes whatever is buffered."""
        while not self._stopping:
            await self.poll_once()
            await self._refresh_metrics_due()
        await self.flush_all()

    async def _refresh_metrics_due(self) -> None:
        now = asyncio.get_running_loop().time()
        if now - self._last_metrics >= self.metrics_interval_sec:
            self._last_metrics = now
            await self.refresh_metrics()

    async def refresh_metrics(self) -> None:
        """Refresh the producer-side lag gauges: per-partition consumer lag
        (highwater - position) and per-dataset seconds-since-last-flush."""
        now = time.time()
        for dataset, flushed_at in self._last_flush.items():
            bronze_flush_age.labels(dataset=dataset).set(now - flushed_at)
        for tp in self.consumer.assignment():
            hw = self.consumer.highwater(tp)
            if hw is None:
                continue  # no fetch yet - highwater unknown
            pos = await self.consumer.position(tp)
            bronze_consumer_lag.labels(topic=tp.topic, partition=str(tp.partition)).set(
                max(hw - pos, 0)
            )

    async def poll_once(self, timeout_ms: int = 1000) -> int:
        """One consume-buffer-flush cycle; returns records consumed."""
        batches = await self.consumer.getmany(timeout_ms=timeout_ms)
        n = 0
        for tp, msgs in batches.items():
            for msg in msgs:
                await self._append(tp, record_from_message(msg))
                n += 1
        await self._flush_due()
        return n

    async def flush_all(self) -> None:
        for tp in list(self._buffers):
            await self._flush(tp)

    async def _append(self, tp: TopicPartition, rec: CorpusRecord) -> None:
        date = record_date(rec.timestamp_ms)
        buf = self._buffers.get(tp)
        if buf is not None and buf.date != date:
            await self._flush(tp)  # date-boundary cut keeps the date partition honest
            buf = None
        if buf is None:
            loop = asyncio.get_running_loop()
            buf = self._buffers[tp] = _Buffer(date=date, first_append=loop.time())
        buf.records.append(rec)
        buf.value_bytes += len(rec.value)
        if buf.value_bytes >= self.flush_bytes:
            await self._flush(tp)

    async def _flush_due(self) -> None:
        now = asyncio.get_running_loop().time()
        due = [
            tp
            for tp, buf in self._buffers.items()
            if now - buf.first_append >= self.flush_interval_sec
        ]
        for tp in due:
            await self._flush(tp)

    async def _flush(self, tp: TopicPartition) -> None:
        buf = self._buffers.pop(tp)
        meta = parse_topic(tp.topic)
        key = object_key(
            meta, self.canonical_map, tp.partition, buf.records[0].offset, buf.date
        )
        table = records_to_table(buf.records)
        path = f"{self.bucket}/{key}"
        # write_table is blocking (S3 PUT); keep the event loop responsive.
        # zstd: smaller than snappy at similar decode speed; DuckDB/ClickHouse-native.
        await asyncio.to_thread(
            pq.write_table, table, path, filesystem=self.fs, compression="zstd"
        )
        # Commit only after the PUT landed; a failure here is fatal by design -
        # the restart re-reads this chunk and overwrites the same key.
        await self.consumer.commit({tp: buf.records[-1].offset + 1})
        self.records_flushed += len(buf.records)
        self._last_flush[meta.dataset] = time.time()
        bronze_records.labels(dataset=meta.dataset).inc(len(buf.records))
        bronze_objects.labels(dataset=meta.dataset).inc()
        bronze_value_bytes.labels(dataset=meta.dataset).inc(buf.value_bytes)
        log.info("bronze PUT %s (%d records)", path, len(buf.records))
