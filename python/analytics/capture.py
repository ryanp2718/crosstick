"""Capture a live window of the market-data log into a golden corpus.

Run manually against a running stack to record a real slice of `md.*` for
replay-based integration tests and research:

    uv run python -m analytics.capture --duration 120 --out corpus.jsonl.gz

The capture consumer uses a **pattern subscription** (`^md\\.`, parallel to the
gateway's regex subscription) so it picks up every topic without enumerating
per-symbol names, and a **fresh, non-committing group** (`auto_offset_reset=
latest`, manual commits off) so it records a forward window from "now" without
disturbing any production consumer group.

For deterministic test fixtures we synthesize instead of capture (see
`analytics/tests/fixtures`); this tool is for real-world corpora.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time

from aiokafka import AIOKafkaConsumer, ConsumerRecord

from analytics.corpus import CorpusRecord, CorpusWriter
from common.kafka_io import brokers_from_env

log = logging.getLogger(__name__)


def record_from_message(msg: ConsumerRecord) -> CorpusRecord:
    """Map an aiokafka ConsumerRecord to a CorpusRecord (verbatim, lossless)."""
    return CorpusRecord(
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
        timestamp_ms=msg.timestamp,
        key=msg.key,
        value=msg.value,
        headers=[(k, v) for k, v in (msg.headers or [])],
    )


async def run_capture(
    consumer: AIOKafkaConsumer,
    writer: CorpusWriter,
    *,
    max_records: int | None = None,
    duration_sec: float | None = None,
) -> int:
    """Drain the consumer into the writer until a stop condition is met.

    Stops at `max_records` (if set) or after `duration_sec` (if set); with
    neither, runs until cancelled (Ctrl+C). Returns the number written.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration_sec if duration_sec is not None else None
    n = 0
    while True:
        timeout_ms = 1000
        if deadline is not None:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            timeout_ms = min(timeout_ms, int(remaining * 1000) + 1)
        batches = await consumer.getmany(timeout_ms=timeout_ms)
        for msgs in batches.values():
            for msg in msgs:
                writer.write(record_from_message(msg))
                n += 1
                if max_records is not None and n >= max_records:
                    return n
    return n


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Capture a live md.* window into a corpus.")
    p.add_argument("--out", default="corpus.jsonl.gz", help="output corpus path")
    p.add_argument("--pattern", default=r"^md\.", help="topic subscription regex")
    p.add_argument("--duration", type=float, default=None, help="capture window seconds")
    p.add_argument("--max-records", type=int, default=None, help="stop after N records")
    p.add_argument(
        "--offset-reset",
        default="latest",
        choices=["latest", "earliest"],
        help="latest = forward window from now; earliest = from log start",
    )
    return p.parse_args(argv)


async def amain(args: argparse.Namespace) -> None:
    # Fresh, unique group so we never join (or commit into) a production group.
    group = f"capture-{os.getpid()}-{time.time_ns()}"
    consumer = AIOKafkaConsumer(
        bootstrap_servers=brokers_from_env(),
        group_id=group,
        client_id="capture",
        auto_offset_reset=args.offset_reset,
        enable_auto_commit=False,
    )
    consumer.subscribe(pattern=args.pattern)
    await consumer.start()
    try:
        with CorpusWriter(args.out) as w:
            n = await run_capture(
                consumer, w, max_records=args.max_records, duration_sec=args.duration
            )
        log.info("captured %d records to %s", n, args.out)
    finally:
        await consumer.stop()


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    args = parse_args()
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
