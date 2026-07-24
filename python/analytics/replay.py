"""Replay a golden corpus back into a broker.

Produces every `CorpusRecord` into its original topic, preserving key, headers
(including `local_recv_ts_ns` / `exchange_ts_ns`), and the record timestamp.
This is the producing half of the integration harness: spin up an ephemeral
Redpanda (see `analytics/tests`), replay a corpus, and assert downstream
behaviour against the recorded outputs.

All records go to **partition 0** by design - the project's `md.*` topics are
single-stream per symbol, so a single partition gives deterministic offsets
(0..N in replay order), which Phase 3's snapshot-offset-seek work relies on.

By default records are produced sequentially at full speed (the bulk mode used
by the harness and as a profiling load generator). With `--speed`, sends are
paced at the corpus's original inter-arrival times scaled by that factor
(1 = real time), which is the offline-demo mode: the dashboard moves like a
live market. Pacing is scheduled against the first record's timestamp rather
than slept per-gap, so send latency never accumulates as drift; cross-topic
timestamp jitter in capture order clamps to "send immediately".

    uv run python -m analytics.replay corpus.jsonl.gz
    uv run python -m analytics.replay --speed 1 corpus.jsonl.gz
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from collections.abc import Iterable

from aiokafka import AIOKafkaProducer

from analytics.corpus import CorpusRecord, read_corpus
from common.kafka_io import make_producer

log = logging.getLogger(__name__)


def pace_delay_sec(
    first_ts_ms: int, ts_ms: int, elapsed_sec: float, speed: float
) -> float:
    """Seconds to wait before sending the record stamped `ts_ms`, given
    `elapsed_sec` already spent replaying at `speed`. Never negative: a record
    behind schedule (or behind cross-topic timestamp jitter) sends now."""
    return max(0.0, (ts_ms - first_ts_ms) / 1000.0 / speed - elapsed_sec)


async def replay_corpus(
    producer: AIOKafkaProducer,
    records: Iterable[CorpusRecord],
    *,
    partition: int = 0,
    speed: float | None = None,
) -> int:
    """Produce each record in order; return the count produced.

    Uses `send_and_wait` per record so ordering is unambiguous (the idempotent
    producer would preserve per-partition order under pipelining too, but this
    keeps the replay's determinism obvious). With `speed`, sends are paced at
    the original inter-arrival times scaled by that factor (module docstring).
    """
    loop = asyncio.get_running_loop()
    first_ts_ms: int | None = None
    start_wall = 0.0
    n = 0
    for r in records:
        if speed is not None:
            if first_ts_ms is None:
                first_ts_ms, start_wall = r.timestamp_ms, loop.time()
            else:
                delay = pace_delay_sec(
                    first_ts_ms, r.timestamp_ms, loop.time() - start_wall, speed
                )
                if delay > 0:
                    await asyncio.sleep(delay)
        await producer.send_and_wait(
            r.topic,
            value=r.value,
            key=r.key,
            partition=partition,
            timestamp_ms=r.timestamp_ms,
            headers=r.headers,
        )
        n += 1
    return n


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay a corpus into the broker.")
    p.add_argument("corpus", help="path to a .jsonl.gz corpus file")
    p.add_argument("--partition", type=int, default=0, help="target partition (default 0)")
    p.add_argument(
        "--speed",
        type=float,
        default=None,
        help="pace at original inter-arrival times scaled by this factor "
        "(1 = real time); omit for full-speed sequential replay",
    )
    args = p.parse_args(argv)
    if args.speed is not None and args.speed <= 0:
        p.error("--speed must be > 0")
    return args


async def amain(args: argparse.Namespace) -> None:
    producer = await make_producer(client_id="replay")
    try:
        n = await replay_corpus(
            producer, read_corpus(args.corpus), partition=args.partition, speed=args.speed
        )
        log.info("replayed %d records from %s", n, args.corpus)
    finally:
        await producer.stop()


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    args = parse_args()
    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
