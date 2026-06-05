"""Replay a golden corpus back into a broker.

Produces every `CorpusRecord` into its original topic, preserving key, headers
(including `local_recv_ts_ns` / `exchange_ts_ns`), and the record timestamp.
This is the producing half of the integration harness: spin up an ephemeral
Redpanda (see `analytics/tests`), replay a corpus, and assert downstream
behaviour against the recorded outputs.

All records go to **partition 0** by design — the project's `md.*` topics are
single-stream per symbol, so a single partition gives deterministic offsets
(0..N in replay order), which Phase 3's snapshot-offset-seek work relies on.

    uv run python -m analytics.replay corpus.jsonl.gz
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


async def replay_corpus(
    producer: AIOKafkaProducer,
    records: Iterable[CorpusRecord],
    *,
    partition: int = 0,
) -> int:
    """Produce each record in order; return the count produced.

    Uses `send_and_wait` per record so ordering is unambiguous (the idempotent
    producer would preserve per-partition order under pipelining too, but this
    keeps the replay's determinism obvious).
    """
    n = 0
    for r in records:
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
    return p.parse_args(argv)


async def amain(args: argparse.Namespace) -> None:
    producer = await make_producer(client_id="replay")
    try:
        n = await replay_corpus(
            producer, read_corpus(args.corpus), partition=args.partition
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
