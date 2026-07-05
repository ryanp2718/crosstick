"""Offline-demo entrypoint: seed topics, wait for the gateway, replay paced.

Two subcommands, one per side of the gateway's startup ordering (wired
together by demo/docker-compose.yml):

  create-topics  Runs BEFORE the gateway starts. The gateway's kafkajs regex
                 subscription only matches topics that exist at subscribe time
                 (node/gateway/src/server.ts), so every topic in the corpus is
                 created up front, single-partition for per-topic total order.

  replay         Runs AFTER the gateway is up. Polls its /metrics until
                 gateway_warmstart_planned reports 1, then replays the corpus
                 at real-time pace (--speed 1). The wait is load-bearing: a
                 record produced before the warm-start seeks land carries an
                 old timestamp and would be seeked past, silently dropping the
                 head of the corpus (the book snapshots that light the
                 dashboard up).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import urllib.request
from collections.abc import Iterable

from analytics.corpus import CorpusRecord, read_corpus
from analytics.kafka_admin import create_single_partition_topics
from analytics.replay import replay_corpus
from common.kafka_io import make_producer

log = logging.getLogger(__name__)

WARMSTART_WAIT_SEC = 120.0


def corpus_topics(records: Iterable[CorpusRecord]) -> list[str]:
    """Sorted distinct topics appearing in a corpus."""
    return sorted({r.topic for r in records})


def warmstart_planned(metrics_text: str) -> bool:
    """True when a Prometheus scrape reports gateway_warmstart_planned >= 1."""
    for line in metrics_text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "gateway_warmstart_planned":
            try:
                return float(parts[1]) >= 1
            except ValueError:
                return False
    return False


async def wait_for_warmstart(url: str, timeout_sec: float) -> float:
    """Poll the gateway's /metrics until the warm-start plan is applied;
    return the seconds waited."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    deadline = start + timeout_sec
    while loop.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if warmstart_planned(resp.read().decode()):
                    return loop.time() - start
        except OSError:
            pass
        await asyncio.sleep(0.5)
    raise TimeoutError(f"gateway warm-start not planned within {timeout_sec:.0f}s at {url}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Offline-demo topic seeding and paced replay.")
    sub = p.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("create-topics", help="create every topic in the corpus (pre-gateway)")
    t.add_argument("corpus", help="path to a .jsonl.gz corpus file")

    r = sub.add_parser("replay", help="wait for gateway warm-start, then replay paced")
    r.add_argument("corpus", help="path to a .jsonl.gz corpus file")
    r.add_argument("--speed", type=float, default=1.0, help="pacing factor (default 1 = real time)")
    r.add_argument(
        "--gateway-metrics",
        default="http://gateway:8080/metrics",
        help="gateway /metrics URL polled for gateway_warmstart_planned",
    )

    args = p.parse_args(argv)
    if args.cmd == "replay" and args.speed <= 0:
        p.error("--speed must be > 0")
    return args


async def amain(args: argparse.Namespace) -> None:
    if args.cmd == "create-topics":
        topics = corpus_topics(read_corpus(args.corpus))
        await create_single_partition_topics(topics)
        log.info("created %d topics from %s", len(topics), args.corpus)
        return

    waited = await wait_for_warmstart(args.gateway_metrics, WARMSTART_WAIT_SEC)
    log.info(
        "gateway warm-start planned after %.1fs; replaying %s at %gx",
        waited,
        args.corpus,
        args.speed,
    )
    producer = await make_producer(client_id="demo-replay")
    try:
        n = await replay_corpus(producer, read_corpus(args.corpus), speed=args.speed)
    finally:
        await producer.stop()
    log.info("replayed %d records; feed is now idle (dashboard will report a stall)", n)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    asyncio.run(amain(parse_args()))


if __name__ == "__main__":
    main()
