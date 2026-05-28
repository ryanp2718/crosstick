"""Kafka transport helpers (Redpanda-compatible) via aiokafka.

Topic naming conventions (documented in `docs/data-contracts.md`):
- md.trades.{exchange}.{symbol}
- md.book.{exchange}.{symbol}.snapshots
- md.book.{exchange}.{symbol}.deltas
- md.bbo.{exchange}.{symbol}           (published by gateway)

Symbols are normalized: '/' is replaced with '-' (Kafka disallows '/' in topic
names). The native exchange symbol form is preserved as a record header for
downstream consumers that need to round-trip it.
"""
from __future__ import annotations

import os
import re

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Characters disallowed in Kafka topic names beyond '/': *, ?, [, ], space, etc.
# Whitelist: alphanumeric, dot, hyphen, underscore.
_UNSAFE = re.compile(r"[^a-zA-Z0-9._\-]")


def brokers_from_env() -> list[str]:
    raw = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    brokers = [b.strip() for b in raw.split(",") if b.strip()]
    if not brokers:
        raise ValueError(
            f"KAFKA_BROKERS={raw!r} produced an empty broker list; "
            "set KAFKA_BROKERS to a comma-separated host:port list"
        )
    return brokers


def normalize_symbol(symbol: str) -> str:
    """Coerce an exchange symbol to a valid Kafka topic name component.

    Substitutes any character outside [a-zA-Z0-9._-] (including '/') with '-'.
    Kraken's BTC/USD becomes BTC-USD.
    """
    return _UNSAFE.sub("-", symbol)


def trade_topic(exchange: str, symbol: str) -> str:
    return f"md.trades.{exchange}.{normalize_symbol(symbol)}"


def book_snapshot_topic(exchange: str, symbol: str) -> str:
    return f"md.book.{exchange}.{normalize_symbol(symbol)}.snapshots"


def book_delta_topic(exchange: str, symbol: str) -> str:
    return f"md.book.{exchange}.{normalize_symbol(symbol)}.deltas"


def bbo_topic(exchange: str, symbol: str) -> str:
    return f"md.bbo.{exchange}.{normalize_symbol(symbol)}"


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


# Architectural ceiling for an individual Kafka message (pre-compression).
# Coinbase's full L2 snapshot re-encodes to ~1.1 MiB on the wire, over the
# 1 MiB defaults at the producer, broker, and consumer. 8 MiB gives ~7x
# headroom for snapshot growth; anything beyond that should fail loudly and
# trigger a re-evaluation (cap depth? chunked snapshots?) rather than silent
# limit-bumping. Mirror this constant in:
#   - docker-compose.yml redpanda `--set redpanda.kafka_batch_max_bytes`
#   - node/gateway/src/server.ts consumer `maxBytesPerPartition`
MAX_MESSAGE_BYTES = 8 * 1024 * 1024


async def make_producer(
    client_id: str | None = None,
    *,
    acks: str | int = "all",
    enable_idempotence: bool = True,
    compression_type: str | None = "gzip",
    linger_ms: int = 5,
    max_batch_size: int = 64 * 1024,
    max_request_size: int = MAX_MESSAGE_BYTES,
) -> AIOKafkaProducer:
    """Idempotent producer.

    - `acks=all` + `enable_idempotence=True` → exactly-once-into-broker semantics
      (within a producer session). Caller still owns end-to-end dedup.
    - `gzip` compression: native on both sides — aiokafka via stdlib (no
      cramjam/snappy/lz4 needed) and kafkajs decodes it built-in (no extra npm
      codec). The lz4 path looked attractive (faster) but the Node lz4 codecs
      are unmaintained against current Node, so gzip is the practical choice
      end-to-end; at our message rates the extra CPU is negligible. (Note:
      compression doesn't relax `max_request_size` — aiokafka checks pre-
      compression in producer.py:_serialize.)
    - `linger_ms=5`: small wait to batch; trades a few ms of latency for
      significant throughput gain (the firehose path uses pipelined `send()`,
      so this batching actually engages — see BaseIngester._emit).
    - `max_request_size=MAX_MESSAGE_BYTES` (8 MiB) holds the architectural
      ceiling end-to-end. A snapshot that exceeds this raises
      MessageSizeTooLargeError at produce time — loud, not silent.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=brokers_from_env(),
        client_id=client_id,
        acks=acks,
        enable_idempotence=enable_idempotence,
        compression_type=compression_type,
        linger_ms=linger_ms,
        max_batch_size=max_batch_size,
        max_request_size=max_request_size,
    )
    await producer.start()
    return producer


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


async def make_consumer(
    *topics: str,
    group_id: str | None,
    client_id: str | None = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    max_poll_records: int = 500,
) -> AIOKafkaConsumer:
    """Consumer with manual commits.

    `enable_auto_commit=False` is intentional — the materializer (and any
    consumer that materializes side effects) must commit offsets only AFTER
    the side effect lands. Auto-commit would commit before the Parquet PUT
    completes, breaking idempotency on crash.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=brokers_from_env(),
        group_id=group_id,
        client_id=client_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        max_poll_records=max_poll_records,
    )
    await consumer.start()
    return consumer


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


def latency_headers(local_recv_ts_ns: int, exchange_ts_ns: int) -> list[tuple[str, bytes]]:
    """Record headers for end-to-end latency tracking across hops."""
    return [
        ("local_recv_ts_ns", str(local_recv_ts_ns).encode()),
        ("exchange_ts_ns", str(exchange_ts_ns).encode()),
    ]


def header_value(headers: list[tuple[str, bytes]] | None, key: str) -> bytes | None:
    if not headers:
        return None
    for k, v in headers:
        if k == key:
            return v
    return None
