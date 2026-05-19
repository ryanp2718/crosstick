"""Redis transport helpers.

Two channels:
- **Streams** for ordered, gap-detectable book deltas + trades.
- **Pub/Sub** for ephemeral derived signals (BBO, spread, latency).
"""
from __future__ import annotations

import os

from redis.asyncio import Redis


def redis_from_env() -> Redis:
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return Redis.from_url(url, decode_responses=False)


def book_stream_key(exchange: str, symbol: str) -> str:
    return f"streams:book:{exchange}:{symbol}"


def trade_stream_key(exchange: str, symbol: str) -> str:
    return f"streams:trade:{exchange}:{symbol}"


def bbo_channel(exchange: str, symbol: str) -> str:
    return f"pubsub:bbo:{exchange}:{symbol}"


def spread_channel(symbol: str) -> str:
    return f"pubsub:spread:{symbol}"


def vwap_channel(exchange: str, symbol: str, window_sec: int) -> str:
    return f"pubsub:vwap:{exchange}:{symbol}:{window_sec}"


async def xadd(redis: Redis, key: str, payload: bytes, maxlen: int = 10_000) -> bytes:
    return await redis.xadd(key, {b"d": payload}, maxlen=maxlen, approximate=True)


async def publish(redis: Redis, channel: str, payload: bytes) -> int:
    return await redis.publish(channel, payload)
