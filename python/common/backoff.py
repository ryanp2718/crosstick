"""Exponential backoff with full jitter (AWS architecture blog pattern).

    delay = min(cap, base * 2 ** attempt)
    sleep = uniform(0, delay)

Full jitter dominates equal-jitter for retry storms: it spreads reconnect
attempts uniformly across the window instead of clumping near `delay`.
"""
from __future__ import annotations

import asyncio
import random


class FullJitterBackoff:
    def __init__(self, base: float = 0.5, cap: float = 30.0, max_attempts: int | None = None):
        if base <= 0 or cap <= 0:
            raise ValueError("base and cap must be positive")
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.base = base
        self.cap = cap
        self.max_attempts = max_attempts
        self.attempt = 0

    def reset(self) -> None:
        self.attempt = 0

    def next_delay(self) -> float:
        ceiling = min(self.cap, self.base * (2**self.attempt))
        self.attempt += 1
        return random.uniform(0.0, ceiling)

    async def sleep(self) -> float:
        if self.max_attempts is not None and self.attempt >= self.max_attempts:
            raise RuntimeError(f"backoff exhausted after {self.attempt} attempts")
        delay = self.next_delay()
        await asyncio.sleep(delay)
        return delay
