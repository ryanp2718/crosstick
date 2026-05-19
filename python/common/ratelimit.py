"""Async token bucket — refills continuously at `rate` tokens/sec up to `capacity`."""
from __future__ import annotations

import asyncio
import time


class AsyncTokenBucket:
    def __init__(self, rate: float, capacity: float):
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate and capacity must be positive")
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def tokens(self) -> float:
        return self._tokens

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last = now

    async def acquire(self, tokens: float = 1.0) -> None:
        if tokens > self.capacity:
            raise ValueError(f"requested {tokens} > capacity {self.capacity}")
        while True:
            async with self._lock:
                self._refill_locked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait = deficit / self.rate
            await asyncio.sleep(wait)
