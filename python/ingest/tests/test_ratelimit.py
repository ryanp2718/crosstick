"""Tests for AsyncTokenBucket."""

from __future__ import annotations

import asyncio
import time

import pytest

from common.ratelimit import AsyncTokenBucket


@pytest.mark.asyncio
async def test_initial_capacity_acquires_immediately() -> None:
    b = AsyncTokenBucket(rate=10, capacity=5)
    t0 = time.monotonic()
    for _ in range(5):
        await b.acquire(1)
    elapsed = time.monotonic() - t0
    assert elapsed < 0.05  # all 5 tokens drained instantly


@pytest.mark.asyncio
async def test_blocks_when_empty_and_refills() -> None:
    b = AsyncTokenBucket(rate=20, capacity=1)
    await b.acquire(1)  # drain
    t0 = time.monotonic()
    await b.acquire(1)  # must wait ~1/20 = 50ms
    elapsed = time.monotonic() - t0
    assert 0.03 <= elapsed <= 0.2


@pytest.mark.asyncio
async def test_acquire_more_than_capacity_raises() -> None:
    b = AsyncTokenBucket(rate=1, capacity=5)
    with pytest.raises(ValueError):
        await b.acquire(10)


def test_invalid_params() -> None:
    with pytest.raises(ValueError):
        AsyncTokenBucket(rate=0, capacity=1)
    with pytest.raises(ValueError):
        AsyncTokenBucket(rate=1, capacity=0)


def test_backward_clock_updates_last_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    """After a backward clock jump, _last is updated to the new (backward) time.
    Without max(0.0, elapsed), _last stays at the old future value so every
    subsequent _refill_locked sees negative elapsed and never refills."""
    import common.ratelimit as rl_module

    bucket = AsyncTokenBucket(rate=1.0, capacity=5.0)
    bucket._tokens = 3.0
    bucket._last = 100.0

    monkeypatch.setattr(rl_module.time, "monotonic", lambda: 99.0)
    bucket._refill_locked()

    # tokens unchanged (elapsed clamped to 0)
    assert bucket._tokens == 3.0
    # _last must be updated to 99.0 so the NEXT refill computes elapsed correctly
    assert bucket._last == 99.0, (
        "_last not updated after backward jump - "
        "max(0.0, elapsed) fix is missing, causing future refills to skip"
    )


@pytest.mark.asyncio
async def test_concurrent_acquires_serialize() -> None:
    """N concurrent acquirers at rate r should take ~N/r seconds total."""
    b = AsyncTokenBucket(rate=50, capacity=1)
    await b.acquire(1)  # drain initial
    t0 = time.monotonic()
    await asyncio.gather(*[b.acquire(1) for _ in range(5)])
    elapsed = time.monotonic() - t0
    # 5 acquires at 50/sec ≈ 0.1s, with slack
    assert 0.05 <= elapsed <= 0.5
