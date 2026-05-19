"""Tests for exponential-backoff-with-full-jitter."""
from __future__ import annotations

import asyncio

import pytest

from common.backoff import FullJitterBackoff


def test_ceiling_doubles_then_caps() -> None:
    """next_delay() upper bound: min(cap, base * 2**attempt)."""
    b = FullJitterBackoff(base=1.0, cap=10.0)
    # attempt 0: ceiling = min(10, 1*1) = 1
    # attempt 1: ceiling = min(10, 1*2) = 2
    # attempt 2: ceiling = 4
    # attempt 3: ceiling = 8
    # attempt 4: ceiling = 10 (capped)
    samples = [b.next_delay() for _ in range(5)]
    assert all(0 <= s for s in samples)
    assert samples[0] <= 1
    assert samples[1] <= 2
    assert samples[2] <= 4
    assert samples[3] <= 8
    assert samples[4] <= 10


def test_reset() -> None:
    b = FullJitterBackoff(base=1.0, cap=10.0)
    for _ in range(5):
        b.next_delay()
    assert b.attempt == 5
    b.reset()
    assert b.attempt == 0


@pytest.mark.asyncio
async def test_sleep_runs() -> None:
    b = FullJitterBackoff(base=0.001, cap=0.01)
    elapsed = await b.sleep()
    assert elapsed >= 0
    assert elapsed <= 0.01


def test_max_attempts_raises() -> None:
    b = FullJitterBackoff(base=0.001, cap=0.01, max_attempts=2)
    b.next_delay()
    b.next_delay()
    # attempt == 2 now, max == 2
    import asyncio as _a
    with pytest.raises(RuntimeError, match="exhausted"):
        _a.run(b.sleep())


def test_invalid_params() -> None:
    with pytest.raises(ValueError):
        FullJitterBackoff(base=0)
    with pytest.raises(ValueError):
        FullJitterBackoff(cap=-1)
    with pytest.raises(ValueError):
        FullJitterBackoff(max_attempts=0)
