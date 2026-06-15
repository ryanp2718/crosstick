"""merge_latest: running-latest as-of merge, backward-only (no lookahead)."""
from __future__ import annotations

from common.asof import merge_latest


def test_running_latest_carries_forward() -> None:
    streams = {"a": [(1, "a1"), (5, "a2")], "b": [(3, "b1")]}
    assert list(merge_latest(streams)) == [
        (1, {"a": "a1"}),
        (3, {"a": "a1", "b": "b1"}),
        (5, {"a": "a2", "b": "b1"}),
    ]


def test_same_ts_events_applied_together() -> None:
    streams = {"a": [(1, "a1")], "b": [(1, "b1")]}
    assert list(merge_latest(streams)) == [(1, {"a": "a1", "b": "b1"})]


def test_none_value_carried_like_any_other() -> None:
    # callers use None as an eviction sentinel; it must propagate, not be dropped.
    streams = {"a": [(1, "a1"), (2, None), (4, "a3")]}
    assert list(merge_latest(streams)) == [
        (1, {"a": "a1"}),
        (2, {"a": None}),
        (4, {"a": "a3"}),
    ]


def test_no_lookahead_property() -> None:
    # truncating every input at T must leave all snapshots for ts <= T unchanged.
    streams = {"a": [(1, "a1"), (4, "a2"), (7, "a3")], "b": [(2, "b1"), (6, "b2")]}
    full = list(merge_latest(streams))
    for cutoff in (1, 2, 4, 5, 6, 7):
        truncated = {
            k: [(ts, v) for ts, v in seq if ts <= cutoff] for k, seq in streams.items()
        }
        expected = [(ts, snap) for ts, snap in full if ts <= cutoff]
        assert list(merge_latest(truncated)) == expected, f"lookahead leaked at {cutoff}"
