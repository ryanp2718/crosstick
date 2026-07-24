"""merge_latest: running-latest as-of merge, backward-only (no lookahead).
reorder: bounded watermark re-sort of a nearly-sorted stream, fail-loud past W."""
from __future__ import annotations

import pytest

from common.asof import LatenessError, merge_latest, reorder


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


def test_accepts_lazy_iterators() -> None:
    # streams may be generators (the streaming callers feed per-partition iterators);
    # the merge pulls them lazily and yields the same snapshots as the list form.
    streams = {"a": iter([(1, "a1"), (5, "a2")]), "b": iter([(3, "b1")])}
    assert list(merge_latest(streams)) == [
        (1, {"a": "a1"}),
        (3, {"a": "a1", "b": "b1"}),
        (5, {"a": "a2", "b": "b1"}),
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


def test_reorder_passthrough_already_sorted() -> None:
    # W=0 is strict: an in-order stream passes straight through.
    events = [(1, "a"), (2, "b"), (3, "c")]
    assert list(reorder(events, 0)) == events


def test_reorder_emits_incrementally_and_keeps_equal_ts_order() -> None:
    # a straggler within W is placed in ts order; equal-ts entries keep arrival
    # (fold) order via the read_idx tiebreak - reproducing a stable sort.
    events = [(1, "x"), (1, "y"), (3, "z"), (10, "w"), (8, "late")]  # 'late' 2 behind, < W
    assert list(reorder(events, 5)) == [(1, "x"), (1, "y"), (3, "z"), (8, "late"), (10, "w")]


def test_reorder_matches_stable_sort_within_window() -> None:
    # every backward step is < W behind the running max -> output is the stable sort.
    events = [(0, 0), (50, 1), (30, 2), (90, 3), (60, 4), (120, 5), (110, 6)]
    assert list(reorder(events, 100)) == sorted(events, key=lambda e: e[0])


def test_reorder_raises_beyond_window() -> None:
    # 10 then a 2 arrives -> 8 behind a value already emitted, > W -> cannot place.
    events = [(1, "a"), (10, "b"), (20, "c"), (2, "way late")]
    with pytest.raises(LatenessError):
        list(reorder(events, 5))
