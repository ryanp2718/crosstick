"""As-of merge for the silver/gold layers - the one definition of "as-of".

`merge_latest` folds several timestamped streams onto their union timeline,
carrying each stream's latest value forward. It is the shared primitive behind
two reconstructions:
  - silver NBBO: combine a canonical's per-venue quote streams (max-bid/min-ask
    on each tick), with a `None` value used to evict a venue's leg.
  - gold basis: combine two canonical NBBO streams (compute the spread on each
    tick, only where both legs are present).

Backward-only **by construction**: a snapshot at time T reflects only events with
ts <= T, never a future one - so point-in-time correctness is structural, not a
runtime guard. (A no-lookahead property test pins this.)
"""

from __future__ import annotations

import heapq
from collections.abc import Hashable, Iterable, Iterator, Mapping
from itertools import groupby
from typing import Any

# Max age a leg's last quote stays valid in an as-of NBBO/basis before it is
# evicted as stale. `merge_latest` carries a value forward indefinitely, so a
# venue that goes quiet (without a status-down) would otherwise be carried into a
# crossed/wide NBBO (multi-venue) or a frozen mid (single-venue), distorting the
# basis. 10s sits above every venue's normal requote p99.9 (<=5s, measured) so a
# healthy-but-quiet leg is not evicted, and well inside silver's WINDOW_NS reorder
# window (silver/main.py, the one home for that value).
# The offline analog of the gateway's venue-health eviction (DESIGN_nbbo.md).
MAX_LEG_AGE_NS = 10_000_000_000  # 10s


class LatenessError(Exception):
    """An event arrived more than the allowed window behind the last emitted ts, so
    it cannot be placed without breaking the sorted-input invariant `merge_latest`
    relies on. Raised by `reorder` - a fail-loud signal of disorder beyond tolerance
    (e.g. a host-clock regression past the reconnect-overlap window)."""


def _tag(key: Hashable, seq: Iterable[tuple[int, Any]]) -> Iterator[tuple[int, Hashable, Any]]:
    """Tag a stream's events with their key (bound at call time, so the per-stream
    iterators don't all close over the loop's final key)."""
    for ts, val in seq:
        yield ts, key, val


def merge_latest(
    streams: Mapping[Hashable, Iterable[tuple[int, Any]]],
) -> Iterator[tuple[int, dict[Hashable, Any]]]:
    """Merge per-key timestamped streams into running-latest snapshots.

    Each stream is an iterable of `(ts_ns, value)` sorted ascending by ts (a list
    or a lazy generator - the merge pulls them lazily, so a streaming caller never
    holds a whole stream). Yields `(ts_ns, snapshot)` once per distinct timestamp
    in the union, where snapshot maps each key to its latest value with event
    ts <= the current ts. A key is absent from the snapshot until its first event;
    a `None` value is carried like any other (callers use it as an eviction
    sentinel and filter it out).

    A k-way heap merge of the (already-sorted) inputs, not a sort of their union:
    memory is the merge frontier (one element per stream), not every event.
    """
    merged = heapq.merge(
        *(_tag(key, seq) for key, seq in streams.items()),
        key=lambda e: e[0],
    )
    latest: dict[Hashable, Any] = {}
    for ts, group in groupby(merged, key=lambda e: e[0]):
        for _, key, val in group:  # apply all events at this tick first
            latest[key] = val
        yield ts, dict(latest)


def reorder(events: Iterable[tuple[int, Any]], window_ns: int) -> Iterator[tuple[int, Any]]:
    """Re-sort a nearly-sorted `(ts, value)` stream within a bounded lateness window.

    Inputs are ts-ascending except for bounded out-of-order arrivals (silver quote
    files are fold-ordered: ts-ascending within an epoch, with small reconnect-seam
    inversions). Buffers events in a min-heap keyed `(ts, read_idx)` and emits one
    once the watermark (max ts seen) has advanced `window_ns` past it - then no later
    event can undercut it, so output is strictly ts-ascending. `read_idx` breaks
    equal-ts ties by arrival (fold) order, reproducing a stable sort with no reliance
    on a tiebreak column. Memory is the window, not the stream: O(events within the
    last `window_ns`).

    Fail-loud: an event arriving with `ts < last_emitted` is more than `window_ns`
    late and cannot be placed - raise `LatenessError` rather than emit it out of order
    and corrupt a downstream `merge_latest`.
    """
    heap: list[tuple[int, int, Any]] = []
    watermark = 0
    last_emitted: int | None = None
    for idx, (ts, val) in enumerate(events):
        if last_emitted is not None and ts < last_emitted:
            raise LatenessError(
                f"event ts {ts} is {last_emitted - ts} ns behind last emitted "
                f"{last_emitted} (> window {window_ns} ns)"
            )
        heapq.heappush(heap, (ts, idx, val))
        watermark = max(watermark, ts)
        cutoff = watermark - window_ns
        while heap and heap[0][0] <= cutoff:
            ets, _, eval_ = heapq.heappop(heap)
            last_emitted = ets
            yield ets, eval_
    while heap:  # drain the tail in ts order at end-of-stream
        ets, _, eval_ = heapq.heappop(heap)
        yield ets, eval_
