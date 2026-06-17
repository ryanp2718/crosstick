"""As-of merge for the silver/gold layers — the one definition of "as-of".

`merge_latest` folds several timestamped streams onto their union timeline,
carrying each stream's latest value forward. It is the shared primitive behind
two reconstructions:
  - silver NBBO: combine a canonical's per-venue quote streams (max-bid/min-ask
    on each tick), with a `None` value used to evict a venue's leg.
  - gold basis: combine two canonical NBBO streams (compute the spread on each
    tick, only where both legs are present).

Backward-only **by construction**: a snapshot at time T reflects only events with
ts <= T, never a future one — so point-in-time correctness is structural, not a
runtime guard. (A no-lookahead property test pins this.)
"""
from __future__ import annotations

import heapq
from collections.abc import Hashable, Iterable, Iterator, Mapping
from itertools import groupby
from typing import Any


def _tag(
    key: Hashable, seq: Iterable[tuple[int, Any]]
) -> Iterator[tuple[int, Hashable, Any]]:
    """Tag a stream's events with their key (bound at call time, so the per-stream
    iterators don't all close over the loop's final key)."""
    for ts, val in seq:
        yield ts, key, val


def merge_latest(
    streams: Mapping[Hashable, Iterable[tuple[int, Any]]],
) -> Iterator[tuple[int, dict[Hashable, Any]]]:
    """Merge per-key timestamped streams into running-latest snapshots.

    Each stream is an iterable of `(ts_ns, value)` sorted ascending by ts (a list
    or a lazy generator — the merge pulls them lazily, so a streaming caller never
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
