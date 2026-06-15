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

from collections.abc import Hashable, Iterator, Mapping, Sequence
from typing import Any


def merge_latest(
    streams: Mapping[Hashable, Sequence[tuple[int, Any]]],
) -> Iterator[tuple[int, dict[Hashable, Any]]]:
    """Merge per-key timestamped streams into running-latest snapshots.

    Each stream is a sequence of `(ts_ns, value)` sorted ascending by ts. Yields
    `(ts_ns, snapshot)` once per distinct timestamp in the union, where snapshot
    maps each key to its latest value with event ts <= the current ts. A key is
    absent from the snapshot until its first event; a `None` value is carried
    like any other (callers use it as an eviction sentinel and filter it out).
    """
    events = sorted(
        ((ts, key, val) for key, seq in streams.items() for ts, val in seq),
        key=lambda e: e[0],
    )
    latest: dict[Hashable, Any] = {}
    i, n = 0, len(events)
    while i < n:
        ts = events[i][0]
        while i < n and events[i][0] == ts:  # apply all events at this tick first
            _, key, val = events[i]
            latest[key] = val
            i += 1
        yield ts, dict(latest)
