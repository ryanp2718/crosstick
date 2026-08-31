"""The book fold's `(epoch, sequence)` precondition, enforced.

`fold_book_partition` merges its two streams with `heapq.merge`, which does not
validate its inputs: an unsorted stream yields an unsorted merge, silently. That
matters because `_fold` rebuilds the book on any epoch change, so a backward epoch
step throws away a live book and starts reconstructing from the older generation.
It leaves no trace in the output - the next delta refills the touch, and depth
recovers within a few records - so nothing downstream can detect it. These pin the
raise instead.

A non-monotonic epoch (the ingester reconnects after the wall clock steps backwards)
reaches the two paths differently. The streaming driver folds bronze in arrival order
and never sorts, so the key check sees the descent. `build_silver` sorts each stream
by the key first, which cannot leave the key unsorted, so it needs the second check:
each stream must stay in bronze-offset order through the merge. Both are pinned below.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from silver.dq import FoldOrderError, _book_sort_key, _BookRec, _fold, fold_book_partition


def _rec(kind: str, seq: int, epoch: int = 1, offset: int | None = None) -> _BookRec:
    return _BookRec(
        exchange="coinbase",
        symbol="BTC-USD",
        canonical="BTC-USD",
        date="2026-07-27",
        offset=seq if offset is None else offset,
        kind=kind,
        sequence=seq,
        epoch=epoch,
        bids=[(Decimal("100"), Decimal("1"))],
        asks=[(Decimal("101"), Decimal("1"))],
        exchange_ts_ns=1_000,
        local_ts_ns=2_000,
        local_recv_ts_ns=3_000,
    )


def test_an_ordered_partition_folds() -> None:
    snaps = [_rec("snap", 1)]
    deltas = [_rec("delta", 2), _rec("delta", 3)]
    events = list(fold_book_partition(snaps, deltas, "coinbase"))
    assert [ev.rec.sequence for ev in events] == [1, 2, 3]


def test_a_backward_epoch_raises() -> None:
    """The reconnect-storm shape: a stream carries a new generation's records and then
    the previous generation's, so the fold would rebuild the book from the older one.
    Both input streams being individually sorted is what the merge relies on, so this
    is the violation the merge cannot catch on its own."""
    deltas = [_rec("delta", 7, epoch=2), _rec("delta", 5000, epoch=1)]
    with pytest.raises(FoldOrderError, match="coinbase BTC-USD"):
        list(fold_book_partition([], deltas, "coinbase"))


def test_a_backward_sequence_within_an_epoch_raises() -> None:
    with pytest.raises(FoldOrderError, match=r"\(1, 9, 1\) -> \(1, 4, 1\)"):
        list(fold_book_partition([], [_rec("delta", 9), _rec("delta", 4)], "coinbase"))


def test_the_snap_first_tiebreak_is_part_of_the_order() -> None:
    """A re-snapshot replaces the book, so a delta at the same sequence must not
    precede it. The merge gets this right; `_fold` rejects a stream that does not."""
    ordered = fold_book_partition([_rec("snap", 5)], [_rec("delta", 5)], "coinbase")
    assert [ev.rec.kind for ev in ordered] == ["snap", "delta"]
    with pytest.raises(FoldOrderError, match=r"\(1, 5, 1\) -> \(1, 5, 0\)"):
        list(_fold([_rec("delta", 5), _rec("snap", 5)], contiguous=False))


def test_a_clock_inverted_epoch_raises_in_arrival_order() -> None:
    """The reconnect mints a LOWER epoch than the connection it replaces, because the
    wall clock stepped backwards. This is the streaming driver's view: bronze in disk
    order, unsorted, so the newer generation is physically first and the key descends."""
    snaps = [
        _rec("snap", 1, epoch=200, offset=10),  # first to arrive, higher epoch
        _rec("snap", 1, epoch=100, offset=20),  # reconnect after a backwards step
    ]
    with pytest.raises(FoldOrderError, match=r"\(200, 1, 0\) -> \(100, 1, 0\)"):
        list(fold_book_partition(snaps, [], "coinbase"))


def test_a_clock_inverted_epoch_raises_after_sorting() -> None:
    """The same inversion as seen by `build_silver`, which sorts each stream by the key
    before merging. That sort cannot leave the key unsorted, so the key check is vacuous
    here and the fold would carry the older generation's book. The offsets still hold
    the truth: the older epoch arrived second."""
    ordered = sorted(
        [_rec("snap", 1, epoch=200, offset=10), _rec("snap", 1, epoch=100, offset=20)],
        key=_book_sort_key,
    )
    with pytest.raises(FoldOrderError, match=r"offset 20 -> 10 at epoch 200"):
        list(_fold(ordered, contiguous=False))


def test_arrival_order_is_checked_per_stream() -> None:
    """Snapshot and delta offsets come from different topics, so they are independent
    sequences and interleaving them is not a descent."""
    snaps = [_rec("snap", 1, offset=500)]
    deltas = [_rec("delta", 2, offset=7), _rec("delta", 3, offset=8)]
    events = list(fold_book_partition(snaps, deltas, "coinbase"))
    assert [ev.rec.offset for ev in events] == [500, 7, 8]


def test_repeated_keys_are_not_a_violation() -> None:
    """Equal keys are allowed - a re-snapshot borrowing a delta's sequence is a real
    shape the fold already handles, so the check is strictly backward-only."""
    events = list(fold_book_partition([], [_rec("delta", 4), _rec("delta", 4)], "coinbase"))
    assert len(events) == 2
