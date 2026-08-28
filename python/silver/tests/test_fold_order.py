"""The book fold's `(epoch, sequence)` precondition, enforced.

`fold_book_partition` merges its two streams with `heapq.merge`, which does not
validate its inputs: an unsorted stream yields an unsorted merge, silently. That
matters because `_fold` rebuilds the book on any epoch change, so a backward epoch
step throws away a live book and starts reconstructing from the older generation.
It leaves no trace in the output - the next delta refills the touch, and depth
recovers within a few records - so nothing downstream can detect it. These pin the
raise instead.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from silver.dq import FoldOrderError, _BookRec, _fold, fold_book_partition


def _rec(kind: str, seq: int, epoch: int = 1) -> _BookRec:
    return _BookRec(
        exchange="coinbase",
        symbol="BTC-USD",
        canonical="BTC-USD",
        date="2026-07-27",
        offset=seq,
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


def test_repeated_keys_are_not_a_violation() -> None:
    """Equal keys are allowed - a re-snapshot borrowing a delta's sequence is a real
    shape the fold already handles, so the check is strictly backward-only."""
    events = list(fold_book_partition([], [_rec("delta", 4), _rec("delta", 4)], "coinbase"))
    assert len(events) == 2
