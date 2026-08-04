"""Depth-beyond-the-touch columns on silver `quotes`.

These pin the two claims a cross-venue model rests on: the cumulative sizes are
really cumulative (not per-level), and the rungs are capped at a depth every venue
can supply. The cap matters more than it looks - Kraken hard-trims to 10 levels, so
a deeper rung would be a feature only three of four venues could fill, and a
lead-lag model would read that gap as venue skill.
"""

from __future__ import annotations

from decimal import Decimal

from silver.dq import DEPTH_LEVELS, MAX_DEPTH, _BookRec, fold_book_partition


def _rec(bids: list[tuple[str, str]], asks: list[tuple[str, str]], seq: int = 1) -> _BookRec:
    return _BookRec(
        exchange="coinbase",
        symbol="BTC-USD",
        canonical="BTC-USD",
        date="2026-07-27",
        offset=seq,
        kind="snap",
        sequence=seq,
        epoch=1,
        bids=[(Decimal(p), Decimal(s)) for p, s in bids],
        asks=[(Decimal(p), Decimal(s)) for p, s in asks],
        exchange_ts_ns=1_000,
        local_ts_ns=2_000,
        local_recv_ts_ns=3_000,
    )


def _ladder(n: int, size: str = "1") -> tuple[list, list]:
    """`n` levels a side, one dollar apart, `size` on each."""
    bids = [(str(100 - i), size) for i in range(n)]
    asks = [(str(101 + i), size) for i in range(n)]
    return bids, asks


def _quote(bids, asks) -> dict:
    from silver.dq import _quote_row

    (ev,) = fold_book_partition([_rec(bids, asks)], [], "coinbase")
    row = _quote_row(ev)
    assert row is not None
    return row


# ── cumulative, not per-level ───────────────────────────────────────────────


def test_depth_is_cumulative_over_the_best_n_levels() -> None:
    q = _quote(*_ladder(20, size="2"))
    # 2 units on every level, so rung N holds exactly 2N.
    assert q["bid_depth_5"] == Decimal(10)
    assert q["bid_depth_10"] == Decimal(20)
    assert q["ask_depth_5"] == Decimal(10)
    assert q["ask_depth_10"] == Decimal(20)


def test_depth_includes_the_touch() -> None:
    """The touch is level 1, so the shallowest rung must contain bid_sz."""
    bids = [("100", "7"), ("99", "1"), ("98", "1"), ("97", "1"), ("96", "1")]
    asks = [("101", "3"), ("102", "1"), ("103", "1"), ("104", "1"), ("105", "1")]
    q = _quote(bids, asks)
    assert q["bid_sz"] == Decimal(7)
    assert q["bid_depth_5"] == Decimal(11)  # 7 + 4x1
    assert q["ask_depth_5"] == Decimal(7)  # 3 + 4x1


def test_depth_is_monotone_in_the_rung() -> None:
    q = _quote(*_ladder(20))
    for side in ("bid", "ask"):
        rungs = [q[f"{side}_depth_{n}"] for n in sorted(DEPTH_LEVELS)]
        assert rungs == sorted(rungs)


# ── the venue-symmetry cap ──────────────────────────────────────────────────


def test_depth_never_reaches_past_the_shallowest_venue() -> None:
    """A 500-level book must report the same rung a 10-level Kraken book can."""
    deep = _quote(*_ladder(500))
    trimmed = _quote(*_ladder(MAX_DEPTH))
    assert deep["bid_depth_10"] == trimmed["bid_depth_10"] == Decimal(MAX_DEPTH)
    assert deep["bid_px_10"] == trimmed["bid_px_10"]


def test_no_rung_is_deeper_than_max_depth() -> None:
    assert max(DEPTH_LEVELS) == MAX_DEPTH


# ── thin books ──────────────────────────────────────────────────────────────


def test_a_thin_book_reports_its_whole_side_not_a_null() -> None:
    """Three levels a side: the 10-rung is the whole side, and the price it reports
    is the worst level that exists, so (size, distance) stay a consistent pair."""
    q = _quote(*_ladder(3))
    assert q["bid_depth_10"] == Decimal(3)
    assert q["bid_depth_5"] == Decimal(3)
    assert q["bid_px_10"] == Decimal(98)  # 100, 99, 98
    assert q["ask_px_10"] == Decimal(103)  # 101, 102, 103


def test_depth_price_is_the_worst_level_walked() -> None:
    q = _quote(*_ladder(20))
    assert q["bid_px_10"] == Decimal(91)  # 100 down to 91 is ten levels
    assert q["ask_px_10"] == Decimal(110)
    assert q["bid_px_10"] < q["best_bid"]
    assert q["ask_px_10"] > q["best_ask"]


# ── it must not cost anything on the rows that carry no quote ───────────────


def test_resyncing_events_carry_no_depth() -> None:
    """A crossed snapshot yields no quote at all, so the fold must not have paid
    for a depth walk on it."""
    from silver.dq import _quote_row

    crossed = _rec([("102", "1")], [("101", "1")])
    (ev,) = fold_book_partition([crossed], [], "coinbase")
    assert ev.invariant_kind == "snapshot_crossed"
    assert ev.bid_levels == [] and ev.ask_levels == []
    assert _quote_row(ev) is None
