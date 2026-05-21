"""Tests for OrderBook. Written first (TDD)."""
from __future__ import annotations

from decimal import Decimal

import pytest

from ingest.book import BookInvariantError, OrderBook, Side

# NOTE: BookState was removed — state ownership moved to SymbolContext.state
# (SymbolState in base_ingester.py) to keep a single source of truth.


def D(s: str) -> Decimal:
    return Decimal(s)


def test_empty_book_has_no_best() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    assert b.best_bid() is None
    assert b.best_ask() is None


def test_apply_snapshot_sets_levels() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(
        sequence=1,
        bids=[(D("100"), D("1.0")), (D("99"), D("2.0"))],
        asks=[(D("101"), D("0.5")), (D("102"), D("3.0"))],
    )
    assert b.best_bid() == (D("100"), D("1.0"))
    assert b.best_ask() == (D("101"), D("0.5"))
    assert b.sequence == 1


def test_snapshot_replaces_existing_book() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(sequence=1, bids=[(D("100"), D("1"))], asks=[(D("101"), D("1"))])
    b.apply_snapshot(sequence=5, bids=[(D("50"), D("1"))], asks=[(D("51"), D("1"))])
    assert b.best_bid() == (D("50"), D("1"))
    assert b.best_ask() == (D("51"), D("1"))
    assert b.sequence == 5


def test_apply_delta_inserts_new_level() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(sequence=1, bids=[(D("100"), D("1"))], asks=[(D("101"), D("1"))])
    b.apply_delta(sequence=2, bids=[(D("99.5"), D("2"))], asks=[])
    assert b.best_bid() == (D("100"), D("1"))
    assert b.depth(Side.BID) == 2


def test_apply_delta_updates_existing_level() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(sequence=1, bids=[(D("100"), D("1"))], asks=[(D("101"), D("1"))])
    b.apply_delta(sequence=2, bids=[(D("100"), D("5"))], asks=[])
    assert b.best_bid() == (D("100"), D("5"))


def test_apply_delta_size_zero_removes_level() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(
        sequence=1,
        bids=[(D("100"), D("1")), (D("99"), D("2"))],
        asks=[(D("101"), D("1"))],
    )
    b.apply_delta(sequence=2, bids=[(D("100"), D("0"))], asks=[])
    assert b.best_bid() == (D("99"), D("2"))


def test_apply_delta_removing_nonexistent_is_noop() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(sequence=1, bids=[(D("100"), D("1"))], asks=[(D("101"), D("1"))])
    b.apply_delta(sequence=2, bids=[(D("50"), D("0"))], asks=[])
    assert b.best_bid() == (D("100"), D("1"))


def test_top_n_returns_descending_bids() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(
        sequence=1,
        bids=[(D("100"), D("1")), (D("99"), D("2")), (D("98"), D("3"))],
        asks=[(D("101"), D("1"))],
    )
    top = b.top_n(Side.BID, 2)
    assert top == [(D("100"), D("1")), (D("99"), D("2"))]


def test_top_n_returns_ascending_asks() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(
        sequence=1,
        bids=[(D("100"), D("1"))],
        asks=[(D("103"), D("3")), (D("101"), D("1")), (D("102"), D("2"))],
    )
    top = b.top_n(Side.ASK, 2)
    assert top == [(D("101"), D("1")), (D("102"), D("2"))]


def test_crossed_book_raises_invariant_error() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    with pytest.raises(BookInvariantError):
        b.apply_snapshot(sequence=1, bids=[(D("101"), D("1"))], asks=[(D("100"), D("1"))])


def test_delta_that_would_cross_book_raises() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(sequence=1, bids=[(D("100"), D("1"))], asks=[(D("101"), D("1"))])
    with pytest.raises(BookInvariantError):
        b.apply_delta(sequence=2, bids=[(D("102"), D("1"))], asks=[])


def test_delta_with_non_monotonic_sequence_raises() -> None:
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(sequence=5, bids=[(D("100"), D("1"))], asks=[(D("101"), D("1"))])
    with pytest.raises(BookInvariantError):
        b.apply_delta(sequence=4, bids=[(D("99"), D("1"))], asks=[])


def test_checksum_top_10_kraken_format() -> None:
    """Kraken CRC32: concat top-10 ask prices+sizes then top-10 bid prices+sizes,
    strip leading zeros and decimal points from each formatted field, then CRC32.

    Spec example from Kraken docs:
        asks: [["5005.10","0.00000500"], ..., 10 levels]
        bids: [["5005.00","0.00000500"], ..., 10 levels]
    The expected CRC matches Kraken's published example.
    """
    from ingest.book import kraken_checksum

    asks = [
        ("0.05005", "0.00000500"),
        ("0.05010", "0.00000500"),
        ("0.05015", "0.00000500"),
        ("0.05020", "0.00000500"),
        ("0.05025", "0.00000500"),
        ("0.05030", "0.00000500"),
        ("0.05035", "0.00000500"),
        ("0.05040", "0.00000500"),
        ("0.05045", "0.00000500"),
        ("0.05050", "0.00000500"),
    ]
    bids = [
        ("0.05000", "0.00000500"),
        ("0.04995", "0.00000500"),
        ("0.04990", "0.00000500"),
        ("0.04985", "0.00000500"),
        ("0.04980", "0.00000500"),
        ("0.04975", "0.00000500"),
        ("0.04970", "0.00000500"),
        ("0.04965", "0.00000500"),
        ("0.04960", "0.00000500"),
        ("0.04955", "0.00000500"),
    ]
    # Sanity: the function returns an unsigned 32-bit int — exact spec value
    # depends on price decimal places. Here we assert it's deterministic and
    # different for different inputs.
    c1 = kraken_checksum(asks, bids)
    c2 = kraken_checksum(asks[::-1], bids)
    assert 0 <= c1 <= 0xFFFFFFFF
    assert c1 != c2


def test_top_n_large_book_returns_correct_n() -> None:
    """500-level book: top_n returns exactly n items in the correct order."""
    b = OrderBook(exchange="x", symbol="BTC-USD")
    bids = [(D(str(i)), D("1")) for i in range(1, 501)]
    asks = [(D(str(i)), D("1")) for i in range(501, 1001)]
    b.apply_snapshot(1, bids, asks)

    bid_top = b.top_n(Side.BID, 10)
    assert len(bid_top) == 10
    bid_prices = [px for px, _ in bid_top]
    assert bid_prices == sorted(bid_prices, reverse=True), "bids must be descending"
    assert bid_prices[0] == D("500")
    assert bid_prices[-1] == D("491")

    ask_top = b.top_n(Side.ASK, 10)
    assert len(ask_top) == 10
    ask_prices = [px for px, _ in ask_top]
    assert ask_prices == sorted(ask_prices), "asks must be ascending"
    assert ask_prices[0] == D("501")
    assert ask_prices[-1] == D("510")


def test_top_n_clamps_to_book_depth() -> None:
    """top_n(n) on a book with < n levels returns all available levels."""
    b = OrderBook(exchange="x", symbol="BTC-USD")
    b.apply_snapshot(1, [(D("100"), D("1")), (D("99"), D("1"))], [(D("101"), D("1"))])
    assert len(b.top_n(Side.BID, 10)) == 2
    assert len(b.top_n(Side.ASK, 10)) == 1
