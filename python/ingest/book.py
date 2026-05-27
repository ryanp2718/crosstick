"""L2 order book backed by SortedDict.

Hot-path invariants enforced after every mutation:
    1. monotonic sequence numbers
    2. best_bid < best_ask
    3. no zero-size levels persist

On any violation, raise BookInvariantError — caller's responsibility to mark
the symbol STALE (via SymbolContext.state) and trigger a resync.
We do NOT silently patch.

State ownership: BookState was removed.  All lifecycle state lives in
SymbolContext.state (SymbolState enum in base_ingester.py) — a single
source of truth that prevents the two state machines from diverging.
"""
from __future__ import annotations

import zlib
from collections.abc import Iterable
from decimal import Decimal

from sortedcontainers import SortedDict

from common.models import Side

# A level is (price, size). Tuple, not dataclass, because allocation matters.
Level = tuple[Decimal, Decimal]


class BookInvariantError(Exception):
    """Raised when book state would violate an invariant.

    The caller MUST treat this as a signal to resync from snapshot.
    """


class OrderBook:
    __slots__ = ("_asks", "_bids", "exchange", "sequence", "symbol")

    def __init__(self, exchange: str, symbol: str):
        self.exchange = exchange
        self.symbol = symbol
        self._bids: SortedDict[Decimal, Decimal] = SortedDict()
        self._asks: SortedDict[Decimal, Decimal] = SortedDict()
        self.sequence: int = -1

    # ---- queries ---------------------------------------------------------

    def best_bid(self) -> Level | None:
        if not self._bids:
            return None
        px, sz = self._bids.peekitem(-1)
        return (px, sz)

    def best_ask(self) -> Level | None:
        if not self._asks:
            return None
        px, sz = self._asks.peekitem(0)
        return (px, sz)

    def depth(self, side: Side) -> int:
        return len(self._bids if side is Side.BID else self._asks)

    def top_n(self, side: Side, n: int) -> list[Level]:
        # SortedItemsView supports O(log k) indexed access and yields (price, size)
        # tuples directly — one tree walk per item instead of two (key then value).
        if side is Side.BID:
            items = self._bids.items()
            count = min(n, len(items))
            return [items[-1 - i] for i in range(count)]
        items = self._asks.items()
        count = min(n, len(items))
        return [items[i] for i in range(count)]

    # ---- mutations -------------------------------------------------------

    def apply_snapshot(
        self,
        sequence: int,
        bids: Iterable[Level],
        asks: Iterable[Level],
    ) -> None:
        new_bids: SortedDict[Decimal, Decimal] = SortedDict()
        new_asks: SortedDict[Decimal, Decimal] = SortedDict()
        for px, sz in bids:
            if sz > 0:
                new_bids[px] = sz
        for px, sz in asks:
            if sz > 0:
                new_asks[px] = sz
        # Validate before commit — snapshot itself must not be crossed.
        if new_bids and new_asks:
            bid_top = new_bids.peekitem(-1)[0]
            ask_top = new_asks.peekitem(0)[0]
            if bid_top >= ask_top:
                raise BookInvariantError(
                    f"snapshot crossed: best_bid={bid_top} >= best_ask={ask_top}"
                )
        self._bids = new_bids
        self._asks = new_asks
        self.sequence = sequence

    def apply_delta(
        self,
        sequence: int,
        bids: Iterable[Level],
        asks: Iterable[Level],
    ) -> None:
        if sequence <= self.sequence:
            raise BookInvariantError(
                f"non-monotonic sequence: prev={self.sequence} new={sequence}"
            )
        # Apply, then validate. If validate fails, restoring would be costly;
        # we leave the book in the post-apply state and the caller marks STALE.
        for px, sz in bids:
            if sz == 0:
                self._bids.pop(px, None)
            else:
                self._bids[px] = sz
        for px, sz in asks:
            if sz == 0:
                self._asks.pop(px, None)
            else:
                self._asks[px] = sz
        if self._bids and self._asks:
            bid_top = self._bids.peekitem(-1)[0]
            ask_top = self._asks.peekitem(0)[0]
            if bid_top >= ask_top:
                raise BookInvariantError(
                    f"crossed after delta: best_bid={bid_top} >= best_ask={ask_top}"
                )
        self.sequence = sequence

    def trim(self, depth: int) -> tuple[list[Decimal], list[Decimal]]:
        """Drop levels beyond the `depth` best on each side; return evicted prices.

        Depth-limited feeds (Kraken v2 book) only track the top N levels. When a
        level falls out of that window the exchange drops it WITHOUT sending an
        explicit delete, so we must discard it ourselves — otherwise a stale deep
        level resurfaces into our top-N when better levels are removed and the
        book silently diverges from the exchange's (failing the checksum).

        Returns (evicted_bid_prices, evicted_ask_prices) so the caller can relay
        explicit deletes downstream, keeping the published delta self-consistent.
        """
        evicted_bids: list[Decimal] = []
        evicted_asks: list[Decimal] = []
        while len(self._bids) > depth:
            evicted_bids.append(self._bids.popitem(0)[0])   # lowest bid = worst
        while len(self._asks) > depth:
            evicted_asks.append(self._asks.popitem(-1)[0])  # highest ask = worst
        return evicted_bids, evicted_asks

    def clear(self) -> None:
        """Reset to empty state. Called by _reset_contexts() before each reconnect."""
        self._bids.clear()
        self._asks.clear()
        self.sequence = -1


# ---------------------------------------------------------------------------
# Kraken v2 CRC32 checksum
#
# Per the v2 spec: concatenate the top-10 ask price-and-qty fields, then the
# top-10 bid price-and-qty fields. For each value, strip the decimal point and
# any leading zeros — produce the raw significant digits. CRC32 of that ASCII
# string, compared as unsigned 32-bit int to the `checksum` field.
# ---------------------------------------------------------------------------


def _kraken_strip(value: str) -> str:
    """Strip decimal point and leading zeros for CRC input."""
    s = value.replace(".", "").lstrip("0")
    return s if s else "0"


def kraken_checksum(
    asks_top10: Iterable[tuple[str, str]],
    bids_top10: Iterable[tuple[str, str]],
) -> int:
    buf = []
    for px, sz in asks_top10:
        buf.append(_kraken_strip(px))
        buf.append(_kraken_strip(sz))
    for px, sz in bids_top10:
        buf.append(_kraken_strip(px))
        buf.append(_kraken_strip(sz))
    return zlib.crc32("".join(buf).encode("ascii"))
