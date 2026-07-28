"""Silver data-quality transforms: bronze records -> validated DQ facts.

Pure (no I/O): everything here operates on an iterable of `CorpusRecord`s and a
`CanonicalMap`, so the *same* code runs in CI against the golden corpus and in
the batch job over the lake (silver/main.py). This is the validated/reconstructed
slice of silver: the remaining Phase-4 silver products
(full-depth cadence book-state, as-of-joined enriched trades, the full 3-way
oracle, checksum verification) extend the same layer.

Five fact streams, each canonical-resolved:
  - book_quality : one row per book event with crossed/invariant flags (from the
                   real ingest.book.OrderBook) and a sequence-gap count.
  - latency      : per-hop latency (ns) for every firehose record with headers.
  - status_events: typed venue up/down transitions with downtime.
  - quotes       : per-venue reconstructed top-of-book (best bid/ask + sizes) plus
                   cumulative depth at the DEPTH_LEVELS rungs, at each book event
                   with a valid two-sided book - the same OrderBook fold as
                   book_quality, one pass.
  - nbbo         : per-canonical reconstructed NBBO (max-bid/min-ask across a
                   canonical's venues, evicting a venue while its status is down).

Plus the four tape datasets (TAPE_DATASETS): trades, liquidations, mark_price,
open_interest, carried over from bronze verbatim rather than derived. They are
market-data content, not quality facts, and they exist ONLY in bronze upstream -
which expires on a lifecycle rule - so silver is what makes them durable.

Sequence-gap policy is per-exchange and *grounded in each driver* (cry-wolf is
worse than a miss for a quality floor):
  - kraken synthesizes a per-book counter (`seq = last_seq + 1`, kraken.py), so a
    hole is a genuine ingest->bronze loss -> CONTIGUOUS.
  - coinbase stamps a connection-wide counter shared across channels (trades and
    heartbeats legitimately consume numbers between book deltas, coinbase.py), so
    contiguity on book deltas alone would false-positive; its real gaps are
    already caught at ingest -> MONOTONIC.
  - binance / binance-futures stamp update-ids (`u`); the true continuity field
    (`U`/`pu`) does not survive normalization -> MONOTONIC.
Non-monotonic regressions are caught for *all* exchanges by the OrderBook fold
(it raises `non_monotonic_seq`); the contiguous policy only adds detection of
monotonic-but-missing gaps that the OrderBook cannot see (e.g. kraken 6 -> 8).
One exception is folded in `_fold`: a periodic re-snapshot is stamped at an
existing delta's sequence, so that delta sorts right after it (snap-first) and is
already incorporated -> skipped as redundant, not flagged as a regression.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal

import pyarrow as pa

from analytics.corpus import CorpusRecord
from common.asof import MAX_LEG_AGE_NS, merge_latest
from common.kafka_io import header_value
from common.models import (
    BookDelta,
    BookSnapshot,
    Liquidation,
    MarkPrice,
    OpenInterest,
    Side,
    Status,
    Trade,
    decode,
)
from ingest.book import BookInvariantError, Level, OrderBook
from materializer.bronze import CanonicalMap, parse_topic, record_date

# Exchanges whose book-delta sequence is a per-book contiguous counter; all
# others are treated as monotonic-only (forward jumps are normal).
CONTIGUOUS_SEQ_EXCHANGES = frozenset({"kraken"})

BOOK_DATASETS = frozenset({"book_snapshots", "book_deltas"})
# Firehose datasets that carry latency headers (status/bbo/nbbo carry none).
LATENCY_DATASETS = frozenset(
    {"book_snapshots", "book_deltas", "trades", "liquidations", "mark_price", "open_interest"}
)
CROSSED_KINDS = frozenset({"snapshot_crossed", "crossed_after_delta"})

# Depth rungs carried into `quotes` beyond the touch, as cumulative size over the
# best N levels a side.
#
# Ten is not a tuning choice, it is the floor across venues: Kraken's v2 book
# channel is subscribed at depth 10 (ingest/kraken.py DEFAULT_DEPTH) and hard-trims
# past it, so ten is the deepest rung EVERY venue can supply. Going deeper - or
# using price-relative windows ("size within 25bps"), which coinbase and binance
# could fill and kraken structurally could not - would hand three venues a feature
# the fourth cannot have, and a cross-venue lead-lag model reads that asymmetry as
# venue skill. Symmetry beats resolution here.
MAX_DEPTH = 10
DEPTH_LEVELS = (5, 10)


# Bronze tape datasets lifted into silver verbatim, canonical-resolved and on the
# quotes clock. The facts above *describe* the feed; these ARE the feed, and bronze
# expires on a lifecycle rule, so anything not lifted here is unrecoverable.
# Silver keeps the bronze dataset names (the funding rate is mark_price.funding_rate).
TAPE_DATASETS = ("trades", "liquidations", "mark_price", "open_interest")


@dataclass
class SilverFacts:
    """The silver fact streams produced from a slice of bronze."""

    book_quality: list[dict] = field(default_factory=list)
    latency: list[dict] = field(default_factory=list)
    status_events: list[dict] = field(default_factory=list)
    quotes: list[dict] = field(default_factory=list)
    nbbo: list[dict] = field(default_factory=list)
    trades: list[dict] = field(default_factory=list)
    mark_price: list[dict] = field(default_factory=list)
    open_interest: list[dict] = field(default_factory=list)
    liquidations: list[dict] = field(default_factory=list)


@dataclass
class _BookRec:
    exchange: str
    symbol: str
    canonical: str
    date: str
    offset: int
    kind: str  # "snap" | "delta"
    sequence: int
    epoch: int
    bids: list[Level]
    asks: list[Level]
    exchange_ts_ns: int
    local_ts_ns: int
    local_recv_ts_ns: int | None


def _levels(msg: BookSnapshot | BookDelta) -> tuple[list[Level], list[Level]]:
    bids = [(Decimal(level.price), Decimal(level.size)) for level in msg.bids]
    asks = [(Decimal(level.price), Decimal(level.size)) for level in msg.asks]
    return bids, asks


def _recv_ns(rec: CorpusRecord) -> int | None:
    raw = header_value(rec.headers, "local_recv_ts_ns")
    return int(raw) if raw is not None else None


def build_silver(records: Iterable[CorpusRecord], canonical: CanonicalMap) -> SilverFacts:
    """Transform a slice of bronze into the three silver DQ-fact streams."""
    facts = SilverFacts()
    # (exchange, native symbol) -> book records, folded after classification so
    # snapshots and deltas (separate topics) merge into one ordered stream.
    books: dict[tuple[str, str], list[_BookRec]] = {}
    status_by_exchange: dict[str, list[CorpusRecord]] = {}

    for rec in records:
        meta = parse_topic(rec.topic)
        if meta.dataset == "status":
            status_by_exchange.setdefault(meta.exchange or "", []).append(rec)
            continue
        if meta.dataset not in LATENCY_DATASETS:
            continue  # bbo / nbbo: derived, validated live by the gateway
        msg = decode(rec.value)
        exchange = meta.exchange or ""
        canon = canonical.resolve(exchange, msg.symbol)
        date = record_date(rec.timestamp_ms)
        recv = _recv_ns(rec)

        lat = _latency_dict(
            exchange,
            canon,
            date,
            meta.dataset,
            rec.offset,
            msg.exchange_ts_ns,
            msg.local_ts_ns,
            recv,
        )
        if lat is not None:
            facts.latency.append(lat)

        if meta.dataset in _TAPE_ROW:
            base = _tape_base(exchange, canon, date, rec.offset, msg, recv)
            getattr(facts, meta.dataset).append(_TAPE_ROW[meta.dataset](base, msg))

        if meta.dataset in BOOK_DATASETS:
            bids, asks = _levels(msg)
            books.setdefault((exchange, msg.symbol), []).append(
                _BookRec(
                    exchange=exchange,
                    symbol=msg.symbol,
                    canonical=canon,
                    date=date,
                    offset=rec.offset,
                    kind="snap" if isinstance(msg, BookSnapshot) else "delta",
                    sequence=msg.sequence,
                    epoch=msg.epoch,
                    bids=bids,
                    asks=asks,
                    exchange_ts_ns=msg.exchange_ts_ns,
                    local_ts_ns=msg.local_ts_ns,
                    local_recv_ts_ns=recv,
                )
            )

    # One OrderBook fold per (exchange, symbol) feeds BOTH book_quality and
    # quotes - same reconstruction, so the two never disagree on best bid/ask.
    # Snapshots and deltas are sorted per stream then merged, the same path the
    # streaming driver feeds from disk-ordered files (silver/main.py).
    for (exchange, _symbol), recs in books.items():
        snaps = sorted((r for r in recs if r.kind == "snap"), key=_book_sort_key)
        deltas = sorted((r for r in recs if r.kind == "delta"), key=_book_sort_key)
        for ev in fold_book_partition(snaps, deltas, exchange):
            facts.book_quality.append(_book_quality_row(ev))
            quote = _quote_row(ev)
            if quote is not None:
                facts.quotes.append(quote)
    for exchange, recs in status_by_exchange.items():
        facts.status_events.extend(_status_transitions(exchange, recs))
    facts.nbbo.extend(_build_nbbo(facts.quotes, facts.status_events))
    return facts


@dataclass
class _BookEvent:
    """One reconstructed book event - the shared output of the fold that both
    book_quality (quality flags) and quotes (top-of-book series) derive from.

    `bid_levels`/`ask_levels` are the best `MAX_DEPTH` levels, populated only when
    the event will produce a quote (valid, two-sided). book_quality is emitted for
    every event including resyncing ones, so best_bid/best_ask stay separate rather
    than being derived from the level lists.
    """

    rec: _BookRec
    seq_gap: int
    invariant_kind: str | None
    best_bid: Level | None
    best_ask: Level | None
    bid_levels: list[Level] = field(default_factory=list)
    ask_levels: list[Level] = field(default_factory=list)


# snap before delta at equal sequence (a re-snapshot replaces the book).
def _book_sort_key(r: _BookRec) -> tuple[int, int, int]:
    return (r.epoch, r.sequence, 0 if r.kind == "snap" else 1)


def fold_book_partition(
    snaps: Iterable[_BookRec], deltas: Iterable[_BookRec], exchange: str
) -> Iterator[_BookEvent]:
    """Reconstruct one (exchange, symbol) book, yielding an event per record.

    `snaps` and `deltas` must each already be in `(epoch, sequence)` order - they
    are merged lazily by `(epoch, sequence, snap-first)` (so neither whole stream
    need be resident), the regime the bronze topics are written in. In-memory
    callers sort each stream first; the streaming driver feeds disk-ordered files.
    """
    merged = heapq.merge(snaps, deltas, key=_book_sort_key)
    yield from _fold(merged, exchange in CONTIGUOUS_SEQ_EXCHANGES)


def _fold(book_recs: Iterable[_BookRec], contiguous: bool) -> Iterator[_BookEvent]:
    book: OrderBook | None = None
    epoch: int | None = None
    for r in book_recs:
        if book is None or r.epoch != epoch:
            book = OrderBook(r.exchange, r.symbol)
            epoch = r.epoch

        seq_gap = 0
        if (
            r.kind == "delta"
            and contiguous
            and book.sequence >= 0
            and r.sequence > book.sequence + 1
        ):
            seq_gap = r.sequence - book.sequence - 1

        invariant_kind: str | None = None
        try:
            if r.kind == "snap":
                book.apply_snapshot(r.sequence, r.bids, r.asks)
            elif r.sequence == book.sequence:
                pass  # re-snapshot borrowed this delta's seq; already incorporated
            else:
                book.apply_delta(r.sequence, r.bids, r.asks)
        except BookInvariantError as e:
            invariant_kind = e.kind

        bb, ba = book.best_bid(), book.best_ask()
        # Walking depth costs a tree read per level, so only do it on events that
        # will actually emit a quote (`_quote_row` drops the rest).
        deep = invariant_kind is None and bb is not None and ba is not None
        yield _BookEvent(
            r,
            seq_gap,
            invariant_kind,
            bb,
            ba,
            book.top_n(Side.BID, MAX_DEPTH) if deep else [],
            book.top_n(Side.ASK, MAX_DEPTH) if deep else [],
        )
        if invariant_kind is not None:
            book.clear()  # resync from the next snapshot, as the ingester does


def book_partition_rows(
    snaps: Iterable[_BookRec], deltas: Iterable[_BookRec], exchange: str
) -> Iterator[tuple[dict, dict | None, dict | None]]:
    """Per book event, the three output rows it produces: (book_quality, quote or
    None, book latency or None). The streaming driver's clean seam over the fold."""
    for ev in fold_book_partition(snaps, deltas, exchange):
        yield _book_quality_row(ev), _quote_row(ev), book_latency_row(ev.rec)


def to_book_recs(records: Iterable[CorpusRecord], canonical: CanonicalMap) -> Iterator[_BookRec]:
    """Decode bronze book records (snapshots or deltas) into `_BookRec`s - the
    streaming driver's per-partition adapter into `fold_book_partition`."""
    for rec in records:
        meta = parse_topic(rec.topic)
        msg = decode(rec.value)
        exchange = meta.exchange or ""
        bids, asks = _levels(msg)
        yield _BookRec(
            exchange=exchange,
            symbol=msg.symbol,
            canonical=canonical.resolve(exchange, msg.symbol),
            date=record_date(rec.timestamp_ms),
            offset=rec.offset,
            kind="snap" if isinstance(msg, BookSnapshot) else "delta",
            sequence=msg.sequence,
            epoch=msg.epoch,
            bids=bids,
            asks=asks,
            exchange_ts_ns=msg.exchange_ts_ns,
            local_ts_ns=msg.local_ts_ns,
            local_recv_ts_ns=_recv_ns(rec),
        )


def book_latency_row(r: _BookRec) -> dict | None:
    """Latency fact for a book record (the streaming driver emits these during
    the fold, since the fold already holds the decoded record)."""
    dataset = "book_snapshots" if r.kind == "snap" else "book_deltas"
    return _latency_dict(
        r.exchange,
        r.canonical,
        r.date,
        dataset,
        r.offset,
        r.exchange_ts_ns,
        r.local_ts_ns,
        r.local_recv_ts_ns,
    )


def tape_and_latency_rows(
    records: Iterable[CorpusRecord], canonical: CanonicalMap, dataset: str
) -> Iterator[tuple[dict, dict | None]]:
    """One decode pass over a non-book partition -> `(tape row, latency row)`, the
    latency row None where there is no exchange clock. The tape datasets are exactly
    the non-book latency datasets, so the driver writes both from one bronze read."""
    build = _TAPE_ROW[dataset]
    for rec in records:
        meta = parse_topic(rec.topic)
        msg = decode(rec.value)
        exchange = meta.exchange or ""
        canon = canonical.resolve(exchange, msg.symbol)
        date = record_date(rec.timestamp_ms)
        recv = _recv_ns(rec)
        lat = _latency_dict(
            exchange,
            canon,
            date,
            meta.dataset,
            rec.offset,
            msg.exchange_ts_ns,
            msg.local_ts_ns,
            recv,
        )
        yield build(_tape_base(exchange, canon, date, rec.offset, msg, recv), msg), lat


def _latency_dict(
    exchange: str,
    canon: str,
    date: str,
    dataset: str,
    offset: int,
    exchange_ts_ns: int,
    local_ts_ns: int,
    recv: int | None,
) -> dict | None:
    # Locally-generated records have no exchange clock (exchange_ts_ns == 0:
    # re-emitted snapshots, binance(-futures) snapshots) - skip their latency.
    if exchange_ts_ns == 0 or recv is None:
        return None
    return {
        "exchange": exchange,
        "canonical_symbol": canon,
        "date": date,
        "dataset": dataset,
        "offset": offset,
        "exchange_ts_ns": exchange_ts_ns,
        "exchange_to_recv_ns": recv - exchange_ts_ns,
        "exchange_to_emit_ns": local_ts_ns - exchange_ts_ns,
    }


def _tape_base(exchange: str, canon: str, date: str, offset: int, msg, recv: int | None) -> dict:
    """Identity + the clock every tape row shares with `quotes`: local_recv_ts_ns
    (one gateway clock, so cross-venue joins are valid) falling back to the emit
    clock. `exchange_ts_ns` stays for reference under the clock-domain caveat."""
    return {
        "exchange": exchange,
        "canonical_symbol": canon,
        "date": date,
        "ts_ns": recv if recv is not None else msg.local_ts_ns,
        "offset": offset,
        "exchange_ts_ns": msg.exchange_ts_ns,
        "local_ts_ns": msg.local_ts_ns,
    }


def _trades_row(base: dict, msg: Trade) -> dict:
    # `side` is the taker/aggressor direction in every driver (BID = buyer-initiated),
    # so order-flow sign needs no Lee-Ready inference.
    return {
        **base,
        "trade_id": msg.trade_id,
        "price": Decimal(msg.price),
        "size": Decimal(msg.size),
        "side": str(msg.side),
    }


def _mark_price_row(base: dict, msg: MarkPrice) -> dict:
    return {
        **base,
        "mark_price": Decimal(msg.mark_price),
        "index_price": Decimal(msg.index_price),
        "est_settle_price": Decimal(msg.est_settle_price),
        "funding_rate": Decimal(msg.funding_rate),
        "next_funding_ts_ns": msg.next_funding_ts_ns,
    }


def _open_interest_row(base: dict, msg: OpenInterest) -> dict:
    return {**base, "open_interest": Decimal(msg.open_interest)}


def _liquidations_row(base: dict, msg: Liquidation) -> dict:
    return {
        **base,
        "side": str(msg.side),
        "price": Decimal(msg.price),
        "avg_price": Decimal(msg.avg_price),
        "orig_size": Decimal(msg.orig_size),
        "filled_size": Decimal(msg.filled_size),
        "status": msg.status,
    }


_TAPE_ROW = {
    "trades": _trades_row,
    "mark_price": _mark_price_row,
    "open_interest": _open_interest_row,
    "liquidations": _liquidations_row,
}


def _book_quality_row(ev: _BookEvent) -> dict:
    r = ev.rec
    return {
        "exchange": r.exchange,
        "canonical_symbol": r.canonical,
        "date": r.date,
        "kind": r.kind,
        "offset": r.offset,
        "sequence": r.sequence,
        "epoch": r.epoch,
        "exchange_ts_ns": r.exchange_ts_ns,
        "local_ts_ns": r.local_ts_ns,
        "local_recv_ts_ns": r.local_recv_ts_ns,
        "best_bid": ev.best_bid[0] if ev.best_bid else None,
        "best_ask": ev.best_ask[0] if ev.best_ask else None,
        "seq_gap": ev.seq_gap,
        "crossed": ev.invariant_kind in CROSSED_KINDS,
        "invariant_kind": ev.invariant_kind,
    }


def _cum_size(levels: list[Level], n: int) -> Decimal:
    """Cumulative resting size over the best `n` levels, or over the whole side when
    the book is thinner than that."""
    return sum((sz for _, sz in levels[:n]), Decimal(0))


def _quote_row(ev: _BookEvent) -> dict | None:
    """A quote is the top of a *valid* two-sided book - skip events that raised
    an invariant (the book is resyncing) or are one-sided.

    Beyond the touch it carries cumulative size at the DEPTH_LEVELS rungs plus the
    worst price reached, which together give book slope: `bid_depth_10` is how much
    size is available and `bid_px_10` is how far down you walked to find it. Both
    are truncated to the levels the side actually has, so a book thinner than a rung
    reports its whole side rather than a null (and the pair stays consistent).
    """
    if ev.invariant_kind is not None or ev.best_bid is None or ev.best_ask is None:
        return None
    r = ev.rec
    ts = r.local_recv_ts_ns if r.local_recv_ts_ns is not None else r.local_ts_ns
    row = {
        "exchange": r.exchange,
        "canonical_symbol": r.canonical,
        "date": r.date,
        "ts_ns": ts,
        "best_bid": ev.best_bid[0],
        "best_ask": ev.best_ask[0],
        "bid_sz": ev.best_bid[1],
        "ask_sz": ev.best_ask[1],
    }
    for n in DEPTH_LEVELS:
        row[f"bid_depth_{n}"] = _cum_size(ev.bid_levels, n)
        row[f"ask_depth_{n}"] = _cum_size(ev.ask_levels, n)
    row["bid_px_10"] = ev.bid_levels[:MAX_DEPTH][-1][0] if ev.bid_levels else None
    row["ask_px_10"] = ev.ask_levels[:MAX_DEPTH][-1][0] if ev.ask_levels else None
    return row


def iter_nbbo(
    canonical: str,
    venue_streams: Mapping[str, Iterable[tuple[int, tuple | None]]],
    max_age_ns: int = MAX_LEG_AGE_NS,
) -> Iterator[dict]:
    """Per-canonical NBBO from already ts-sorted per-venue `(ts, (qts,bid,ask)|None)`
    streams: max-bid / min-ask across the live venues on each tick. A venue's leg is
    evicted either by a `None` value (status-down eviction, DESIGN_nbbo.md) or when
    its last quote is older than `max_age_ns` (staleness eviction - a quiet venue
    would otherwise be carried into a crossed/wide NBBO). Each value embeds its own
    quote ts (`qts`) so the carried-forward age is visible without changing
    `merge_latest`. The shared core of the in-memory `_build_nbbo` oracle and the
    streaming driver (silver/main.py) - the only difference is whether the venue
    streams are materialized sorted lists or lazy reorder+merge iterators.
    Backward-only via `merge_latest`. Ties on best bid/ask are broken
    deterministically by venue name so `bid_venue`/`ask_venue` don't depend on
    stream order (the price is identical either way)."""
    for ts, snap in merge_latest(venue_streams):
        live: dict[str, tuple] = {}
        for ex, q in snap.items():
            if q is None or ts - q[0] > max_age_ns:
                continue  # status-down, or a stale (frozen/quiet) leg
            live[ex] = (q[1], q[2])
        if not live:
            continue
        bid_venue, bid_q = max(live.items(), key=lambda kv: (kv[1][0], kv[0]))
        ask_venue, ask_q = min(live.items(), key=lambda kv: (kv[1][1], kv[0]))
        yield {
            "canonical_symbol": canonical,
            "date": record_date(ts // 1_000_000),
            "ts_ns": ts,
            "best_bid": bid_q[0],
            "best_ask": ask_q[1],
            "bid_venue": bid_venue,
            "ask_venue": ask_venue,
            "n_venues": len(live),
        }


def downs_by_exchange(status_events: Iterable[dict]) -> dict[str, list[int]]:
    """Down-transition timestamps per exchange - the NBBO eviction points. Shared by
    the `_build_nbbo` oracle and the streaming driver so the two can't disagree on
    what evicts a venue's leg."""
    downs: dict[str, list[int]] = defaultdict(list)
    for s in status_events:
        if s["state"] == "down" and s["is_transition"]:
            downs[s["exchange"]].append(s["ts_ns"])
    return downs


def _build_nbbo(
    quotes: list[dict], status_events: list[dict], max_age_ns: int = MAX_LEG_AGE_NS
) -> list[dict]:
    """Per-canonical NBBO from per-venue quotes - the in-memory oracle; the batch
    path streams per partition (silver/main.py). Builds each venue's ts-sorted stream
    (quotes + a `None` eviction sentinel at each down transition, carried forward
    until the venue requotes) and delegates the cross-venue merge to `iter_nbbo`.
    Each quote value embeds its own ts so iter_nbbo can evict a stale leg."""
    downs = downs_by_exchange(status_events)

    by_canon: dict[str, dict[str, list[tuple[int, tuple | None]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for q in quotes:
        by_canon[q["canonical_symbol"]][q["exchange"]].append(
            (q["ts_ns"], (q["ts_ns"], q["best_bid"], q["best_ask"]))
        )

    rows: list[dict] = []
    for canonical, venues in by_canon.items():
        streams: dict[str, list[tuple[int, tuple | None]]] = {}
        for exchange, evs in venues.items():
            evs = [*evs, *((dts, None) for dts in downs.get(exchange, []))]
            streams[exchange] = sorted(evs, key=lambda e: e[0])
        rows.extend(iter_nbbo(canonical, streams, max_age_ns))
    return rows


def _status_transitions(exchange: str, recs: list[CorpusRecord]) -> list[dict]:
    recs = sorted(recs, key=lambda rec: decode(rec.value).ts_ns)
    rows: list[dict] = []
    prev_state: str | None = None
    down_since: int | None = None
    for rec in recs:
        st: Status = decode(rec.value)
        downtime_ns = None
        if st.state == "up" and prev_state == "down" and down_since is not None:
            downtime_ns = st.ts_ns - down_since
        if st.state == "down":
            down_since = st.ts_ns
        rows.append(
            {
                "exchange": exchange,
                "date": record_date(rec.timestamp_ms),
                "ts_ns": st.ts_ns,
                "state": st.state,
                "prev_state": prev_state,
                "is_transition": prev_state is not None and st.state != prev_state,
                "downtime_ns": downtime_ns,
            }
        )
        prev_state = st.state
    return rows


# Typed silver output contract. DECIMAL(38,18) is the portable canonical scale;
# ns timestamps and offsets are int64.
_PRICE = pa.decimal128(38, 18)

BOOK_QUALITY_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("kind", pa.string()),
        ("offset", pa.int64()),
        ("sequence", pa.int64()),
        ("epoch", pa.int64()),
        ("exchange_ts_ns", pa.int64()),
        ("local_ts_ns", pa.int64()),
        ("local_recv_ts_ns", pa.int64()),
        ("best_bid", _PRICE),
        ("best_ask", _PRICE),
        ("seq_gap", pa.int64()),
        ("crossed", pa.bool_()),
        ("invariant_kind", pa.string()),
    ]
)

LATENCY_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("dataset", pa.string()),
        ("offset", pa.int64()),
        ("exchange_ts_ns", pa.int64()),
        ("exchange_to_recv_ns", pa.int64()),
        ("exchange_to_emit_ns", pa.int64()),
    ]
)

STATUS_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("state", pa.string()),
        ("prev_state", pa.string()),
        ("is_transition", pa.bool_()),
        ("downtime_ns", pa.int64()),
    ]
)

QUOTES_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("best_bid", _PRICE),
        ("best_ask", _PRICE),
        ("bid_sz", _PRICE),
        ("ask_sz", _PRICE),
        # Depth beyond the touch: cumulative size at each DEPTH_LEVELS rung, and the
        # worst price the deepest rung reaches (size + distance = book slope).
        *[(f"{side}_depth_{n}", _PRICE) for n in DEPTH_LEVELS for side in ("bid", "ask")],
        ("bid_px_10", _PRICE),
        ("ask_px_10", _PRICE),
    ]
)

NBBO_SCHEMA = pa.schema(
    [
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("best_bid", _PRICE),
        ("best_ask", _PRICE),
        ("bid_venue", pa.string()),
        ("ask_venue", pa.string()),
        ("n_venues", pa.int64()),
    ]
)

# Tape schemas. The leading seven columns are `_tape_base` and are identical across
# all four, so a feature build can join any tape to `quotes` on (exchange, symbol, ts_ns).
_TAPE_BASE_FIELDS = [
    ("exchange", pa.string()),
    ("canonical_symbol", pa.string()),
    ("date", pa.string()),
    ("ts_ns", pa.int64()),
    ("offset", pa.int64()),
    ("exchange_ts_ns", pa.int64()),
    ("local_ts_ns", pa.int64()),
]

TRADES_SCHEMA = pa.schema(
    [
        *_TAPE_BASE_FIELDS,
        ("trade_id", pa.string()),
        ("price", _PRICE),
        ("size", _PRICE),
        ("side", pa.string()),
    ]
)

MARK_PRICE_SCHEMA = pa.schema(
    [
        *_TAPE_BASE_FIELDS,
        ("mark_price", _PRICE),
        ("index_price", _PRICE),
        ("est_settle_price", _PRICE),
        ("funding_rate", _PRICE),
        ("next_funding_ts_ns", pa.int64()),
    ]
)

OPEN_INTEREST_SCHEMA = pa.schema([*_TAPE_BASE_FIELDS, ("open_interest", _PRICE)])

LIQUIDATIONS_SCHEMA = pa.schema(
    [
        *_TAPE_BASE_FIELDS,
        ("side", pa.string()),
        ("price", _PRICE),
        ("avg_price", _PRICE),
        ("orig_size", _PRICE),
        ("filled_size", _PRICE),
        ("status", pa.string()),
    ]
)

TAPE_SCHEMAS = {
    "trades": TRADES_SCHEMA,
    "mark_price": MARK_PRICE_SCHEMA,
    "open_interest": OPEN_INTEREST_SCHEMA,
    "liquidations": LIQUIDATIONS_SCHEMA,
}
