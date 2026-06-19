"""Silver data-quality transforms: bronze records -> validated DQ facts.

Pure (no I/O): everything here operates on an iterable of `CorpusRecord`s and a
`CanonicalMap`, so the *same* code runs in CI against the golden corpus and in
the batch job over the lake (silver/main.py). This is the validated/reconstructed
slice of silver (`DESIGN_analytics.md`): the remaining Phase-4 silver products
(full-depth cadence book-state, as-of-joined enriched trades, the full 3-way
oracle, checksum verification) extend the same layer.

Five fact streams, each canonical-resolved:
  - book_quality : one row per book event with crossed/invariant flags (from the
                   real ingest.book.OrderBook) and a sequence-gap count.
  - latency      : per-hop latency (ns) for every firehose record with headers.
  - status_events: typed venue up/down transitions with downtime.
  - quotes       : per-venue reconstructed top-of-book (best bid/ask + sizes) at
                   each book event with a valid two-sided book — the same
                   OrderBook fold as book_quality, one pass.
  - nbbo         : per-canonical reconstructed NBBO (max-bid/min-ask across a
                   canonical's venues, evicting a venue while its status is down).

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
"""
from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal

import pyarrow as pa

from analytics.corpus import CorpusRecord
from common.asof import merge_latest
from common.kafka_io import header_value
from common.models import BookDelta, BookSnapshot, Status, decode
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


@dataclass
class SilverFacts:
    """The silver fact streams produced from a slice of bronze."""

    book_quality: list[dict] = field(default_factory=list)
    latency: list[dict] = field(default_factory=list)
    status_events: list[dict] = field(default_factory=list)
    quotes: list[dict] = field(default_factory=list)
    nbbo: list[dict] = field(default_factory=list)


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
            exchange, canon, date, meta.dataset, rec.offset,
            msg.exchange_ts_ns, msg.local_ts_ns, recv,
        )
        if lat is not None:
            facts.latency.append(lat)

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
    # quotes — same reconstruction, so the two never disagree on best bid/ask.
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
    """One reconstructed book event — the shared output of the fold that both
    book_quality (quality flags) and quotes (top-of-book series) derive from."""

    rec: _BookRec
    seq_gap: int
    invariant_kind: str | None
    best_bid: Level | None
    best_ask: Level | None


# snap before delta at equal sequence (a re-snapshot replaces the book).
def _book_sort_key(r: _BookRec) -> tuple[int, int, int]:
    return (r.epoch, r.sequence, 0 if r.kind == "snap" else 1)


def fold_book_partition(
    snaps: Iterable[_BookRec], deltas: Iterable[_BookRec], exchange: str
) -> Iterator[_BookEvent]:
    """Reconstruct one (exchange, symbol) book, yielding an event per record.

    `snaps` and `deltas` must each already be in `(epoch, sequence)` order — they
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
            else:
                book.apply_delta(r.sequence, r.bids, r.asks)
        except BookInvariantError as e:
            invariant_kind = e.kind

        yield _BookEvent(r, seq_gap, invariant_kind, book.best_bid(), book.best_ask())
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
    """Decode bronze book records (snapshots or deltas) into `_BookRec`s — the
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
        r.exchange, r.canonical, r.date, dataset, r.offset,
        r.exchange_ts_ns, r.local_ts_ns, r.local_recv_ts_ns,
    )


def latency_rows(records: Iterable[CorpusRecord], canonical: CanonicalMap) -> Iterator[dict]:
    """Latency facts for a non-book latency dataset (trades/liquidations/
    mark_price/open_interest), streamed per partition by the driver."""
    for rec in records:
        meta = parse_topic(rec.topic)
        msg = decode(rec.value)
        exchange = meta.exchange or ""
        lat = _latency_dict(
            exchange, canonical.resolve(exchange, msg.symbol), record_date(rec.timestamp_ms),
            meta.dataset, rec.offset, msg.exchange_ts_ns, msg.local_ts_ns, _recv_ns(rec),
        )
        if lat is not None:
            yield lat


def _latency_dict(
    exchange: str, canon: str, date: str, dataset: str, offset: int,
    exchange_ts_ns: int, local_ts_ns: int, recv: int | None,
) -> dict | None:
    # Locally-generated records have no exchange clock (exchange_ts_ns == 0:
    # re-emitted snapshots, binance(-futures) snapshots) — skip their latency.
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


def _quote_row(ev: _BookEvent) -> dict | None:
    """A quote is the top of a *valid* two-sided book — skip events that raised
    an invariant (the book is resyncing) or are one-sided."""
    if ev.invariant_kind is not None or ev.best_bid is None or ev.best_ask is None:
        return None
    r = ev.rec
    ts = r.local_recv_ts_ns if r.local_recv_ts_ns is not None else r.local_ts_ns
    return {
        "exchange": r.exchange,
        "canonical_symbol": r.canonical,
        "date": r.date,
        "ts_ns": ts,
        "best_bid": ev.best_bid[0],
        "best_ask": ev.best_ask[0],
        "bid_sz": ev.best_bid[1],
        "ask_sz": ev.best_ask[1],
    }


def iter_nbbo(
    canonical: str, venue_streams: Mapping[str, Iterable[tuple[int, tuple | None]]]
) -> Iterator[dict]:
    """Per-canonical NBBO from already ts-sorted per-venue `(ts, (bid,ask)|None)`
    streams: max-bid / min-ask across the live venues on each tick, a `None` value
    evicting that venue's leg (DESIGN_nbbo.md connection-state eviction). The shared
    core of the in-memory `_build_nbbo` oracle and the streaming driver
    (silver/main.py) — the only difference is whether the venue streams are
    materialized sorted lists or lazy reorder+merge iterators. Backward-only via
    `merge_latest`. Ties on best bid/ask are broken deterministically by venue name
    so `bid_venue`/`ask_venue` don't depend on stream order (the price is identical
    either way)."""
    for ts, snap in merge_latest(venue_streams):
        live = {ex: q for ex, q in snap.items() if q is not None}
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
    """Down-transition timestamps per exchange — the NBBO eviction points. Shared by
    the `_build_nbbo` oracle and the streaming driver so the two can't disagree on
    what evicts a venue's leg."""
    downs: dict[str, list[int]] = defaultdict(list)
    for s in status_events:
        if s["state"] == "down" and s["is_transition"]:
            downs[s["exchange"]].append(s["ts_ns"])
    return downs


def _build_nbbo(quotes: list[dict], status_events: list[dict]) -> list[dict]:
    """Per-canonical NBBO from per-venue quotes — the in-memory oracle; the batch
    path streams per partition (silver/main.py). Builds each venue's ts-sorted stream
    (quotes + a `None` eviction sentinel at each down transition, carried forward
    until the venue requotes) and delegates the cross-venue merge to `iter_nbbo`."""
    downs = downs_by_exchange(status_events)

    by_canon: dict[str, dict[str, list[tuple[int, tuple | None]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for q in quotes:
        by_canon[q["canonical_symbol"]][q["exchange"]].append(
            (q["ts_ns"], (q["best_bid"], q["best_ask"]))
        )

    rows: list[dict] = []
    for canonical, venues in by_canon.items():
        streams: dict[str, list[tuple[int, tuple | None]]] = {}
        for exchange, evs in venues.items():
            evs = [*evs, *((dts, None) for dts in downs.get(exchange, []))]
            streams[exchange] = sorted(evs, key=lambda e: e[0])
        rows.extend(iter_nbbo(canonical, streams))
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


# Typed silver output contract. DECIMAL(38,18) is the portable canonical scale
# (DESIGN_analytics.md); ns timestamps and offsets are int64.
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
