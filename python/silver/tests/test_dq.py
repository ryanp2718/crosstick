"""Unit tests for the silver DQ transforms (pure, no infrastructure)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from analytics.corpus import CorpusRecord
from common.kafka_io import (
    book_delta_topic,
    book_snapshot_topic,
    latency_headers,
    status_topic,
    trade_topic,
)
from common.models import (
    BookDelta,
    BookLevel,
    BookSnapshot,
    Side,
    Status,
    Trade,
    encode,
)
from common.schemas import table_from_rows
from materializer.bronze import CanonicalMap
from silver.dq import BOOK_QUALITY_SCHEMA, book_partition_rows, build_silver, to_book_recs

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"
TS_MS = 1_700_000_000_000  # -> date 2023-11-14


def cmap() -> CanonicalMap:
    return CanonicalMap.from_yaml(INSTRUMENTS_FILE)


def _lv(px: str, sz: str) -> BookLevel:
    return BookLevel(px, sz)


def _rec(topic: str, msg, offset: int, *, recv_ns: int | None = None) -> CorpusRecord:
    headers = latency_headers(recv_ns, msg.exchange_ts_ns) if recv_ns is not None else []
    return CorpusRecord(
        topic=topic,
        partition=0,
        offset=offset,
        timestamp_ms=TS_MS,
        key=b"k",
        value=encode(msg),
        headers=headers,
    )


def _snap(ex, sym, seq, bids, asks, ex_ts=5, lo_ts=10, engine_ts=0):
    return BookSnapshot(
        exchange=ex,
        symbol=sym,
        sequence=seq,
        bids=[_lv(*b) for b in bids],
        asks=[_lv(*a) for a in asks],
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
        engine_ts_ns=engine_ts,
    )


def _delta(ex, sym, seq, bids, asks, ex_ts=5, lo_ts=10, engine_ts=0):
    return BookDelta(
        exchange=ex,
        symbol=sym,
        sequence=seq,
        bids=[_lv(*b) for b in bids],
        asks=[_lv(*a) for a in asks],
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
        engine_ts_ns=engine_ts,
    )


def _book_rows(facts, sequence):
    return [r for r in facts.book_quality if r["sequence"] == sequence]


def test_kraken_contiguous_gap_is_flagged() -> None:
    # kraken synthesizes a per-book +1 counter, so 6 -> 8 is a real hole.
    recs = [
        _rec(
            book_snapshot_topic("kraken", "BTC/USD"),
            _snap("kraken", "BTC/USD", 5, [("100", "1")], [("101", "1")]),
            0,
        ),
        _rec(
            book_delta_topic("kraken", "BTC/USD"),
            _delta("kraken", "BTC/USD", 6, [], [("101", "0.5")]),
            0,
        ),
        _rec(
            book_delta_topic("kraken", "BTC/USD"),
            _delta("kraken", "BTC/USD", 8, [("100", "2")], []),
            1,
        ),
    ]
    facts = build_silver(recs, cmap())
    (gapped,) = _book_rows(facts, 8)
    assert gapped["seq_gap"] == 1
    assert gapped["canonical_symbol"] == "BTC-USD"
    # the in-between delta is contiguous, no gap
    assert _book_rows(facts, 6)[0]["seq_gap"] == 0


def test_binance_update_ids_are_not_gaps() -> None:
    # binance stamps update-ids: monotonic but non-contiguous on a healthy stream.
    recs = [
        _rec(
            book_snapshot_topic("binance", "BTCUSDT"),
            _snap("binance", "BTCUSDT", 1000, [("100", "1")], [("101", "1")]),
            0,
        ),
        _rec(
            book_delta_topic("binance", "BTCUSDT"),
            _delta("binance", "BTCUSDT", 1007, [("100", "2")], []),
            0,
        ),
    ]
    facts = build_silver(recs, cmap())
    assert _book_rows(facts, 1007)[0]["seq_gap"] == 0


def test_crossed_delta_is_flagged() -> None:
    recs = [
        _rec(
            book_snapshot_topic("binance", "BTCUSDT"),
            _snap("binance", "BTCUSDT", 1000, [("64970", "5")], [("65030", "4")]),
            0,
        ),
        _rec(
            book_delta_topic("binance", "BTCUSDT"),
            _delta("binance", "BTCUSDT", 1001, [], [("65030", "3")]),
            0,
        ),
        _rec(
            book_delta_topic("binance", "BTCUSDT"),
            _delta("binance", "BTCUSDT", 1002, [("65040", "1")], []),
            1,
        ),
    ]
    facts = build_silver(recs, cmap())
    crossed = _book_rows(facts, 1002)[0]
    assert crossed["invariant_kind"] == "crossed_after_delta"
    assert crossed["crossed"] is True


def test_resnapshot_collision_is_not_a_non_monotonic_violation() -> None:
    # A periodic re-snapshot is stamped at the current sequence (here 7, an
    # existing delta's seq). Sorted snap-first, it precedes its same-seq delta,
    # which is then already incorporated -> skip, do not raise + clear the book.
    recs = [
        _rec(
            book_snapshot_topic("coinbase", "BTC-USD"),
            _snap("coinbase", "BTC-USD", 5, [("100", "1")], [("101", "1")]),
            0,
        ),
        _rec(
            book_delta_topic("coinbase", "BTC-USD"),
            _delta("coinbase", "BTC-USD", 6, [("100.5", "2")], []),
            0,
        ),
        _rec(
            book_delta_topic("coinbase", "BTC-USD"),
            _delta("coinbase", "BTC-USD", 7, [("100.6", "2")], []),
            1,
        ),
        _rec(
            book_snapshot_topic("coinbase", "BTC-USD"),
            _snap("coinbase", "BTC-USD", 7, [("100.6", "2")], [("101", "1")]),
            2,
        ),
        _rec(
            book_delta_topic("coinbase", "BTC-USD"),
            _delta("coinbase", "BTC-USD", 8, [("100.7", "2")], []),
            3,
        ),
    ]
    facts = build_silver(recs, cmap())
    # Neither the re-snapshot nor its colliding delta is flagged.
    assert all(r["invariant_kind"] is None for r in _book_rows(facts, 7))
    # Book is intact (not cleared): the next delta keeps both sides, so the
    # spread stays tight instead of going one-sided onto an emptied book.
    (eight,) = _book_rows(facts, 8)
    assert eight["best_bid"] == Decimal("100.7")
    assert eight["best_ask"] == Decimal("101")  # ask survived -> book was not cleared
    assert eight["invariant_kind"] is None


def test_book_quality_carries_engine_time_null_where_the_venue_has_none() -> None:
    """coinbase fills engine_ts_ns on its book messages; kraken publishes none, and
    neither does bronze from before the field. The column holds that stamp or
    null, never a 0 that reads as 1970, and exchange_ts_ns is untouched."""
    cb_snap = _rec(
        book_snapshot_topic("coinbase", "BTC-USD"),
        _snap("coinbase", "BTC-USD", 0, [("100", "1")], [("101", "1")], engine_ts=3),
        0,
    )
    cb_delta = _rec(
        book_delta_topic("coinbase", "BTC-USD"),
        _delta("coinbase", "BTC-USD", 1, [("100", "2")], [], engine_ts=4),
        0,
    )
    kraken = _rec(
        book_snapshot_topic("kraken", "BTC/USD"),
        _snap("kraken", "BTC/USD", 5, [("100", "1")], [("101", "1")]),
        0,
    )
    facts = build_silver([cb_snap, cb_delta, kraken], cmap())
    got = {(r["exchange"], r["kind"]): r["engine_ts_ns"] for r in facts.book_quality}
    assert got == {("coinbase", "snap"): 3, ("coinbase", "delta"): 4, ("kraken", "snap"): None}
    assert {r["exchange_ts_ns"] for r in facts.book_quality} == {5}

    table = table_from_rows(facts.book_quality, BOOK_QUALITY_SCHEMA)
    assert table.schema.field("engine_ts_ns").type == pa.int64()
    assert table.column("engine_ts_ns").null_count == 1

    # The streaming driver builds its records separately (to_book_recs), so check it too.
    streamed = book_partition_rows(
        to_book_recs([cb_snap], cmap()), to_book_recs([cb_delta], cmap()), "coinbase"
    )
    assert [bq["engine_ts_ns"] for bq, _, _ in streamed] == [3, 4]


def test_latency_skips_locally_generated_records() -> None:
    zero = Trade(
        exchange="binance",
        symbol="BTCUSDT",
        trade_id="1",
        price="1",
        size="1",
        side=Side.BID,
        exchange_ts_ns=0,
        local_ts_ns=10,
    )
    real = Trade(
        exchange="binance",
        symbol="BTCUSDT",
        trade_id="2",
        price="1",
        size="1",
        side=Side.BID,
        exchange_ts_ns=5,
        local_ts_ns=10,
    )
    recs = [
        _rec(trade_topic("binance", "BTCUSDT"), zero, 0, recv_ns=8),
        _rec(trade_topic("binance", "BTCUSDT"), real, 1, recv_ns=8),
    ]
    facts = build_silver(recs, cmap())
    assert len(facts.latency) == 1
    row = facts.latency[0]
    assert row["dataset"] == "trades"
    assert row["exchange_to_recv_ns"] == 3
    assert row["exchange_to_emit_ns"] == 5


def test_status_transitions_and_downtime() -> None:
    recs = [
        _rec(status_topic("binance"), Status(exchange="binance", state="up", ts_ns=100), 0),
        _rec(status_topic("binance"), Status(exchange="binance", state="down", ts_ns=200), 1),
        _rec(status_topic("binance"), Status(exchange="binance", state="up", ts_ns=350), 2),
    ]
    facts = build_silver(recs, cmap())
    events = sorted(facts.status_events, key=lambda r: r["ts_ns"])
    assert [e["is_transition"] for e in events] == [False, True, True]
    assert events[1]["state"] == "down"
    assert events[2]["downtime_ns"] == 150
