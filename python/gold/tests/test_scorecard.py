"""Unit tests for the gold scorecard aggregation (pure, over silver rows)."""

from __future__ import annotations

import json
from decimal import Decimal

from gold.scorecard import SCORECARD_SCHEMA, build_scorecard, scorecard_table


def bq(**over) -> dict:
    row = {
        "exchange": "binance",
        "canonical_symbol": "BTC-USDT",
        "date": "2026-06-12",
        "kind": "delta",
        "offset": 0,
        "sequence": 1,
        "epoch": 0,
        "exchange_ts_ns": 0,
        "local_ts_ns": 0,
        "local_recv_ts_ns": None,
        "best_bid": None,
        "best_ask": None,
        "seq_gap": 0,
        "crossed": False,
        "invariant_kind": None,
    }
    row.update(over)
    return row


def lat(emit_ns: int, **over) -> dict:
    row = {
        "exchange": "binance",
        "canonical_symbol": "BTC-USDT",
        "date": "2026-06-12",
        "dataset": "trades",
        "offset": 0,
        "exchange_ts_ns": 0,
        "exchange_to_recv_ns": 0,
        "exchange_to_emit_ns": emit_ns,
    }
    row.update(over)
    return row


def st(state: str, ts_ns: int, **over) -> dict:
    row = {
        "exchange": "binance",
        "date": "2026-06-12",
        "ts_ns": ts_ns,
        "state": state,
        "prev_state": None,
        "is_transition": False,
        "downtime_ns": None,
    }
    row.update(over)
    return row


def by_check(rows: list[dict]) -> dict[str, dict]:
    return {r["check"]: r for r in rows}


def test_sequence_gap_counts_and_total_missing() -> None:
    rows = build_scorecard([bq(kind="snap"), bq(seq_gap=0), bq(seq_gap=2)], [], [])
    r = by_check(rows)["sequence_gap"]
    assert r["n_records"] == 2 and r["n_violations"] == 1
    assert json.loads(r["detail"]) == {"total_missing": 2, "max_gap": 2}


def test_book_invariant_breaks_down_by_kind_and_locked() -> None:
    rows = build_scorecard(
        [
            bq(
                invariant_kind="crossed_after_delta",
                crossed=True,
                best_bid=Decimal("101"),
                best_ask=Decimal("100"),
            ),
            bq(
                invariant_kind="crossed_after_delta",
                crossed=True,
                best_bid=Decimal("100"),
                best_ask=Decimal("100"),
            ),
            bq(),
        ],
        [],
        [],
    )
    r = by_check(rows)["book_invariant"]
    assert r["n_records"] == 3 and r["n_violations"] == 2
    detail = json.loads(r["detail"])
    assert detail["crossed_after_delta"] == 2 and detail["locked"] == 1


def test_latency_percentiles_in_ms() -> None:
    rows = build_scorecard([], [lat(1_000_000), lat(3_000_000)], [])
    r = by_check(rows)["latency.trades"]
    assert r["n_records"] == 2 and r["n_violations"] == 0
    assert r["p50_ms"] == 2.0


def test_venue_uptime_counts_down_transitions() -> None:
    rows = build_scorecard(
        [],
        [],
        [
            st("up", 100),
            st("down", 200, is_transition=True, prev_state="up"),
            st("up", 350, is_transition=True, prev_state="down", downtime_ns=150),
        ],
    )
    r = by_check(rows)["venue_uptime"]
    assert r["n_violations"] == 1  # one down-transition
    detail = json.loads(r["detail"])
    assert detail["down_transitions"] == 1 and detail["final_state"] == "up"


def test_rows_match_the_published_schema() -> None:
    rows = build_scorecard([bq(seq_gap=1)], [lat(1_000_000)], [st("up", 1)])
    table = scorecard_table(rows)
    assert table.schema == SCORECARD_SCHEMA
