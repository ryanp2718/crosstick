"""Gold data-quality scorecard: aggregate silver DQ facts into a mart.

Pure (no I/O): aggregates the three silver fact streams into a keyed fact table
`(exchange, canonical_symbol, date, check)`. A gold mart reads *silver*, never
bronze — the silver layer already did the decode + reconstruction + flagging, so
this is plain group-by/rollup (the natural shape for SQL/dbt later, when the
gold layer has several marts; one mart doesn't justify the warehouse yet).

`n_violations` is the headline pass/fail count per check; `p*_ms` carry latency
percentiles; `detail` is a compact JSON breakdown (by-kind counts, downtime,
etc.). Input rows are the dicts `silver.dq.build_silver` emits, or the same rows
read back from silver Parquet (`Table.to_pylist()`).
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Iterable

import numpy as np
import pyarrow as pa

SCORECARD_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("check", pa.string()),
        ("n_records", pa.int64()),
        ("n_violations", pa.int64()),
        ("p50_ms", pa.float64()),
        ("p95_ms", pa.float64()),
        ("p99_ms", pa.float64()),
        ("detail", pa.string()),
    ]
)


def _row(
    exchange: str,
    canonical_symbol: str | None,
    date: str,
    check: str,
    n_records: int,
    n_violations: int,
    *,
    p50_ms: float | None = None,
    p95_ms: float | None = None,
    p99_ms: float | None = None,
    detail: dict | None = None,
) -> dict:
    return {
        "exchange": exchange,
        "canonical_symbol": canonical_symbol,
        "date": date,
        "check": check,
        "n_records": n_records,
        "n_violations": n_violations,
        "p50_ms": p50_ms,
        "p95_ms": p95_ms,
        "p99_ms": p99_ms,
        "detail": json.dumps(detail, sort_keys=True) if detail is not None else None,
    }


def _group(rows: Iterable[dict], *keys: str) -> dict[tuple, list[dict]]:
    out: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        out[tuple(row[k] for k in keys)].append(row)
    return out


def _book_checks(book_quality: Iterable[dict]) -> list[dict]:
    rows: list[dict] = []
    for (exchange, symbol, date), group in _group(
        book_quality, "exchange", "canonical_symbol", "date"
    ).items():
        deltas = [r for r in group if r["kind"] == "delta"]
        snaps = len(group) - len(deltas)
        gaps = [r["seq_gap"] for r in deltas if (r["seq_gap"] or 0) > 0]
        rows.append(
            _row(
                exchange, symbol, date, "sequence_gap",
                n_records=len(deltas), n_violations=len(gaps),
                detail={"total_missing": sum(gaps), "max_gap": max(gaps, default=0)},
            )
        )
        invariants = [r for r in group if r["invariant_kind"]]
        by_kind = Counter(r["invariant_kind"] for r in invariants)
        locked = sum(
            1 for r in invariants if r["crossed"] and r["best_bid"] == r["best_ask"]
        )
        rows.append(
            _row(
                exchange, symbol, date, "book_invariant",
                n_records=len(group), n_violations=len(invariants),
                detail={**by_kind, "locked": locked},
            )
        )
        rows.append(
            _row(
                exchange, symbol, date, "coverage",
                n_records=len(group), n_violations=0,
                detail={"snapshots": snaps, "deltas": len(deltas)},
            )
        )
    return rows


def _latency_checks(latency: Iterable[dict]) -> list[dict]:
    rows: list[dict] = []
    for (exchange, symbol, date, dataset), group in _group(
        latency, "exchange", "canonical_symbol", "date", "dataset"
    ).items():
        vals = np.array([r["exchange_to_emit_ns"] for r in group], dtype=np.float64)
        p50, p95, p99 = (float(x) / 1e6 for x in np.percentile(vals, [50, 95, 99]))
        rows.append(
            _row(
                exchange, symbol, date, f"latency.{dataset}",
                n_records=len(group), n_violations=0,
                p50_ms=p50, p95_ms=p95, p99_ms=p99,
                detail={"max_ms": float(vals.max()) / 1e6},
            )
        )
    return rows


def _status_checks(status_events: Iterable[dict]) -> list[dict]:
    rows: list[dict] = []
    for (exchange, date), group in _group(status_events, "exchange", "date").items():
        group = sorted(group, key=lambda r: r["ts_ns"])
        transitions = [r for r in group if r["is_transition"]]
        downs = [r for r in transitions if r["state"] == "down"]
        downtime_ns = sum(r["downtime_ns"] or 0 for r in group)
        rows.append(
            _row(
                exchange, None, date, "venue_uptime",
                n_records=len(group), n_violations=len(downs),
                detail={
                    "transitions": len(transitions),
                    "down_transitions": len(downs),
                    "downtime_sec": downtime_ns / 1e9,
                    "final_state": group[-1]["state"],
                },
            )
        )
    return rows


def build_scorecard(
    book_quality: Iterable[dict],
    latency: Iterable[dict],
    status_events: Iterable[dict],
) -> list[dict]:
    """Roll the three silver fact streams up into scorecard rows."""
    return [
        *_book_checks(book_quality),
        *_latency_checks(latency),
        *_status_checks(status_events),
    ]


def scorecard_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCORECARD_SCHEMA)
