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
import pyarrow.compute as pc

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


def _clock_monotonic_row(exchange: str, symbol: str, date: str, group: list[dict]) -> dict:
    """Host capture-clock health from book_quality (fold-ordered on disk). Counts
    backward steps in `local_recv_ts_ns` between consecutive DELTA records of the
    SAME epoch (= host clock stepped back / paused; a stopped w32time shows
    thousands), separately from epoch-change backward steps (= benign reconnect
    overlap). Snapshots are excluded: they are folded in by sequence but fetched at
    a different time, so they read as recv backward steps that are reordering, not a
    clock fault — counting them inflated this ~2x on clock-clean days. Deltas arrive
    in sequence order, so fold order ≈ arrival order and a backward step is a real
    clock regression. This is the canary the Phase-2 reorder can't mask — it reads
    the raw recv clock the fold persisted, before any sort. Records with no recv
    clock are skipped."""
    intra = inter = worst = n = 0
    prev_recv = prev_epoch = None
    for r in group:
        if r["kind"] != "delta":
            continue
        n += 1
        recv = r["local_recv_ts_ns"]
        if recv is None:
            continue
        epoch = r["epoch"]
        if prev_recv is not None and recv < prev_recv:
            if epoch == prev_epoch:
                intra += 1
                worst = max(worst, prev_recv - recv)
            else:
                inter += 1
        prev_recv, prev_epoch = recv, epoch
    return _row(
        exchange, symbol, date, "clock_monotonic",
        n_records=n, n_violations=intra,
        detail={"worst_lateness_ms": worst / 1e6, "inter_epoch_steps": inter},
    )


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
        rows.append(_clock_monotonic_row(exchange, symbol, date, group))
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


# --- per-partition accumulators (the streaming gold path) -------------------
# Every scorecard group key nests inside one silver partition (book_quality and
# latency are written `exchange=/symbol=canonical/date=`), so gold can fold one
# partition at a time instead of materializing a whole day of rows. The dict
# functions above stay the simple oracle these are pinned against (test_streaming).
# `update` is table-granularity-agnostic: today it gets one table per partition
# object; a future row-group reader would feed it batches with no change here.


class BookCheckAccumulator:
    """Additive, columnar fold of one book_quality partition (constant
    exchange/canonical_symbol/date) into its three book rows. O(1) state and no
    per-row dict materialization — the same checks as `_book_checks`, mergeable
    across `update` calls so this also drops into a map-reduce later."""

    def __init__(self, exchange: str, canonical_symbol: str, date: str):
        self.exchange = exchange
        self.canonical_symbol = canonical_symbol
        self.date = date
        self._n_total = 0
        self._n_deltas = 0
        self._n_gap = 0
        self._total_missing = 0
        self._max_gap = 0
        self._n_invariant = 0
        self._by_kind: Counter = Counter()
        self._locked = 0
        self._clock_intra = 0
        self._clock_inter = 0
        self._clock_worst = 0
        self._last_recv: int | None = None
        self._last_epoch: int | None = None

    def update(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        is_delta = pc.equal(table.column("kind"), "delta")
        self._n_total += table.num_rows
        self._n_deltas += pc.sum(pc.cast(is_delta, pa.int64())).as_py() or 0

        seq_gap = table.column("seq_gap")
        gaps = pc.filter(seq_gap, pc.and_(is_delta, pc.greater(pc.fill_null(seq_gap, 0), 0)))
        if gaps.length():
            self._n_gap += gaps.length()
            self._total_missing += pc.sum(gaps).as_py()
            self._max_gap = max(self._max_gap, pc.max(gaps).as_py())

        inv_kind = table.column("invariant_kind")
        inv_mask = pc.is_valid(inv_kind)  # silver never emits "", so non-null == flagged
        self._n_invariant += pc.sum(pc.cast(inv_mask, pa.int64())).as_py() or 0
        for entry in pc.value_counts(pc.filter(inv_kind, inv_mask)).to_pylist():
            self._by_kind[entry["values"]] += entry["counts"]
        same = pc.fill_null(pc.equal(table.column("best_bid"), table.column("best_ask")), False)
        locked = pc.and_(pc.and_(inv_mask, pc.fill_null(table.column("crossed"), False)), same)
        self._locked += pc.sum(pc.cast(locked, pa.int64())).as_py() or 0

        self._clock_update(table)

    def _clock_update(self, table: pa.Table) -> None:
        """recv-clock backward steps between consecutive DELTA records, classified
        intra- vs inter-epoch, carried across batches via `_last_*` (the same count
        as `_clock_monotonic_row`). Snapshots are excluded: folded in by sequence
        but fetched at a different time, they read as reordering, not a clock fault."""
        mask = pc.and_(
            pc.equal(table.column("kind"), "delta"),
            pc.is_valid(table.column("local_recv_ts_ns")),
        )
        recv = pc.filter(table.column("local_recv_ts_ns"), mask).to_numpy(zero_copy_only=False)
        if len(recv) == 0:
            return
        epoch = pc.filter(table.column("epoch"), mask).to_numpy(zero_copy_only=False)
        if self._last_recv is not None:
            recv = np.concatenate(([self._last_recv], recv))
            epoch = np.concatenate(([self._last_epoch], epoch))
        back = recv[1:] < recv[:-1]
        same_epoch = epoch[1:] == epoch[:-1]
        intra = back & same_epoch
        self._clock_intra += int(intra.sum())
        self._clock_inter += int((back & ~same_epoch).sum())
        if intra.any():
            self._clock_worst = max(self._clock_worst, int((recv[:-1] - recv[1:])[intra].max()))
        self._last_recv = int(recv[-1])
        self._last_epoch = int(epoch[-1])

    def rows(self) -> list[dict]:
        return [
            _row(
                self.exchange, self.canonical_symbol, self.date, "sequence_gap",
                n_records=self._n_deltas, n_violations=self._n_gap,
                detail={"total_missing": self._total_missing, "max_gap": self._max_gap},
            ),
            _row(
                self.exchange, self.canonical_symbol, self.date, "book_invariant",
                n_records=self._n_total, n_violations=self._n_invariant,
                detail={**self._by_kind, "locked": self._locked},
            ),
            _row(
                self.exchange, self.canonical_symbol, self.date, "coverage",
                n_records=self._n_total, n_violations=0,
                detail={"snapshots": self._n_total - self._n_deltas, "deltas": self._n_deltas},
            ),
            _row(
                self.exchange, self.canonical_symbol, self.date, "clock_monotonic",
                n_records=self._n_deltas, n_violations=self._clock_intra,
                detail={"worst_lateness_ms": self._clock_worst / 1e6,
                        "inter_epoch_steps": self._clock_inter},
            ),
        ]


class LatencyAccumulator:
    """Collect one latency partition's `exchange_to_emit_ns` per dataset and emit
    a `latency.<dataset>` row with p50/95/99. Exact percentiles need every value,
    so this holds one partition's values (columnar, ~tens of MB) — not the day;
    the out-of-core fix (DuckDB approx_quantile) is P3."""

    def __init__(self, exchange: str, canonical_symbol: str, date: str):
        self.exchange = exchange
        self.canonical_symbol = canonical_symbol
        self.date = date
        self._vals: dict[str, list[np.ndarray]] = defaultdict(list)

    def update(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        datasets = table.column("dataset")
        emit = table.column("exchange_to_emit_ns")
        for ds in pc.unique(datasets).to_pylist():
            vals = pc.filter(emit, pc.equal(datasets, ds)).to_numpy(zero_copy_only=False)
            self._vals[ds].append(vals.astype(np.float64))

    def rows(self) -> list[dict]:
        out: list[dict] = []
        for ds, chunks in self._vals.items():
            vals = np.concatenate(chunks)
            p50, p95, p99 = (float(x) / 1e6 for x in np.percentile(vals, [50, 95, 99]))
            out.append(
                _row(
                    self.exchange, self.canonical_symbol, self.date, f"latency.{ds}",
                    n_records=len(vals), n_violations=0,
                    p50_ms=p50, p95_ms=p95, p99_ms=p99,
                    detail={"max_ms": float(vals.max()) / 1e6},
                )
            )
        return out


def build_scorecard(
    book_quality: Iterable[dict],
    latency: Iterable[dict],
    status_events: Iterable[dict],
) -> list[dict]:
    """Roll the three silver fact streams up into scorecard rows (the in-memory
    oracle; gold's batch path folds per partition — see the accumulators above)."""
    return [
        *_book_checks(book_quality),
        *_latency_checks(latency),
        *_status_checks(status_events),
    ]


def scorecard_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCORECARD_SCHEMA)
