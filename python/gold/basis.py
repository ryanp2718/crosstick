"""Gold stablecoin-basis mart: the USDT/USD basis from silver NBBO.

Pure (no I/O). As-of joins a base's USD-quote and USDT-quote NBBO series (e.g.
`BTC-USD` vs `BTC-USDT`) and emits the cross-quote basis on each tick where both
legs have a valid two-sided NBBO. The as-of merge (`common.asof.merge_latest`)
is backward-only, so every basis observation is point-in-time correct. A gold
mart reads *silver*, never bronze.

  - `basis`         : the event-grain series (the research artifact).
  - `basis_summary` : the daily rollup per base (the queryable mart row).

The basis is the first signal driven through the full spine; it is deliberately
a near-trivial transform so the engineering effort
lands on the reusable spine (as-of join, mart conventions, PIT), not the signal.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from decimal import Decimal
from typing import Any

import numpy as np
import pyarrow as pa

from common.asof import MAX_LEG_AGE_NS, merge_latest

# Re-exported: declared in common.schemas, imported from here by convention.
from common.schemas import BASIS_SCHEMA, BASIS_SUMMARY_SCHEMA
from materializer.bronze import record_date


def _by_canonical(nbbo_rows: Iterable[dict]) -> dict[str, list[tuple[int, tuple]]]:
    out: dict[str, list[tuple[int, tuple]]] = defaultdict(list)
    for r in nbbo_rows:
        out[r["canonical_symbol"]].append((r["ts_ns"], (r["ts_ns"], r["best_bid"], r["best_ask"])))
    for series in out.values():
        series.sort(key=lambda e: e[0])
    return out


def iter_basis(
    base: str,
    usd_stream: Iterable[tuple[int, Any]],
    usdt_stream: Iterable[tuple[int, Any]],
    max_age_ns: int = MAX_LEG_AGE_NS,
) -> Iterator[dict]:
    """Basis rows for one base from its two sorted NBBO legs (`(ts, (qts, bid, ask))`).

    A leg whose NBBO is older than `max_age_ns` is treated as stale and the tick is
    skipped - the single-venue USDT leg in particular simply gaps when its venue
    freezes, and merge_latest would otherwise carry the frozen mid forward into a
    bogus basis. The value embeds its own NBBO ts (`qts`) so the carried-forward age
    is visible. The shared core of both the in-memory `build_basis` and the streaming
    gold driver: the only difference between them is whether the legs are
    materialized sorted lists or lazy per-partition iterators. Backward-only via
    merge_latest.
    """
    for ts, snap in merge_latest({"usd": usd_stream, "usdt": usdt_stream}):
        if "usd" not in snap or "usdt" not in snap:
            continue  # one leg hasn't quoted yet - no basis
        usd_ts, usd_bid, usd_ask = snap["usd"]
        usdt_ts, usdt_bid, usdt_ask = snap["usdt"]
        if ts - usd_ts > max_age_ns or ts - usdt_ts > max_age_ns:
            continue  # a stale (frozen/quiet) NBBO leg - gap rather than emit a lie
        usd_mid = (usd_bid + usd_ask) / Decimal(2)
        usdt_mid = (usdt_bid + usdt_ask) / Decimal(2)
        basis_abs = usd_mid - usdt_mid
        yield {
            "base": base,
            "date": record_date(ts // 1_000_000),
            "ts_ns": ts,
            "usd_mid": usd_mid,
            "usdt_mid": usdt_mid,
            "basis_abs": basis_abs,
            "basis_bps": float(basis_abs / usd_mid) * 1e4,
            "usd_bid": usd_bid,
            "usd_ask": usd_ask,
            "usdt_bid": usdt_bid,
            "usdt_ask": usdt_ask,
        }


def build_basis(
    nbbo_rows: Iterable[dict],
    pairs: list[tuple[str, str, str]],
    max_age_ns: int = MAX_LEG_AGE_NS,
) -> list[dict]:
    """Event-grain basis rows for each (base, usd_canonical, usdt_canonical) - the
    in-memory oracle; the batch path streams per partition (gold/main)."""
    by_canon = _by_canonical(nbbo_rows)
    rows: list[dict] = []
    for base, usd_c, usdt_c in pairs:
        usd, usdt = by_canon.get(usd_c, []), by_canon.get(usdt_c, [])
        if not usd or not usdt:
            continue
        rows.extend(iter_basis(base, usd, usdt, max_age_ns))
    return rows


def summary_row(base: str, date: str, bps: Sequence[float], ts: Sequence[int]) -> dict:
    """One basis_summary row from a (base, date) group's bps + ts values. Shared by
    build_basis_summary and the streaming driver, so the rollup can't diverge."""
    arr = np.asarray(bps, dtype=np.float64)
    p1, p99 = np.percentile(arr, [1, 99])
    return {
        "base": base,
        "date": date,
        "n_obs": len(arr),
        "basis_bps_mean": float(arr.mean()),
        "basis_bps_std": float(arr.std()),
        # median + p1/p99 so the mart isn't defined by a handful of stale-leg ticks;
        # raw min/max kept as the outlier alarm.
        "basis_bps_median": float(np.median(arr)),
        "basis_bps_min": float(arr.min()),
        "basis_bps_max": float(arr.max()),
        "basis_bps_p1": float(p1),
        "basis_bps_p99": float(p99),
        "coverage_ns": max(ts) - min(ts),
    }


def build_basis_summary(basis_rows: Iterable[dict]) -> list[dict]:
    """Daily per-base rollup of the basis series."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in basis_rows:
        groups[(r["base"], r["date"])].append(r)
    return [
        summary_row(base, date, [r["basis_bps"] for r in g], [r["ts_ns"] for r in g])
        for (base, date), g in groups.items()
    ]


def basis_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=BASIS_SCHEMA)


def basis_summary_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=BASIS_SUMMARY_SCHEMA)
