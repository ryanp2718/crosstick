"""Gold stablecoin-basis mart: the USDT/USD basis from silver NBBO.

Pure (no I/O). As-of joins a base's USD-quote and USDT-quote NBBO series (e.g.
`BTC-USD` vs `BTC-USDT`) and emits the cross-quote basis on each tick where both
legs have a valid two-sided NBBO. The as-of merge (`common.asof.merge_latest`)
is backward-only, so every basis observation is point-in-time correct. A gold
mart reads *silver*, never bronze.

  - `basis`         : the event-grain series (the research artifact).
  - `basis_summary` : the daily rollup per base (the queryable mart row).

The basis is the first signal driven through the full spine (`RESEARCH_thesis.md`
§5/§7.3); it is deliberately a near-trivial transform so the engineering effort
lands on the reusable spine (as-of join, mart conventions, PIT), not the signal.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal

import numpy as np
import pyarrow as pa

from common.asof import merge_latest
from materializer.bronze import record_date

_PRICE = pa.decimal128(38, 18)

BASIS_SCHEMA = pa.schema(
    [
        ("base", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("usd_mid", _PRICE),
        ("usdt_mid", _PRICE),
        ("basis_abs", _PRICE),
        ("basis_bps", pa.float64()),
        ("usd_bid", _PRICE),
        ("usd_ask", _PRICE),
        ("usdt_bid", _PRICE),
        ("usdt_ask", _PRICE),
    ]
)

BASIS_SUMMARY_SCHEMA = pa.schema(
    [
        ("base", pa.string()),
        ("date", pa.string()),
        ("n_obs", pa.int64()),
        ("basis_bps_mean", pa.float64()),
        ("basis_bps_std", pa.float64()),
        ("basis_bps_min", pa.float64()),
        ("basis_bps_max", pa.float64()),
        ("coverage_ns", pa.int64()),
    ]
)


def _by_canonical(nbbo_rows: Iterable[dict]) -> dict[str, list[tuple[int, tuple]]]:
    out: dict[str, list[tuple[int, tuple]]] = defaultdict(list)
    for r in nbbo_rows:
        out[r["canonical_symbol"]].append((r["ts_ns"], (r["best_bid"], r["best_ask"])))
    for series in out.values():
        series.sort(key=lambda e: e[0])
    return out


def build_basis(nbbo_rows: Iterable[dict], pairs: list[tuple[str, str, str]]) -> list[dict]:
    """Event-grain basis rows for each (base, usd_canonical, usdt_canonical)."""
    by_canon = _by_canonical(nbbo_rows)
    rows: list[dict] = []
    for base, usd_c, usdt_c in pairs:
        usd, usdt = by_canon.get(usd_c, []), by_canon.get(usdt_c, [])
        if not usd or not usdt:
            continue
        for ts, snap in merge_latest({"usd": usd, "usdt": usdt}):
            if "usd" not in snap or "usdt" not in snap:
                continue  # one leg hasn't quoted yet — no basis
            usd_bid, usd_ask = snap["usd"]
            usdt_bid, usdt_ask = snap["usdt"]
            usd_mid = (usd_bid + usd_ask) / Decimal(2)
            usdt_mid = (usdt_bid + usdt_ask) / Decimal(2)
            basis_abs = usd_mid - usdt_mid
            rows.append(
                {
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
            )
    return rows


def build_basis_summary(basis_rows: Iterable[dict]) -> list[dict]:
    """Daily per-base rollup of the basis series."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in basis_rows:
        groups[(r["base"], r["date"])].append(r)
    rows: list[dict] = []
    for (base, date), group in groups.items():
        bps = np.array([r["basis_bps"] for r in group], dtype=np.float64)
        ts = [r["ts_ns"] for r in group]
        rows.append(
            {
                "base": base,
                "date": date,
                "n_obs": len(group),
                "basis_bps_mean": float(bps.mean()),
                "basis_bps_std": float(bps.std()),
                "basis_bps_min": float(bps.min()),
                "basis_bps_max": float(bps.max()),
                "coverage_ns": max(ts) - min(ts),
            }
        )
    return rows


def basis_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=BASIS_SCHEMA)


def basis_summary_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=BASIS_SUMMARY_SCHEMA)
