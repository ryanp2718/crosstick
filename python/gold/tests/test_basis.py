"""Gold basis mart: cross-quote spread math, pairing, and the daily rollup."""
from __future__ import annotations

from decimal import Decimal

import pytest

from gold.basis import build_basis, build_basis_summary

PAIRS = [("BTC", "BTC-USD", "BTC-USDT")]


def _nbbo(canonical: str, ts: int, bid: str, ask: str) -> dict:
    return {
        "canonical_symbol": canonical,
        "ts_ns": ts,
        "best_bid": Decimal(bid),
        "best_ask": Decimal(ask),
    }


def test_basis_math() -> None:
    nbbo = [
        _nbbo("BTC-USD", 10, "65000", "65010"),
        _nbbo("BTC-USDT", 11, "64990", "65000"),
    ]
    rows = build_basis(nbbo, PAIRS)
    assert len(rows) == 1
    r = rows[0]
    assert r["usd_mid"] == Decimal("65005")
    assert r["usdt_mid"] == Decimal("64995")
    assert r["basis_abs"] == Decimal("10")
    assert r["basis_bps"] == pytest.approx(10 / 65005 * 1e4)
    assert r["base"] == "BTC"


def test_basis_skips_until_both_legs_present() -> None:
    nbbo = [
        _nbbo("BTC-USD", 5, "100", "102"),
        _nbbo("BTC-USD", 8, "101", "103"),
        _nbbo("BTC-USDT", 9, "100", "101"),
    ]
    rows = build_basis(nbbo, PAIRS)
    # only ts=9 has both legs; the USD leg is as-of ts=8 (mid 102).
    assert [r["ts_ns"] for r in rows] == [9]
    assert rows[0]["usd_mid"] == Decimal("102")


def test_basis_missing_leg_yields_no_rows() -> None:
    assert build_basis([_nbbo("BTC-USD", 5, "100", "102")], PAIRS) == []


def test_basis_summary_stats() -> None:
    series = [
        {"base": "BTC", "date": "2026-06-12", "ts_ns": 10, "basis_bps": 1.0},
        {"base": "BTC", "date": "2026-06-12", "ts_ns": 30, "basis_bps": 3.0},
    ]
    s = build_basis_summary(series)[0]
    assert s["n_obs"] == 2
    assert s["basis_bps_mean"] == 2.0
    assert s["basis_bps_median"] == 2.0
    assert (s["basis_bps_min"], s["basis_bps_max"]) == (1.0, 3.0)
    assert s["coverage_ns"] == 20


def test_basis_summary_robust_to_outliers() -> None:
    # 99 clean ticks at -4bps plus the two stale-leg tails (-129 / +491): the robust
    # stats must stay pinned at -4 while raw min/max still flag the extremes.
    bps = [-4.0] * 99 + [-129.0, 491.0]
    series = [
        {"base": "BTC", "date": "2026-06-12", "ts_ns": i, "basis_bps": v}
        for i, v in enumerate(bps)
    ]
    s = build_basis_summary(series)[0]
    assert s["n_obs"] == 101
    assert s["basis_bps_median"] == -4.0
    assert s["basis_bps_p1"] == -4.0
    assert s["basis_bps_p99"] == -4.0
    assert (s["basis_bps_min"], s["basis_bps_max"]) == (-129.0, 491.0)
