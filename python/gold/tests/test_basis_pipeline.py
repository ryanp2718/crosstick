"""Headline basis test: golden corpus -> silver -> gold basis, end to end (no Docker).

The corpus carries coinbase+kraken BTC-USD and binance BTC-USDT, so the first
research signal (the stablecoin basis) is reconstructable from it. Binance's
planted crossed book ends its leg, producing a realistic basis gap.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from analytics.tests.golden import build_golden_records
from gold.basis import build_basis
from materializer.bronze import CanonicalMap
from silver.dq import build_silver

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"


@pytest.fixture(scope="module")
def basis() -> list[dict]:
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    facts = build_silver(build_golden_records(), canonical)
    return build_basis(facts.nbbo, canonical.pairs_by_base())


def test_btc_basis_value(basis) -> None:
    btc = [r for r in basis if r["base"] == "BTC"]
    assert btc, "expected a BTC basis series"
    # USD NBBO settles 64995/65015 (coinbase bid, kraken ask) -> mid 65005;
    # BTC-USDT 64970/65030 -> mid 65000; so basis = +5 (~0.77 bps).
    assert all(r["basis_abs"] == Decimal("5") for r in btc)
    assert btc[0]["usd_mid"] == Decimal("65005")
    assert btc[0]["usdt_mid"] == Decimal("65000")
    assert btc[0]["basis_bps"] == pytest.approx(5 / 65005 * 1e4, abs=1e-6)


def test_basis_gap_when_binance_book_crosses(basis) -> None:
    # The crossed delta clears BTC-USDT (and the venue later goes down), so the
    # series holds only the pre-cross observations - it does not carry a stale
    # USDT leg forward.
    btc = [r for r in basis if r["base"] == "BTC"]
    assert len(btc) == 2


def test_only_spot_bases_paired(basis) -> None:
    # the perp canonical (BTC-USDT-PERP) must not appear as a basis leg.
    assert {r["base"] for r in basis} == {"BTC"}
