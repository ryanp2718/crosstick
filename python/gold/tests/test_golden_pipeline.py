"""Headline DQ test: golden corpus -> silver -> gold, end to end (no Docker).

The golden corpus (analytics/tests/golden.py) plants three hard events and one
false-positive trap. This asserts the scorecard catches the incidents and does
NOT trip on the trap - the Phase 2 "CI against the planted gap" contract.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from analytics.tests.golden import build_golden_records
from gold.scorecard import build_scorecard
from materializer.bronze import CanonicalMap
from silver.dq import build_silver

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"


@pytest.fixture(scope="module")
def scorecard() -> dict[tuple, dict]:
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    facts = build_silver(build_golden_records(), canonical)
    rows = build_scorecard(facts.book_quality, facts.latency, facts.status_events)
    return {(r["check"], r["exchange"], r["canonical_symbol"]): r for r in rows}


def test_kraken_planted_sequence_gap(scorecard) -> None:
    r = scorecard[("sequence_gap", "kraken", "BTC-USD")]
    assert r["n_violations"] == 1
    assert json.loads(r["detail"])["total_missing"] == 1


def test_binance_planted_crossed_book(scorecard) -> None:
    r = scorecard[("book_invariant", "binance", "BTC-USDT")]
    assert r["n_violations"] == 1
    assert json.loads(r["detail"])["crossed_after_delta"] == 1


def test_binance_planted_venue_down(scorecard) -> None:
    r = scorecard[("venue_uptime", "binance", None)]
    assert r["n_violations"] == 1
    assert json.loads(r["detail"])["final_state"] == "down"


def test_perp_update_ids_do_not_false_positive(scorecard) -> None:
    # binance-futures sequences are update-ids (monotonic, non-contiguous) - a
    # naive gap detector would flag this healthy stream.
    r = scorecard[("sequence_gap", "binance-futures", "BTC-USDT-PERP")]
    assert r["n_violations"] == 0


def test_clean_venue_has_no_violations(scorecard) -> None:
    assert scorecard[("book_invariant", "coinbase", "BTC-USD")]["n_violations"] == 0
    assert scorecard[("sequence_gap", "coinbase", "BTC-USD")]["n_violations"] == 0
