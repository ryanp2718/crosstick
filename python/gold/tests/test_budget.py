"""Unit tests for the per-check DQ budget (pure, over scorecard rows)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from common.lake import dq_budget_path_from_env
from gold.budget import FAIL_CLOSED, Budget, BudgetError, Limit
from gold.scorecard import CHECKS, LATENCY_PREFIX


def budget(yml: str, tmp_path: Path) -> Budget:
    path = tmp_path / "dq_budgets.yml"
    path.write_text(textwrap.dedent(yml), encoding="utf-8")
    return Budget.from_yaml(path)


def row(check: str = "sequence_gap", **over) -> dict:
    out = {
        "exchange": "kraken",
        "canonical_symbol": "BTC-USD",
        "date": "2026-07-13",
        "check": check,
        "n_records": 1_000_000,
        "n_violations": 0,
        "p50_ms": None,
        "p95_ms": None,
        "p99_ms": None,
        "detail": None,
    }
    out.update(over)
    return out


def test_an_unlisted_check_fails_closed(tmp_path: Path) -> None:
    b = budget("checks: {}", tmp_path)
    assert b.limit_for("book_invariant", "kraken", "BTC-USD") == FAIL_CLOSED
    assert not b.row_breaches(row("book_invariant"))
    (breach,) = b.row_breaches(row("book_invariant", n_violations=1))
    assert breach.limit_name == "max_violations"


def test_an_empty_file_still_budgets(tmp_path: Path) -> None:
    """A file that says nothing is the strictest file, not the loosest."""
    assert budget("", tmp_path).limit_for("coverage", "kraken") == FAIL_CLOSED


def test_a_named_check_does_not_inherit_the_fail_closed_default(tmp_path: Path) -> None:
    """The footgun this design avoids: a rate limit on sequence_gap must not leave
    `max_violations: 0` underneath it, or the rate would never get a chance to fire."""
    b = budget(
        """
        checks:
          sequence_gap:
            max_rate: 0.01
        """,
        tmp_path,
    )
    assert b.limit_for("sequence_gap", "kraken", "BTC-USD") == Limit(max_rate=0.01)
    assert not b.row_breaches(row(n_violations=100))  # 0.01% of a million


def test_the_default_block_replaces_the_built_in_one(tmp_path: Path) -> None:
    b = budget("default: {max_violations: 5}", tmp_path)
    assert b.limit_for("book_invariant", "kraken") == Limit(max_violations=5)
    assert not b.row_breaches(row("book_invariant", n_violations=5))
    assert b.row_breaches(row("book_invariant", n_violations=6))


def test_scopes_merge_field_wise(tmp_path: Path) -> None:
    b = budget(
        """
        checks:
          sequence_gap:
            max_rate: 0.01
            min_records: 1000
            exchanges:
              kraken:
                max_rate: 0.0
                symbols:
                  BTC-USD: {min_records: 500000}
        """,
        tmp_path,
    )
    assert b.limit_for("sequence_gap", "coinbase", "BTC-USD") == Limit(
        max_rate=0.01, min_records=1000
    )
    assert b.limit_for("sequence_gap", "kraken", "ETH-USD") == Limit(max_rate=0.0, min_records=1000)
    # The symbol sets only min_records, so kraken's stricter rate survives.
    assert b.limit_for("sequence_gap", "kraken", "BTC-USD") == Limit(
        max_rate=0.0, min_records=500_000
    )


def test_a_symbol_scope_needs_its_exchange(tmp_path: Path) -> None:
    """Symbol limits hang off an exchange, so the same symbol on another venue is
    untouched (BTC-USD trades on both kraken and coinbase)."""
    b = budget(
        """
        checks:
          clock_monotonic:
            max_violations: 1000
            exchanges:
              kraken:
                symbols:
                  BTC-USD: {max_violations: 10}
        """,
        tmp_path,
    )
    assert b.limit_for("clock_monotonic", "kraken", "BTC-USD").max_violations == 10
    assert b.limit_for("clock_monotonic", "coinbase", "BTC-USD").max_violations == 1000


def test_a_row_with_no_symbol_resolves(tmp_path: Path) -> None:
    """venue_uptime is per exchange; its canonical_symbol is null."""
    b = budget(
        """
        checks:
          venue_uptime:
            max_violations: 4
            exchanges:
              kraken: {max_violations: 20}
        """,
        tmp_path,
    )
    assert b.limit_for("venue_uptime", "kraken", None).max_violations == 20
    assert not b.row_breaches(
        row("venue_uptime", canonical_symbol=None, n_records=50, n_violations=20)
    )


def test_the_latency_family_covers_every_dataset(tmp_path: Path) -> None:
    b = budget("checks:\n  latency: {max_p99_ms: 5000}", tmp_path)
    for dataset in ("quotes", "trades", "open_interest"):
        assert b.limit_for(f"{LATENCY_PREFIX}{dataset}", "kraken", "BTC-USD").max_p99_ms == 5000


def test_an_exact_latency_check_overrides_its_family(tmp_path: Path) -> None:
    b = budget(
        """
        checks:
          latency:
            max_p99_ms: 5000
            min_records: 1000
          latency.trades:
            max_p99_ms: 60000
        """,
        tmp_path,
    )
    assert b.limit_for("latency.trades", "kraken", "BTC-USD") == Limit(
        max_p99_ms=60000, min_records=1000
    )
    assert b.limit_for("latency.quotes", "kraken", "BTC-USD") == Limit(
        max_p99_ms=5000, min_records=1000
    )


def test_a_family_exchange_scope_outranks_an_exact_global_one(tmp_path: Path) -> None:
    """Documented ordering: scope dominates, and the exact name only breaks ties
    within a scope."""
    b = budget(
        """
        checks:
          latency:
            exchanges:
              kraken: {max_p99_ms: 100}
          latency.trades:
            max_p99_ms: 60000
        """,
        tmp_path,
    )
    assert b.limit_for("latency.trades", "kraken", "BTC-USD").max_p99_ms == 100
    assert b.limit_for("latency.trades", "coinbase", "BTC-USD").max_p99_ms == 60000


def test_a_rate_needs_a_denominator(tmp_path: Path) -> None:
    """An empty partition has no rate to speak of; min_records is what catches it."""
    b = budget("checks:\n  sequence_gap: {max_rate: 0.0, min_records: 100}", tmp_path)
    (breach,) = b.row_breaches(row(n_records=0, n_violations=0))
    assert breach.limit_name == "min_records"


def test_min_records_catches_a_venue_that_nearly_stopped(tmp_path: Path) -> None:
    b = budget("checks:\n  coverage: {min_records: 100000}", tmp_path)
    (breach,) = b.row_breaches(row("coverage", n_records=93))
    assert breach.value == 93
    assert breach.bound == 100000
    assert "n_records 93 < min_records 100000" in str(breach)


def test_a_latency_tail_only_trips_where_a_percentile_exists(tmp_path: Path) -> None:
    b = budget("checks:\n  latency: {max_p99_ms: 500}", tmp_path)
    assert not b.row_breaches(row("latency.trades", p99_ms=None))
    assert not b.row_breaches(row("latency.trades", p99_ms=500.0))
    (breach,) = b.row_breaches(row("latency.trades", p99_ms=500.1))
    assert breach.limit_name == "max_p99_ms"


def test_one_row_can_breach_several_limits(tmp_path: Path) -> None:
    b = budget(
        "checks:\n  sequence_gap: {max_violations: 10, max_rate: 0.001, min_records: 1000}",
        tmp_path,
    )
    breaches = b.row_breaches(row(n_records=100, n_violations=50))
    assert [x.limit_name for x in breaches] == ["max_violations", "max_rate", "min_records"]


def test_breaches_reports_every_failing_row(tmp_path: Path) -> None:
    b = budget("checks: {}", tmp_path)
    rows = [row("coverage"), row("book_invariant", n_violations=1), row("book_invariant")]
    (breach,) = b.breaches(rows)
    assert breach.check == "book_invariant"
    assert str(breach) == (
        "book_invariant kraken/BTC-USD 2026-07-13: n_violations 1 > max_violations 0"
    )


def test_a_breach_on_a_symbolless_check_reads_cleanly(tmp_path: Path) -> None:
    b = budget("checks: {}", tmp_path)
    (breach,) = b.breaches([row("venue_uptime", canonical_symbol=None, n_violations=3)])
    assert str(breach).startswith("venue_uptime kraken/- 2026-07-13:")


def test_an_unknown_check_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(BudgetError, match="unknown check 'sequence_gaps'"):
        budget("checks:\n  sequence_gaps: {max_violations: 0}", tmp_path)


def test_a_misspelled_limit_is_rejected(tmp_path: Path) -> None:
    """The whole point of a budget file is that a typo cannot quietly disable it."""
    with pytest.raises(BudgetError, match=r"unknown key\(s\) \['max_violation'\]"):
        budget("checks:\n  coverage: {max_violation: 0}", tmp_path)
    with pytest.raises(BudgetError, match=r"unknown key\(s\) \['exchange'\]"):
        budget("checks:\n  coverage: {exchange: {kraken: {}}}", tmp_path)
    with pytest.raises(BudgetError, match=r"unknown key\(s\) \['symbol'\]"):
        budget("checks:\n  coverage:\n    exchanges:\n      kraken: {symbol: {}}", tmp_path)
    with pytest.raises(BudgetError, match=r"unknown key\(s\) \['checkz'\]"):
        budget("checkz: {}", tmp_path)


def test_a_nonsense_value_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(BudgetError, match="must be a non-negative number, got -1"):
        budget("checks:\n  coverage: {max_violations: -1}", tmp_path)
    with pytest.raises(BudgetError, match="must be a non-negative number, got 'lots'"):
        budget("checks:\n  coverage: {max_violations: lots}", tmp_path)
    # `True` is an int in Python; a boolean limit is a mistake, not a bound of 1.
    with pytest.raises(BudgetError, match="must be a non-negative number, got True"):
        budget("checks:\n  coverage: {max_violations: true}", tmp_path)


def test_the_shipped_budget_loads_and_covers_every_check() -> None:
    """The real ops/dq_budgets.yml, resolved for every check the scorecard emits."""
    shipped = Budget.from_yaml(dq_budget_path_from_env())
    for check in [*CHECKS, f"{LATENCY_PREFIX}quotes", f"{LATENCY_PREFIX}trades"]:
        limit = shipped.limit_for(check, "kraken", "BTC-USD")
        assert limit != Limit(), f"{check} resolved to no limit at all"


def test_the_budget_path_follows_the_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("DQ_BUDGET_FILE", raising=False)
    assert dq_budget_path_from_env().name == "dq_budgets.yml"
    monkeypatch.setenv("DQ_BUDGET_FILE", str(tmp_path / "other.yml"))
    assert dq_budget_path_from_env() == tmp_path / "other.yml"
