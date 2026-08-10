"""The `gold.main` exit gate: does a breach actually fail the build?

The lake I/O is stubbed out. What is under test is the decision - which budget file
is loaded, when the process exits non-zero, and that a broken budget is loud - not
the marts, which the streaming and integration tests already cover.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from gold import main as gold_main
from gold.budget import BudgetError

DATE = "2026-07-13"

BUDGET = """
    checks:
      clock_monotonic:
        max_violations: 100
"""


def scorecard_rows(n_violations: int) -> list[dict]:
    return [
        {
            "exchange": "kraken",
            "canonical_symbol": "BTC-USD",
            "date": DATE,
            "check": "clock_monotonic",
            "n_records": 1_000_000,
            "n_violations": n_violations,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "detail": None,
        }
    ]


@pytest.fixture
def run(monkeypatch, tmp_path: Path):
    """Run `gold.main.main()` over one date with the lake stubbed, returning the
    scorecard rows it was fed and the exit code it chose (None for a clean exit)."""

    def go(rows: list[dict], *argv: str, budget: str = BUDGET) -> int | None:
        path = tmp_path / "budget.yml"
        path.write_text(textwrap.dedent(budget), encoding="utf-8")
        monkeypatch.setenv("DQ_BUDGET_FILE", str(path))
        monkeypatch.setattr("sys.argv", ["gold.main", DATE, *argv])
        monkeypatch.setattr(gold_main, "filesystem_from_env", lambda: None)
        monkeypatch.setattr(gold_main, "build_for_date", lambda *a: rows)
        monkeypatch.setattr(gold_main, "write_object", lambda *a: "gold/scorecard/x")
        monkeypatch.setattr(gold_main, "write_basis_for_date", lambda *a: {})
        monkeypatch.setattr(gold_main, "write_freshness_markers", lambda *a: None)
        try:
            gold_main.main()
        except SystemExit as exit_:
            return exit_.code
        return None

    return go


def test_a_clean_date_exits_zero(run) -> None:
    assert run(scorecard_rows(30), "--fail-on-violation") is None


def test_a_breach_fails_the_build(run) -> None:
    assert run(scorecard_rows(101), "--fail-on-violation") == 1


def test_violations_within_budget_do_not_fail_the_build(run) -> None:
    """The point of the change: 100 clock steps is a normal day, and the flat
    violation sum this replaced failed on the first one."""
    assert run(scorecard_rows(100), "--fail-on-violation") is None


def test_without_the_flag_a_breach_is_only_logged(run, caplog) -> None:
    with caplog.at_level("WARNING"):
        assert run(scorecard_rows(101)) is None
    assert "budget: clock_monotonic kraken/BTC-USD" in caplog.text


def test_the_budget_file_can_be_overridden_per_run(run, tmp_path: Path) -> None:
    """A backfill points at a looser file rather than editing the shipped one."""
    loose = tmp_path / "loose.yml"
    loose.write_text("checks:\n  clock_monotonic: {max_violations: 1000}\n", encoding="utf-8")
    assert run(scorecard_rows(101), "--fail-on-violation", "--dq-budget", str(loose)) is None


def test_a_broken_budget_is_loud_without_the_flag(run) -> None:
    """Loading up front, unconditionally: a run that would not have gated on the
    budget still reports that the gate is broken."""
    with pytest.raises(BudgetError, match="unknown check 'clock_monotonics'"):
        run(scorecard_rows(0), budget="checks:\n  clock_monotonics: {max_violations: 1}")


def test_a_date_with_no_silver_facts_neither_breaches_nor_fails(run) -> None:
    assert run([], "--fail-on-violation") is None
