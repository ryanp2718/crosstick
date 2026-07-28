"""Guarantees the error bars depend on.

A confidence interval is only worth printing if the resampling respects the structure
of the data. Two claims are pinned here: folds never train on their own future, and the
bootstrap resamples whole days rather than rows. Break either and the pipeline still
produces a tidy interval, just a much narrower one than the evidence supports - which
is the most dangerous kind of bug in this file.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from research.model import _hit_rate, _r2_vs_zero
from research.validation import (
    OutOfSample,
    bootstrap_ci,
    fold_spread,
    placebo_target,
    walk_forward,
)


def _frame(n_dates: int, n_per_date: int = 40) -> pl.DataFrame:
    rows = []
    for d in range(1, n_dates + 1):
        for i in range(n_per_date):
            rows.append(
                {
                    "ts_ns": d * 10_000 + i,
                    "date": f"2026-01-{d:02d}",
                    "coinbase_ofi": float(i),
                    "kraken_ofi": float(-i),
                    "y_kraken_ret_bps_5": 0.1,
                }
            )
    return pl.DataFrame(rows)


# ── folds ───────────────────────────────────────────────────────────────────


def test_every_fold_trains_only_on_its_own_past() -> None:
    """The whole point of walking forward. If one fold's training window reaches past
    its test window, every metric downstream is contaminated and nothing else checks."""
    splits = walk_forward(_frame(12), "y_kraken_ret_bps_5", 5, min_train=4, test_size=2, step=2)
    assert splits
    for s in splits:
        assert max(s.train_dates) < min(s.test_dates)


def test_the_training_window_expands_and_the_test_window_advances() -> None:
    splits = walk_forward(_frame(12), "y_kraken_ret_bps_5", 5, min_train=4, test_size=2, step=2)
    assert [len(s.train_dates) for s in splits] == sorted({len(s.train_dates) for s in splits})
    starts = [s.test_dates[0] for s in splits]
    assert starts == sorted(starts)


def test_stepping_by_the_test_size_scores_each_date_at_most_once() -> None:
    """Overlapping test windows would put the same date in the pooled series twice, and
    the date-block bootstrap would then treat one day as two independent ones."""
    splits = walk_forward(_frame(12), "y_kraken_ret_bps_5", 5, min_train=4, test_size=2, step=2)
    tested = [d for s in splits for d in s.test_dates]
    assert len(tested) == len(set(tested))


def test_too_few_dates_yields_no_folds_rather_than_a_smaller_geometry() -> None:
    """Silently shrinking the window would report folds the caller did not ask for."""
    assert walk_forward(_frame(5), "y_kraken_ret_bps_5", 5, min_train=8, test_size=3, step=3) == []


# ── bootstrap ───────────────────────────────────────────────────────────────


def _oos(n_dates: int, n_per_date: int, y_fn) -> OutOfSample:
    dates = np.repeat([f"2026-01-{d:02d}" for d in range(1, n_dates + 1)], n_per_date)
    y = np.array([y_fn(d) for d in range(n_dates) for _ in range(n_per_date)])
    return OutOfSample(dates=dates, y=y, pred=np.ones_like(y))


def test_the_interval_brackets_the_point_estimate() -> None:
    rng = np.random.default_rng(0)
    oos = _oos(20, 50, lambda d: rng.normal())
    lo, hi = bootstrap_ci(oos, _r2_vs_zero, n_boot=200)
    assert lo <= _r2_vs_zero(oos.y, oos.pred) <= hi


def test_resampling_is_by_date_so_within_day_repetition_does_not_narrow_it() -> None:
    """The failure this guards: a day's rows are near-identical, so resampling ROWS
    would shrink the interval toward zero width as the grid gets denser, even though
    no new information arrived. Blocking by date keeps the width tied to the number of
    days actually captured, which is the real sample size.
    """
    day_effect = [-1.0, 1.0, -0.5, 0.5, -2.0, 2.0, -0.2, 0.2]
    sparse = _oos(8, 10, lambda d: day_effect[d])
    dense = _oos(8, 200, lambda d: day_effect[d])
    lo_s, hi_s = bootstrap_ci(sparse, _hit_rate, n_boot=300)
    lo_d, hi_d = bootstrap_ci(dense, _hit_rate, n_boot=300)
    assert (hi_s - lo_s) == (hi_d - lo_d) > 0


def test_a_single_test_date_admits_no_interval() -> None:
    """One block cannot be resampled into a distribution, and reporting a zero-width
    interval would be worse than reporting none."""
    lo, hi = bootstrap_ci(_oos(1, 50, lambda d: 1.0), _r2_vs_zero, n_boot=50)
    assert np.isnan(lo) and np.isnan(hi)


def test_an_undefined_metric_is_not_bootstrapped_into_a_number() -> None:
    """A model that makes no directional call has no hit rate, and resampling it must
    return nan rather than a percentile over an all-nan distribution."""
    oos = _oos(10, 20, lambda d: float(d) - 5)
    oos.pred = np.zeros_like(oos.y)
    assert np.isnan(_hit_rate(oos.y, oos.pred))
    assert all(np.isnan(v) for v in bootstrap_ci(oos, _hit_rate, n_boot=50))


# ── placebo ─────────────────────────────────────────────────────────────────


def test_placebo_moves_the_target_and_nothing_else() -> None:
    """If it perturbed a feature too, a null result would prove nothing - the model
    would have been handed damaged inputs rather than a broken pairing."""
    df = _frame(4, n_per_date=20)
    out = placebo_target(df, "y_kraken_ret_bps_5", lag_bars=5)
    assert out.drop("y_kraken_ret_bps_5").equals(df.drop("y_kraken_ret_bps_5"))
    assert out["y_kraken_ret_bps_5"].to_list()[:5] == df["y_kraken_ret_bps_5"].to_list()[5:10]


def test_placebo_never_pairs_a_row_with_another_dates_return() -> None:
    """Shifted within each date, so the last rows of a date lose their target rather
    than borrowing the next date's - which would also break the bootstrap's blocks."""
    df = _frame(3, n_per_date=20).with_columns(
        pl.int_range(pl.len()).cast(pl.Float64).alias("y_kraken_ret_bps_5")
    )
    out = placebo_target(df, "y_kraken_ret_bps_5", lag_bars=5)
    tail = out.filter(pl.col("y_kraken_ret_bps_5").is_null())
    assert tail.height == 3 * 5
    assert tail.group_by("date").len()["len"].to_list() == [5, 5, 5]


# ── fold spread ─────────────────────────────────────────────────────────────


def test_fold_spread_is_mean_and_sd_over_folds() -> None:
    folds = [{"gbt": {"r2": 0.1}}, {"gbt": {"r2": 0.3}}]
    mean, sd = fold_spread(folds, "gbt", "r2")
    assert mean == pytest.approx(0.2)
    assert sd == pytest.approx(0.1)
