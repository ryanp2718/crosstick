"""Guarantees the error bars depend on.

A confidence interval is only worth printing if the resampling respects the structure
of the data. Two claims are pinned here: folds never train on their own future, and the
bootstrap resamples whole days rather than rows. Break either and the pipeline still
produces a tidy interval, just a much narrower one than the evidence supports - which
is the most dangerous kind of bug in this file.
"""

from __future__ import annotations

import warnings

import numpy as np
import polars as pl
import pytest

from research.model import METRICS, _hit_rate, _r2_vs_zero
from research.validation import (
    Blocks,
    OutOfSample,
    as_classes,
    as_classes_at_coverage,
    bootstrap_ci,
    class_confidence_intervals,
    coverage_confidence_intervals,
    evaluate_walk_forward,
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


def _gathering_reference(oos: OutOfSample, metric, n_boot: int, seed: int = 0):
    """The bootstrap written the obvious way: build each draw's rows, rescan them.

    What the fast path has to agree with. Kept here rather than in the module because
    this is the definition and that is the optimisation, and an optimisation with no
    independent statement of what it computes is just an assertion about itself.
    """
    dates = np.unique(oos.dates)
    rows_for = {d: np.flatnonzero(oos.dates == d) for d in dates}
    rng = np.random.default_rng(seed)
    stats = np.empty(n_boot)
    for b in range(n_boot):
        picked = rng.choice(dates, size=len(dates), replace=True)
        idx = np.concatenate([rows_for[d] for d in picked])
        stats[b] = metric(oos.y[idx], oos.pred[idx])
    return tuple(float(v) for v in np.nanpercentile(stats, [2.5, 97.5]))


@pytest.mark.parametrize("metric_name", ["r2_vs_zero", "hit_rate", "gross_bps_per_trade"])
def test_the_block_sums_agree_with_gathering_every_draws_rows(metric_name: str) -> None:
    """Every metric is a ratio of sums over rows and a draw is a multiset of whole dates,
    so the sums can be accumulated per date once instead of per draw. That identity is
    the entire justification for not touching a row per draw, and it is exact up to
    summation order.
    """
    rng = np.random.default_rng(7)
    oos = _oos(14, 60, lambda d: rng.normal())
    oos.pred = rng.normal(size=len(oos.y)) * 0.3

    metric = METRICS[metric_name]
    fast = bootstrap_ci(oos, metric, n_boot=200)
    slow = _gathering_reference(oos, metric, n_boot=200)
    assert fast == pytest.approx(slow, abs=1e-12)


def test_a_draw_that_calls_nothing_leaves_the_metric_undefined() -> None:
    """The denominator is a count of calls, and a resample can contain only the dates the
    model stayed flat on. That draw has no hit rate, and averaging it in as a zero would
    drag the lower bound down with a value that was never measured.
    """
    oos = _oos(9, 20, lambda d: float(d) - 4)
    oos.pred = np.where(oos.dates == "2026-01-01", 1.0, 0.0)

    blocks = Blocks(oos.dates, n_boot=200, seed=0)
    called = METRICS["hit_rate"].parts(oos.y, oos.pred)[1]
    silent = blocks.drawn(called) == 0
    assert silent.any(), "fixture should produce draws that miss the only active date"

    lo, hi = bootstrap_ci(oos, METRICS["hit_rate"], n_boot=200)
    assert np.isfinite(lo) and np.isfinite(hi)


def test_every_draw_picks_as_many_dates_as_there_are() -> None:
    """A resample is the same size as the sample; the weights are how often each date was
    drawn, so they have to sum to the block count in every row."""
    blocks = Blocks(np.repeat(["a", "b", "c", "d"], 5), n_boot=50, seed=0)
    assert blocks.weights.shape == (50, 4)
    assert np.all(blocks.weights.sum(axis=1) == 4)


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


def test_a_metric_undefined_on_every_fold_is_quiet() -> None:
    """The `zero` baseline calls no direction, so its hit rate has no value on any fold.
    That is a documented case, not an error path, and numpy should not say otherwise."""
    folds = [{"zero": {"hit": float("nan")}} for _ in range(4)]
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        mean, sd = fold_spread(folds, "zero", "hit")
    assert np.isnan(mean) and np.isnan(sd)


def test_a_metric_defined_on_some_folds_still_averages_those() -> None:
    """The guard must not swallow a partially defined metric along with the empty one."""
    folds = [{"gbt": {"r2": v}} for v in (0.10, float("nan"), 0.20)]
    mean, sd = fold_spread(folds, "gbt", "r2")
    assert mean == pytest.approx(0.15)
    assert sd == pytest.approx(0.05)


# ── dead-zone plumbing ──────────────────────────────────────────────────────


def _dz_frame(n_dates: int, n_per_date: int = 60) -> pl.DataFrame:
    """Folds with a spread column, and a target that genuinely tracks one feature."""
    rows = []
    for d in range(1, n_dates + 1):
        for i in range(n_per_date):
            rows.append(
                {
                    "ts_ns": d * 10_000 + i,
                    "date": f"2026-01-{d:02d}",
                    "coinbase_ofi": float(i % 7) - 3.0,
                    "kraken_ofi": float(i % 5) - 2.0,
                    "kraken_spread_bps": 2.0,
                    "y_kraken_ret_bps_5": (float(i % 7) - 3.0) * 2.0,
                }
            )
    return pl.DataFrame(rows)


def test_the_threshold_reaches_the_pooled_rows() -> None:
    """Without this the dead-zone report silently does not happen: `threshold` stays
    None all the way through and the section is skipped rather than failing."""
    splits = walk_forward(_dz_frame(14), "y_kraken_ret_bps_5", 5, spread_col="kraken_spread_bps")
    assert splits
    assert all(s.test_threshold_bps is not None for s in splits)
    wf = evaluate_walk_forward(splits, 5)
    oos = wf.oos["gbt"]
    assert oos.threshold is not None
    assert len(oos.threshold) == len(oos.y)  # one threshold per pooled row, not one total


def test_omitting_the_spread_column_leaves_dead_zone_scoring_off() -> None:
    splits = walk_forward(_dz_frame(14), "y_kraken_ret_bps_5", 5)
    wf = evaluate_walk_forward(splits, 5)
    assert wf.oos["gbt"].threshold is None
    with pytest.raises(ValueError, match="threshold"):
        as_classes(wf.oos["gbt"])


def test_class_intervals_bracket_their_point_estimates() -> None:
    splits = walk_forward(_dz_frame(14), "y_kraken_ret_bps_5", 5, spread_col="kraken_spread_bps")
    ci = class_confidence_intervals(evaluate_walk_forward(splits, 5).oos["gbt"], n_boot=50)
    assert set(ci) == {"up_precision", "up_recall", "down_precision", "down_recall"}
    for name, (point, lo, hi) in ci.items():
        if not np.isnan(point):
            assert lo <= point <= hi, name
            assert 0.0 <= lo <= 1.0 and 0.0 <= hi <= 1.0, name


def test_the_per_fold_edge_and_classes_are_kept() -> None:
    """`fit_models` scores both on every fold and they used to be dropped on the floor
    here, so `n_trades` and the per-fold support counts were paid for and never seen."""
    splits = walk_forward(_dz_frame(14), "y_kraken_ret_bps_5", 5, spread_col="kraken_spread_bps")
    wf = evaluate_walk_forward(splits, 5)
    assert len(wf.fold_edge) == len(wf.splits)
    assert len(wf.fold_classes) == len(wf.splits)
    assert "n_trades" in wf.fold_edge[0]["gbt"]
    assert "up_support" in wf.fold_classes[0]["gbt"]
    # `zero` takes no position, so it has an entry in neither.
    assert "zero" not in wf.fold_edge[0]
    assert "zero" in wf.fold_metrics[0]


def test_no_classes_are_kept_without_a_spread_column() -> None:
    splits = walk_forward(_dz_frame(14), "y_kraken_ret_bps_5", 5)
    wf = evaluate_walk_forward(splits, 5)
    assert wf.fold_classes[0] == {}
    assert "n_trades" in wf.fold_edge[0]["gbt"]


# ── coverage-matched selectivity ────────────────────────────────────────────


def _cov_oos(pred: list[float], y: list[float], threshold: float = 1.0) -> OutOfSample:
    n = len(pred)
    return OutOfSample(
        dates=np.array([f"2026-01-{i % 4 + 1:02d}" for i in range(n)]),
        y=np.array(y, dtype=float),
        pred=np.array(pred, dtype=float),
        threshold=np.full(n, threshold),
    )


def test_coverage_forces_the_traded_share_regardless_of_prediction_scale() -> None:
    """The whole point: a model whose predictions never clear half a spread still gets
    scored, and one whose predictions always clear it stops being scored on everything."""
    tiny = _cov_oos(pred=[0.01 * i for i in range(100)], y=[0.0] * 100)
    huge = _cov_oos(pred=[100.0 * i for i in range(100)], y=[0.0] * 100)
    for oos in (tiny, huge):
        called = as_classes_at_coverage(oos, 0.10).pred != 0
        assert called.sum() == pytest.approx(10, abs=1)


def test_coverage_keeps_the_strongest_predictions() -> None:
    """Selectivity has to mean high-conviction, not an arbitrary tenth of the rows."""
    oos = _cov_oos(pred=[-5.0, 0.1, -0.2, 4.0, 0.0], y=[0.0] * 5)
    called = as_classes_at_coverage(oos, 0.4).pred
    assert list(called) == [-1.0, 0.0, 0.0, 1.0, 0.0]


def test_truth_still_uses_the_real_spread_not_the_coverage_cut() -> None:
    """Coverage reshapes what the model is allowed to call; it must not reshape what
    counts as a tradeable move, or the metric would grade against a moving target."""
    oos = _cov_oos(pred=[3.0, 3.0], y=[5.0, 0.5], threshold=1.0)
    assert list(as_classes_at_coverage(oos, 1.0).y) == [1.0, 0.0]


def test_coverage_intervals_bracket_their_point_estimates() -> None:
    splits = walk_forward(_dz_frame(14), "y_kraken_ret_bps_5", 5, spread_col="kraken_spread_bps")
    oos = evaluate_walk_forward(splits, 5).oos["gbt"]
    ci = coverage_confidence_intervals(oos, 0.25, n_boot=50)
    assert set(ci) == {"up_precision", "up_recall", "down_precision", "down_recall"}
    for name, (point, lo, hi) in ci.items():
        if not np.isnan(point):
            assert lo <= point <= hi, name


def test_coverage_scoring_needs_a_threshold() -> None:
    splits = walk_forward(_dz_frame(14), "y_kraken_ret_bps_5", 5)
    with pytest.raises(ValueError, match="threshold"):
        as_classes_at_coverage(evaluate_walk_forward(splits, 5).oos["gbt"], 0.1)
