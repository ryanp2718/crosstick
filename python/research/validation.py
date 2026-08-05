"""Walk-forward folds and block-bootstrap confidence intervals.

A single train/test split of a single month is a sample of size one. It produces a
number with no error bar, and a number with no error bar cannot be argued with - which
is exactly why it should not be believed. This module turns the one number into a
distribution, two ways:

  - **Expanding-window folds.** Fit on everything up to a point, test on the next few
    dates, step forward, refit with the tested dates folded in. Every fold is trained
    only on its own past, so the pooled predictions are one continuous out-of-sample
    series, and the spread across folds says whether the result is a property of the
    market or of where the split happened to fall.
  - **Block bootstrap by date.** Resample whole days with replacement and recompute.
    The day is the block because everything inside one is autocorrelated: same regime,
    same participants, often the same trend. Resampling rows would treat 86k correlated
    observations as 86k independent ones and return an interval perhaps an order of
    magnitude too tight.

Expanding rather than rolling: with ~26 dates there is not enough history to throw the
early part away, and nothing here is trying to track a drifting regime - the claim
under test is a structural one about which venue leads.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import polars as pl

from research.model import (
    CLASS_METRICS,
    METRICS,
    Split,
    dead_zone_classes,
    fit_models,
    split_on_dates,
    venue_importance,
)

log = logging.getLogger(__name__)

# Fold geometry. Eight training dates is the point where a fold has seen enough
# weekday/weekend variety to not be a single-regime fit; testing three at a time and
# stepping three tiles the remaining dates exactly, so no date is scored twice.
MIN_TRAIN_DATES = 8
TEST_DATES = 3
STEP_DATES = 3

N_BOOT = 1000

# Below this many test dates the block bootstrap has too few blocks to resample into a
# real distribution - with B blocks there are only C(2B-1, B) distinct draws, so at
# B=2 the interval collapses onto the two fold values and reads as precision when it is
# the exact opposite. Reported intervals are flagged rather than suppressed.
MIN_BOOTSTRAP_BLOCKS = 8

# How far the placebo displaces the target. An hour is far past any horizon modelled
# here, so no genuine microstructure relationship can reach across it, while costing
# only the last hour of each date (~4% of rows) to the shift.
PLACEBO_LAG_BARS = 3600


@dataclass
class OutOfSample:
    """Strided out-of-sample rows pooled across folds, tagged by date.

    Every row was predicted by a model fitted only on earlier dates, and rows are one
    horizon apart so no two share a forward window. Those two properties together are
    what make it legitimate to treat this as a single sample and resample it.
    """

    dates: np.ndarray
    y: np.ndarray
    pred: np.ndarray
    # Half-spread at each row, when dead-zone scoring was asked for. Travels with the
    # rows so a resampled draw keeps each row's own threshold (see `as_classes`).
    threshold: np.ndarray | None = None


@dataclass
class WalkForward:
    """Everything a fold sequence produced, before any of it is formatted."""

    splits: list[Split]
    fold_metrics: list[dict[str, dict[str, float]]]
    oos: dict[str, OutOfSample]
    importance: list[dict[str, float]] = field(default_factory=list)


def walk_forward(
    df: pl.DataFrame,
    target: str,
    horizon: int,
    min_train: int = MIN_TRAIN_DATES,
    test_size: int = TEST_DATES,
    step: int = STEP_DATES,
    exclude_venue: str | None = None,
    spread_col: str | None = None,
) -> list[Split]:
    """Expanding-window splits over the dates present in `df`.

    Returns an empty list when the date count cannot support the geometry, rather than
    quietly shrinking it - a fold count is a thing the caller should have chosen.
    """
    dates = sorted(df["date"].unique().to_list())
    if step < test_size:
        log.warning(
            "step %d < test_size %d: test windows overlap, so dates repeat in the "
            "pooled series and the bootstrap will double-count them",
            step,
            test_size,
        )
    splits = []
    for start in range(min_train, len(dates) - test_size + 1, step):
        split = split_on_dates(
            df,
            target,
            horizon,
            dates[:start],
            dates[start : start + test_size],
            exclude_venue,
            spread_col=spread_col,
        )
        if split is None:
            log.warning("fold at date index %d produced no usable split", start)
            continue
        splits.append(split)
    return splits


def placebo_target(df: pl.DataFrame, target: str, lag_bars: int = PLACEBO_LAG_BARS) -> pl.DataFrame:
    """Displace the target within each date, leaving everything else untouched.

    The cheapest leak detector available. Same rows, same features, same marginal
    distributions, same autocorrelation, same folds - only the pairing between a row's
    features and its outcome is destroyed. A real edge dies here; lookahead in the
    feature construction does not, because it never depended on that pairing.

    Shifted within a date rather than rolled across the whole frame, so no row is ever
    paired with a different day's return and the date-block bootstrap stays coherent.
    The trailing `lag_bars` rows of each date lose their target and `clean` drops them.

    A passing run reports R2 at or slightly below zero and a hit rate at 0.50. Anything
    materially above that is a leak, and no other number from the run is worth reading
    until it is found.
    """
    return df.with_columns(pl.col(target).shift(-lag_bars).over("date").alias(target))


def _strided(split: Split, pred: np.ndarray, horizon: int) -> OutOfSample:
    """One fold's test rows, thinned to non-overlapping forward windows."""
    thr = split.test_threshold_bps
    return OutOfSample(
        dates=split.test_row_dates[::horizon],
        y=split.y_test[::horizon],
        pred=pred[::horizon],
        threshold=None if thr is None else thr[::horizon],
    )


def _concat(parts: list[OutOfSample]) -> OutOfSample:
    have_threshold = all(p.threshold is not None for p in parts)
    return OutOfSample(
        dates=np.concatenate([p.dates for p in parts]),
        y=np.concatenate([p.y for p in parts]),
        pred=np.concatenate([p.pred for p in parts]),
        threshold=np.concatenate([p.threshold for p in parts]) if have_threshold else None,
    )


def as_classes(oos: OutOfSample) -> OutOfSample:
    """The same pooled rows relabelled into dead-zone classes, y and pred alike.

    Reducing both sides to {-1, 0, +1} up front is what lets precision and recall reuse
    `bootstrap_ci` untouched: once classified they are ordinary two-argument metrics, so
    they get the same whole-date resampling as R2 rather than a parallel code path with
    its own subtly different notion of a block.
    """
    if oos.threshold is None:
        raise ValueError("dead-zone scoring needs a threshold; pass spread_col to walk_forward")
    return OutOfSample(
        dates=oos.dates,
        y=dead_zone_classes(oos.y, oos.threshold),
        pred=dead_zone_classes(oos.pred, oos.threshold),
    )


def as_classes_at_coverage(oos: OutOfSample, coverage: float) -> OutOfSample:
    """`as_classes`, but the prediction is cut at whatever |pred| makes the model call
    exactly `coverage` of bars, instead of at half the spread.

    Precision is only comparable between two runs at equal coverage. A model calling 8%
    of bars is answering an easier question than one calling 94%, and the half-spread cut
    lets coverage float with whatever the model happens to output - so a run and its
    placebo land at different points on their own curves and the two precisions cannot be
    compared. Truth keeps the real half-spread threshold: what counts as a tradeable move
    is a property of the book, not of the model.
    """
    if oos.threshold is None:
        raise ValueError("dead-zone scoring needs a threshold; pass spread_col to walk_forward")
    magnitude = np.abs(oos.pred)
    cut = float(np.quantile(magnitude, 1.0 - coverage)) if len(magnitude) else float("inf")
    return OutOfSample(
        dates=oos.dates,
        y=dead_zone_classes(oos.y, oos.threshold),
        pred=np.sign(oos.pred) * (magnitude > cut),
    )


def coverage_confidence_intervals(
    oos: OutOfSample, coverage: float, n_boot: int = N_BOOT, seed: int = 0
) -> dict[str, tuple[float, float, float]]:
    """Dead-zone precision and recall at a fixed traded share, same date blocks."""
    return _intervals(as_classes_at_coverage(oos, coverage), CLASS_METRICS, n_boot, seed)


def evaluate_walk_forward(
    splits: list[Split], horizon: int, venues: list[str] | None = None, n_repeats: int = 5
) -> WalkForward:
    """Fit every model on every fold, keeping both the per-fold scores and the rows.

    Per-fold scores answer "how much does this move around"; the pooled rows answer
    "what is the interval on the headline", and they are different questions - a stable
    mean across folds with a wide bootstrap interval means the effect is consistent but
    small relative to daily noise.
    """
    fold_metrics: list[dict[str, dict[str, float]]] = []
    parts: dict[str, list[OutOfSample]] = {}
    importance: list[dict[str, float]] = []

    for i, split in enumerate(splits, 1):
        log.info(
            "fold %d/%d: train %s..%s (%d rows) -> test %s..%s",
            i,
            len(splits),
            split.train_dates[0],
            split.train_dates[-1],
            len(split.y_train),
            split.test_dates[0],
            split.test_dates[-1],
        )
        results = fit_models(split, horizon)
        fold_metrics.append({name: res["metrics"] for name, res in results.items()})
        for name, res in results.items():
            parts.setdefault(name, []).append(_strided(split, res["pred"], horizon))
        if venues:
            importance.append(
                venue_importance(results["gbt"]["model"], split, horizon, venues, n_repeats)
            )

    return WalkForward(
        splits=splits,
        fold_metrics=fold_metrics,
        oos={name: _concat(p) for name, p in parts.items()},
        importance=importance,
    )


def bootstrap_ci(
    oos: OutOfSample,
    metric,
    n_boot: int = N_BOOT,
    seed: int = 0,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Percentile interval for one metric, resampling whole dates with replacement.

    The bootstrap distribution over a couple of dozen days is coarse, and that
    coarseness is the honest answer rather than a defect to smooth over: this capture
    holds days, not years, and an interval that pretended otherwise would be the lie.
    """
    dates = np.unique(oos.dates)
    if len(dates) < 2 or np.isnan(metric(oos.y, oos.pred)):
        return (float("nan"), float("nan"))
    rows_for = {d: np.flatnonzero(oos.dates == d) for d in dates}
    rng = np.random.default_rng(seed)
    stats = np.empty(n_boot)
    for b in range(n_boot):
        picked = rng.choice(dates, size=len(dates), replace=True)
        idx = np.concatenate([rows_for[d] for d in picked])
        stats[b] = metric(oos.y[idx], oos.pred[idx])
    lo, hi = np.nanpercentile(stats, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return float(lo), float(hi)


def _intervals(oos: OutOfSample, metrics: dict, n_boot: int, seed: int):
    return {
        name: (metric(oos.y, oos.pred), *bootstrap_ci(oos, metric, n_boot, seed))
        for name, metric in metrics.items()
    }


def confidence_intervals(
    oos: OutOfSample, n_boot: int = N_BOOT, seed: int = 0
) -> dict[str, tuple[float, float, float]]:
    """Every reported metric as (point estimate, lo, hi)."""
    return _intervals(oos, METRICS, n_boot, seed)


def class_confidence_intervals(
    oos: OutOfSample, n_boot: int = N_BOOT, seed: int = 0
) -> dict[str, tuple[float, float, float]]:
    """Dead-zone precision and recall as (point estimate, lo, hi), same date blocks.

    Precision on a thin high-conviction slice is exactly where a point estimate flatters
    itself, because the denominator can be a handful of bars on a handful of days - so
    it gets the same interval discipline as R2 rather than being quoted bare.
    """
    return _intervals(as_classes(oos), CLASS_METRICS, n_boot, seed)


def fold_spread(fold_metrics: list[dict[str, dict[str, float]]], model: str, key: str):
    """Mean and standard deviation of one metric across folds."""
    vals = np.array([m[model][key] for m in fold_metrics if model in m], dtype=float)
    if not len(vals):
        return float("nan"), float("nan")
    return float(np.nanmean(vals)), float(np.nanstd(vals))
