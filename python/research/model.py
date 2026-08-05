"""Short-horizon price-discovery model over the silver feature matrix.

The question is not "can a model fit tick data" - it is *whose* order flow moves the
consolidated price. So the model is a means: fit forward NBBO mid returns from
per-venue book and flow features, then read grouped permutation importance to see
which venue's flow carries the information. That is the price-discovery rung of the
research ladder, answered with a model rather than a VECM.

Four disciplines make the number believable, and all four cost accuracy:

  - **Time-ordered splits, never shuffled.** Train on the earliest dates, test on the
    latest, with an embargo of one horizon at the boundary so no training row's
    forward window overlaps a test row. Early stopping gets its own time-ordered
    slice carved off the end of train, for the same reason.
  - **Stride-subsampled test metrics.** A 30-bar target on a 1-bar grid overlaps 30x;
    scoring every row would count each move 30 times and shrink the error bars to
    fiction. Headline metrics are computed on every h-th test row.
  - **Baselines that are hard to beat.** R2 is measured against predicting zero (the
    honest null for a return series), and a linear OFI-only model stands in for "what
    the microstructure literature already knew" - a tree that cannot beat it has
    learned nothing worth having.
  - **Signed features stay signed.** Order flow is constrained monotone in the target,
    so the tree cannot learn "heavy buying predicts a fall" from one month of one
    regime just because it fits.

Repeating any of this over expanding-window folds, and putting error bars on it, is
`research.validation`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

# Re-exported: the staleness tolerances live with the builder that stamps the age columns,
# so `clean` dropping a row and a feature window refusing to reach across a hole cannot
# disagree about what "stale" means.
from research.features import DEAD_ZONE_SPREADS, MAX_AGE_MS, MAX_TAPE_AGE_MS
from research.schema import FeatureSchema

log = logging.getLogger(__name__)

# Share of the training period held back, as its latest rows, to stop boosting early.
VAL_FRAC = 0.15

GBT_NOTE = """Why the tree stops where it stops, measured rather than assumed.

On one real fold (coinbase -> kraken, 5s, 213k train rows), letting boosting run to a
1000-iteration ceiling and stopping on each rule's own validation curve:

    stopping rule          n_iter   test R2   test hit
    random 15% of train       814   0.03844     0.6832
    time-ordered tail         467   0.04774     0.7049

The shuffled holdout shares forward windows with rows still being fit, so it keeps
reporting improvement for another ~350 iterations after the model has stopped
generalising forward, and pays about a fifth of the R2 for it. That is the entire case
for carving the validation slice by time.

The earlier hand-set `max_iter=200` scores higher still (0.05063) - but it was picked
by looking at test results, so adopting it would be choosing a hyperparameter on the
test set. The ~0.003 R2 between it and the val-chosen 467 is the cost of not peeking,
and it is the number that gets reported.
"""

# Features whose sign is fixed by theory rather than by fit. Order-flow imbalance and
# signed taker volume both measure net buying pressure, and every price-impact model
# from Kyle onward has impact rising with it. Constraining them costs training fit and
# buys a model that cannot conclude "heavy buying predicts a fall" from one month of
# one regime - which an unconstrained tree will happily do wherever noise supports it.
MONOTONE_POSITIVE = ("_ofi", "_signed_vol")


@dataclass
class Split:
    """One time-ordered train/val/test split, embargoed at both boundaries.

    `x_val` is the tail of the training period, not a random slice, so early stopping
    is judged on the most recent data the model is allowed to see. `test_row_dates`
    carries each test row's date so the bootstrap can resample whole days.
    """

    x_train: np.ndarray
    y_train: np.ndarray
    x_val: np.ndarray
    y_val: np.ndarray
    x_test: np.ndarray
    y_test: np.ndarray
    test_row_dates: np.ndarray
    feature_names: list[str]
    train_dates: list[str]
    test_dates: list[str]
    # Half-spread at each test row, when the caller asked for dead-zone scoring. Carried
    # per row rather than as one number because the threshold IS the prevailing spread.
    test_threshold_bps: np.ndarray | None = None


def feature_columns(df: pl.DataFrame, exclude_venue: str | None = None) -> list[str]:
    """Model inputs: the columns `research.schema` declares with the `feature` role.

    This used to exclude a tuple of name suffixes instead, which got the same answer for
    the wrong reason. Under exclusion a new family is a model input unless its names
    happen to collide with the drop list, so whether a feature reaches the model was a
    property of how it was spelled. Roles are declared once, next to the builder that
    emits the column.

    `exclude_venue` drops that venue's own features, which is what makes the cross-venue
    test honest: predicting Coinbase's next move using only Kraken's book and flow shares
    no term with the target.
    """
    return FeatureSchema.from_columns(df.columns).features(df.columns, exclude_venue)


def clean(
    df: pl.DataFrame,
    target: str,
    max_age_ms: int = MAX_AGE_MS,
    max_tape_age_ms: int = MAX_TAPE_AGE_MS,
) -> pl.DataFrame:
    """Drop rows the model must not see: stale books, and rows with no target (the
    tail of each date, where the forward window runs off the end).

    Tightening `max_age_ms` is how the catch-up hypothesis gets tested. If venue A only
    "predicts" venue B because B's book is lagging and has not caught up yet, the edge
    dies once both books are required to be fresh; if it is real price discovery, it
    survives.

    Two tolerances, because two clocks. A book age belongs to a venue's `book` family;
    every other staleness column times a tape stream that legitimately updates far slower,
    and holding it to the book's tolerance would throw the day away for a staleness that
    is the feed's normal cadence rather than an outage.
    """
    book_ages, tape_ages = FeatureSchema.from_columns(df.columns).age_columns()
    out = df.filter(pl.col(target).is_not_null())
    for columns, tol in ((book_ages, max_age_ms), (tape_ages, max_tape_age_ms)):
        for c in columns:
            out = out.filter(pl.col(c).is_null() | (pl.col(c) <= tol))
    return out


def split_on_dates(
    df: pl.DataFrame,
    target: str,
    horizon: int,
    train_dates: list[str],
    test_dates: list[str],
    exclude_venue: str | None = None,
    val_frac: float = VAL_FRAC,
    spread_col: str | None = None,
) -> Split | None:
    """Build one split from an explicit date assignment.

    The training period is cut twice. First the last `horizon` rows go, because their
    forward windows reach into the test period. Then the newest `val_frac` of what is
    left becomes the early-stopping set, with another `horizon` gap ahead of it for the
    same reason. Both embargoes are what keep "the model stopped here" from being a
    decision informed by data the model was not supposed to have.

    Layout, oldest to newest:  [ fit ] gap [ val ] gap [ test ]
    """
    cols = feature_columns(df, exclude_venue)
    train = df.filter(pl.col("date").is_in(train_dates))
    test = df.filter(pl.col("date").is_in(test_dates))
    if train.is_empty() or test.is_empty():
        return None

    train = train.head(max(0, train.height - horizon))
    n_val = round(train.height * val_frac)
    val = train.tail(n_val)
    fit = train.head(max(0, train.height - n_val - horizon))
    if fit.is_empty() or val.is_empty():
        log.warning("train period too short to carve a validation slice, %d rows", train.height)
        return None

    threshold = None
    if spread_col is not None:
        threshold = test[spread_col].to_numpy() * DEAD_ZONE_SPREADS

    return Split(
        x_train=fit.select(cols).to_numpy(),
        y_train=fit[target].to_numpy(),
        x_val=val.select(cols).to_numpy(),
        y_val=val[target].to_numpy(),
        x_test=test.select(cols).to_numpy(),
        y_test=test[target].to_numpy(),
        test_row_dates=test["date"].to_numpy(),
        feature_names=cols,
        train_dates=train_dates,
        test_dates=test_dates,
        test_threshold_bps=threshold,
    )


def make_split(
    df: pl.DataFrame,
    target: str,
    horizon: int,
    test_frac: float = 0.3,
    exclude_venue: str | None = None,
    val_frac: float = VAL_FRAC,
    spread_col: str | None = None,
) -> Split | None:
    """One holdout split: the earliest dates train, the latest `test_frac` test.

    Splitting on whole dates (not rows) keeps a day's intraday autocorrelation on one
    side of the boundary. Kept for single-shot diagnostics; the reported numbers come
    from `research.validation.walk_forward`, because one split of one month is a
    sample of size one.
    """
    dates = sorted(df["date"].unique().to_list())
    if len(dates) < 2:
        log.warning("need >= 2 dates to split, got %d", len(dates))
        return None
    n_test = max(1, round(len(dates) * test_frac))
    train_dates, test_dates = dates[:-n_test], dates[-n_test:]
    if not train_dates:
        return None
    return split_on_dates(
        df, target, horizon, train_dates, test_dates, exclude_venue, val_frac, spread_col
    )


def _r2_vs_zero(y: np.ndarray, pred: np.ndarray) -> float:
    """Out-of-sample R2 against predicting zero, not against the test mean.

    For a return series the honest null is "no view"; scoring against the realised
    test mean quietly credits the model for a drift it could not have known.
    """
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum(y**2))
    return 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")


def _hit_rate(y: np.ndarray, pred: np.ndarray) -> float:
    """Share of directional calls that were right, over rows where a call was made.

    Two exclusions, both to stop the number being gamed by inaction. Flat rows do not
    count, because on a 1s grid many bars do not move and scoring them would let a
    model that predicts a hair above zero report a hit rate near one. Rows the model
    predicted exactly zero on do not count either, because that is not a wrong call,
    it is no call - the same rows `_gross_bps` declines to trade. The `zero` baseline
    therefore has no hit rate at all, which is the honest answer for it.
    """
    called = (np.abs(y) > 1e-9) & (np.abs(pred) > 0)
    if not called.any():
        return float("nan")
    return float(np.mean(np.sign(pred[called]) == np.sign(y[called])))


def _gross_bps(y: np.ndarray, pred: np.ndarray) -> float:
    """Mean signed return from taking a unit position in the predicted direction."""
    take = np.abs(pred) > 0
    if not take.any():
        return float("nan")
    return float(np.mean(np.sign(pred[take]) * y[take]))


# Metrics a confidence interval is reported for. Each takes already-strided rows, so
# `research.validation` can resample them without re-deriving what a metric means.
METRICS = {"r2_vs_zero": _r2_vs_zero, "hit_rate": _hit_rate, "gross_bps_per_trade": _gross_bps}


def evaluate(y: np.ndarray, pred: np.ndarray, stride: int) -> dict[str, float]:
    """Metrics on non-overlapping test rows only."""
    y_s, p_s = y[::stride], pred[::stride]
    corr = float(np.corrcoef(y_s, p_s)[0, 1]) if len(y_s) > 2 and p_s.std() > 0 else float("nan")
    return {
        "n": float(len(y_s)),
        "r2_vs_zero": _r2_vs_zero(y_s, p_s),
        "hit_rate": _hit_rate(y_s, p_s),
        "corr": corr,
        "pred_std": float(p_s.std()),
        "y_std": float(y_s.std()),
    }


def _linear() -> object:
    """Ridge with median imputation.

    Nulls here are structural, not sampling noise: a rolling window has no value until
    it fills, and a venue that was down is null for the whole date. The tree handles
    those natively (it learns a direction for missing), so only the linear baselines
    impute - which means they are the more flattered of the two on missing-venue rows,
    not the less.
    """
    return make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=1.0))


def dead_zone_classes(values: np.ndarray, threshold_bps: np.ndarray) -> np.ndarray:
    """{-1, 0, +1} against a per-row threshold: flat unless the move clears it.

    Mirrors `features._dead_zone` so a prediction is judged by the same bar the target
    was labelled with. Applying it to the prediction too is the strict reading and the
    only honest one - a model whose view never clears the cost of crossing has no
    tradeable opinion, however well its sign correlates.
    """
    return np.sign(values) * (np.abs(values) > threshold_bps)


def _class_precision(cls: int):
    """Of the bars called `cls`, how many really were - the number that decides whether
    an edge exists at all. Undefined rather than zero when no call was made."""

    def metric(truth: np.ndarray, call: np.ndarray) -> float:
        called = call == cls
        if not called.any():
            return float("nan")
        return float(np.sum(called & (truth == cls)) / called.sum())

    return metric


def _class_recall(cls: int):
    """Of the bars that really were `cls`, how many were called. Says how much of the
    opportunity is left behind, which a high-precision model may well trade none of."""

    def metric(truth: np.ndarray, call: np.ndarray) -> float:
        actual = truth == cls
        if not actual.any():
            return float("nan")
        return float(np.sum(actual & (call == cls)) / actual.sum())

    return metric


# Metrics over ALREADY-CLASSIFIED rows, so the pooled date-block bootstrap can resample
# them with exactly the same machinery as the return metrics (validation.as_classes).
CLASS_METRICS = {
    "up_precision": _class_precision(1),
    "up_recall": _class_recall(1),
    "down_precision": _class_precision(-1),
    "down_recall": _class_recall(-1),
}


def class_report(
    y: np.ndarray, pred: np.ndarray, threshold_bps: np.ndarray, stride: int
) -> dict[str, float]:
    """Precision and recall on the two non-flat classes, plus how often each was seen.

    Hit rate answers "when it called a direction, was the sign right", which counts a
    quarter-tick wiggle as a win. This answers the question that survives costs: was the
    move big enough to be worth trading, and did the model say so.
    """
    truth = dead_zone_classes(y[::stride], threshold_bps[::stride])
    call = dead_zone_classes(pred[::stride], threshold_bps[::stride])
    out = {name: metric(truth, call) for name, metric in CLASS_METRICS.items()}
    out["traded_share"] = float(np.mean(call != 0)) if len(call) else float("nan")
    out["movable_share"] = float(np.mean(truth != 0)) if len(truth) else float("nan")
    out["up_support"] = float(np.sum(truth == 1))
    out["down_support"] = float(np.sum(truth == -1))
    return out


def edge_summary(y: np.ndarray, pred: np.ndarray, stride: int) -> dict[str, float]:
    """Gross edge per trade, which IS the break-even round-trip cost.

    Take a unit position in the predicted direction, hold one horizon, close. The mean
    signed return is the gross bps a trade captures before costs, so a round trip
    costing more than that loses money by definition. Reporting it this way avoids
    picking a fee tier and then arguing about it: compare the number to whatever the
    real all-in cost is (taker fee both sides, plus the spread crossed twice).

    This is deliberately generous to the model - no slippage, no queue, no latency,
    fills at the mid. A number that fails here fails by more in reality.
    """
    y_s, p_s = y[::stride], pred[::stride]
    n_trades = float((np.abs(p_s) > 0).sum())
    if not n_trades:
        return {"n_trades": 0.0, "gross_bps_per_trade": float("nan")}
    gross = _gross_bps(y_s, p_s)
    return {
        "n_trades": n_trades,
        "gross_bps_per_trade": gross,
        "gross_bps_total": gross * n_trades,
    }


def monotonic_constraints(feature_names: list[str]) -> list[int]:
    """+1 for features whose effect on a forward return is signed by theory, 0 else.

    Applied to the tree only. The linear baselines are left unconstrained on purpose:
    if a ridge fit wants a negative OFI coefficient, that is a diagnostic worth seeing
    rather than something to legislate away.
    """
    return [1 if any(p in c for p in MONOTONE_POSITIVE) else 0 for c in feature_names]


def fit_models(split: Split, horizon: int) -> dict[str, dict]:
    """Fit the baselines and the tree, and score them all on the same test rows.

    Each entry carries the fitted model, its test-set predictions and its metrics.
    Predictions are kept rather than recomputed because callers need the raw rows -
    walk-forward pools them across folds and bootstraps them.
    """
    results: dict[str, dict] = {}

    results["zero"] = {"model": None, "pred": np.zeros_like(split.y_test)}

    ofi_idx = [i for i, c in enumerate(split.feature_names) if "_ofi" in c]
    if ofi_idx:
        ridge_ofi = _linear()
        ridge_ofi.fit(split.x_train[:, ofi_idx], split.y_train)
        results["ridge_ofi"] = {
            "model": ridge_ofi,
            "pred": ridge_ofi.predict(split.x_test[:, ofi_idx]),
        }

    ridge = _linear()
    ridge.fit(split.x_train, split.y_train)
    results["ridge_all"] = {"model": ridge, "pred": ridge.predict(split.x_test)}

    # Depth and leaf size are constrained on purpose: unregularised, this returned
    # R2 = -34 on the no-signal direction with a prediction std 10x the target's.
    # `max_iter` is a ceiling, not a tuning knob - the stopping point is the val set's
    # to choose (see GBT_NOTE).
    gbt = HistGradientBoostingRegressor(
        max_iter=1000,
        learning_rate=0.03,
        max_depth=4,
        min_samples_leaf=200,
        l2_regularization=1.0,
        early_stopping=True,
        validation_fraction=None,
        n_iter_no_change=20,
        monotonic_cst=monotonic_constraints(split.feature_names),
        random_state=0,
    )
    gbt.fit(split.x_train, split.y_train, X_val=split.x_val, y_val=split.y_val)
    results["gbt"] = {"model": gbt, "pred": gbt.predict(split.x_test)}

    for res in results.values():
        res["metrics"] = evaluate(split.y_test, res["pred"], horizon)
        if res["model"] is not None:
            res["edge"] = edge_summary(split.y_test, res["pred"], horizon)
            if split.test_threshold_bps is not None:
                res["classes"] = class_report(
                    split.y_test, res["pred"], split.test_threshold_bps, horizon
                )
    return results


def venue_importance(
    model, split: Split, horizon: int, venues: list[str], n_repeats: int = 5
) -> dict[str, float]:
    """Permutation importance summed per venue: the price-discovery answer.

    Permutation is model-agnostic and measures what the *fitted* model actually leans
    on, unlike a tree's split-count importance which inflates high-cardinality inputs.
    Computed on the strided test rows so the shuffling is not fighting overlap.
    """
    x, y = split.x_test[::horizon], split.y_test[::horizon]
    imp = permutation_importance(model, x, y, n_repeats=n_repeats, random_state=0, scoring="r2")
    schema = FeatureSchema.from_columns(split.feature_names)
    by_venue = dict.fromkeys(venues, 0.0)
    for name, mean in zip(split.feature_names, imp.importances_mean, strict=True):
        venue = schema.venue_of(name)
        if venue is not None:
            by_venue[venue] += float(mean)
    return by_venue
