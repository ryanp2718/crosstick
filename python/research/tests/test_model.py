"""Model-side guarantees: honest scoring, honest splits, honest cross-venue features.

Each test here pins a claim the reported numbers depend on. The feature-exclusion one
matters most: if it regresses, the cross-venue result silently becomes the tautology it
was designed to avoid, and nothing else in the pipeline would notice.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from research.model import (
    VAL_FRAC,
    _hit_rate,
    _r2_vs_zero,
    class_report,
    clean,
    dead_zone_classes,
    edge_summary,
    evaluate,
    feature_columns,
    leg_of,
    make_split,
    monotonic_constraints,
    venue_prefixes,
)


def _frame(dates: list[str], n_per_date: int = 50) -> pl.DataFrame:
    rows = []
    for d in dates:
        for i in range(n_per_date):
            rows.append(
                {
                    "ts_ns": i,
                    "date": d,
                    "coinbase_ofi": float(i),
                    "coinbase_imbalance": 0.5,
                    "coinbase_mid": 100.0,
                    "coinbase_age_ms": 1.0,
                    "kraken_ofi": float(-i),
                    "kraken_imbalance": 0.25,
                    "nbbo_spread_bps": 1.0,
                    "y_kraken_ret_bps_5": 0.1,
                }
            )
    return pl.DataFrame(rows)


# ── cross-venue feature integrity ───────────────────────────────────────────


def test_exclude_venue_drops_that_venue_and_the_nbbo() -> None:
    """The whole cross-venue claim rests on this: predicting kraken must not see any
    kraken feature, nor an NBBO column built partly from kraken."""
    cols = feature_columns(_frame(["2026-01-01"]), exclude_venue="kraken")
    assert not [c for c in cols if c.startswith("kraken_")]
    assert not [c for c in cols if c.startswith("nbbo_")]
    assert "coinbase_ofi" in cols


def test_price_levels_are_never_features() -> None:
    """Raw mids are non-stationary; a tree would memorise the level of BTC."""
    cols = feature_columns(_frame(["2026-01-01"]))
    assert not [c for c in cols if c.endswith(("_mid", "_bid", "_ask", "_age_ms"))]


# ── scoring ─────────────────────────────────────────────────────────────────


def test_r2_denominator_is_sum_of_squares_not_variance() -> None:
    """The null is "predict zero", not "predict the test mean": for a return series the
    realised mean is a drift the model could not have known, and the classic
    variance-denominator R2 quietly hands it over."""
    y = np.array([10.0, 12.0])
    # classic R2 against the mean would be exactly 0.0 for this prediction
    assert _r2_vs_zero(y, np.full(2, y.mean())) == 1 - 2.0 / 244.0
    # the honest null scores exactly zero, and a perfect fit scores one
    assert _r2_vs_zero(y, np.zeros(2)) == 0.0
    assert _r2_vs_zero(y, y) == 1.0


def test_metrics_use_only_every_stride_th_row() -> None:
    """Overlapping targets would otherwise be counted `stride` times over."""
    y = np.arange(100, dtype=float)
    assert evaluate(y, y, stride=10)["n"] == 10
    assert evaluate(y, y, stride=1)["n"] == 100


def test_a_model_with_no_view_has_no_hit_rate_rather_than_a_zero_one() -> None:
    """The `zero` baseline predicts zero everywhere. Scoring that as 0% right would
    read as a uniquely terrible model, when in fact it declined to call anything - and
    it is the row set `edge_summary` already refuses to trade."""
    y = np.array([2.0, -4.0, 6.0])
    assert np.isnan(_hit_rate(y, np.zeros(3)))
    # a model that calls only some rows is scored on the rows it called
    assert _hit_rate(y, np.array([1.0, 0.0, -1.0])) == 0.5


def test_edge_is_the_mean_signed_return() -> None:
    y = np.array([2.0, -4.0, 6.0, -8.0])
    pred = np.array([1.0, -1.0, -1.0, 1.0])  # right, right, wrong, wrong
    # signed: +2, +4, -6, -8 -> mean -2.0
    assert edge_summary(y, pred, stride=1)["gross_bps_per_trade"] == -2.0


# ── splitting ───────────────────────────────────────────────────────────────


def test_split_is_time_ordered_with_no_shared_dates() -> None:
    split = make_split(_frame([f"2026-01-0{i}" for i in range(1, 6)]), "y_kraken_ret_bps_5", 5)
    assert split is not None
    assert max(split.train_dates) < min(split.test_dates)
    assert not set(split.train_dates) & set(split.test_dates)


def test_split_embargoes_the_training_tail() -> None:
    """The last `horizon` training rows see forward into the test period, and the
    validation slice needs the same gap ahead of it."""
    df = _frame(["2026-01-01", "2026-01-02", "2026-01-03"], n_per_date=50)
    horizon = 5
    split = make_split(df, "y_kraken_ret_bps_5", horizon)
    assert split is not None
    # 2 train dates x 50 rows, minus the test embargo, is the training period
    train = 100 - horizon
    n_val = round(train * VAL_FRAC)
    assert len(split.y_val) == n_val
    assert len(split.y_train) == train - n_val - horizon


def test_single_date_cannot_be_split() -> None:
    assert make_split(_frame(["2026-01-01"]), "y_kraken_ret_bps_5", 5) is None


def test_validation_slice_is_the_newest_training_rows_not_a_random_sample() -> None:
    """Early stopping on a shuffled holdout is the failure this exists to prevent: with
    an overlapping target the holdout shares forward windows with rows still being fit,
    so it keeps improving after the model has stopped generalising forward. Overwriting
    `coinbase_ofi` with the global row index makes "newest" checkable as "largest"."""
    df = _frame(["2026-01-01", "2026-01-02", "2026-01-03"], n_per_date=50).with_columns(
        pl.int_range(pl.len()).cast(pl.Float64).alias("coinbase_ofi")
    )
    split = make_split(df, "y_kraken_ret_bps_5", 5)
    assert split is not None
    col = split.feature_names.index("coinbase_ofi")
    assert split.x_val[:, col].min() > split.x_train[:, col].max()


def test_test_rows_carry_their_date_for_the_bootstrap() -> None:
    """Blocking the bootstrap by date needs a date per row, not per split."""
    df = _frame(["2026-01-01", "2026-01-02", "2026-01-03"], n_per_date=50)
    split = make_split(df, "y_kraken_ret_bps_5", 5)
    assert split is not None
    assert len(split.test_row_dates) == len(split.y_test)
    assert set(split.test_row_dates) == set(split.test_dates)


# ── constraints ─────────────────────────────────────────────────────────────


def test_only_signed_flow_features_are_constrained_monotone() -> None:
    """Book imbalance and spread are deliberately left free: the sign of their effect
    is an empirical question, while order flow's is not."""
    names = [
        "coinbase_ofi",
        "coinbase_ofi_300",
        "kraken_signed_vol_5",
        "kraken_imbalance",
        "nbbo_spread_bps",
        "coinbase_depth_imb_10",
    ]
    assert monotonic_constraints(names) == [1, 1, 1, 0, 0, 0]


# ── leg resolution across venues whose names prefix each other ──────────────


def _binance_frame() -> pl.DataFrame:
    """A frame with both binance legs, which is the case cross-symbol legs create."""
    return pl.DataFrame(
        {
            "ts_ns": [0, 1],
            "date": ["2026-01-01"] * 2,
            "binance_ofi": [1.0, 2.0],
            "binance_ret_bps_5": [0.1, 0.2],
            "binance_futures_ofi": [3.0, 4.0],
            "binance_futures_ret_bps_5": [0.3, 0.4],
            "coinbase_ofi": [5.0, 6.0],
            "y_coinbase_ret_bps_5": [0.5, 0.6],
        }
    )


def test_excluding_binance_keeps_the_perp() -> None:
    """`binance_futures_ofi` starts with `binance_`, so a naive prefix match would drop
    two venues while reporting one. That would understate the feature set of every
    cross-venue run without changing a single printed label."""
    cols = feature_columns(_binance_frame(), exclude_venue="binance")
    assert not [c for c in cols if c.startswith("binance_ofi")]
    assert "binance_futures_ofi" in cols
    assert "binance_futures_ret_bps_5" in cols


def test_excluding_the_perp_keeps_binance_spot() -> None:
    cols = feature_columns(_binance_frame(), exclude_venue="binance_futures")
    assert not [c for c in cols if c.startswith("binance_futures")]
    assert "binance_ofi" in cols


def test_venue_prefixes_are_longest_first() -> None:
    """Load-bearing ordering: `leg_of` takes the first match."""
    venues = venue_prefixes(_binance_frame())
    assert set(venues) == {"binance", "binance_futures", "coinbase"}
    assert venues.index("binance_futures") < venues.index("binance")


def test_leg_of_resolves_the_longer_venue_name() -> None:
    venues = venue_prefixes(_binance_frame())
    assert leg_of("binance_futures_ofi", venues) == "binance_futures"
    assert leg_of("binance_ofi", venues) == "binance"
    assert leg_of("nbbo_spread_bps", venues) is None


def test_trailing_returns_survive_into_the_feature_set() -> None:
    """They are the only channel a cross-quote-asset leg has, so a drop-suffix rule
    that swallowed them would make the extra legs nearly inert."""
    cols = feature_columns(_binance_frame())
    assert "binance_ret_bps_5" in cols
    assert "binance_futures_ret_bps_5" in cols


# ── staleness tolerances ────────────────────────────────────────────────────


def _aged(book_ms: float, tape_ms: float) -> pl.DataFrame:
    """One venue leg (its `_ofi` is what makes it a venue) plus a perp tape age."""
    return pl.DataFrame(
        {
            "coinbase_ofi": [1.0],
            "coinbase_age_ms": [book_ms],
            "binance_futures_oi_age_ms": [tape_ms],
            "y_ret_bps_5": [1.0],
        }
    )


def test_a_tape_age_is_not_held_to_the_book_tolerance() -> None:
    """Open interest is a 10s poll, so it is routinely older than the 5s a book may be.
    Judging it by the book's tolerance would drop nearly every row of every date - a
    silent near-total data loss that still produces a plausible-looking model."""
    assert clean(_aged(book_ms=100.0, tape_ms=9_000.0), "y_ret_bps_5").height == 1


def test_a_stale_book_still_drops_the_row() -> None:
    assert clean(_aged(book_ms=99_000.0, tape_ms=100.0), "y_ret_bps_5").is_empty()


def test_a_genuinely_dead_tape_drops_the_row() -> None:
    """Minutes of carried-forward open interest is an outage, not a slow feed."""
    assert clean(_aged(book_ms=100.0, tape_ms=600_000.0), "y_ret_bps_5").is_empty()


def test_liquidation_flow_is_not_constrained_monotone() -> None:
    """Forced flow must not inherit the taker-flow prior by name. A cascade that
    overshoots and reverts within seconds is the case where the sign is the open
    question - `_signed_vol` as a substring would have legislated an answer."""
    names = ["binance_futures_liq_flow_5", "binance_futures_signed_vol_5", "coinbase_ofi"]
    assert monotonic_constraints(names) == [0, 1, 1]


# ── dead-zone scoring ───────────────────────────────────────────────────────


def test_dead_zone_classes_need_to_clear_the_threshold() -> None:
    vals = np.array([2.0, -2.0, 0.4, -0.4, 0.0])
    thr = np.full(5, 1.0)
    assert list(dead_zone_classes(vals, thr)) == [1.0, -1.0, 0.0, -0.0, 0.0]


def test_a_view_that_never_clears_the_cost_trades_nothing() -> None:
    """The strict reading: a model whose predictions all sit inside the dead zone has no
    tradeable opinion, however well its sign lines up. That has to show as zero traded
    share rather than as a flattering precision on calls it would never have made."""
    y = np.array([5.0, -5.0, 5.0, -5.0])
    pred = np.array([0.1, -0.1, 0.1, -0.1])  # right sign every time, far inside the zone
    got = class_report(y, pred, np.full(4, 1.0), stride=1)
    assert got["traded_share"] == 0.0
    assert got["movable_share"] == 1.0
    assert np.isnan(got["up_precision"])  # no calls made, so precision is undefined
    assert got["up_recall"] == 0.0  # the opportunity was there and was missed


def test_precision_and_recall_split_the_two_failure_modes() -> None:
    y = np.array([5.0, 5.0, -5.0, 0.0])
    pred = np.array([5.0, 0.1, 5.0, 5.0])  # 1 right up-call, 2 wrong, 1 up missed
    got = class_report(y, pred, np.full(4, 1.0), stride=1)
    assert got["up_precision"] == 1 / 3  # three up-calls, one correct
    assert got["up_recall"] == 1 / 2  # two real ups, one caught
    assert got["up_support"] == 2.0
    assert got["traded_share"] == 3 / 4


def test_class_report_honours_the_stride() -> None:
    """Overlapping targets are scored on non-overlapping rows, exactly as `evaluate` is,
    or the class counts inherit the same 30x double counting."""
    y = np.array([5.0, -5.0, 5.0, -5.0])
    pred = np.array([5.0, 5.0, 5.0, 5.0])
    got = class_report(y, pred, np.full(4, 1.0), stride=2)
    assert got["up_support"] == 2.0  # rows 0 and 2 only
    assert got["up_precision"] == 1.0
