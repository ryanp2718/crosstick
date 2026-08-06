"""Does the leak detector detect?

`placebo_target` is the only guard between this pipeline and a number that looks like
an edge but is a bug, and a guard that cannot fail is not a guard. These two tests are
a matched pair over synthetic data whose answer is known by construction: an edge the
walk-forward must find, and the same frame with the feature-to-outcome pairing
displaced, which it must then fail to find. Either one alone proves nothing - "the
placebo found nothing" is satisfied just as well by a pipeline that can find nothing
at all.

Deliberately synthetic. The real placebo over 20 dates of silver is the run that says
something about the market (R2 -0.00010 [-0.00028, 0.00011]); this one says the
machinery that reported it works, and says so in seconds with no lake behind it.

Scope: the placebo asks whether the reported skill depends on each row's own outcome,
and a number that survives displacement was never a prediction. Not everything wrong
with a pipeline dies here. A tree held to `min_samples_leaf=200` cannot memorise a
scrambled target even when a fold is handed its own test dates, so fold contamination
reads clean under a placebo and needs the separate test it has in test_validation.py.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from research.model import _hit_rate, _r2_vs_zero, clean
from research.validation import bootstrap_ci, evaluate_walk_forward, placebo_target, walk_forward

DATES = 20
ROWS_PER_DATE = 400
HORIZON = 5
TARGET = "y_kraken_ret_bps_5"
# Bars to displace the target by. Small enough that most rows keep a target (the
# trailing `LAG` of each date lose theirs), large enough to clear the horizon.
LAG = 40


def _frame(signal: float, seed: int = 0) -> pl.DataFrame:
    """One venue's OFI drives the other's forward return, contemporaneously.

    Rows are drawn independently, so displacing the target severs the relationship
    completely: any surviving R2 is the machinery's doing rather than autocorrelation
    left in the fixture. The lead is positively signed because the tree carries a
    monotone-increasing constraint on `_ofi` features.
    """
    rng = np.random.default_rng(seed)
    n = DATES * ROWS_PER_DATE
    lead = rng.standard_normal(n)
    return pl.DataFrame(
        {
            "ts_ns": np.arange(n, dtype=np.int64),
            "date": np.repeat([f"2026-03-{d:02d}" for d in range(1, DATES + 1)], ROWS_PER_DATE),
            "coinbase_ofi": lead,
            "kraken_ofi": rng.standard_normal(n),
            TARGET: signal * lead + rng.standard_normal(n),
        }
    )


def _walk(df: pl.DataFrame) -> tuple[float, tuple[float, float], float]:
    """(R2, its date-block interval, hit rate) for the tree, walked forward.

    Placebo then clean, the order `research.main` uses: displacing the target strands
    the trailing rows of each date without one, and clean is what drops them.
    """
    splits = walk_forward(clean(df, TARGET), TARGET, HORIZON)
    assert splits, "fixture produced no folds"
    oos = evaluate_walk_forward(splits, HORIZON).oos["gbt"]
    return (
        _r2_vs_zero(oos.y, oos.pred),
        bootstrap_ci(oos, _r2_vs_zero, n_boot=200),
        _hit_rate(oos.y, oos.pred),
    )


def test_an_edge_that_is_really_there_is_found() -> None:
    r2, (lo, _), hit = _walk(_frame(signal=2.0))
    assert r2 > 0.2
    assert lo > 0.0, "a real edge should clear the date-block interval"
    assert hit > 0.6


def test_displacing_the_target_kills_the_edge() -> None:
    r2, (lo, _), hit = _walk(placebo_target(_frame(signal=2.0), TARGET, lag_bars=LAG))
    assert r2 < 0.01
    assert lo < 0.0, "the placebo interval must not establish positive skill"
    assert 0.45 < hit < 0.55
