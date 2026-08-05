"""Known-answer tests for the information-share estimator.

There is no reference implementation in the project to check against, so correctness is
pinned by simulating systems whose answer is known by construction: a pure leader/follower
pair, a symmetric pair, and a pair whose roles are swapped. An estimator that gets the
easy cases wrong cannot be trusted on the real tape, where nothing is known in advance.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from research.infoshare import (
    DEFAULT_LAGS,
    MIN_ROWS,
    OBS_PER_PARAMETER,
    by_date,
    fit,
    information_shares,
    min_rows_for,
    price_panel,
    report,
)

VENUES = ("coinbase", "kraken")


def _leader_follower(
    n: int = 20_000, adjust: float = 0.5, noise: float = 0.02, seed: int = 0
) -> np.ndarray:
    """Venue 0 IS the efficient price; venue 1 closes a fraction of the gap each bar.

    Partial adjustment, not `roll(efficient, 1)`: setting the follower to the leader's
    lagged price makes the error-correction term identical to the leader's own lagged
    return, and the two regressors then trade coefficients off against each other. Here
    the gap is a persistent AR process, which is what a VECM is actually specified for.

    Every innovation originates at venue 0, so its information share is 1 by construction
    and alpha is (0, adjust).
    """
    rng = np.random.default_rng(seed)
    leader = np.cumsum(rng.normal(size=n))
    idiosyncratic = rng.normal(scale=noise, size=n)
    follower = np.empty(n)
    follower[0] = leader[0]
    for t in range(1, n):
        follower[t] = (
            follower[t - 1] + adjust * (leader[t - 1] - follower[t - 1]) + idiosyncratic[t]
        )
    return np.column_stack([leader, follower])


def _symmetric(n: int = 20_000, noise: float = 0.02, seed: int = 0) -> np.ndarray:
    """Both venues observe the same efficient price with independent noise and no lag."""
    rng = np.random.default_rng(seed)
    efficient = np.cumsum(rng.normal(size=n))
    return np.column_stack(
        [efficient + rng.normal(scale=noise, size=n), efficient + rng.normal(scale=noise, size=n)]
    )


def _two_sided(
    n: int = 40_000, w: float = 0.5, adjust: float = 0.3, noise: float = 0.01, seed: int = 0
) -> np.ndarray:
    """Both venues quote one efficient price; venue 0 sees share `w` of the news first.

    The realistic case, and the only one here with a TUNABLE known answer: venue 0's
    information share is `w` by construction, so the estimator has to track `w` rather
    than merely get one configuration right. Neither venue has a permanent component of
    its own, so the pair stays cointegrated - adding an independent random walk to one
    venue destroys that and drives every error-correction loading to zero.
    """
    rng = np.random.default_rng(seed)
    news = [rng.normal(scale=np.sqrt(w), size=n), rng.normal(scale=np.sqrt(1.0 - w), size=n)]
    efficient = np.cumsum(news[0] + news[1])
    idiosyncratic = [rng.normal(scale=noise, size=n), rng.normal(scale=noise, size=n)]
    prices = np.empty((n, 2))
    prices[0] = efficient[0]
    for t in range(1, n):
        for v in (0, 1):
            prices[t, v] = (
                prices[t - 1, v]
                + news[v][t]
                + adjust * (efficient[t - 1] - prices[t - 1, v])
                + idiosyncratic[v][t]
            )
    return prices


# ── the estimator on known systems ──────────────────────────────────────────


def test_the_leader_takes_essentially_all_of_the_information_share() -> None:
    got = information_shares(_leader_follower(), VENUES)
    lower, upper = got.bounds[0]
    assert lower > 0.9, f"leader lower bound {lower:.3f}"
    assert upper > 0.9


@pytest.mark.parametrize("w", [0.1, 0.3, 0.5, 0.7, 0.9])
def test_the_information_share_tracks_the_true_news_split(w: float) -> None:
    """The load-bearing test. One correct configuration proves nothing; the estimate has
    to move with the truth across the range."""
    got = information_shares(_two_sided(w=w), VENUES)
    assert got.midpoints[0] == pytest.approx(w, abs=0.03), f"w={w} -> {got.midpoints[0]:.4f}"


def test_the_follower_error_corrects_and_the_leader_does_not() -> None:
    """alpha ~ 0 on the leader is the mechanism behind the share, so pin it directly -
    a share that came out right with the wrong alphas would be a coincidence.

    Only the follower's loading is asserted. The leader's is NOT separately identified in
    a one-sided system: the error-correction term is a geometric sum of the leader's own
    past innovations, which are themselves regressors, so OLS splits the coefficient
    between them arbitrarily. Measured across lag orders it ran +0.24 to -0.23 while the
    share stayed at 0.9998+. That is the fragility `conditioning` exists to flag.
    """
    alpha, _, conditioning = fit(_leader_follower(adjust=0.5), lags=2)
    assert alpha[1] == pytest.approx(0.5, abs=0.05), f"follower correction {alpha[1]:.4f}"
    assert conditioning > 10.0


def test_gonzalo_granger_measures_adjustment_speed_not_news_share() -> None:
    """The two measures are not interchangeable and this is where they part company.

    Both venues adjust at the same speed here, so neither is more of an anchor and GG is
    right to say 50/50 - even though venue 0 originates 90% of the news, which is what
    Hasbrouck reports. A reader given only one of these numbers would draw the opposite
    conclusion depending on which one it was.
    """
    got = information_shares(_two_sided(w=0.9), VENUES)
    assert got.midpoints[0] == pytest.approx(0.9, abs=0.03)
    assert got.component_shares[0] == pytest.approx(0.5, abs=0.1)


def test_a_component_share_outside_the_unit_interval_is_flagged() -> None:
    """GG leaving [0, 1] is the signal that alpha was not identified; it must be
    detectable rather than silently printed as if it were a share."""
    clean = information_shares(_two_sided(w=0.5), VENUES)
    assert clean.component_shares_usable
    degenerate = information_shares(_leader_follower(adjust=0.5), VENUES, lags=1)
    assert not degenerate.component_shares_usable
    assert degenerate.component_shares[0] > 1.0


def test_swapping_the_columns_swaps_the_answer() -> None:
    """Guards an index slip in the orthogonal complement, which would otherwise be
    invisible: every other test would still pass with the venues silently transposed."""
    panel = _leader_follower()
    forward = information_shares(panel, VENUES)
    reverse = information_shares(panel[:, ::-1], VENUES[::-1])
    assert forward.bounds[0][0] > 0.9
    assert reverse.bounds[1][0] > 0.9
    assert forward.component_shares[0] == pytest.approx(reverse.component_shares[1], abs=1e-6)


def test_a_symmetric_pair_splits_the_share() -> None:
    got = information_shares(_symmetric(), VENUES)
    assert got.midpoints[0] == pytest.approx(0.5, abs=0.15)
    assert got.midpoints[1] == pytest.approx(0.5, abs=0.15)


def test_component_shares_sum_to_one() -> None:
    for panel in (_leader_follower(), _symmetric()):
        assert information_shares(panel, VENUES).component_shares.sum() == pytest.approx(1.0)


def test_bounds_are_ordered_and_the_pair_brackets_one() -> None:
    """Hasbrouck shares are only bounds; what must hold is that each venue's interval is
    ordered and that lower bounds cannot sum above 1 nor upper bounds below it."""
    got = information_shares(_symmetric(), VENUES)
    assert np.all(got.bounds[:, 0] <= got.bounds[:, 1])
    assert got.bounds[:, 0].sum() <= 1.0 + 1e-9
    assert got.bounds[:, 1].sum() >= 1.0 - 1e-9


def test_a_share_never_leaves_the_unit_interval() -> None:
    """A Hasbrouck share is a proportion of variance, so [0, 1] holds by construction, but
    the ratio computing it lands an ulp outside on a near-degenerate system. A published
    bound of 1.0000000000000002 reads as a defect in the estimator rather than as float
    noise, and it is a share, so it is clipped rather than flagged the way GG is."""
    for panel in (_leader_follower(noise=1e-6), _symmetric(), _two_sided(w=0.9)):
        bounds = information_shares(panel, VENUES).bounds
        assert np.all(bounds >= 0.0)
        assert np.all(bounds <= 1.0)


def test_simultaneous_discovery_widens_the_bounds_and_separated_discovery_narrows_them() -> None:
    """The order-dependence IS the uncertainty, and it is driven by residual correlation.

    When one venue leads by a whole bar the two residuals are independent, the Cholesky
    ordering cannot matter, and the interval collapses to a point. When both venues see
    the same news in the same bar their residuals are dominated by that shared innovation,
    no factorisation can attribute it, and the interval opens toward [0, 1]. Reporting a
    wide interval as if it were a measurement is the failure this pins.
    """
    separated = information_shares(_leader_follower(), VENUES)
    simultaneous = information_shares(_symmetric(), VENUES)
    assert np.diff(separated.bounds[0])[0] < 0.05
    assert np.diff(simultaneous.bounds[0])[0] > 0.5
    assert abs(separated.residual_corr) < 0.2
    assert simultaneous.residual_corr > 0.8


# ── input handling ──────────────────────────────────────────────────────────


def test_conditioning_rises_with_the_lag_order() -> None:
    """Cheap sanity on the diagnostic itself: more lags, more collinearity to worry about."""
    panel = _leader_follower()
    assert fit(panel, lags=10)[2] > fit(panel, lags=2)[2]


def test_a_panel_that_is_not_two_columns_is_rejected() -> None:
    with pytest.raises(ValueError, match=r"\(T, 2\)"):
        fit(np.zeros((100, 3)))


def test_too_few_rows_for_the_lag_order_is_rejected() -> None:
    with pytest.raises(ValueError, match="too few"):
        fit(np.zeros((8, 2)), lags=10)


# ── the polars adapter ──────────────────────────────────────────────────────


def _frame(n_dates: int, n_per_date: int = MIN_ROWS + 100) -> pl.DataFrame:
    frames = []
    for d in range(1, n_dates + 1):
        panel = np.exp(_leader_follower(n_per_date, seed=d) * 0.001) * 60_000
        frames.append(
            pl.DataFrame(
                {
                    "ts_ns": np.arange(n_per_date, dtype=np.int64) + d * 10**12,
                    "date": [f"2026-01-{d:02d}"] * n_per_date,
                    "coinbase_mid": panel[:, 0],
                    "kraken_mid": panel[:, 1],
                    "coinbase_age_ms": np.zeros(n_per_date),
                    "kraken_age_ms": np.zeros(n_per_date),
                }
            )
        )
    return pl.concat(frames)


def test_the_panel_is_log_prices_in_venue_order() -> None:
    df = _frame(1)
    panel = price_panel(df, VENUES)
    assert panel.shape[1] == 2
    assert panel[0, 0] == pytest.approx(np.log(df["coinbase_mid"][0]))


def test_stride_subsamples_the_panel() -> None:
    df = _frame(1)
    assert len(price_panel(df, VENUES, stride=10)) == pytest.approx(len(df) / 10, rel=0.01)


def test_the_staleness_cut_drops_rows() -> None:
    df = _frame(1).with_columns(
        pl.when(pl.int_range(pl.len()) % 2 == 0).then(9_999.0).otherwise(0.0).alias("kraken_age_ms")
    )
    assert len(price_panel(df, VENUES, max_age_ms=100)) < len(df) * 0.6


def test_a_missing_price_column_names_itself() -> None:
    with pytest.raises(ValueError, match="binance_mid"):
        price_panel(_frame(1), ("binance", "kraken"))


def test_by_date_returns_one_estimate_per_date() -> None:
    got = by_date(_frame(3), VENUES)
    assert len(got) == 3
    assert [g.date for g in got] == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert all(g.bounds[0][0] > 0.9 for g in got)


def test_the_report_runs_and_names_both_measures(capsys) -> None:
    """Formatting slips in a report only surface at the end of a long run."""
    report(by_date(_frame(2), VENUES))
    out = capsys.readouterr().out
    assert "information shares" in out
    assert "Gonzalo-Granger" in out and "Hasbrouck" in out


def test_the_report_survives_having_nothing_to_say(capsys) -> None:
    report([])
    assert "no information-share estimates" in capsys.readouterr().out


def test_by_date_skips_dates_with_too_little_data() -> None:
    """A short date must vanish rather than contribute a wild estimate to the spread."""
    short = _frame(1, n_per_date=200)
    assert by_date(short, VENUES) == []


def test_the_row_floor_scales_with_the_stride() -> None:
    """The floor asks how much of the day the sample covers, not how many rows survive
    striding. Sized for the 1s grid and left fixed, a 30s sweep of full days falls under
    it on every date and the whole analysis returns nothing without a word."""
    assert min_rows_for(1) == MIN_ROWS
    assert min_rows_for(5) == MIN_ROWS // 5
    # Past some coarseness the identification floor binds instead and stops scaling.
    identified = OBS_PER_PARAMETER * (2 * DEFAULT_LAGS + 2)
    assert min_rows_for(30) == identified
    assert min_rows_for(10_000) == identified


def test_a_full_day_at_a_coarse_stride_survives_the_floor() -> None:
    df = _frame(1, n_per_date=80_000)
    assert len(by_date(df, VENUES, stride=30)) == 1


def test_a_sample_too_thin_to_identify_the_fit_is_still_dropped() -> None:
    """255 rows for 22 parameters is not an estimate, however coarse the stride."""
    assert by_date(_frame(1), VENUES, stride=20) == []
    assert len(by_date(_frame(1), VENUES, stride=20, min_rows=200)) == 1
