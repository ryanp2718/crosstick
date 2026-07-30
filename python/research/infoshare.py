"""Hasbrouck information shares and Gonzalo-Granger component shares.

The forecasting model in `research.model` answers "does venue A's book predict venue B's
next move". That question cannot separate a venue that *discovers* price from one that
merely *tracks* it slowly: a coarse, slowly-requoted book is mechanically forecastable
from any outside information, and produces the same asymmetry as genuine leadership.
Measured 2026-07-29, tightening the freshness cut to 200ms left the asymmetry intact,
which rules out a stale feed but not a slow quoter.

This module asks the other question. Two venues quoting one instrument share a single
efficient price; arbitrage keeps their difference stationary. So their prices are
cointegrated with a cointegrating vector known a priori to be (1, -1), and the innovations
to the common efficient price can be attributed back to the venues.

Two measures are reported, because they answer different questions and the difference is
not a technicality. Gonzalo-Granger uses only the error-correction speeds: it asks which
venue the other one moves toward, i.e. which acts as the anchor. Hasbrouck additionally
weights by innovation variance: it asks whose quote revisions carry the news. Simulated
2026-07-29 on a system where both venues adjust at identical speed but one contributes
90% of the news, Hasbrouck recovered 0.91 and Gonzalo-Granger returned 0.50 - both
correct, for different questions. Quoting either alone answers "which venue leads"
misleadingly, so `report` prints them side by side.

Knowing the cointegrating vector is what makes this tractable without statsmodels: there
is no rank test and no Johansen step, just OLS on a differenced system with a known
error-correction term.

Two properties of this estimator that must be quoted with any number it produces:

- **It is order-dependent.** Hasbrouck's share is not point-identified when the venues'
  residuals are contemporaneously correlated - the Cholesky factorisation has to assign
  the shared innovation to whichever venue comes first. Both orderings are computed and
  the result is an INTERVAL. A wide interval is not a failed measurement, it is the
  honest statement that at this sampling frequency the data cannot separate them.
- **It depends on sampling frequency.** Sampled coarsely enough, every venue looks
  simultaneous and shares converge on the residual correlation. `stride` is exposed for
  exactly this reason and the answer should be reported as a function of it.
- **Gonzalo-Granger is the fragile one.** It is a ratio of error-correction loadings, so
  when the loadings are poorly identified it leaves [0, 1] and stops being a share at
  all. That happens under collinearity between the error-correction term and the lagged
  differences: on a one-sided simulation GG read 1.93 at 1 lag and 0.69 at 10 while the
  truth was 1.0, with the regressor condition number climbing 64 -> 363. Hasbrouck's
  share was 0.9998+ throughout. `conditioning` is carried on every estimate for this
  reason; a GG outside [0, 1] means read the condition number, not the share.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl

# Lagged differences in the VECM. On a 1s grid this spans the horizon the forecasting
# side found signal at, without spending degrees of freedom on lags past it.
DEFAULT_LAGS = 10

# Minimum usable rows before an estimate is worth reporting for a date. Sized for the 1s
# grid; a coarse stride legitimately falls below it, so `by_date` takes an override
# rather than silently returning nothing (a 30s stride leaves 2880 bars a date, which is
# ample for ~21 parameters but under this floor).
MIN_ROWS = 5_000


@dataclass
class InfoShare:
    """One venue pair, one date, one sampling stride."""

    venues: tuple[str, str]
    n: int
    # Error-correction loadings. The venue with alpha ~ 0 does not adjust to the other,
    # which is the definition of the price leader in this framework.
    alpha: np.ndarray
    # Gonzalo-Granger component shares, summing to 1. Point-identified.
    component_shares: np.ndarray
    # Hasbrouck bounds per venue: (lower, upper), from the two Cholesky orderings.
    bounds: np.ndarray
    # Correlation of the two venues' VECM residuals. Drives how wide the bounds are.
    residual_corr: float
    # Condition number of the VECM regressors. High means alpha - and so the
    # Gonzalo-Granger share, which is a ratio of alphas - is not separately identified.
    conditioning: float
    date: str | None = None

    @property
    def midpoints(self) -> np.ndarray:
        """The bounds' midpoint, the conventional summary. Never quote it alone."""
        return self.bounds.mean(axis=1)

    @property
    def component_shares_usable(self) -> bool:
        """Whether the Gonzalo-Granger numbers are shares at all rather than artefacts."""
        return bool(np.all(self.component_shares >= 0.0) and np.all(self.component_shares <= 1.0))


def _stack_lags(dp: np.ndarray, lags: int) -> np.ndarray:
    """Lagged differences as regressors, most recent first."""
    return np.hstack([dp[lags - k : -k] for k in range(1, lags + 1)])


def fit(prices: np.ndarray, lags: int = DEFAULT_LAGS) -> tuple[np.ndarray, np.ndarray, float]:
    """OLS the two-equation VECM, returning the error-correction loadings and residuals.

        d p_t = alpha (p1 - p2)_{t-1} + sum_k Gamma_k d p_{t-k} + e_t

    `prices` is (T, 2) LOG prices, so the error-correction term is the relative spread
    between the venues and alpha is in the same units for both. Each equation is a
    separate OLS on identical regressors, which is the efficient estimator here because
    the regressor set is shared (SUR collapses to equation-by-equation OLS).
    """
    if prices.ndim != 2 or prices.shape[1] != 2:
        raise ValueError(f"need a (T, 2) price panel, got {prices.shape}")
    if len(prices) <= lags + 2:
        raise ValueError(f"{len(prices)} rows is too few for {lags} lags")

    dp = np.diff(prices, axis=0)
    z = (prices[:, 0] - prices[:, 1])[lags:-1, None]
    y = dp[lags:]
    x = np.hstack([z, _stack_lags(dp, lags), np.ones((len(y), 1))])

    coef, *_ = np.linalg.lstsq(x, y, rcond=None)
    resid = y - x @ coef
    return coef[0], resid, float(np.linalg.cond(x))


def _shares_for_order(psi: np.ndarray, omega: np.ndarray, order: list[int]) -> np.ndarray:
    """Hasbrouck shares under one Cholesky ordering, mapped back to venue order."""
    permuted = omega[np.ix_(order, order)]
    factor = np.linalg.cholesky(permuted)
    contribution = (psi[order] @ factor) ** 2
    total = float(psi @ omega @ psi)
    shares = np.empty(2)
    shares[order] = contribution / total
    return shares


def information_shares(
    prices: np.ndarray, venues: tuple[str, str], lags: int = DEFAULT_LAGS
) -> InfoShare:
    """Hasbrouck bounds and Gonzalo-Granger shares for one venue pair.

    The common-factor weights are the normalised orthogonal complement of alpha: a venue
    that does not adjust (alpha ~ 0) carries all the weight, because the shared price has
    to move to it rather than the reverse. Hasbrouck then splits the variance of that
    common innovation, `psi' Omega psi`, using a Cholesky factor of the residual
    covariance - and because that factorisation depends on variable order, both orders
    are computed and returned as an interval.
    """
    alpha, resid, conditioning = fit(prices, lags)
    omega = np.cov(resid, rowvar=False)

    perp = np.array([alpha[1], -alpha[0]])
    denom = perp.sum()
    if not np.isfinite(denom) or abs(denom) < 1e-15:
        raise ValueError("alpha is degenerate; neither venue error-corrects")
    psi = perp / denom

    forward = _shares_for_order(psi, omega, [0, 1])
    reverse = _shares_for_order(psi, omega, [1, 0])
    bounds = np.sort(np.vstack([forward, reverse]).T, axis=1)

    sd = np.sqrt(np.diag(omega))
    corr = float(omega[0, 1] / (sd[0] * sd[1])) if np.all(sd > 0) else float("nan")
    return InfoShare(
        venues=venues,
        n=len(resid),
        alpha=alpha,
        component_shares=psi,
        bounds=bounds,
        residual_corr=corr,
        conditioning=conditioning,
    )


def price_panel(
    df: pl.DataFrame, venues: tuple[str, str], stride: int = 1, max_age_ms: int | None = None
) -> np.ndarray:
    """Log mid prices for two venues on the shared grid, gaps dropped.

    Dropping a row rather than forward-filling matters here: a carried-forward mid is a
    fabricated zero return, and zero returns bias an error-correction loading toward
    "this venue does not adjust", which is the exact quantity being measured. A venue
    with a slower feed would be handed a spurious leadership result.
    """
    cols = [f"{v}_mid" for v in venues]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"price panel needs {missing}")

    out = df.select(["ts_ns", *cols, *[f"{v}_age_ms" for v in venues]]).drop_nulls()
    if max_age_ms is not None:
        for v in venues:
            out = out.filter(pl.col(f"{v}_age_ms") <= max_age_ms)
    out = out.sort("ts_ns")[::stride]
    return np.log(out.select(cols).to_numpy())


def by_date(
    df: pl.DataFrame,
    venues: tuple[str, str],
    stride: int = 1,
    lags: int = DEFAULT_LAGS,
    max_age_ms: int | None = None,
    min_rows: int = MIN_ROWS,
) -> list[InfoShare]:
    """One estimate per date, never one over the concatenated tape.

    Splicing dates would put an overnight gap inside a lag window and let the
    error-correction term absorb a jump that no venue traded through. Per-date estimates
    also give the across-date spread, which is the only error bar this method gets.
    """
    out = []
    for (date,), part in df.sort("ts_ns").group_by(["date"], maintain_order=True):
        panel = price_panel(part, venues, stride, max_age_ms)
        if len(panel) < min_rows:
            continue
        try:
            estimate = information_shares(panel, venues, lags)
        except (ValueError, np.linalg.LinAlgError):
            continue
        estimate.date = str(date)
        out.append(estimate)
    return out


def report(estimates: list[InfoShare]) -> None:
    """Per-date estimates and their across-date spread, both measures side by side.

    The spread across dates is the error bar. There is no bootstrap here on purpose: a
    VECM fitted to one date already pools that date's rows, so resampling within a date
    would only re-measure the same fit. Dates are the independent replications.
    """
    if not estimates:
        print("\nno information-share estimates (every date was too short or degenerate)")
        return

    first, second = estimates[0].venues
    print(f"\ninformation shares: {first} vs {second}, one VECM per date")
    print(
        f"{'date':<12} {'n':>8} {'alpha ' + first:>14} {'alpha ' + second:>14} "
        f"{'IS ' + first:>18} {'GG ' + first:>8} {'corr':>7} {'cond':>8}"
    )
    print("-" * 100)
    for e in estimates:
        lo, hi = e.bounds[0]
        flag = "" if e.component_shares_usable else " !"
        print(
            f"{e.date or '?':<12} {e.n:>8} {e.alpha[0]:>14.4f} {e.alpha[1]:>14.4f} "
            f"{lo:>8.4f} - {hi:<7.4f} {e.component_shares[0]:>7.3f}{flag:<1} "
            f"{e.residual_corr:>7.3f} {e.conditioning:>8.1f}"
        )

    mids = np.array([e.midpoints[0] for e in estimates])
    widths = np.array([np.diff(e.bounds[0])[0] for e in estimates])
    print(
        f"\n  {first} information share: {mids.mean():.4f} +/- {mids.std():.4f} "
        f"over {len(estimates)} dates (mean bound width {widths.mean():.4f})"
    )
    unusable = [e.date for e in estimates if not e.component_shares_usable]
    if unusable:
        print(
            f"  ! Gonzalo-Granger left [0, 1] on {len(unusable)} date(s): {', '.join(unusable[:5])}"
            f"{' ...' if len(unusable) > 5 else ''}.\n"
            "    That is an identification failure in alpha, not a share - check 'cond'.\n"
            "    The Hasbrouck bounds do not depend on alpha alone and are unaffected."
        )
    print(
        "\n  The two measures answer different questions. Gonzalo-Granger uses only the\n"
        "  error-correction speeds (which venue is the anchor); Hasbrouck also weights by\n"
        "  innovation variance (whose quote revisions carry the news). They diverge when\n"
        "  venues adjust at similar speeds but contribute unequal news, which is a result\n"
        "  rather than a contradiction. Bound width is driven by residual correlation: a\n"
        "  wide interval means this sampling frequency cannot separate the venues."
    )
