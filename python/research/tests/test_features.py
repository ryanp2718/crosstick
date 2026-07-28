"""Feature-matrix transforms, with the leakage discipline pinned.

The point of these tests is not that the arithmetic runs - it is that a feature at
grid time `t` cannot see `t`. A lookahead bug here does not crash or look wrong; it
produces a beautiful out-of-sample R2 that evaporates in production, so the as-of
boundary gets an explicit test rather than a comment.
"""

from __future__ import annotations

from unittest import mock

import polars as pl
import pyarrow as pa
import pytest

from research.features import (
    _LIVE_COL,
    BAR_NS,
    MAX_AGE_MS,
    _add_targets,
    _align,
    _bar_index,
    _book_features,
    _dead_zone,
    _flow_features,
    _grid,
    _liq_features,
    _mark_features,
    _ofi,
    _oi_features,
    _quote_legs,
    _returns,
    build_features,
    leg_prefix,
)


def _quotes(rows: list[tuple[int, float, float, float, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema=["ts_ns", "best_bid", "best_ask", "bid_sz", "ask_sz"],
        orient="row",
    )


# ── as-of alignment ─────────────────────────────────────────────────────────


def test_align_never_sees_the_future() -> None:
    """A quote landing mid-bar must not appear at the grid point before it."""
    grid = _grid(0, 3 * BAR_NS)
    series = pl.DataFrame(
        {
            "ts_ns": [BAR_NS + 1, 2 * BAR_NS + 1],
            "v_mid": [100.0, 200.0],
            "v_quote_ts": [BAR_NS + 1, 2 * BAR_NS + 1],
        }
    )
    out = _align(grid, series, "v").sort("ts_ns")
    mids = out["v_mid"].to_list()
    # t=0 and t=BAR precede the first quote (which is at BAR+1) -> null, not 100.
    assert mids[0] is None
    assert mids[1] is None
    assert mids[2] == 100.0  # t=2*BAR sees only the BAR+1 quote
    assert mids[3] == 200.0


def test_align_reports_staleness() -> None:
    grid = _grid(0, 2 * BAR_NS)
    series = pl.DataFrame({"ts_ns": [0], "v_mid": [1.0], "v_quote_ts": [0]})
    out = _align(grid, series, "v").sort("ts_ns")
    # age grows with the grid while the venue stays silent: 0ms, 1000ms, 2000ms.
    assert out["v_age_ms"].to_list() == [0.0, 1000.0, 2000.0]


# ── bar boundaries ──────────────────────────────────────────────────────────


def test_bar_is_half_open_on_the_right() -> None:
    """An event exactly at a bar close belongs to that bar, not the next one."""
    ts = pl.DataFrame({"t": [1, BAR_NS - 1, BAR_NS, BAR_NS + 1]})
    got = ts.select(_bar_index(pl.col("t")).alias("bar"))["bar"].to_list()
    assert got == [BAR_NS, BAR_NS, BAR_NS, 2 * BAR_NS]


# ── order-flow imbalance ────────────────────────────────────────────────────


def test_ofi_bid_side_signs() -> None:
    """Ask held constant so each row isolates one bid-side case."""
    quotes = _quotes(
        [
            (1, 100.0, 105.0, 10.0, 10.0),
            (2, 100.0, 105.0, 15.0, 10.0),  # same price, size added: +5
            (3, 101.0, 105.0, 8.0, 10.0),  # price improved: +8, the whole new size
            (4, 100.0, 105.0, 4.0, 10.0),  # price worsened: -8, the size that left
        ]
    )
    ofi = _ofi(quotes, "v")["v_ofi"].to_list()
    assert ofi[0] == 0.0  # first row has no predecessor
    assert ofi[1] == 5.0
    assert ofi[2] == 8.0
    assert ofi[3] == -8.0


def test_ofi_ask_side_is_the_opposite_sign() -> None:
    """Ask-side pressure subtracts: more ask depth is downward pressure."""
    quotes = _quotes([(1, 100.0, 101.0, 10.0, 10.0), (2, 100.0, 101.0, 10.0, 14.0)])
    assert _ofi(quotes, "v")["v_ofi"].to_list()[1] == -4.0


def test_ofi_adds_both_sides_when_the_whole_book_lifts() -> None:
    """Bid improving AND ask retreating are both bullish, so they accumulate."""
    quotes = _quotes([(1, 100.0, 101.0, 10.0, 10.0), (2, 101.0, 102.0, 8.0, 10.0)])
    # bid improved -> +8 (new bid size); ask retreated -> +10 (the ask size that left)
    assert _ofi(quotes, "v")["v_ofi"].to_list()[1] == 18.0


# ── trade flow ──────────────────────────────────────────────────────────────


def _trades(rows: list[tuple[int, float, float, str]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=["ts_ns", "price", "size", "side"], orient="row")


def test_signed_volume_uses_the_taker_side() -> None:
    trades = _trades(
        [
            (1, 100.0, 2.0, "bid"),  # buyer-initiated: +2
            (2, 100.0, 3.0, "ask"),  # seller-initiated: -3
            (BAR_NS + 1, 100.0, 1.0, "bid"),  # next bar
        ]
    )
    grid = _grid(0, 2 * BAR_NS)
    out = _flow_features(trades, grid, "v").sort("ts_ns")
    by_ts = dict(zip(out["ts_ns"].to_list(), out["v_signed_vol"].to_list(), strict=True))
    assert by_ts[BAR_NS] == -1.0  # 2 - 3
    assert by_ts[2 * BAR_NS] == 1.0


def test_bars_without_trades_are_zero_not_null() -> None:
    """No trades in a bar is real information (no flow), not a missing value."""
    trades = _trades([(1, 100.0, 2.0, "bid")])
    out = _flow_features(trades, _grid(0, 2 * BAR_NS), "v").sort("ts_ns")
    assert out["v_signed_vol"].to_list() == [0.0, 2.0, 0.0]
    assert out["v_n_trades"].to_list() == [0, 1, 0]
    assert out["v_signed_vol"].null_count() == 0


def test_vwap_is_null_only_where_there_is_no_volume() -> None:
    trades = _trades([(1, 100.0, 2.0, "bid"), (2, 110.0, 2.0, "bid")])
    out = _flow_features(trades, _grid(0, BAR_NS), "v").sort("ts_ns")
    vwap = out["v_vwap"].to_list()
    assert vwap[0] is None  # empty bar
    assert vwap[1] == 105.0


# ── targets ─────────────────────────────────────────────────────────────────


def _nbbo_grid(
    grid: pl.DataFrame, mids: list[float], ages: list[float] | None = None
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ts_ns": grid["ts_ns"],
            "nbbo_mid": mids,
            "nbbo_spread_bps": [1.0] * len(mids),
            "nbbo_n_venues": [2] * len(mids),
            "nbbo_age_ms": ages if ages is not None else [0.0] * len(mids),
        }
    )


def test_targets_look_forward_and_run_off_the_end() -> None:
    grid = _grid(0, 3 * BAR_NS)
    out = _add_targets(grid, _nbbo_grid(grid, [100.0, 101.0, 102.0, 103.0]), []).sort("ts_ns")
    got = out["y_ret_bps_1"].to_list()
    assert got[0] == (101.0 / 100.0 - 1) * 1e4  # forward, not backward
    # the last row has no future left, so the target is null rather than stale
    assert got[-1] is None


def test_per_venue_targets_track_that_venue_not_the_nbbo() -> None:
    """The cross-venue test needs a target with no NBBO term in it at all."""
    grid = _grid(0, 2 * BAR_NS)
    feats = grid.with_columns(
        pl.Series("v_mid", [100.0, 110.0, 121.0]),
        pl.Series("v_spread_bps", [1.0, 1.0, 1.0]),  # a leg always carries both
        pl.Series("v_age_ms", [0.0, 0.0, 0.0]),
    )
    out = _add_targets(feats, _nbbo_grid(grid, [100.0, 100.0, 100.0]), ["v"]).sort("ts_ns")
    assert out["y_ret_bps_1"].to_list()[0] == 0.0  # the NBBO did not move
    assert out["y_v_ret_bps_1"].to_list()[0] == (110.0 / 100.0 - 1) * 1e4  # the venue did


# ── depth beyond the touch ──────────────────────────────────────────────────


def _deep_quotes() -> pl.DataFrame:
    """One quote with a bid-heavy book: 10 bid levels reach 1bps down, asks 2bps up."""
    return pl.DataFrame(
        {
            "ts_ns": [0],
            "best_bid": [100.0],
            "best_ask": [100.02],
            "bid_sz": [1.0],
            "ask_sz": [1.0],
            "bid_depth_5": [6.0],
            "ask_depth_5": [2.0],
            "bid_depth_10": [9.0],
            "ask_depth_10": [3.0],
            "bid_px_10": [99.99],
            "ask_px_10": [100.03],
        }
    )


def test_depth_imbalance_is_signed_toward_the_heavier_side() -> None:
    out = _book_features(_deep_quotes(), "v")
    assert out["v_depth_imb_5"][0] == (6.0 - 2.0) / 8.0
    assert out["v_depth_imb_10"][0] == (9.0 - 3.0) / 12.0


def test_depth_spans_are_positive_on_both_sides() -> None:
    """Span is distance from mid outward, so both sides are positive regardless of
    which direction the price index runs."""
    out = _book_features(_deep_quotes(), "v")
    assert out["v_bid_span_bps"][0] > 0
    assert out["v_ask_span_bps"][0] > 0
    # asks reach further out here, so their span must be the larger of the two
    assert out["v_ask_span_bps"][0] > out["v_bid_span_bps"][0]


def test_missing_depth_columns_become_null_not_zero() -> None:
    """Dates whose silver predates the depth columns must read as *unknown* depth.
    Zero would be a lie the tree could act on (it means "no resting size at all")."""
    shallow = _quotes([(0, 100.0, 100.02, 1.0, 1.0)])
    out = _book_features(shallow, "v")
    for col in ("v_depth_imb_5", "v_depth_imb_10", "v_bid_span_bps", "v_depth_10"):
        assert out[col][0] is None, col


def test_raw_depth_prices_never_reach_the_model() -> None:
    """bid_px_10 is a price level: non-stationary, and a tree would memorise it.
    Only the derived span survives into the feature frame."""
    out = _book_features(_deep_quotes(), "v")
    assert "bid_px_10" not in out.columns
    assert "v_bid_px_10" not in out.columns
    assert "v_bid_span_bps" in out.columns


# ── cross-symbol legs ───────────────────────────────────────────────────────


def test_trailing_returns_look_backward_only() -> None:
    """The one feature family derived from the mid, so it is the one most likely to
    be accidentally forward-looking. A rising series must report positive trailing
    returns, and the first bar of a window has no history to measure against."""
    grid = _grid(0, 3 * BAR_NS).with_columns(
        pl.Series("v_mid", [100.0, 101.0, 102.0, 103.0]),
        pl.Series("v_age_ms", [0.0, 0.0, 0.0, 0.0]),
    )
    out = grid.with_columns(_returns("v")).sort("ts_ns")
    assert out["v_ret_bps_1"].to_list()[0] is None  # nothing precedes the first bar
    assert out["v_ret_bps_1"].to_list()[1] == pytest.approx((101.0 / 100.0 - 1) * 1e4)
    assert out["v_ret_bps_1"].to_list()[3] == pytest.approx((103.0 / 102.0 - 1) * 1e4)


def test_a_trailing_return_never_reaches_forward() -> None:
    """A jump at the last bar must not show up in any earlier bar's trailing return."""
    grid = _grid(0, 3 * BAR_NS).with_columns(
        pl.Series("v_mid", [100.0, 100.0, 100.0, 200.0]),
        pl.Series("v_age_ms", [0.0, 0.0, 0.0, 0.0]),
    )
    out = grid.with_columns(_returns("v")).sort("ts_ns")
    assert out["v_ret_bps_1"].to_list()[1:3] == [0.0, 0.0]
    assert out["v_ret_bps_1"].to_list()[3] > 0


# ── capture holes ───────────────────────────────────────────────────────────
#
# 2026-07-08 holds 13 gaps over a minute, the longest 5.9h, and 07-09 another 10. The
# grid is uniform and `_asof` carries the last quote across them, so every window that
# reaches over a hole measures unobserved time. `model.clean` cannot catch any of it:
# the rows on the lips of a hole have perfectly fresh books of their own.


def _holed(grid: pl.DataFrame, dead: set[int]) -> pl.DataFrame:
    """The same grid, with those bar indices marked as capture downtime."""
    return grid.with_columns(pl.Series(_LIVE_COL, [i not in dead for i in range(grid.height)]))


def test_a_trailing_return_will_not_reach_across_a_capture_hole() -> None:
    stale = float(MAX_AGE_MS + 1)
    grid = _grid(0, 3 * BAR_NS).with_columns(
        pl.Series("v_mid", [100.0, 100.0, 100.0, 200.0]),
        pl.Series("v_age_ms", [0.0, 0.0, stale, 0.0]),  # bar 2 is inside the hole
    )
    got = grid.with_columns(_returns("v")).sort("ts_ns")["v_ret_bps_1"].to_list()
    assert got[3] is None  # would have been +10000bps of move nobody recorded
    assert got[1] == 0.0  # a lookback that stays on live bars is untouched


def test_a_target_will_not_reach_forward_across_a_capture_hole() -> None:
    """Worse than a contaminated feature: a fabricated label trains the model on a move
    that was never observed, and the bar it lands on looks pristine."""
    grid = _grid(0, 2 * BAR_NS)
    ages = [0.0, float(MAX_AGE_MS + 1), 0.0]
    out = _add_targets(grid, _nbbo_grid(grid, [100.0, 100.0, 500.0], ages), []).sort("ts_ns")
    assert out["y_ret_bps_1"].to_list()[0] is None
    assert out["y_dz_1"].to_list()[0] is None  # and the class sibling does not go flat


def test_a_flow_roll_spans_a_quiet_venue_but_not_a_dead_capture() -> None:
    """Two ways to see no trades in a bar, and they are not the same fact. A venue that
    printed nothing really did see no flow; a capture that was down saw nothing at all,
    and summing its zeros invents a quiet market that was never measured."""
    trades = _trades([(5 * BAR_NS + 1, 100.0, 2.0, "bid")])  # lands in bar 6
    grid = _grid(0, 9 * BAR_NS)

    quiet = _flow_features(trades, grid, "v").sort("ts_ns")["v_signed_vol_5"].to_list()
    assert quiet[6] == 2.0  # eight silent bars around one trade is a real, measured 2.0

    got = _flow_features(trades, _holed(grid, {4}), "v").sort("ts_ns")["v_signed_vol_5"].to_list()
    assert got[6] is None  # the trailing 5 bars cover the dead one
    assert got[9] == 2.0  # by here the window has cleared it


def test_ofi_does_not_book_a_capture_hole_as_a_single_tick() -> None:
    """`_ofi` differences consecutive quote *events*, not grid bars, so it needs its own
    guard: the first quote back would otherwise charge hours of book evolution to one tick."""
    gap_ns = (MAX_AGE_MS + 1) * 1_000_000
    quotes = _quotes([(0, 100.0, 101.0, 5.0, 5.0), (gap_ns, 100.0, 101.0, 50.0, 5.0)])
    got = _ofi(quotes, "v")["v_ofi"].to_list()
    assert got[1] == 0.0  # not +45 of depth that appeared while nobody was watching


def test_the_perp_tape_keeps_its_own_lookback_tolerance() -> None:
    """Open interest is a 10s poll, so it is never fresh by book standards. Holding its
    change features to the 5s book tolerance would null them on every bar of every date -
    the guard has to bound a real outage, not the stream's normal cadence."""
    oi = pl.DataFrame({"ts_ns": [0, 8 * BAR_NS], "open_interest": [100.0, 110.0]})
    got = _oi_features(oi, _grid(0, 12 * BAR_NS), "p").sort("ts_ns")
    # bar 12 looks back 5 bars to bar 7, which is 7s stale: past the book tolerance,
    # well inside the tape's, and a genuine observation of the poll before last.
    assert got["p_oi_age_ms"].to_list()[7] == 7000.0
    assert got["p_oi_chg_bps_5"].to_list()[12] == pytest.approx(1000.0)


def test_leg_prefix_is_the_exchange_not_the_symbol() -> None:
    """Column names have to stay stable as symbols are added, or every cached matrix
    and every venue-importance grouping silently changes meaning."""
    assert leg_prefix("binance-futures") == "binance_futures"
    assert leg_prefix("coinbase") == "coinbase"


def test_one_exchange_quoting_two_requested_symbols_is_rejected() -> None:
    """Silently overwriting one leg with the other would produce a frame that looks
    fine and answers a different question than the caller asked."""

    quotes = pl.DataFrame({"ts_ns": [0], "exchange": ["coinbase"]})
    with mock.patch("research.features.load_quotes", return_value=quotes):
        with pytest.raises(ValueError, match="collide"):
            _quote_legs(None, "silver", "2026-06-27", ["BTC-USD", "ETH-USD"])


def test_an_extra_symbol_alone_builds_nothing() -> None:
    """Some dates hold only the perp. Without a primary leg there is no grid to anchor
    and no target to predict, so the date is skipped rather than half-built."""
    perp = pl.DataFrame({"ts_ns": [0], "exchange": ["binance-futures"]})

    def _quotes_for(_fs, _bucket, _date, symbol):
        return perp if symbol == "BTC-USDT-PERP" else None

    with mock.patch("research.features.load_quotes", side_effect=_quotes_for):
        with mock.patch("research.features.load_nbbo", return_value=pl.DataFrame({"ts_ns": [0]})):
            got = build_features(None, "silver", "2026-07-01", "BTC-USD", ("BTC-USDT-PERP",))
    assert got is None


def test_a_venue_without_depth_loads_beside_one_with_it() -> None:
    """Mid-backfill a single date holds mixed silver vintages: one venue's quotes carry
    the depth columns, another's do not. The date must still load, with the older
    venue's depth reading as unknown - not as zero, and not as a failed date."""
    common = {"ts_ns": [0], "best_bid": [100.0], "best_ask": [100.02]}
    new = pa.table(
        {
            "exchange": ["new"],
            **common,
            "bid_sz": [1.0],
            "ask_sz": [1.0],
            "bid_depth_5": [6.0],
            "ask_depth_5": [2.0],
            "bid_depth_10": [9.0],
            "ask_depth_10": [3.0],
            "bid_px_10": [99.99],
            "ask_px_10": [100.03],
        }
    )
    old = pa.table({"exchange": ["old"], **common, "bid_sz": [1.0], "ask_sz": [1.0]})

    df = pl.from_arrow(pa.concat_tables([new, old], promote_options="permissive"))
    assert df.height == 2

    with_depth = _book_features(df.filter(pl.col("exchange") == "new"), "v")
    assert with_depth["v_depth_imb_10"][0] == (9.0 - 3.0) / 12.0
    without = _book_features(df.filter(pl.col("exchange") == "old"), "v")
    assert without["v_depth_imb_10"][0] is None
    assert without["v_bid_span_bps"][0] is None


# ── perp microstructure ─────────────────────────────────────────────────────


def _mark(rows: list[tuple[int, float, float, float, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema=["ts_ns", "mark_price", "index_price", "funding_rate", "next_funding_ts_ns"],
        orient="row",
    )


def test_basis_is_the_premium_to_the_index() -> None:
    mark = _mark([(0, 100.10, 100.00, 0.0001, 10 * BAR_NS)])
    got = _mark_features(mark, _grid(0, 0), "p")
    assert got["p_basis_bps"][0] == pytest.approx(10.0)  # 0.10 on 100 == 10bps
    assert got["p_funding_rate"][0] == pytest.approx(0.0001)


def test_a_zero_index_price_is_unknown_basis_not_infinity() -> None:
    """A venue publishing a zero index must not poison the column with inf."""
    mark = _mark([(0, 100.0, 0.0, 0.0001, 10 * BAR_NS)])
    assert _mark_features(mark, _grid(0, 0), "p")["p_basis_bps"][0] is None


def test_funding_countdown_runs_on_the_grid_clock() -> None:
    """Time to funding decays between mark ticks. Carrying the tick-time value forward
    would freeze the countdown at whatever it was when the venue last spoke."""
    mark = _mark([(0, 100.0, 100.0, 0.0, 10 * BAR_NS)])  # one tick, then silence
    got = _mark_features(mark, _grid(0, 3 * BAR_NS), "p").sort("ts_ns")
    assert got["p_funding_in_s"].to_list() == [10.0, 9.0, 8.0, 7.0]
    assert got["p_mark_age_ms"].to_list() == [0.0, 1000.0, 2000.0, 3000.0]


def test_open_interest_yields_change_not_level() -> None:
    """The level is a non-stationary stock and must not survive into the frame."""
    oi = pl.DataFrame({"ts_ns": [0, BAR_NS], "open_interest": [1000.0, 1010.0]})
    got = _oi_features(oi, _grid(0, BAR_NS), "p").sort("ts_ns")
    assert "_oi_level" not in got.columns
    assert not [c for c in got.columns if c == "p_oi"]
    # 1000 -> 1010 over one bar is +100bps; the 5-bar window has no history yet.
    assert got["p_oi_chg_bps_5"].to_list() == [None, None]


def test_a_long_liquidation_signs_negative() -> None:
    """`side` is the forced order's own side: an ASK is a long being sold out, which
    pushes price down and must sign like aggressive selling."""
    liq = pl.DataFrame(
        {
            "ts_ns": [1, 2],
            "side": ["ask", "bid"],
            "filled_size": [3.0, 1.0],
        }
    )
    got = _liq_features(liq, _grid(0, BAR_NS), "p").sort("ts_ns")
    assert got["p_liq_flow"].to_list() == [0.0, -2.0]  # -3 long + 1 short, one bar
    assert got["p_n_liqs"].to_list() == [0, 2]


def test_a_bar_with_no_liquidation_is_a_real_zero() -> None:
    """Most bars hold no forced flow. That is measured absence, not missing data - a
    null would make the model impute a cascade where there was none."""
    liq = pl.DataFrame({"ts_ns": [1], "side": ["ask"], "filled_size": [3.0]})
    got = _liq_features(liq, _grid(0, 3 * BAR_NS), "p").sort("ts_ns")
    assert got["p_liq_flow"].to_list() == [0.0, -3.0, 0.0, 0.0]


def test_a_perp_leg_joins_without_disturbing_the_grid() -> None:
    """End to end: the perp tape must widen the frame, never lengthen it.

    Each perp stream runs on its own clock and is joined separately, so a duplicate key
    in any one of them would multiply rows - and a matrix that quietly grew rows would
    still train, still score, and be wrong about how much data it had.
    """
    bars = [0, BAR_NS, 2 * BAR_NS, 3 * BAR_NS]
    spot = pl.DataFrame(
        {
            "ts_ns": bars,
            "exchange": ["coinbase"] * 4,
            "best_bid": [100.0, 100.1, 100.2, 100.3],
            "best_ask": [100.02, 100.12, 100.22, 100.32],
            "bid_sz": [1.0] * 4,
            "ask_sz": [1.0] * 4,
        }
    )
    perp = spot.with_columns(pl.lit("binance-futures").alias("exchange"))
    nbbo = pl.DataFrame(
        {
            "ts_ns": bars,
            "best_bid": [100.0, 100.1, 100.2, 100.3],
            "best_ask": [100.02, 100.12, 100.22, 100.32],
            "n_venues": [2] * 4,
        }
    )
    perp_only = {"BTC-USDT-PERP"}
    mark = pl.DataFrame(
        {
            "ts_ns": bars,
            "exchange": ["binance-futures"] * 4,
            "mark_price": [100.01, 100.11, 100.21, 100.31],
            "index_price": [100.0, 100.1, 100.2, 100.3],
            "funding_rate": [0.0001] * 4,
            "next_funding_ts_ns": [10 * BAR_NS] * 4,
        }
    )
    oi = pl.DataFrame(
        {"ts_ns": bars, "exchange": ["binance-futures"] * 4, "open_interest": [10.0] * 4}
    )
    liq = pl.DataFrame(
        {"ts_ns": [1], "exchange": ["binance-futures"], "side": ["ask"], "filled_size": [2.0]}
    )

    def _for_perp(df):
        return lambda _fs, _bucket, _date, symbol: df if symbol in perp_only else None

    with (
        mock.patch(
            "research.features.load_quotes",
            side_effect=lambda _f, _b, _d, s: perp if s in perp_only else spot,
        ),
        mock.patch("research.features.load_nbbo", return_value=nbbo),
        mock.patch("research.features.load_trades", return_value=None),
        mock.patch("research.features.load_mark_price", side_effect=_for_perp(mark)),
        mock.patch("research.features.load_open_interest", side_effect=_for_perp(oi)),
        mock.patch("research.features.load_liquidations", side_effect=_for_perp(liq)),
    ):
        got = build_features(None, "silver", "2026-06-30", "BTC-USD", ("BTC-USDT-PERP",))

    assert got is not None
    assert got.height == len(bars)  # widened, not lengthened
    assert got["ts_ns"].n_unique() == len(bars)
    for col in (
        "binance_futures_basis_bps",
        "binance_futures_funding_rate",
        "binance_futures_funding_in_s",
        "binance_futures_oi_chg_bps_5",
        "binance_futures_liq_flow",
        "binance_futures_mark_age_ms",
        "binance_futures_oi_age_ms",
    ):
        assert col in got.columns, col
    # the spot venue publishes no perp tape, so it gets no perp columns at all
    assert not [c for c in got.columns if c.startswith("coinbase_basis")]
    assert got["binance_futures_basis_bps"][0] == pytest.approx(1.0)  # 0.01 on 100 == 1bps
    # every builder takes the grid and so sees `_LIVE_COL`; any that forgets to drop it
    # leaks a constant column the model would happily take as a feature.
    assert not [c for c in got.columns if c.startswith("_")], "private column leaked"


# ── dead-zone target ────────────────────────────────────────────────────────


def test_dead_zone_flattens_a_move_inside_half_the_spread() -> None:
    """A 2bps book cannot be traded on a 0.5bps move: crossing to get in costs more
    than the move is worth, so it is the flat class however confidently it is called."""
    df = pl.DataFrame({"ret": [0.5, -0.5, 2.0, -2.0], "spread": [2.0] * 4})
    got = df.select(_dead_zone(pl.col("ret"), pl.col("spread")).alias("dz"))["dz"].to_list()
    assert got == [0, 0, 1, -1]  # +/-0.5 is inside the 1.0 half-spread, +/-2.0 clears it


def test_dead_zone_scales_with_the_prevailing_spread() -> None:
    """The same move is a signal in a tight book and noise in a wide one - which is the
    whole reason the threshold is not a fixed bps cut."""
    df = pl.DataFrame({"ret": [1.0, 1.0], "spread": [0.5, 10.0]})
    got = df.select(_dead_zone(pl.col("ret"), pl.col("spread")).alias("dz"))["dz"].to_list()
    assert got == [1, 0]


def test_dead_zone_keeps_a_null_return_null() -> None:
    """The tail of every date has no forward window. Collapsing that into the flat class
    would hand the model a confident "no move" for rows whose future is unknown, on
    every date, and nothing downstream would flag it."""
    df = pl.DataFrame({"ret": [None, 1e9], "spread": [2.0, None]})
    got = df.select(_dead_zone(pl.col("ret"), pl.col("spread")).alias("dz"))["dz"].to_list()
    assert got == [None, None]


def test_targets_carry_a_dead_zone_sibling_per_horizon() -> None:
    grid = _grid(0, 3 * BAR_NS)
    feats = grid.with_columns(
        pl.Series("v_mid", [100.0, 100.0, 100.0, 100.0]),
        pl.Series("v_spread_bps", [1.0] * 4),
        pl.Series("v_age_ms", [0.0] * 4),
    )
    out = _add_targets(feats, _nbbo_grid(grid, [100.0, 100.0, 100.0, 100.0]), ["v"])
    for h in (1, 5, 30):
        assert f"y_dz_{h}" in out.columns
        assert f"y_v_dz_{h}" in out.columns
    # a perfectly flat series is the flat class, never a direction
    assert set(out["y_dz_1"].drop_nulls().to_list()) == {0}
