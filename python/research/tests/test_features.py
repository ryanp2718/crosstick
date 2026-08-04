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
    BAR_NS,
    _add_targets,
    _align,
    _bar_index,
    _book_features,
    _flow_features,
    _grid,
    _ofi,
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


def _nbbo_grid(grid: pl.DataFrame, mids: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ts_ns": grid["ts_ns"],
            "nbbo_mid": mids,
            "nbbo_spread_bps": [1.0] * len(mids),
            "nbbo_n_venues": [2] * len(mids),
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
    feats = grid.with_columns(pl.Series("v_mid", [100.0, 110.0, 121.0]))
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
    grid = _grid(0, 3 * BAR_NS).with_columns(pl.Series("v_mid", [100.0, 101.0, 102.0, 103.0]))
    out = grid.with_columns(_returns("v")).sort("ts_ns")
    assert out["v_ret_bps_1"].to_list()[0] is None  # nothing precedes the first bar
    assert out["v_ret_bps_1"].to_list()[1] == pytest.approx((101.0 / 100.0 - 1) * 1e4)
    assert out["v_ret_bps_1"].to_list()[3] == pytest.approx((103.0 / 102.0 - 1) * 1e4)


def test_a_trailing_return_never_reaches_forward() -> None:
    """A jump at the last bar must not show up in any earlier bar's trailing return."""
    grid = _grid(0, 3 * BAR_NS).with_columns(pl.Series("v_mid", [100.0, 100.0, 100.0, 200.0]))
    out = grid.with_columns(_returns("v")).sort("ts_ns")
    assert out["v_ret_bps_1"].to_list()[1:3] == [0.0, 0.0]
    assert out["v_ret_bps_1"].to_list()[3] > 0


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
