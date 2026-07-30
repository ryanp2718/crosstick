"""The feature schema has to agree with the builder, or it is worse than nothing.

A declaration nobody checks is a second place for the truth to live. The keystone here
is `test_the_declaration_matches_what_the_builder_emits`: it runs the real builder over
a fixture carrying every stream and requires the column set to be exactly the declared
product. Everything else pins a way name-parsing used to get the structure wrong.
"""

from __future__ import annotations

from unittest import mock

import polars as pl
import pyarrow as pa
import pytest

from common.schemas import DEPTH_LEVELS
from common.schemas import dataset as common_dataset
from research import features
from research.features import BAR_NS, DEPTH_RUNGS, build_features
from research.schema import (
    FeatureSchema,
    UnknownColumn,
    expected_columns,
)

SPOT = "coinbase"
PERP = "binance_futures"
PERP_STREAMS = frozenset({"quotes", "trades", "mark_price", "open_interest", "liquidations"})
SPOT_STREAMS = frozenset({"quotes", "trades"})


def _schema(**streams: frozenset[str]) -> FeatureSchema:
    """A schema over an explicit `{venue: streams}`, with everything present."""
    names = [c.name for c in expected_columns(streams)]
    return FeatureSchema(streams, frozenset(names))


# ── the declaration against the builder ──────────────────────────────────────


def _built(with_perp_tape: bool = True) -> pl.DataFrame:
    """One date through the real builder, with every stream a venue can contribute."""
    bars = [0, BAR_NS, 2 * BAR_NS, 3 * BAR_NS]
    spot = pl.DataFrame(
        {
            "ts_ns": bars,
            "exchange": ["coinbase"] * 4,
            "best_bid": [100.0, 100.1, 100.2, 100.3],
            "best_ask": [100.02, 100.12, 100.22, 100.32],
            "bid_sz": [1.0] * 4,
            "ask_sz": [1.0] * 4,
            "bid_depth_5": [6.0] * 4,
            "ask_depth_5": [2.0] * 4,
            "bid_depth_10": [9.0] * 4,
            "ask_depth_10": [3.0] * 4,
            "bid_px_10": [99.9] * 4,
            "ask_px_10": [100.4] * 4,
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

    def _trades(exchange: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "ts_ns": bars,
                "exchange": [exchange] * 4,
                "side": ["bid", "ask", "bid", "ask"],
                "size": [1.0] * 4,
                "price": [100.0] * 4,
            }
        )

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
        return lambda _fs, _b, _d, symbol: df if with_perp_tape and symbol in perp_only else None

    with (
        mock.patch(
            "research.features.load_quotes",
            side_effect=lambda _f, _b, _d, s: perp if s in perp_only else spot,
        ),
        mock.patch("research.features.load_nbbo", return_value=nbbo),
        mock.patch(
            "research.features.load_trades",
            side_effect=lambda _f, _b, _d, s: _trades(
                "binance-futures" if s in perp_only else "coinbase"
            ),
        ),
        mock.patch("research.features.load_mark_price", side_effect=_for_perp(mark)),
        mock.patch("research.features.load_open_interest", side_effect=_for_perp(oi)),
        mock.patch("research.features.load_liquidations", side_effect=_for_perp(liq)),
    ):
        got = build_features(None, "silver", "2026-06-30", "BTC-USD", ("BTC-USDT-PERP",))
    assert got is not None
    return got


def test_the_declaration_matches_what_the_builder_emits() -> None:
    """The one test that makes the rest of the module worth having.

    Both directions matter. A column the builder emits and the schema omits is rejected
    at load and never reaches a model; a column the schema declares and the builder never
    emits is reported as a permanent gap on every date.
    """
    declared = {c.name for c in expected_columns({SPOT: SPOT_STREAMS, PERP: PERP_STREAMS})}
    # `date` is stamped by `main.cached_features`; a builder handles one date.
    assert set(_built().columns) | {"date"} == declared


def test_a_venue_with_no_perp_tape_gets_the_declared_subset() -> None:
    """Absent, not null-filled. A spot venue publishes no funding, and the difference
    between "no column" and "a column of nulls" is the difference between a reported gap
    and a feature the model quietly imputes."""
    built = set(_built(with_perp_tape=False).columns)
    assert built | {"date"} == {
        c.name for c in expected_columns({SPOT: SPOT_STREAMS, PERP: SPOT_STREAMS})
    }
    assert not [c for c in built if "basis" in c or "_oi_" in c or "_liq_" in c]


def test_the_builder_emits_no_private_columns() -> None:
    """`_capture_live` rides the grid into every builder. One that forgets to drop it
    would hand the model a constant, and the schema would reject the whole date."""
    assert not [c for c in _built().columns if c.startswith("_")]


# ── venue resolution: the trap the suffix rules fell into ────────────────────


def test_a_venue_name_that_prefixes_another_resolves_to_itself() -> None:
    """`binance_futures_ofi` starts with `binance_`, so the old prefix match attributed
    it to Binance. Excluding Binance then dropped two venues while reporting one, and
    every cross-venue number was computed from fewer venues than it claimed."""
    schema = _schema(binance=SPOT_STREAMS, binance_futures=PERP_STREAMS, coinbase=SPOT_STREAMS)
    assert schema.venue_of("binance_futures_ofi") == "binance_futures"
    assert schema.venue_of("binance_ofi") == "binance"
    assert schema.venue_of("y_binance_futures_ret_bps_5") == "binance_futures"


def test_the_longest_declared_suffix_wins() -> None:
    """`binance_futures_oi_age_ms` ends with both `_oi_age_ms` and `_age_ms`. The short
    match invents a venue called `binance_futures_oi` and hands it the perp's columns."""
    schema = FeatureSchema.from_columns(["ts_ns", "binance_futures_oi_age_ms"])
    assert schema.venues == ["binance_futures"]
    assert schema.of("binance_futures_oi_age_ms").family == "open_interest"


def test_the_consolidated_book_is_not_a_venue() -> None:
    """It is prefixed like one and has no order flow of its own."""
    schema = _schema(coinbase=SPOT_STREAMS)
    assert schema.venues == ["coinbase"]
    assert schema.venue_of("nbbo_spread_bps") is None
    assert schema.venue_of("y_ret_bps_5") is None


def test_venue_names_that_would_alias_are_rejected() -> None:
    """`a_oi_age_ms` is both `a`'s open-interest clock and `a_oi`'s book clock. Discovery
    would split the frame's columns between the two arbitrarily, and every per-venue
    number would be wrong with nothing to show for it."""
    with pytest.raises(UnknownColumn, match=r"collide.*a_oi_age_ms"):
        _schema(a=frozenset({"quotes", "open_interest"}), a_oi=frozenset({"quotes"}))


# ── roles ────────────────────────────────────────────────────────────────────


def test_price_levels_are_declared_levels_rather_than_excluded_by_spelling() -> None:
    """Raw mids are non-stationary and a tree would memorise the level of BTC. Under the
    old suffix rule that held because of how they were named, not because of what they
    are: a level called `coinbase_last` would have walked straight in."""
    schema = _schema(coinbase=SPOT_STREAMS)
    levels = ("coinbase_mid", "coinbase_bid", "coinbase_ask", "coinbase_vwap", "nbbo_mid")
    inputs = schema.features(schema.names)
    for name in levels:
        assert schema.role_of(name) == "level", name
        assert name not in inputs, name


def test_trailing_returns_are_features_even_though_they_come_from_a_level() -> None:
    """The only channel a leg on a different quote asset has: a move of n bps is the same
    underlying move whatever the price level."""
    schema = _schema(coinbase=SPOT_STREAMS)
    assert schema.role_of("coinbase_ret_bps_300") == "feature"


def test_staleness_columns_are_diagnostics_not_inputs() -> None:
    schema = _schema(binance_futures=PERP_STREAMS)
    inputs = schema.features(schema.names)
    for name in ("binance_futures_age_ms", "binance_futures_oi_age_ms", "nbbo_age_ms"):
        assert schema.role_of(name) == "diagnostic", name
        assert name not in inputs, name


def test_the_book_clock_and_the_tape_clocks_are_kept_apart() -> None:
    """Open interest is a 10s poll, so judging it by the book's 5s tolerance would drop
    nearly every row of every date. Only a venue's own quotes are a book clock."""
    schema = _schema(coinbase=SPOT_STREAMS, binance_futures=PERP_STREAMS)
    book, tape = schema.age_columns()
    assert sorted(book) == ["binance_futures_age_ms", "coinbase_age_ms"]
    assert sorted(tape) == [
        "binance_futures_mark_age_ms",
        "binance_futures_oi_age_ms",
        "nbbo_age_ms",
    ]


def test_y_is_reserved_for_targets() -> None:
    """The leakage discipline in one rule: if a column is not `y_*` it was knowable at
    `t`. So `y_coinbase_ret_bps_5` can never be read as coinbase's trailing return."""
    schema = _schema(coinbase=SPOT_STREAMS)
    assert schema.role_of("y_coinbase_ret_bps_5") == "target"
    assert schema.role_of("coinbase_ret_bps_5") == "feature"
    assert not [c for c in schema.features(schema.names) if c.startswith("y_")]


# ── feature selection ────────────────────────────────────────────────────────


def test_excluding_a_venue_drops_it_and_the_consolidated_book() -> None:
    """The whole cross-venue claim rests on this. The NBBO is built from the excluded
    venue's quotes, so leaving it in smuggles the target back through the consolidation."""
    schema = _schema(coinbase=SPOT_STREAMS, kraken=SPOT_STREAMS)
    cols = schema.features(schema.names, exclude_venue="kraken")
    assert not [c for c in cols if schema.venue_of(c) == "kraken"]
    assert not [c for c in cols if c.startswith("nbbo_")]
    assert "coinbase_ofi" in cols


def test_features_keep_the_callers_column_order() -> None:
    """The feature matrix is positional, so reordering its columns changes which of two
    equal-gain splits the tree takes and moves the reported R2."""
    schema = _schema(coinbase=SPOT_STREAMS)
    order = ["coinbase_ofi_5", "nbbo_spread_bps", "coinbase_imbalance"]
    assert schema.features(order) == order


# ── failure modes ────────────────────────────────────────────────────────────


def test_an_undeclared_column_is_rejected() -> None:
    """Silence is the expensive failure. An undeclared feature family reaches no model,
    and the run still prints a plausible number from the narrower set."""
    with pytest.raises(UnknownColumn, match="matches no declared feature family"):
        FeatureSchema.from_columns(["ts_ns", "coinbase_ofi", "coinbase_sentiment"])


def test_a_bare_suffix_with_no_venue_is_rejected() -> None:
    with pytest.raises(UnknownColumn):
        FeatureSchema.from_columns(["_ofi"])


def test_asking_about_a_column_outside_the_schema_raises() -> None:
    schema = _schema(coinbase=SPOT_STREAMS)
    with pytest.raises(UnknownColumn, match="not in this schema"):
        schema.role_of("kraken_ofi")


# ── gaps ─────────────────────────────────────────────────────────────────────


def test_a_missing_stream_is_reported_by_family() -> None:
    """`pl.concat(..., how="diagonal")` null-fills a date that lacks a venue and says
    nothing, so a fortnight where Kraken was down reads like a fortnight where it was
    quiet."""
    schema = _schema(coinbase=SPOT_STREAMS, kraken=SPOT_STREAMS)
    present = [c for c in schema.names if schema.venue_of(c) != "kraken"]
    gaps = schema.missing_by_family(present)
    assert set(gaps) == {
        "kraken/book",
        "kraken/ofi",
        "kraken/returns",
        "kraken/flow",
        "kraken/target",
    }
    assert "kraken_ofi_300" in gaps["kraken/ofi"]


def test_a_complete_frame_reports_no_gaps() -> None:
    assert _schema(coinbase=SPOT_STREAMS).missing_by_family() == {}


# ── cross-layer ──────────────────────────────────────────────────────────────


def test_the_depth_rungs_mirror_the_silver_schema() -> None:
    """`DEPTH_RUNGS` names the rungs silver actually writes. If silver grew a rung and
    research did not follow, the new depth columns would be dropped on the floor with no
    error anywhere - the features simply would not exist."""
    assert DEPTH_RUNGS == DEPTH_LEVELS


@pytest.mark.parametrize(
    ("loader", "dataset"),
    [
        (features.load_quotes, "quotes"),
        (features.load_nbbo, "nbbo"),
        (features.load_trades, "trades"),
        (features.load_mark_price, "mark_price"),
        (features.load_open_interest, "open_interest"),
        (features.load_liquidations, "liquidations"),
    ],
)
def test_every_silver_decimal_reaches_the_matrix_as_a_float(loader, dataset: str) -> None:
    """Each loader names the DECIMAL(38,18) columns it has to cast for arithmetic, and
    those lists are hand-written against silver's schemas.

    Add a decimal column to a silver dataset and the cast list does not follow it: the
    column arrives as a polars Decimal, and any feature built from it either raises deep
    in an expression or silently degrades. Asserted as behaviour rather than by
    re-stating the lists, which would only move the duplication into the test.
    """
    schema = common_dataset("silver", dataset).schema
    row = pa.Table.from_arrays([pa.nulls(1, type=f.type) for f in schema], schema=schema)
    with mock.patch("research.features._read_symbol", return_value=row):
        got = loader(None, "silver", "2026-06-30", "BTC-USD")
    assert got is not None
    left = [c for c, dtype in zip(got.columns, got.dtypes, strict=True) if dtype == pl.Decimal]
    assert not left, f"{dataset} decimals not cast for arithmetic: {left}"
