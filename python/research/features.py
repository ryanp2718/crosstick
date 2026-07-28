"""Point-in-time feature matrix for short-horizon price-discovery modelling.

Reads silver (never bronze) and lands one row per grid time on a uniform clock-time
grid, carrying a venue leg per (exchange, canonical symbol) pair. One primary symbol
anchors the grid and owns the targets; further symbols contribute legs only, which is
what lets a venue quoting a *different* canonical symbol into the model at all - see
`build_features`. Every feature is computed from data with `ts_ns <= t`:
book state comes from a backward as-of join, flow features aggregate the half-open
bar `(t - BAR, t]`. The target is the only forward-looking column, and it is named
as such. That split is the whole leakage discipline - if a column is not `y_*`, it
was knowable at `t`.

Two silver facts make this honest rather than approximate:
  - `quotes` and `trades` share the `local_recv_ts_ns` clock, so a trade aligns with
    the book it hit without a cross-clock conversion (data-contracts.md, tape slice).
  - `trades.side` is the taker direction as the venue reported it, so order-flow sign
    is measured, not inferred by a Lee-Ready tick rule.

A derivatives venue also contributes perp microstructure - basis to the index, funding,
open-interest change and forced-liquidation flow - on the same grid and the same PIT
rules. Those streams run on their own much slower clocks (open interest is a 10s poll),
so they carry their own `_age_ms` columns and `model.clean` holds them to a separate
tolerance; only a venue's book may claim the bare `{venue}_age_ms` name.

Staleness is explicit: `{venue}_age_ms` is how old the book was at `t`, and callers
drop rows past a tolerance rather than silently modelling a flat-lined quote (Kraken
runs ~10-minute quote gaps in this capture).
"""

from __future__ import annotations

import logging

import polars as pl
import pyarrow as pa
from pyarrow import fs as pafs

from common.lake import iter_partition_tables, list_partitions

log = logging.getLogger(__name__)

NS_PER_S = 1_000_000_000
# Grid spacing. 1s over a day is 86.4k rows per symbol: dense enough for flow
# features to accumulate, coarse enough that 25 dates fit in memory on one box.
BAR_NS = NS_PER_S
# Trailing windows (in bars) for flow aggregates.
FLOW_WINDOWS = (5, 30, 300)
# Trailing windows (in bars) for realised returns. Includes a single bar because the
# move that just happened is the most direct lead-lag channel there is.
RETURN_WINDOWS = (1, 5, 30, 300)
# Forward horizons (in bars) the target is computed at.
HORIZONS = (1, 5, 30)
# Half-spreads of forward move required before a bar counts as a real direction rather
# than the flat class. Half a spread is the cost of crossing to get in (see `_dead_zone`).
DEAD_ZONE_SPREADS = 0.5
# Silver datasets a feature matrix is derived from (the cache keys on their mtimes).
SOURCE_DATASETS = ("quotes", "nbbo", "trades", "mark_price", "open_interest", "liquidations")
# Depth rungs to build imbalance from; mirrors silver.dq.DEPTH_LEVELS. Dates whose
# silver predates the depth columns simply produce nulls (see _depth_features).
DEPTH_RUNGS = (5, 10)


def _to_polars(table: pa.Table | None, decimal_cols: tuple[str, ...]) -> pl.DataFrame | None:
    """Silver Parquet -> polars, with DECIMAL(38,18) cast to f64 for arithmetic.

    The lake stores prices as exact decimals (the portable canonical scale); f64 is
    the modelling representation and is exact well past the precision any venue quotes.
    """
    if table is None or table.num_rows == 0:
        return None
    df = pl.from_arrow(table)
    return df.with_columns([pl.col(c).cast(pl.Float64) for c in decimal_cols if c in df.columns])


def _read_symbol(
    fs: pafs.FileSystem, bucket: str, dataset: str, date: str, symbol: str
) -> pa.Table | None:
    """Read only the partitions belonging to one canonical symbol.

    Silver partitions by symbol, so this is a file-selection filter rather than a
    post-read one: a whole day of every symbol does not fit in memory on this box.
    """
    tables = [
        table
        for part in list_partitions(fs, bucket, dataset, date)
        if part.get("symbol") == symbol
        for table in iter_partition_tables(fs, bucket, dataset, date, part)
    ]
    if not tables:
        return None
    # Venues of one symbol can be of different silver vintages - a backfill rewrites a
    # date venue by venue, so mid-run one venue carries the depth columns and another
    # does not. Promote rather than fail: a column a venue lacks becomes null, which is
    # what `_depth_features` already treats as "depth unknown" (never as zero depth).
    return pa.concat_tables(tables, promote_options="permissive")


QUOTE_DECIMALS = (
    "best_bid",
    "best_ask",
    "bid_sz",
    "ask_sz",
    "bid_depth_5",
    "ask_depth_5",
    "bid_depth_10",
    "ask_depth_10",
    "bid_px_10",
    "ask_px_10",
)


def load_quotes(fs: pafs.FileSystem, bucket: str, date: str, symbol: str) -> pl.DataFrame | None:
    df = _to_polars(_read_symbol(fs, bucket, "quotes", date, symbol), QUOTE_DECIMALS)
    return None if df is None else df.sort("ts_ns")


def load_trades(fs: pafs.FileSystem, bucket: str, date: str, symbol: str) -> pl.DataFrame | None:
    df = _to_polars(_read_symbol(fs, bucket, "trades", date, symbol), ("price", "size"))
    return None if df is None else df.sort("ts_ns")


def load_nbbo(fs: pafs.FileSystem, bucket: str, date: str, symbol: str) -> pl.DataFrame | None:
    df = _to_polars(_read_symbol(fs, bucket, "nbbo", date, symbol), ("best_bid", "best_ask"))
    return None if df is None else df.sort("ts_ns")


MARK_DECIMALS = ("mark_price", "index_price", "est_settle_price", "funding_rate")


def load_mark_price(
    fs: pafs.FileSystem, bucket: str, date: str, symbol: str
) -> pl.DataFrame | None:
    df = _to_polars(_read_symbol(fs, bucket, "mark_price", date, symbol), MARK_DECIMALS)
    return None if df is None else df.sort("ts_ns")


def load_open_interest(
    fs: pafs.FileSystem, bucket: str, date: str, symbol: str
) -> pl.DataFrame | None:
    df = _to_polars(_read_symbol(fs, bucket, "open_interest", date, symbol), ("open_interest",))
    return None if df is None else df.sort("ts_ns")


def load_liquidations(
    fs: pafs.FileSystem, bucket: str, date: str, symbol: str
) -> pl.DataFrame | None:
    decimals = ("price", "avg_price", "orig_size", "filled_size")
    df = _to_polars(_read_symbol(fs, bucket, "liquidations", date, symbol), decimals)
    return None if df is None else df.sort("ts_ns")


def _grid(start_ns: int, end_ns: int) -> pl.DataFrame:
    """Uniform grid of bar-close times covering [start, end], aligned to BAR_NS so
    grids from different dates concatenate without a seam."""
    first = (start_ns // BAR_NS) * BAR_NS
    last = (end_ns // BAR_NS) * BAR_NS
    return pl.DataFrame({"ts_ns": range(first, last + BAR_NS, BAR_NS)}).with_columns(
        pl.col("ts_ns").cast(pl.Int64)
    )


def _imbalance(bid: pl.Expr, ask: pl.Expr) -> pl.Expr:
    """Signed share of resting size on the bid, in [-1, 1]. Scale-free, so it is
    comparable across venues that quote in different size units."""
    return (bid - ask) / (bid + ask)


def _depth_features(have: set[str], prefix: str) -> list[pl.Expr]:
    """Book shape past the touch, from the DEPTH_LEVELS rungs silver carries.

    Two scale-free views, because raw sizes are not comparable venue to venue:
    imbalance at each rung, and how far in *price* ten levels reaches (`span_bps`).
    Size and distance together are the book slope - a thick book that spans 0.5bps
    absorbs flow that the same size spread over 20bps does not.

    Dates built before silver carried depth have no such columns; `have` gates them
    to nulls so the model treats them as missing rather than as zero depth.
    """
    missing = pl.lit(None, dtype=pl.Float64)
    mid = (pl.col("best_bid") + pl.col("best_ask")) / 2
    out: list[pl.Expr] = []
    for n in DEPTH_RUNGS:
        b, a = f"bid_depth_{n}", f"ask_depth_{n}"
        expr = _imbalance(pl.col(b), pl.col(a)) if b in have else missing
        out.append(expr.alias(f"{prefix}_depth_imb_{n}"))
    if "bid_px_10" in have:
        out += [
            ((mid - pl.col("bid_px_10")) / mid * 1e4).alias(f"{prefix}_bid_span_bps"),
            ((pl.col("ask_px_10") - mid) / mid * 1e4).alias(f"{prefix}_ask_span_bps"),
            (pl.col("bid_depth_10") + pl.col("ask_depth_10")).alias(f"{prefix}_depth_10"),
        ]
    else:
        out += [
            missing.alias(f"{prefix}_bid_span_bps"),
            missing.alias(f"{prefix}_ask_span_bps"),
            missing.alias(f"{prefix}_depth_10"),
        ]
    return out


def _book_features(quotes: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Per-quote book state for one venue, before any grid alignment.

    `microprice` is the size-weighted touch (Stoikov): it leans toward the side with
    less size, which is the direction the next trade is more likely to push price.
    """
    mid = (pl.col("best_bid") + pl.col("best_ask")) / 2
    depth = pl.col("bid_sz") + pl.col("ask_sz")
    return quotes.select(
        pl.col("ts_ns"),
        mid.alias(f"{prefix}_mid"),
        ((pl.col("best_ask") - pl.col("best_bid")) / mid * 1e4).alias(f"{prefix}_spread_bps"),
        _imbalance(pl.col("bid_sz"), pl.col("ask_sz")).alias(f"{prefix}_imbalance"),
        (
            (pl.col("best_bid") * pl.col("ask_sz") + pl.col("best_ask") * pl.col("bid_sz")) / depth
            - mid
        ).alias(f"{prefix}_micro_dev"),
        *_depth_features(set(quotes.columns), prefix),
        pl.col("best_bid").alias(f"{prefix}_bid"),
        pl.col("best_ask").alias(f"{prefix}_ask"),
        pl.col("bid_sz").alias(f"{prefix}_bid_sz"),
        pl.col("ask_sz").alias(f"{prefix}_ask_sz"),
        pl.col("ts_ns").alias(f"{prefix}_quote_ts"),
    )


def _ofi(quotes: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Level-1 order-flow imbalance per quote update (Cont-Kukanov-Stoikov).

    Each side contributes its size when the touch improves, minus the previous size
    when it worsens, and the size *change* when the price is unchanged. Summed over a
    bar this is the net signed depth pressure at the touch, the single most reliable
    short-horizon predictor in the microstructure literature.
    """
    b, a = pl.col("best_bid"), pl.col("best_ask")
    bs, as_ = pl.col("bid_sz"), pl.col("ask_sz")
    pb, pa_ = b.shift(1), a.shift(1)
    pbs, pas = bs.shift(1), as_.shift(1)
    bid_term = pl.when(b > pb).then(bs).when(b < pb).then(-pbs).otherwise(bs - pbs)
    ask_term = pl.when(a < pa_).then(as_).when(a > pa_).then(-pas).otherwise(as_ - pas)
    return quotes.select(
        pl.col("ts_ns"),
        (bid_term - ask_term).fill_null(0.0).alias(f"{prefix}_ofi"),
    )


def _bar_index(ts: pl.Expr) -> pl.Expr:
    """Bar close a timestamp belongs to: the half-open bar (t - BAR, t]."""
    return ((ts + BAR_NS - 1) // BAR_NS) * BAR_NS


def _flow_features(trades: pl.DataFrame, grid: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Signed trade flow aggregated into each bar, then rolled over trailing windows.

    `side` is the taker direction, so signed size is the real aggressor imbalance.
    Bars with no trades are genuine zeros (no flow), not missing data.
    """
    signed = pl.when(pl.col("side") == "bid").then(pl.col("size")).otherwise(-pl.col("size"))
    per_bar = (
        trades.with_columns(_bar_index(pl.col("ts_ns")).alias("ts_ns"), signed.alias("signed_size"))
        .group_by("ts_ns")
        .agg(
            pl.col("signed_size").sum().alias(f"{prefix}_signed_vol"),
            pl.col("size").sum().alias(f"{prefix}_volume"),
            pl.len().alias(f"{prefix}_n_trades"),
            (pl.col("price") * pl.col("size")).sum().alias("_notional"),
        )
        .sort("ts_ns")
    )
    out = grid.join(per_bar, on="ts_ns", how="left").with_columns(
        pl.col(f"{prefix}_signed_vol").fill_null(0.0),
        pl.col(f"{prefix}_volume").fill_null(0.0),
        pl.col(f"{prefix}_n_trades").fill_null(0),
        (pl.col("_notional") / pl.col(f"{prefix}_volume")).alias(f"{prefix}_vwap"),
    )
    rolls = []
    for w in FLOW_WINDOWS:
        rolls += [
            pl.col(f"{prefix}_signed_vol").rolling_sum(w).alias(f"{prefix}_signed_vol_{w}"),
            pl.col(f"{prefix}_volume").rolling_sum(w).alias(f"{prefix}_volume_{w}"),
        ]
    return out.with_columns(rolls).drop("_notional")


def _mark_features(mark: pl.DataFrame, grid: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Perp basis and funding state, as-of the grid.

    `basis_bps` is the perp's premium to the index it settles against. It is the
    cleanest read on directional pressure in a perpetual: the contract has no expiry to
    pull it back, so leveraged demand shows up as the mark drifting off the index and is
    paid for through funding rather than arbitraged away instantly.

    `funding_in_s` is computed on the grid clock rather than at the mark tick, because
    time to the next funding stamp decays continuously between ticks - carrying a
    tick-time value forward would freeze a countdown.
    """
    index = pl.col("index_price")
    series = mark.select(
        pl.col("ts_ns"),
        pl.when(index > 0)
        .then((pl.col("mark_price") - index) / index * 1e4)
        .alias(f"{prefix}_basis_bps"),
        pl.col("funding_rate").alias(f"{prefix}_funding_rate"),
        pl.col("next_funding_ts_ns"),
        pl.col("ts_ns").alias(f"{prefix}_mark_ts"),
    )
    out = _asof(grid, series, f"{prefix}_mark_ts", f"{prefix}_mark_age_ms")
    basis = pl.col(f"{prefix}_basis_bps")
    return out.with_columns(
        ((pl.col("next_funding_ts_ns") - pl.col("ts_ns")) / NS_PER_S).alias(
            f"{prefix}_funding_in_s"
        ),
        *[(basis - basis.shift(w)).alias(f"{prefix}_basis_chg_{w}") for w in FLOW_WINDOWS],
    ).drop("next_funding_ts_ns")


def _oi_features(oi: pl.DataFrame, grid: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Open-interest *changes*, never the level.

    The level is a non-stationary stock that a tree would happily memorise; the flow is
    the microstructure signal. Rising OI into a rising price is new leveraged longs
    opening, whereas falling OI into the same move is shorts closing out - the same
    price path with opposite implications for what happens next.
    """
    series = oi.select(
        pl.col("ts_ns"),
        pl.col("open_interest").alias("_oi_level"),
        pl.col("ts_ns").alias(f"{prefix}_oi_ts"),
    )
    out = _asof(grid, series, f"{prefix}_oi_ts", f"{prefix}_oi_age_ms")
    level = pl.col("_oi_level")
    return out.with_columns(
        *[
            pl.when(level.shift(w) > 0)
            .then((level / level.shift(w) - 1) * 1e4)
            .alias(f"{prefix}_oi_chg_bps_{w}")
            for w in FLOW_WINDOWS
        ]
    ).drop("_oi_level")


def _liq_features(liq: pl.DataFrame, grid: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Forced-liquidation flow per bar, signed like taker flow.

    `side` is the side of the forced order, so an ASK is a long being sold out and signs
    negative - the same convention as `_flow_features`, and the direction the flow
    actually pushes price. `filled_size` is used rather than the order size because only
    the filled part ever reached the book.

    SAMPLED, and the feature name cannot say so: Binance publishes only the largest
    liquidation per symbol per second (common.models.Liquidation), so these columns are
    a lower bound on forced flow and their magnitude is not comparable to `_signed_vol`.
    They are deliberately NOT named `_signed_vol`, which would pull them into
    `model.MONOTONE_POSITIVE` - a cascade overshooting and reverting within seconds is
    exactly the case where the sign is an open question rather than a prior.
    """
    size = pl.col("filled_size")
    signed = pl.when(pl.col("side") == "bid").then(size).otherwise(-size)
    per_bar = (
        liq.with_columns(_bar_index(pl.col("ts_ns")).alias("ts_ns"), signed.alias("_signed"))
        .group_by("ts_ns")
        .agg(
            pl.col("_signed").sum().alias(f"{prefix}_liq_flow"),
            pl.len().alias(f"{prefix}_n_liqs"),
        )
        .sort("ts_ns")
    )
    out = grid.join(per_bar, on="ts_ns", how="left").with_columns(
        pl.col(f"{prefix}_liq_flow").fill_null(0.0),
        pl.col(f"{prefix}_n_liqs").fill_null(0),
    )
    rolls = []
    for w in FLOW_WINDOWS:
        rolls += [
            pl.col(f"{prefix}_liq_flow").rolling_sum(w).alias(f"{prefix}_liq_flow_{w}"),
            pl.col(f"{prefix}_n_liqs").rolling_sum(w).alias(f"{prefix}_n_liqs_{w}"),
        ]
    return out.with_columns(rolls)


def _perp_features(
    fs: pafs.FileSystem, bucket: str, date: str, symbol: str, grid: pl.DataFrame
) -> list[pl.DataFrame]:
    """Grid-aligned perp frames, one per venue publishing any of the perp tape.

    Only a derivatives venue publishes these datasets, so a spot symbol contributes
    nothing and the columns are simply absent - the same shape the model already handles
    for a venue that was down all date. A venue publishing one stream but not another
    gets nulls for the missing one rather than being dropped entirely.
    """
    mark = load_mark_price(fs, bucket, date, symbol)
    oi = load_open_interest(fs, bucket, date, symbol)
    liq = load_liquidations(fs, bucket, date, symbol)
    frames: list[pl.DataFrame] = []
    for venue in sorted({v for df in (mark, oi, liq) if df is not None for v in df["exchange"]}):
        prefix = leg_prefix(venue)
        for df, build in ((mark, _mark_features), (oi, _oi_features), (liq, _liq_features)):
            if df is None:
                continue
            rows = df.filter(pl.col("exchange") == venue)
            if not rows.is_empty():
                frames.append(build(rows, grid, prefix))
    return frames


def _asof(grid: pl.DataFrame, series: pl.DataFrame, stamp: str, age: str) -> pl.DataFrame:
    """Backward as-of join: at each grid time, the last observation at or before it.

    This is the PIT primitive - `strategy="backward"` cannot see the future - and it
    turns the observation's own timestamp into an explicit staleness column, so a caller
    can always tell a fresh value from one carried forward across a gap.
    """
    joined = grid.join_asof(series, on="ts_ns", strategy="backward")
    return joined.with_columns(((pl.col("ts_ns") - pl.col(stamp)) / 1e6).alias(age)).drop(stamp)


def _align(grid: pl.DataFrame, series: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """As-of join a venue's book onto the grid, aged as `{prefix}_age_ms`.

    That exact name is what `model.clean` treats as a *book* age and holds to the tight
    freshness tolerance, so only a venue's quotes may claim it.
    """
    return _asof(grid, series, f"{prefix}_quote_ts", f"{prefix}_age_ms")


def _returns(prefix: str) -> list[pl.Expr]:
    """Trailing realised return over each window, in bps, from the grid-aligned mid.

    Backward-looking by construction - `shift(w)` reaches into the past - and computed
    within one date's grid, so no window spans a date boundary.

    This is also what makes a leg on a different quote asset usable at all. BTC-USDT's
    *price* is not comparable to BTC-USD's, since the USDT basis sits between them, but
    a move of n bps is the same underlying move in both to first order over seconds.
    Returns are deliberately left out of `model.MONOTONE_POSITIVE`: that a leading
    venue's past move predicts a follower's next one is the hypothesis under test, not
    a prior to impose on it.
    """
    mid = pl.col(f"{prefix}_mid")
    return [((mid / mid.shift(w) - 1) * 1e4).alias(f"{prefix}_ret_bps_{w}") for w in RETURN_WINDOWS]


def leg_prefix(exchange: str) -> str:
    """Column prefix for a venue leg. The exchange alone, never the symbol.

    Keeping the symbol out of the name is what lets a cross-symbol leg join the matrix
    without renaming every existing column, and keeps `model.venue_prefixes` grouping
    by the thing price discovery is actually about - the venue.
    """
    return exchange.replace("-", "_")


def _quote_legs(
    fs: pafs.FileSystem, bucket: str, date: str, symbols: list[str]
) -> list[tuple[str, str, pl.DataFrame]]:
    """(prefix, symbol, quotes) per venue leg, across every requested canonical symbol.

    Two symbols quoted by the same exchange would collide on the prefix, so that is
    rejected outright rather than silently overwriting one leg with the other. It is
    also not a thing this matrix is shaped for: joining BTC-USD and ETH-USD as if they
    were venues of one instrument asks a cross-asset question, not a price-discovery one.
    """
    legs: list[tuple[str, str, pl.DataFrame]] = []
    owner: dict[str, str] = {}
    for symbol in symbols:
        quotes = load_quotes(fs, bucket, date, symbol)
        if quotes is None or quotes.is_empty():
            log.warning("no quotes for %s on %s - leg skipped", symbol, date)
            continue
        for venue in sorted(quotes["exchange"].unique()):
            prefix = leg_prefix(venue)
            if prefix in owner:
                raise ValueError(
                    f"{venue} quotes both {owner[prefix]} and {symbol}, which would collide "
                    f"on the {prefix!r} column prefix. Feature legs must be one symbol per "
                    f"exchange; model cross-asset relationships separately."
                )
            owner[prefix] = symbol
            legs.append((prefix, symbol, quotes.filter(pl.col("exchange") == venue).sort("ts_ns")))
    return legs


def build_features(
    fs: pafs.FileSystem,
    bucket: str,
    date: str,
    symbol: str,
    extra_symbols: tuple[str, ...] = (),
) -> pl.DataFrame | None:
    """One date -> the aligned feature matrix (targets included).

    `symbol` is the primary instrument: it anchors the grid, supplies the NBBO, and is
    the only symbol a target is computed against. `extra_symbols` contribute venue legs
    and nothing else, which is how a venue that trades a *different* canonical symbol
    gets into the model - Binance quotes BTC-USDT and BTC-USDT-PERP, never BTC-USD, so
    without this the most liquid venue in the capture is structurally invisible.

    Legs are as-of joined onto the primary grid, so an extra symbol widens the feature
    set without moving the clock or the target. Venues are whatever silver holds that
    date, so a venue that was down contributes null features and a non-null age rather
    than vanishing.
    """
    nbbo = load_nbbo(fs, bucket, date, symbol)
    legs = _quote_legs(fs, bucket, date, [symbol, *extra_symbols])
    primary_legs = [q for _p, sym, q in legs if sym == symbol]
    if nbbo is None or nbbo.is_empty() or not primary_legs:
        # An extra symbol alone is not enough: with no primary leg there is no grid to
        # anchor and no target to predict (some dates hold only the perp).
        log.warning("no quotes/nbbo for %s on %s", symbol, date)
        return None

    # The grid is anchored to the primary symbol alone: the target lives there, so an
    # extra leg must never extend the clock into hours the target does not cover.
    grid = _grid(
        min(q["ts_ns"].min() for q in primary_legs),
        max(q["ts_ns"].max() for q in primary_legs),
    )
    out = grid

    for prefix, _sym, vq in legs:
        out = _align(out, _book_features(vq, prefix), prefix)
        ofi_bar = (
            _ofi(vq, prefix)
            .with_columns(_bar_index(pl.col("ts_ns")).alias("ts_ns"))
            .group_by("ts_ns")
            .agg(pl.col(f"{prefix}_ofi").sum())
            .sort("ts_ns")
        )
        out = out.join(ofi_bar, on="ts_ns", how="left").with_columns(
            pl.col(f"{prefix}_ofi").fill_null(0.0)
        )
        out = out.with_columns(
            *_returns(prefix),
            *[
                pl.col(f"{prefix}_ofi").rolling_sum(w).alias(f"{prefix}_ofi_{w}")
                for w in FLOW_WINDOWS
            ],
        )

    for sym in (symbol, *extra_symbols):
        trades = load_trades(fs, bucket, date, sym)
        if trades is None or trades.is_empty():
            log.warning("no trades tape for %s on %s - flow features skipped", sym, date)
            continue
        for venue in sorted(trades["exchange"].unique()):
            vt = trades.filter(pl.col("exchange") == venue).sort("ts_ns")
            out = out.join(_flow_features(vt, grid, leg_prefix(venue)), on="ts_ns", how="left")

    for sym in (symbol, *extra_symbols):
        for frame in _perp_features(fs, bucket, date, sym, grid):
            out = out.join(frame, on="ts_ns", how="left")

    return _add_targets(out, _align(grid, _nbbo_series(nbbo), "nbbo"), [p for p, _s, _q in legs])


def _nbbo_series(nbbo: pl.DataFrame) -> pl.DataFrame:
    mid = (pl.col("best_bid") + pl.col("best_ask")) / 2
    return nbbo.select(
        pl.col("ts_ns"),
        mid.alias("nbbo_mid"),
        ((pl.col("best_ask") - pl.col("best_bid")) / mid * 1e4).alias("nbbo_spread_bps"),
        pl.col("n_venues").cast(pl.Int64).alias("nbbo_n_venues"),
        pl.col("ts_ns").alias("nbbo_quote_ts"),
    )


def _dead_zone(ret: pl.Expr, spread_bps: pl.Expr) -> pl.Expr:
    """Forward return -> {-1, 0, +1}, flat inside half the spread prevailing at `t`.

    A 5s forward mid move is mostly quote noise, and scoring direction on it counts
    sub-tick wiggles as wins. Half the spread is the smallest move worth calling: it is
    what crossing to get in costs, so anything smaller is not a signal that could be
    acted on however well it is predicted.

    The threshold is the spread at `t`, which is knowable at `t` - it is a feature, not
    a property of the future. Scaling by the spread also keeps the target comparable
    across venues and across the day, which a fixed bps cut would not: the same 1bps
    move is tradeable in a tight book and inside the noise in a wide one.

    A null return stays null rather than collapsing into the flat class, which would
    otherwise turn every row past the end of the date into a confident "no move".
    """
    threshold = spread_bps * DEAD_ZONE_SPREADS
    return (
        pl.when(ret.is_null() | spread_bps.is_null())
        .then(None)
        .when(ret > threshold)
        .then(1)
        .when(ret < -threshold)
        .then(-1)
        .otherwise(0)
        .cast(pl.Int8)
    )


def _add_targets(
    features: pl.DataFrame, nbbo_grid: pl.DataFrame, venues: list[str]
) -> pl.DataFrame:
    """Forward returns in bps at each horizon. The ONLY forward-looking columns in the
    frame, and the only ones prefixed `y_`.

    Two flavours, because they answer different questions:
      - `y_ret_bps_{h}` on the NBBO mid: what the consolidated price does next.
      - `y_{venue}_ret_bps_{h}` on a single venue's mid: what *that* venue does next.

    The second exists because the NBBO is built from these venues' quotes, so a venue's
    own features partly predict it by construction. Predicting venue A's mid from venue
    B's features alone has no such shared term, and is the honest lead-lag test.

    Every leg gets a per-venue target, including legs from an extra symbol: a return is
    comparable across quote assets even though the price level is not, so
    `y_binance_ret_bps_5` is a well-posed target to predict from Coinbase and Kraken.
    Only the NBBO target is confined to the primary symbol, since that is the only
    consolidated book being built.

    Each return also gets a `y_..._dz_{h}` three-class sibling (see `_dead_zone`), so a
    caller can ask "did it move enough to be worth trading" rather than "did it move".
    """
    out = features.join(nbbo_grid, on="ts_ns", how="left")
    targets = []
    for prefix, mid_col, spread_col in [
        ("y", "nbbo_mid", "nbbo_spread_bps"),
        *((f"y_{v}", f"{v}_mid", f"{v}_spread_bps") for v in venues),
    ]:
        mid = pl.col(mid_col)
        for h in HORIZONS:
            ret = (mid.shift(-h) / mid - 1) * 1e4
            targets += [
                ret.alias(f"{prefix}_ret_bps_{h}"),
                _dead_zone(ret, pl.col(spread_col)).alias(f"{prefix}_dz_{h}"),
            ]
    return out.with_columns(targets)
