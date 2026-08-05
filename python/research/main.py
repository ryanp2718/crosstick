"""Research entrypoint: ``python -m research.main --symbol BTC-USD <date> [<date> ...]``.

Builds the PIT feature matrix per date (cached to local Parquet, since a date costs
a full silver read), optionally joining further canonical symbols as extra venue legs
(``--extra-symbols BTC-USDT BTC-USDT-PERP``), concatenates, then walks an expanding
window forward over the
dates: each fold trains only on its own past, and the folds' predictions pool into one
out-of-sample series that gets bootstrapped by date for confidence intervals. Prints
the model comparison and per-venue permutation importance - the price-discovery answer,
with the error bars that say how much of it to believe.

Every run lands a record in ``python/runs/`` (see ``research.runs``) and the printed
tables are rendered from it, so nothing reaches the terminal that was not saved. The
functions here format; they compute nothing.

Env matches the batch layers: base ``S3_*`` is the silver endpoint, ``SILVER_BUCKET``
(default ``silver``). Reads silver only; bronze is never touched.
"""

from __future__ import annotations

import argparse
import logging
import os
from functools import lru_cache
from hashlib import blake2b
from pathlib import Path

import numpy as np
import polars as pl

from common.lake import filesystem_from_env, list_partitions, partition_key
from research import features, infoshare, runs
from research.features import DEAD_ZONE_SPREADS, HORIZONS, SOURCE_DATASETS, build_features
from research.model import MAX_AGE_MS, clean
from research.runs import RunRecord, RunSpec
from research.schema import FeatureSchema
from research.validation import (
    MIN_BOOTSTRAP_BLOCKS,
    MIN_TRAIN_DATES,
    N_BOOT,
    PLACEBO_LAG_BARS,
    STEP_DATES,
    TEST_DATES,
    evaluate_walk_forward,
    fold_spread,
    placebo_target,
    walk_forward,
)

log = logging.getLogger(__name__)


def source_fingerprint(fs, bucket: str, date: str, symbols: tuple[str, ...]) -> str:
    """Newest silver object backing this date across every symbol, as the cache key.

    A date is only immutable once silver has finished with it. Re-running the backfill
    (adding the trades tape to a date built before it existed) rewrites those objects,
    and a cache keyed on date alone would keep serving the older, narrower feature set
    without a word. Keying on mtime makes a rebuild produce a different filename.
    """
    newest = 0.0
    for dataset in SOURCE_DATASETS:
        for part in list_partitions(fs, bucket, dataset, date):
            if part.get("symbol") not in symbols:
                continue
            info = fs.get_file_info(f"{bucket}/{partition_key(dataset, date=date, **part)}")
            if info.mtime is not None:
                newest = max(newest, info.mtime.timestamp())
    return str(int(newest))


@lru_cache(maxsize=1)
def builder_fingerprint() -> str:
    """Short digest of `research/features.py`, as part of every cache key.

    The source mtime says when the DATA last changed and nothing about what was computed
    from it. Adding a feature family changes the frame without touching a single silver
    object, so an mtime-only key happily serves a matrix built before the columns existed
    - the run then trains on a narrower feature set than it reports and nothing fails.

    Hashing the builder is deliberately blunt: any edit here rebuilds, including one that
    only moved a comment. A needless rebuild costs a silver read; the alternative costs a
    wrong answer that looks right.
    """
    return blake2b(Path(features.__file__).read_bytes(), digest_size=4).hexdigest()


def cache_name(date: str, symbol: str, extra_symbols: tuple[str, ...], fingerprint: str) -> str:
    """Cache filename for one date's feature matrix.

    The extra symbols are part of the identity, not just the mtime: the same date built
    with and without Binance legs are different matrices, and a key that ignored them
    would serve a two-venue frame to a four-venue run.
    """
    legs = "+".join(("", *extra_symbols))
    return f"{symbol}{legs}_{date}_{fingerprint}-{builder_fingerprint()}.parquet"


def cached_features(
    fs,
    bucket: str,
    date: str,
    symbol: str,
    cache: Path,
    refresh: bool = False,
    extra_symbols: tuple[str, ...] = (),
) -> tuple[pl.DataFrame | None, str]:
    """Feature matrix for one date and the silver fingerprint it was built from.

    A date is a whole silver read plus the joins; caching makes iterating on the model
    cheap without re-deriving features from silver that has not changed.

    The fingerprint is returned rather than recomputed by the caller because deriving it
    walks every partition of every source dataset, which is most of the cost of a cache
    hit. It is the data half of a run's provenance: two runs over the same dates whose
    fingerprints differ read different silver.
    """
    all_symbols = (symbol, *extra_symbols)
    fingerprint = source_fingerprint(fs, bucket, date, all_symbols)
    path = cache / cache_name(date, symbol, extra_symbols, fingerprint)
    if path.exists() and not refresh:
        return pl.read_parquet(path), fingerprint
    df = build_features(fs, bucket, date, symbol, extra_symbols)
    if df is None:
        return None, fingerprint
    df = df.with_columns(pl.lit(date).alias("date"))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return df, fingerprint


CLASS_KEYS = ("up_precision", "up_recall", "down_precision", "down_recall")


def _cells(ci: dict[str, tuple[float, float, float]]) -> str:
    return " ".join(f"{ci[k][0]:>6.3f}[{ci[k][1]:>5.3f},{ci[k][2]:>5.3f}]" for k in CLASS_KEYS)


def report(record: RunRecord) -> None:
    """Print the whole run.

    Formatting only: every number here was computed into the record before anything was
    printed, so what is on screen and what is on disk cannot disagree.
    """
    _report_folds(record)
    _report_spread(record)
    _report_pooled(record)
    _report_dead_zone(record)
    _report_selectivity(record)
    _report_importance(record)
    _report_infoshare(record)


def _report_folds(record: RunRecord) -> None:
    """Per-fold scores. The point of printing every fold rather than only the mean is
    that a result driven by one unusual week is visible here and nowhere else."""
    print(f"\nper fold ({len(record.folds)} folds, expanding window)")
    print(f"{'fold':>4}  {'train':<24} {'test':<24} {'n':>7} {'gbt R2':>10} {'gbt hit':>9}")
    print("-" * 84)
    for i, fold in enumerate(record.folds, 1):
        gbt = fold.metrics["gbt"]
        train = f"{fold.train_dates[0]}..{fold.train_dates[-1]}"
        test = f"{fold.test_dates[0]}..{fold.test_dates[-1]}"
        print(
            f"{i:>4}  {train:<24} {test:<24} {gbt['n']:>7.0f} "
            f"{gbt['r2_vs_zero']:>10.5f} {gbt['hit_rate']:>9.4f}"
        )


def _report_spread(record: RunRecord) -> None:
    print("\nacross folds (mean +/- sd)")
    print(f"{'model':<12} {'R2 vs zero':>22} {'hit rate':>22}")
    print("-" * 58)
    fold_metrics = [f.metrics for f in record.folds]
    for name in record.pooled:
        r2, r2_sd = fold_spread(fold_metrics, name, "r2_vs_zero")
        hit, hit_sd = fold_spread(fold_metrics, name, "hit_rate")
        print(f"{name:<12} {r2:>12.5f} +/- {r2_sd:<6.5f} {hit:>12.4f} +/- {hit_sd:<6.4f}")


def _report_pooled(record: RunRecord) -> None:
    """The headline, with the interval attached to it.

    Every row here was predicted out of sample by a model that had only seen earlier
    dates, and the interval comes from resampling whole days - so it reflects the fact
    that this capture holds a couple of dozen days, not a couple of dozen thousand
    independent observations.
    """
    print(
        f"\npooled out-of-sample: {record.n_test_dates} test dates, "
        f"95% CI from {record.spec.n_boot} date-block resamples"
    )
    if record.bootstrap_is_degenerate:
        print(
            f"  WARNING: {record.n_test_dates} blocks is too few to resample. The interval\n"
            f"  below is degenerate - it collapses onto the per-fold values and understates\n"
            f"  the uncertainty. Do not quote it; it needs >= {MIN_BOOTSTRAP_BLOCKS} test dates."
        )
    print(f"{'model':<12} {'n':>7} {'R2 vs zero':>26} {'hit rate':>24} {'bps/trade':>24}")
    print("-" * 96)
    for name, ci in record.pooled.items():
        cells = []
        for key, width in (("r2_vs_zero", 5), ("hit_rate", 4), ("gross_bps_per_trade", 4)):
            point, lo, hi = ci[key]
            cells.append(f"{point:>9.{width}f} [{lo:>7.{width}f},{hi:>8.{width}f}]")
        print(f"{name:<12} {record.n_oos:>7} " + " ".join(cells))
    print(
        "\n  bps/trade is the gross edge, which IS the break-even round-trip cost: fills at\n"
        "  mid, no slippage, queue or latency. A retail taker round trip on Coinbase/Kraken\n"
        "  is tens of bps and the BTC-USD touch spread alone is ~1-2 bps crossed twice, so\n"
        "  these are upper bounds to compare against a cost, not a P&L."
    )


def _report_dead_zone(record: RunRecord) -> None:
    """Precision and recall once sub-spread wiggles stop counting as wins.

    Hit rate is scored on any move at all, so on a 1s grid it is mostly measuring which
    way the last quote twitched. Here a bar only counts as a direction if it moved more
    than half the prevailing spread, and a prediction only counts as a call if it
    expected the same - which is the version of the question that survives costs.
    """
    if not record.classes:
        return
    print("\ndead-zone classes (move > half the spread at t, same date-block resamples)")
    header = " ".join(f"{k.replace('_', ' '):>19}" for k in CLASS_KEYS)
    print(f"{'model':<12} {'traded':>7} {header}")
    print("-" * 101)
    for name, ci in record.classes.items():
        print(f"{name:<12} {record.traded[name]:>7.3f} " + _cells(ci))

    up, down = record.base_rates["up"], record.base_rates["down"]
    print(
        f"\n  {(up + down) * 100:.1f}% of pooled bars cleared half a spread ({up * 100:.1f}% up, "
        f"{down * 100:.1f}% down). Those two\n"
        "  are the base rates a directional call has to beat to carry any information, and\n"
        "  their sum is the ceiling on recall. 'traded' is the share of bars the model's own\n"
        "  view cleared the same bar on: a model that never clears it has no tradeable\n"
        "  opinion, whatever its hit rate says."
    )


# Traded shares to score the headline model at. Spans two orders of magnitude so the
# curve shows whether precision actually concentrates as the model gets selective.
COVERAGES = (0.02, 0.05, 0.10, 0.25, 0.50)


def _report_selectivity(record: RunRecord, model: str = "gbt") -> None:
    """Precision as the model is forced to call a fixed share of bars.

    The row above floats: each model calls whatever share its own output happens to clear
    half a spread on, which was 0.939 on the 2026-07-29 lead-lag run and 0.085 on its
    placebo. Precision falls as coverage rises, so those two numbers sit at different
    points of different curves and comparing them is meaningless. Fixing the share makes
    runs comparable.

    Read this against the PLACEBO's curve, not against the base rate. Volatility is
    autocorrelated at the hour scale, so a model with no directional skill at all still
    beats the base rate here by flagging bars that clear the threshold and coin-flipping
    the sign: the same placebo scored 0.144/0.147 precision on 0.105/0.100 base rates
    with R2 -0.0001 and hit 0.5035. The base rate is not the null; the placebo is.
    """
    if not record.selectivity:
        return
    print(f"\nselectivity ({model}, precision at a forced traded share)")
    header = " ".join(f"{k.replace('_', ' '):>19}" for k in CLASS_KEYS)
    print(f"{'traded':>7} {'n called':>9} {header}")
    print("-" * 98)
    for row in record.selectivity:
        print(f"{row.coverage:>7.2f} {row.n_called:>9} " + _cells(row.metrics))
    print(
        "\n  Compare a row against the SAME row of the placebo run, never against the base\n"
        "  rate: a model that predicts only volatility and not direction still clears the\n"
        "  base rate here. The gap between this curve and the placebo's is the directional\n"
        "  skill; the placebo's own height above base is the volatility component."
    )


def _report_coverage(schema: FeatureSchema, gaps: dict[str, dict[str, int]]) -> None:
    """The pooled column set, and what each date is missing from it.

    Dates are unioned with `pl.concat(..., how="diagonal")`, which null-fills a date that
    lacks a venue or a stream and says nothing. Without this, a fortnight where Binance
    was down reads exactly like a fortnight where it was up and quiet, and the run reports
    a four-venue feature count either way. The nulls are legitimate and the model handles
    them; the silence is the problem.
    """
    print(f"\nfeature matrix: {len(schema.names)} columns, venues {schema.venues}")
    for date, missing in gaps.items():
        if missing:
            detail = ", ".join(f"{label} ({n})" for label, n in sorted(missing.items()))
            print(f"  {date} carries no {detail}")


def _report_infoshare(record: RunRecord) -> None:
    """The second analysis, one block per sampling frequency.

    Separate from the model's answer because it answers a different question. The model
    says which venue's book forecasts which, and cannot tell a venue that discovers price
    from one that merely requotes slowly: a coarse book is mechanically forecastable from
    any outside information. This says which venue the other one error-corrects toward.
    """
    for run in record.infoshare:
        print(f"\n{'=' * 72}\nstride {run.stride} ({run.stride}s bars), {run.lags} lags")
        infoshare.report(run.estimates)


def _report_importance(record: RunRecord) -> None:
    """Per-venue permutation importance, averaged over folds with its spread.

    A venue that leads only in some folds shows up here as a large sd next to its mean,
    which is the difference between a structural result and a lucky week.
    """
    if not record.importance:
        return
    means = {v: float(np.mean([f[v] for f in record.importance])) for v in record.venues}
    sds = {v: float(np.std([f[v] for f in record.importance])) for v in record.venues}
    total = sum(m for m in means.values() if m > 0)
    print("\nper-venue permutation importance (gbt, mean +/- sd over folds)")
    for venue, mean in sorted(means.items(), key=lambda kv: -kv[1]):
        share = f"{mean / total * 100:>5.1f}%" if total > 0 and mean > 0 else "    -"
        print(f"  {venue:<20} {mean:>10.5f} +/- {sds[venue]:<9.5f} {share}")


def _information_shares(df: pl.DataFrame, spec: RunSpec, max_age_ms: int) -> list[runs.InfoShares]:
    """Fit the VECM once per date per requested sampling frequency.

    Fitted on the cleaned frame the model saw, so the two analyses answer their different
    questions about the same rows. `max_age_ms` is passed through rather than left to
    `clean`'s filtering alone because `price_panel` drops rather than forward-fills: a
    carried-forward mid is a fabricated zero return, and zero returns bias an
    error-correction loading toward "this venue does not adjust", which is the exact
    quantity being measured.
    """
    if not spec.infoshare_venues:
        return []
    out = []
    for stride in spec.infoshare_strides:
        estimates = infoshare.by_date(
            df, spec.infoshare_venues, stride=stride, max_age_ms=max_age_ms
        )
        out.append(
            runs.InfoShares(
                venues=spec.infoshare_venues,
                stride=stride,
                lags=infoshare.DEFAULT_LAGS,
                estimates=estimates,
            )
        )
    return out


def run(
    dates: list[str],
    symbol: str,
    horizon: int,
    cache: Path,
    refresh: bool = False,
    predict_venue: str | None = None,
    max_age_ms: int = MAX_AGE_MS,
    min_train: int = MIN_TRAIN_DATES,
    test_size: int = TEST_DATES,
    step: int = STEP_DATES,
    n_boot: int = N_BOOT,
    placebo: bool = False,
    extra_symbols: tuple[str, ...] = (),
    infoshare_venues: tuple[str, str] | None = None,
    infoshare_strides: tuple[int, ...] = (),
) -> RunRecord | None:
    """Build, fit, score, report and persist one run.

    Returns the record so a caller that is not a terminal (a notebook, a comparison
    script) gets the numbers rather than having to parse them back out of stdout.
    """
    fs = filesystem_from_env()
    bucket = os.environ.get("SILVER_BUCKET", "silver")

    frames = []
    columns_by_date: dict[str, list[str]] = {}
    sources: dict[str, str] = {}
    for date in dates:
        df, sources[date] = cached_features(fs, bucket, date, symbol, cache, refresh, extra_symbols)
        if df is None:
            log.warning("skipping %s (no features)", date)
            continue
        frames.append(df)
        columns_by_date[date] = df.columns
        log.info("%s: %d rows", date, df.height)
    if not frames:
        print("no feature rows built")
        return

    # Over the union, since a gap only exists relative to what the other dates carry.
    schema = FeatureSchema.from_columns(c for cols in columns_by_date.values() for c in cols)
    venues = schema.venues
    gaps = {
        date: {label: len(cols) for label, cols in schema.missing_by_family(columns).items()}
        for date, columns in columns_by_date.items()
    }
    if extra_symbols:
        print(f"\nlegs: {symbol} + {list(extra_symbols)}")
    _report_coverage(schema, gaps)

    df = pl.concat(frames, how="diagonal").sort("ts_ns")
    if predict_venue:
        if predict_venue not in venues:
            print(f"unknown venue {predict_venue!r}; have {venues}")
            return
        target = schema.target(horizon, predict_venue)
        # The threshold is the spread of the book being predicted, even in cross-venue
        # mode where that venue's features are excluded. That is not a leak: it defines
        # what counts as a tradeable move on that book, the model never sees it, and a
        # trader working that venue knows its spread at `t`.
        spread_col = schema.spread(predict_venue)
        print(
            f"\ncross-venue mode: predicting {predict_venue}'s own forward mid using only "
            f"{[v for v in venues if v != predict_venue]} (no NBBO, no {predict_venue} features)"
        )
    else:
        target = schema.target(horizon)
        spread_col = schema.spread()
    if placebo:
        df = placebo_target(df, target)
        print(
            f"\n{'=' * 72}\nPLACEBO MODE: the target is displaced {PLACEBO_LAG_BARS} bars "
            f"within each date.\nEverything below should read as a null - R2 at or just "
            f"below zero, hit rate\n0.50. Anything materially better than that is a leak, "
            f"and no other number\nfrom this run means anything until it is found.\n{'=' * 72}"
        )
    before = df.height
    df = clean(df, target, max_age_ms)
    print(
        f"\n{symbol}: {df.height} usable rows from {before} "
        f"({df.height / before * 100:.1f}% survive the <={max_age_ms}ms staleness cut) "
        f"over {df['date'].n_unique()} dates"
    )

    splits = walk_forward(
        df, target, horizon, min_train, test_size, step, predict_venue, spread_col
    )
    if not splits:
        n = df["date"].n_unique()
        print(
            f"\nno folds: {n} dates cannot support train>={min_train} + test={test_size}.\n"
            f"pass --min-train/--test-size/--step to fit the data you have, e.g. "
            f"--min-train {max(1, n - 2)} --test-size 1 --step 1"
        )
        return
    print(f"\n=== {target} ===")
    print(f"features: {len(splits[0].feature_names)}")

    wf = evaluate_walk_forward(splits, horizon, venues)
    spec = RunSpec(
        dates=tuple(dates),
        symbol=symbol,
        extra_symbols=tuple(extra_symbols),
        horizon=horizon,
        target=target,
        spread_col=spread_col,
        predict_venue=predict_venue,
        max_age_ms=max_age_ms,
        min_train=min_train,
        test_size=test_size,
        step=step,
        n_boot=n_boot,
        placebo=placebo,
        placebo_lag_bars=PLACEBO_LAG_BARS,
        coverages=COVERAGES,
        dead_zone_spreads=DEAD_ZONE_SPREADS,
        infoshare_venues=infoshare_venues,
        infoshare_strides=tuple(infoshare_strides),
    )
    record = runs.build(
        spec,
        runs.provenance(builder_fingerprint(), schema, sources),
        wf,
        schema,
        n_rows=df.height,
        n_rows_before=before,
        coverage_gaps=gaps,
        infoshare=_information_shares(df, spec, max_age_ms),
    )
    report(record)
    print(f"\nrun record: {record.write()}")
    return record


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fit the price-discovery model over silver.")
    p.add_argument("dates", nargs="+", help="UTC dates, e.g. 2026-06-27")
    p.add_argument("--symbol", default="BTC-USD", help="canonical symbol (default BTC-USD)")
    p.add_argument(
        "--extra-symbols",
        nargs="+",
        default=[],
        metavar="SYMBOL",
        help="further canonical symbols to join as venue legs, e.g. BTC-USDT "
        "BTC-USDT-PERP. They contribute features and per-venue targets but never the "
        "grid or the NBBO. This is the only way a venue that does not quote --symbol "
        "(Binance never quotes BTC-USD) can enter the model",
    )
    p.add_argument(
        "--horizon",
        type=int,
        default=5,
        choices=HORIZONS,
        help="forward horizon in bars (default 5)",
    )
    p.add_argument(
        "--cache",
        default=".feature_cache",
        help="local directory for per-date feature Parquet (default .feature_cache)",
    )
    p.add_argument(
        "--refresh",
        action="store_true",
        help="rebuild features even when a cache entry matches (the mtime key already "
        "invalidates on a silver rebuild; this forces it)",
    )
    p.add_argument(
        "--max-age-ms",
        type=int,
        default=MAX_AGE_MS,
        help=f"drop grid rows where any venue book is staler than this (default {MAX_AGE_MS}); "
        "tighten it to test whether an edge is real or just a lagging book catching up",
    )
    p.add_argument(
        "--predict-venue",
        default=None,
        help="cross-venue mode: predict this venue's own forward mid from the OTHER "
        "venues' features only (no NBBO), the lead-lag test with no shared construction",
    )
    p.add_argument(
        "--min-train",
        type=int,
        default=MIN_TRAIN_DATES,
        help=f"dates in the first fold's training window (default {MIN_TRAIN_DATES})",
    )
    p.add_argument(
        "--test-size",
        type=int,
        default=TEST_DATES,
        help=f"dates tested per fold (default {TEST_DATES})",
    )
    p.add_argument(
        "--step",
        type=int,
        default=STEP_DATES,
        help=f"dates the window advances per fold (default {STEP_DATES}); keep it >= "
        "--test-size or folds overlap and the bootstrap double-counts dates",
    )
    p.add_argument(
        "--n-boot",
        type=int,
        default=N_BOOT,
        help=f"date-block bootstrap resamples for the CIs (default {N_BOOT})",
    )
    p.add_argument(
        "--placebo",
        action="store_true",
        help=f"leak check: displace the target {PLACEBO_LAG_BARS} bars within each date "
        "and refit. A clean pipeline reports R2 ~0 and hit ~0.50; anything better is "
        "lookahead. Re-run after every feature-set change",
    )
    p.add_argument(
        "--infoshare",
        nargs=2,
        default=None,
        metavar="VENUE",
        help="also estimate Hasbrouck information shares and Gonzalo-Granger component "
        "shares for this venue pair. The model answers which venue's book forecasts "
        "which, which cannot separate a venue that discovers price from one that "
        "requotes slowly; this answers which venue the other error-corrects toward",
    )
    p.add_argument(
        "--infoshare-stride",
        nargs="+",
        type=int,
        default=[1],
        metavar="N",
        help="sample every Nth bar for --infoshare (default 1). Information shares depend "
        "on sampling frequency - coarse enough and every venue looks simultaneous - so "
        "pass several to report the answer as a function of it",
    )
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    args = parse_args()
    run(
        args.dates,
        args.symbol,
        args.horizon,
        Path(args.cache),
        args.refresh,
        args.predict_venue,
        args.max_age_ms,
        args.min_train,
        args.test_size,
        args.step,
        args.n_boot,
        args.placebo,
        tuple(args.extra_symbols),
        tuple(args.infoshare) if args.infoshare else None,
        tuple(args.infoshare_stride),
    )


if __name__ == "__main__":
    main()
