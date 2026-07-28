"""Research entrypoint: ``python -m research.main --symbol BTC-USD <date> [<date> ...]``.

Builds the PIT feature matrix per date (cached to local Parquet, since a date costs
a full silver read), optionally joining further canonical symbols as extra venue legs
(``--extra-symbols BTC-USDT BTC-USDT-PERP``), concatenates, then walks an expanding
window forward over the
dates: each fold trains only on its own past, and the folds' predictions pool into one
out-of-sample series that gets bootstrapped by date for confidence intervals. Prints
the model comparison and per-venue permutation importance - the price-discovery answer,
with the error bars that say how much of it to believe.

Env matches the batch layers: base ``S3_*`` is the silver endpoint, ``SILVER_BUCKET``
(default ``silver``). Reads silver only; bronze is never touched.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import numpy as np
import polars as pl

from common.lake import filesystem_from_env, list_partitions, partition_key
from research.features import HORIZONS, SOURCE_DATASETS, build_features
from research.model import MAX_AGE_MS, clean, venue_prefixes
from research.validation import (
    MIN_BOOTSTRAP_BLOCKS,
    MIN_TRAIN_DATES,
    N_BOOT,
    PLACEBO_LAG_BARS,
    STEP_DATES,
    TEST_DATES,
    WalkForward,
    confidence_intervals,
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


def cache_name(date: str, symbol: str, extra_symbols: tuple[str, ...], fingerprint: str) -> str:
    """Cache filename for one date's feature matrix.

    The extra symbols are part of the identity, not just the mtime: the same date built
    with and without Binance legs are different matrices, and a key that ignored them
    would serve a two-venue frame to a four-venue run.
    """
    legs = "+".join(("", *extra_symbols))
    return f"{symbol}{legs}_{date}_{fingerprint}.parquet"


def cached_features(
    fs,
    bucket: str,
    date: str,
    symbol: str,
    cache: Path,
    refresh: bool = False,
    extra_symbols: tuple[str, ...] = (),
) -> pl.DataFrame | None:
    """Feature matrix for one date, materialised locally on first build.

    A date is a whole silver read plus the joins; caching makes iterating on the model
    cheap without re-deriving features from silver that has not changed.
    """
    all_symbols = (symbol, *extra_symbols)
    fingerprint = source_fingerprint(fs, bucket, date, all_symbols)
    path = cache / cache_name(date, symbol, extra_symbols, fingerprint)
    if path.exists() and not refresh:
        return pl.read_parquet(path)
    df = build_features(fs, bucket, date, symbol, extra_symbols)
    if df is None:
        return None
    df = df.with_columns(pl.lit(date).alias("date"))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return df


def _report_folds(wf: WalkForward) -> None:
    """Per-fold scores. The point of printing every fold rather than only the mean is
    that a result driven by one unusual week is visible here and nowhere else."""
    print(f"\nper fold ({len(wf.splits)} folds, expanding window)")
    print(f"{'fold':>4}  {'train':<24} {'test':<24} {'n':>7} {'gbt R2':>10} {'gbt hit':>9}")
    print("-" * 84)
    for i, (split, metrics) in enumerate(zip(wf.splits, wf.fold_metrics, strict=True), 1):
        gbt = metrics["gbt"]
        train = f"{split.train_dates[0]}..{split.train_dates[-1]}"
        test = f"{split.test_dates[0]}..{split.test_dates[-1]}"
        print(
            f"{i:>4}  {train:<24} {test:<24} {gbt['n']:>7.0f} "
            f"{gbt['r2_vs_zero']:>10.5f} {gbt['hit_rate']:>9.4f}"
        )


def _report_spread(wf: WalkForward) -> None:
    print("\nacross folds (mean +/- sd)")
    print(f"{'model':<12} {'R2 vs zero':>22} {'hit rate':>22}")
    print("-" * 58)
    for name in wf.oos:
        r2, r2_sd = fold_spread(wf.fold_metrics, name, "r2_vs_zero")
        hit, hit_sd = fold_spread(wf.fold_metrics, name, "hit_rate")
        print(f"{name:<12} {r2:>12.5f} +/- {r2_sd:<6.5f} {hit:>12.4f} +/- {hit_sd:<6.4f}")


def _report_pooled(wf: WalkForward, n_boot: int) -> None:
    """The headline, with the interval attached to it.

    Every row here was predicted out of sample by a model that had only seen earlier
    dates, and the interval comes from resampling whole days - so it reflects the fact
    that this capture holds a couple of dozen days, not a couple of dozen thousand
    independent observations.
    """
    n_dates = len(np.unique(wf.oos["gbt"].dates))
    print(
        f"\npooled out-of-sample: {n_dates} test dates, 95% CI from {n_boot} date-block resamples"
    )
    if n_dates < MIN_BOOTSTRAP_BLOCKS:
        print(
            f"  WARNING: {n_dates} blocks is too few to resample. The interval below is\n"
            f"  degenerate - it collapses onto the per-fold values and understates the\n"
            f"  uncertainty. Do not quote it; it needs >= {MIN_BOOTSTRAP_BLOCKS} test dates."
        )
    print(f"{'model':<12} {'n':>7} {'R2 vs zero':>26} {'hit rate':>24} {'bps/trade':>24}")
    print("-" * 96)
    for name, oos in wf.oos.items():
        ci = confidence_intervals(oos, n_boot)
        cells = []
        for key, width in (("r2_vs_zero", 5), ("hit_rate", 4), ("gross_bps_per_trade", 4)):
            point, lo, hi = ci[key]
            cells.append(f"{point:>9.{width}f} [{lo:>7.{width}f},{hi:>8.{width}f}]")
        print(f"{name:<12} {len(oos.y):>7} " + " ".join(cells))
    print(
        "\n  bps/trade is the gross edge, which IS the break-even round-trip cost: fills at\n"
        "  mid, no slippage, queue or latency. A retail taker round trip on Coinbase/Kraken\n"
        "  is tens of bps and the BTC-USD touch spread alone is ~1-2 bps crossed twice, so\n"
        "  these are upper bounds to compare against a cost, not a P&L."
    )


def _report_importance(wf: WalkForward, venues: list[str]) -> None:
    """Per-venue permutation importance, averaged over folds with its spread.

    A venue that leads only in some folds shows up here as a large sd next to its mean,
    which is the difference between a structural result and a lucky week.
    """
    if not wf.importance:
        return
    means = {v: float(np.mean([f[v] for f in wf.importance])) for v in venues}
    sds = {v: float(np.std([f[v] for f in wf.importance])) for v in venues}
    total = sum(m for m in means.values() if m > 0)
    print("\nper-venue permutation importance (gbt, mean +/- sd over folds)")
    for venue, mean in sorted(means.items(), key=lambda kv: -kv[1]):
        share = f"{mean / total * 100:>5.1f}%" if total > 0 and mean > 0 else "    -"
        print(f"  {venue:<20} {mean:>10.5f} +/- {sds[venue]:<9.5f} {share}")


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
) -> None:
    fs = filesystem_from_env()
    bucket = os.environ.get("SILVER_BUCKET", "silver")

    frames = []
    for date in dates:
        df = cached_features(fs, bucket, date, symbol, cache, refresh, extra_symbols)
        if df is None:
            log.warning("skipping %s (no features)", date)
            continue
        frames.append(df)
        log.info("%s: %d rows", date, df.height)
    if not frames:
        print("no feature rows built")
        return

    df = pl.concat(frames, how="diagonal").sort("ts_ns")
    venues = venue_prefixes(df)
    if extra_symbols:
        print(f"\nlegs: {symbol} + {list(extra_symbols)} -> venues {venues}")
    if predict_venue:
        if predict_venue not in venues:
            print(f"unknown venue {predict_venue!r}; have {venues}")
            return
        target = f"y_{predict_venue}_ret_bps_{horizon}"
        print(
            f"\ncross-venue mode: predicting {predict_venue}'s own forward mid using only "
            f"{[v for v in venues if v != predict_venue]} (no NBBO, no {predict_venue} features)"
        )
    else:
        target = f"y_ret_bps_{horizon}"
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

    splits = walk_forward(df, target, horizon, min_train, test_size, step, predict_venue)
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
    _report_folds(wf)
    _report_spread(wf)
    _report_pooled(wf, n_boot)
    _report_importance(wf, venues)


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
    )


if __name__ == "__main__":
    main()
