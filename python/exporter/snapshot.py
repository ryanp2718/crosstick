"""Pure builders that turn lake state into Prometheus metric families.

Split from the serving/refresh loop (`service.py`) so the row->metric mapping is
unit-testable with plain dicts and the I/O is integration-tested over MinIO. This
exporter only *reads* the already-derived gold rollups + lists object metadata —
it computes nothing new (a gold mart reads silver, never bronze; this reads gold).

Metric taxonomy (domain-prefixed, matching `md_*` / `bronze_*` / `gateway_*`):
  - lake_freshness_seconds{layer,dataset}   age of the newest object per dataset
  - gold_dq_violations{check,exchange,symbol}
  - gold_dq_records{check,exchange,symbol}
  - gold_dq_latency_ms{exchange,symbol,dataset,quantile}
  - gold_dq_clock_worst_ms{exchange,symbol} worst intra-epoch backward recv step
  - gold_scorecard_date_seconds             UTC midnight of the reported date
  - gold_basis_bps{base,stat}  /  gold_basis_obs{base}
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from prometheus_client.core import GaugeMetricFamily
from pyarrow import fs as pafs

from common.lake import latest_date, read_dataset

log = logging.getLogger(__name__)


def _sym(value: str | None) -> str:
    # canonical_symbol is None for venue-wide checks (status uptime/downtime).
    return value or ""


def _date_epoch(date: str) -> float:
    return datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()


# ── pure mappers (rows/metadata -> families) ──────────────────────────────────
def freshness_families(
    by_layer: dict[str, dict[str, float]], now_s: float
) -> list[GaugeMetricFamily]:
    fam = GaugeMetricFamily(
        "lake_freshness_seconds",
        "Seconds since the newest object was written, per layer+dataset",
        labels=["layer", "dataset"],
    )
    for layer, datasets in by_layer.items():
        for dataset, mtime in sorted(datasets.items()):
            fam.add_metric([layer, dataset], max(0.0, now_s - mtime))
    return [fam]


def scorecard_families(rows: list[dict], date: str | None) -> list[GaugeMetricFamily]:
    viol = GaugeMetricFamily(
        "gold_dq_violations", "Gold scorecard violations per check",
        labels=["check", "exchange", "symbol"],
    )
    rec = GaugeMetricFamily(
        "gold_dq_records", "Records evaluated per scorecard check",
        labels=["check", "exchange", "symbol"],
    )
    lat = GaugeMetricFamily(
        "gold_dq_latency_ms", "Exchange->recv latency percentiles from the gold scorecard",
        labels=["exchange", "symbol", "dataset", "quantile"],
    )
    clk = GaugeMetricFamily(
        "gold_dq_clock_worst_ms", "Worst intra-epoch backward recv-clock step (ms)",
        labels=["exchange", "symbol"],
    )
    for r in rows:
        ex, sym, chk = r["exchange"], _sym(r["canonical_symbol"]), r["check"]
        viol.add_metric([chk, ex, sym], float(r["n_violations"]))
        rec.add_metric([chk, ex, sym], float(r["n_records"]))
        if chk.startswith("latency."):
            dataset = chk[len("latency."):]
            for q in ("p50", "p95", "p99"):
                v = r.get(f"{q}_ms")
                if v is not None:
                    lat.add_metric([ex, sym, dataset, q], float(v))
        if chk == "clock_monotonic" and r.get("detail"):
            try:
                worst = json.loads(r["detail"]).get("worst_lateness_ms")
            except (TypeError, ValueError):
                worst = None
            if worst is not None:
                clk.add_metric([ex, sym], float(worst))
    fams = [viol, rec, lat, clk]
    if date:
        sd = GaugeMetricFamily(
            "gold_scorecard_date_seconds",
            "UTC midnight of the date the gold DQ gauges describe",
        )
        sd.add_metric([], _date_epoch(date))
        fams.append(sd)
    return fams


def basis_families(rows: list[dict]) -> list[GaugeMetricFamily]:
    bps = GaugeMetricFamily(
        "gold_basis_bps", "Daily stablecoin basis (bps) per base",
        labels=["base", "stat"],
    )
    obs = GaugeMetricFamily(
        "gold_basis_obs", "Basis observations behind the daily summary", labels=["base"],
    )
    for r in rows:
        for stat in ("mean", "std", "min", "max"):
            v = r.get(f"basis_bps_{stat}")
            if v is not None:
                bps.add_metric([r["base"], stat], float(v))
        obs.add_metric([r["base"]], float(r["n_obs"]))
    return [bps, obs]


# ── I/O orchestration ─────────────────────────────────────────────────────────
def _basename(path: str) -> str:
    return path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


def _newest_parquet_mtime(fs: pafs.FileSystem, directory: str) -> float:
    """Newest .parquet mtime (epoch s) under `directory`, descending only the
    greatest `date=` partition per branch rather than the whole subtree.

    Dates are ISO so lexical-max is newest; the other dims (exchange=/symbol=) fan
    out but are few, so cost tracks one day of data per branch, not the full
    retained history. A newer mtime backfilled into an older date is not reflected,
    the right trade for an "is data still landing" gauge.
    """
    infos = fs.get_file_info(pafs.FileSelector(directory, allow_not_found=True))
    files = (i for i in infos if i.type == pafs.FileType.File and i.path.endswith(".parquet"))
    newest = max((i.mtime.timestamp() for i in files if i.mtime is not None), default=0.0)

    subdirs = [i for i in infos if i.type == pafs.FileType.Directory]
    dates = [d for d in subdirs if _basename(d.path).startswith("date=")]
    branches = [d for d in subdirs if not _basename(d.path).startswith("date=")]
    if dates:
        branches.append(max(dates, key=lambda d: _basename(d.path)))
    for d in branches:
        newest = max(newest, _newest_parquet_mtime(fs, d.path))
    return newest


def _newest_per_dataset(fs: pafs.FileSystem, bucket: str) -> dict[str, float]:
    """Newest object mtime (epoch s) per top-level dataset in a bucket.

    A non-recursive list names the datasets; each is walked date-pruned
    (`_newest_parquet_mtime`) so cost tracks recent data, not the whole bucket,
    which is what stalled a full recursive walk once the lake grew large.
    """
    out: dict[str, float] = {}
    for info in fs.get_file_info(pafs.FileSelector(bucket, allow_not_found=True)):
        if info.type != pafs.FileType.Directory:
            continue
        mtime = _newest_parquet_mtime(fs, info.path)
        if mtime > 0.0:
            out[_basename(info.path)] = mtime
    return out


def _gold_rows(
    fs: pafs.FileSystem, gold_bucket: str, dataset: str
) -> tuple[list[dict], str | None]:
    date = latest_date(fs, gold_bucket, dataset)
    if not date:
        return [], None
    table = read_dataset(fs, gold_bucket, dataset, date)
    return (table.to_pylist() if table is not None else []), date


def build_families(
    bronze_fs: pafs.FileSystem, derived_fs: pafs.FileSystem,
    lake_bucket: str, silver_bucket: str, gold_bucket: str, now_s: float
) -> list[GaugeMetricFamily]:
    # Bronze lives on its own endpoint (bronze_fs); silver/gold and the gold rollups
    # are the derived layers (derived_fs). Equal filesystems when unsplit.
    by_layer = {
        "bronze": _newest_per_dataset(bronze_fs, lake_bucket),
        "silver": _newest_per_dataset(derived_fs, silver_bucket),
        "gold": _newest_per_dataset(derived_fs, gold_bucket),
    }
    sc_rows, sc_date = _gold_rows(derived_fs, gold_bucket, "scorecard")
    basis_rows, _ = _gold_rows(derived_fs, gold_bucket, "basis_summary")
    return [
        *freshness_families(by_layer, now_s),
        *scorecard_families(sc_rows, sc_date),
        *basis_families(basis_rows),
    ]
