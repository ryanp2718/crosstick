"""Pure builders that turn lake state into Prometheus metric families.

Split from the serving/refresh loop (`service.py`) so the row->metric mapping is
unit-testable with plain dicts and the I/O is integration-tested over MinIO. This
exporter only *reads* the already-derived gold rollups + lists object metadata —
it computes nothing new (a gold mart reads silver, never bronze; this reads gold).

Metric taxonomy (domain-prefixed, matching `md_*` / `bronze_*` / `gateway_*`):
  - lake_freshness_seconds{layer,dataset}   age of the newest object per dataset
  - gold_dq_violations{check,exchange,symbol}
  - gold_dq_records{check,exchange,symbol}
  - gold_dq_latency_ms{exchange,symbol,quantile}
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
        labels=["exchange", "symbol", "quantile"],
    )
    clk = GaugeMetricFamily(
        "gold_dq_clock_worst_ms", "Worst intra-epoch backward recv-clock step (ms)",
        labels=["exchange", "symbol"],
    )
    for r in rows:
        ex, sym, chk = r["exchange"], _sym(r["canonical_symbol"]), r["check"]
        viol.add_metric([chk, ex, sym], float(r["n_violations"]))
        rec.add_metric([chk, ex, sym], float(r["n_records"]))
        if chk == "latency":
            for q in ("p50", "p95", "p99"):
                v = r.get(f"{q}_ms")
                if v is not None:
                    lat.add_metric([ex, sym, q], float(v))
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
def _newest_per_dataset(fs: pafs.FileSystem, bucket: str) -> dict[str, float]:
    """Newest object mtime (epoch s) per dataset in a bucket. One recursive list;
    fine at dev scale — at cloud scale move to a prefix-scoped / slower cadence.

    The dataset is the first path segment after the bucket prefix — derived by
    stripping the prefix (not positional indexing) so it holds for an S3 bucket
    (one segment) and a LocalFileSystem path (many) alike."""
    sel = pafs.FileSelector(bucket, recursive=True, allow_not_found=True)
    prefix = bucket.replace("\\", "/").rstrip("/") + "/"
    out: dict[str, float] = {}
    for info in fs.get_file_info(sel):
        if info.type != pafs.FileType.File or not info.path.endswith(".parquet"):
            continue
        rel = info.path.replace("\\", "/")
        rel = rel[len(prefix):] if rel.startswith(prefix) else rel
        dataset = rel.split("/", 1)[0]
        mtime = info.mtime.timestamp() if info.mtime is not None else 0.0
        if mtime > out.get(dataset, 0.0):
            out[dataset] = mtime
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
    fs: pafs.FileSystem, lake_bucket: str, silver_bucket: str, gold_bucket: str, now_s: float
) -> list[GaugeMetricFamily]:
    layers = ((lake_bucket, "bronze"), (silver_bucket, "silver"), (gold_bucket, "gold"))
    by_layer = {layer: _newest_per_dataset(fs, bucket) for bucket, layer in layers}
    sc_rows, sc_date = _gold_rows(fs, gold_bucket, "scorecard")
    basis_rows, _ = _gold_rows(fs, gold_bucket, "basis_summary")
    return [
        *freshness_families(by_layer, now_s),
        *scorecard_families(sc_rows, sc_date),
        *basis_families(basis_rows),
    ]
