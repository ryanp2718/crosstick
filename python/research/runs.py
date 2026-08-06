"""Every run, written down: what was asked for, what produced it, what came out.

Results were `print()` and nothing else. Nothing returned a value, so no number could be
cited, compared against last week's, or handed to anything that was not a terminal, and
the only record of a run was whatever got pasted into a note. That is fine right up to
the moment two runs disagree, which is when the questions are all provenance questions:
same dates, same silver vintage, same feature builder, same sklearn?

So a run now produces a `RunRecord`, and the report functions format the record rather
than compute from the fold sequence. Two consequences worth naming. Every confidence
interval is computed before anything is printed, where they used to be interleaved with
the output, so the tables all appear at once at the end. And the record is complete by
construction: a number that reaches stdout without being in the record cannot exist,
because stdout is rendered from the record.

Two files per run, and the split is deliberate:

  record.json   the whole run except the rows. Small enough to read, diff and paste.
  oos.parquet   the pooled out-of-sample rows, one per non-overlapping test bar.

`oos.parquet` is the artifact worth keeping. Every headline number in the writeup is
recomputable from it with no lake, no silver and no feature cache: R2 with date-block
intervals, hit rate, dead-zone precision, the whole selectivity curve. Fold metrics are
NOT given a second Parquet file, because four folds of six metrics is ninety-six numbers
and they are more useful sitting in the JSON next to the spec that produced them.

Non-finite floats are written as JSON null rather than the `NaN` token Python emits by
default, which is not valid JSON and which every non-Python reader rejects. A metric can
legitimately be undefined here (`hit_rate` for a model that never calls a direction), so
this is a routine case, not an error path.
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from hashlib import blake2b
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import numpy as np
import polars as pl

from research.infoshare import InfoShare
from research.model import Split, dead_zone_classes
from research.schema import FeatureSchema
from research.validation import (
    MIN_BOOTSTRAP_BLOCKS,
    OutOfSample,
    WalkForward,
    as_classes,
    as_classes_at_coverage,
    class_confidence_intervals,
    confidence_intervals,
    coverage_confidence_intervals,
)

log = logging.getLogger(__name__)

# (point estimate, lower, upper) at 95%.
Interval = tuple[float, float, float]

# Versions that can move a number. sklearn fits the tree and numpy drives both the
# bootstrap rng and the linear algebra; polars and pyarrow decide what the frame holds.
PACKAGES = ("scikit-learn", "numpy", "polars", "pyarrow")

REPO = Path(__file__).resolve().parents[2]
RUNS = Path(__file__).resolve().parents[1] / "runs"


@dataclass(frozen=True)
class RunSpec:
    """What the run was asked to do.

    Flat and explicit rather than a reference to the module globals it defaulted from,
    because the globals move and a record has to stay readable after they do.
    """

    dates: tuple[str, ...]
    symbol: str
    extra_symbols: tuple[str, ...]
    horizon: int
    target: str
    spread_col: str
    predict_venue: str | None
    max_age_ms: int
    min_train: int
    test_size: int
    step: int
    n_boot: int
    placebo: bool
    placebo_lag_bars: int
    coverages: tuple[float, ...]
    dead_zone_spreads: float
    infoshare_venues: tuple[str, str] | None = None
    infoshare_strides: tuple[int, ...] = ()


@dataclass(frozen=True)
class Provenance:
    """What produced the numbers, at the three layers that can independently change them.

    The data (`source_fingerprints`, one silver mtime key per date), the code that turned
    it into features (`builder_fingerprint`, `feature_schema_digest`, `git_sha`), and the
    libraries that fitted it. Two runs whose records differ in none of these and whose
    numbers differ anyway are a determinism bug, which is the point of recording them.
    """

    created_at: str
    git_sha: str
    git_dirty: bool
    builder_fingerprint: str
    feature_schema_digest: str
    source_fingerprints: dict[str, str]
    packages: dict[str, str]


@dataclass(frozen=True)
class Fold:
    """One fold, projected down from `Split`.

    `Split` carries the fitted matrices, which for this geometry is over a million rows
    by 27 features per fold. None of that belongs in a record: the rows are already in
    `oos.parquet` and the training matrix is reproducible from the spec.

    `edge` and `classes` were computed per fold and discarded before this existed, so
    `n_trades` and the per-fold support counts were paid for and never seen.
    """

    train_dates: tuple[str, ...]
    test_dates: tuple[str, ...]
    n_train: int
    n_val: int
    n_test: int
    metrics: dict[str, dict[str, float]]
    edge: dict[str, dict[str, float]]
    classes: dict[str, dict[str, float]]


@dataclass(frozen=True)
class Selectivity:
    """Precision at one forced traded share."""

    coverage: float
    n_called: int
    metrics: dict[str, Interval]


@dataclass(frozen=True)
class InfoShares:
    """One venue pair's information shares at one sampling frequency.

    Carried per stride rather than as a single number because the measure depends on it:
    sampled coarsely enough every venue looks simultaneous and the shares converge on the
    residual correlation, so a share quoted without its stride is not an answer.
    """

    venues: tuple[str, str]
    stride: int
    lags: int
    estimates: list[InfoShare]


@dataclass
class RunRecord:
    """One run, whole. Everything the reports print is read from here."""

    spec: RunSpec
    provenance: Provenance
    venues: tuple[str, ...]
    feature_names: tuple[str, ...]
    n_rows: int
    n_rows_before: int
    n_oos: int
    n_test_dates: int
    coverage_gaps: dict[str, dict[str, int]]
    folds: list[Fold]
    pooled: dict[str, dict[str, Interval]]
    classes: dict[str, dict[str, Interval]]
    traded: dict[str, float]
    base_rates: dict[str, float]
    selectivity: list[Selectivity]
    importance: list[dict[str, float]]
    # The second analysis, when asked for: which venue discovers price rather than which
    # venue forecasts which (see research.infoshare).
    infoshare: list[InfoShares] = field(default_factory=list)
    # Not serialised into record.json; written as oos.parquet.
    oos: dict[str, OutOfSample] = field(default_factory=dict, repr=False)

    @property
    def bootstrap_is_degenerate(self) -> bool:
        """Too few date blocks for the interval to mean anything (see MIN_BOOTSTRAP_BLOCKS)."""
        return self.n_test_dates < MIN_BOOTSTRAP_BLOCKS

    def write(self, root: Path = RUNS) -> Path:
        """One directory per run, named so `ls` sorts chronologically."""
        stamp = self.provenance.created_at.replace(":", "").replace("-", "")[:15]
        out = root / f"{stamp}-{self.provenance.git_sha or 'nogit'}"
        out.mkdir(parents=True, exist_ok=True)
        (out / "record.json").write_text(
            json.dumps(_plain(asdict(self)), indent=2, allow_nan=False) + "\n", encoding="utf-8"
        )
        oos_frame(self.oos).write_parquet(out / "oos.parquet")
        return out

    @classmethod
    def read(cls, run: Path) -> RunRecord:
        """The inverse of `write`, for a reader that is not this repo.

        JSON has neither tuples nor NaN, so `write` loses both and this puts them back.
        That is not cosmetic: a metric that was undefined has to come back undefined
        rather than as a null, or it formats as a number that was never measured.

        Every field is indexed rather than `.get()`-ed, so a record written by a version
        that did not have one fails here loudly instead of reporting a default.
        """
        raw = json.loads((run / "record.json").read_text(encoding="utf-8"))
        pair = raw["spec"]["infoshare_venues"]
        spec = raw["spec"] | {
            "dates": tuple(raw["spec"]["dates"]),
            "extra_symbols": tuple(raw["spec"]["extra_symbols"]),
            "coverages": tuple(raw["spec"]["coverages"]),
            "infoshare_venues": tuple(pair) if pair else None,
            "infoshare_strides": tuple(raw["spec"]["infoshare_strides"]),
        }
        return cls(
            spec=RunSpec(**spec),
            provenance=Provenance(**raw["provenance"]),
            venues=tuple(raw["venues"]),
            feature_names=tuple(raw["feature_names"]),
            n_rows=raw["n_rows"],
            n_rows_before=raw["n_rows_before"],
            n_oos=raw["n_oos"],
            n_test_dates=raw["n_test_dates"],
            coverage_gaps=raw["coverage_gaps"],
            folds=[
                Fold(
                    train_dates=tuple(fold["train_dates"]),
                    test_dates=tuple(fold["test_dates"]),
                    n_train=fold["n_train"],
                    n_val=fold["n_val"],
                    n_test=fold["n_test"],
                    metrics=_numbers(fold["metrics"]),
                    edge=_numbers(fold["edge"]),
                    classes=_numbers(fold["classes"]),
                )
                for fold in raw["folds"]
            ],
            pooled=_interval_table(raw["pooled"]),
            classes=_interval_table(raw["classes"]),
            traded={k: _number(v) for k, v in raw["traded"].items()},
            base_rates={k: _number(v) for k, v in raw["base_rates"].items()},
            selectivity=[
                Selectivity(
                    coverage=sel["coverage"],
                    n_called=sel["n_called"],
                    metrics=_intervals(sel["metrics"]),
                )
                for sel in raw["selectivity"]
            ],
            importance=[{k: _number(v) for k, v in each.items()} for each in raw["importance"]],
            infoshare=[
                InfoShares(
                    venues=tuple(block["venues"]),
                    stride=block["stride"],
                    lags=block["lags"],
                    estimates=[_info_share(e) for e in block["estimates"]],
                )
                for block in raw["infoshare"]
            ],
            oos=read_oos(pl.read_parquet(run / "oos.parquet")),
        )


def _number(value) -> float:
    """A JSON null back to the NaN it was written from (see `_plain`)."""
    return float("nan") if value is None else float(value)


def _numbers(table: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    return {name: {k: _number(v) for k, v in row.items()} for name, row in table.items()}


def _intervals(row: dict[str, list]) -> dict[str, Interval]:
    return {k: tuple(_number(v) for v in interval) for k, interval in row.items()}


def _interval_table(table: dict[str, dict[str, list]]) -> dict[str, dict[str, Interval]]:
    return {name: _intervals(row) for name, row in table.items()}


def _info_share(raw: dict) -> InfoShare:
    """`np.array(..., dtype=float)` is what turns the JSON nulls back into NaN."""
    return InfoShare(
        venues=tuple(raw["venues"]),
        n=raw["n"],
        alpha=np.array(raw["alpha"], dtype=float),
        component_shares=np.array(raw["component_shares"], dtype=float),
        bounds=np.array(raw["bounds"], dtype=float),
        residual_corr=_number(raw["residual_corr"]),
        conditioning=_number(raw["conditioning"]),
        date=raw["date"],
    )


def _plain(value):
    """Dataclass tree to something `json.dumps(allow_nan=False)` accepts."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, np.generic):
        return _plain(value.item())
    if isinstance(value, np.ndarray):
        return _plain(value.tolist())
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items() if k != "oos"}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def oos_frame(oos: dict[str, OutOfSample]) -> pl.DataFrame:
    """The pooled rows, one row per test bar and one prediction column per model.

    Wide rather than long. Every model was scored on the same strided rows of the same
    folds, so a long frame would repeat `date`, `y` and `threshold` once per model for no
    gain, and quadruple a file whose whole purpose is to be small enough to commit.
    """
    if not oos:
        return pl.DataFrame()
    first = next(iter(oos.values()))
    for name, other in oos.items():
        if not np.array_equal(other.dates, first.dates) or not np.array_equal(other.y, first.y):
            raise ValueError(f"model {name!r} was scored on different rows than the rest")
    columns = {
        "date": first.dates,
        "y": first.y,
        **({"threshold": first.threshold} if first.threshold is not None else {}),
        **{f"pred_{name}": o.pred for name, o in oos.items()},
    }
    return pl.DataFrame(columns)


def read_oos(frame: pl.DataFrame) -> dict[str, OutOfSample]:
    """The pooled rows back into one `OutOfSample` per model.

    `date`, `y` and `threshold` are shared across models by construction (`oos_frame`
    refuses to write a frame where they are not), so every model gets the same arrays
    rather than a copy each.
    """
    if "date" not in frame.columns:
        return {}
    dates, y = frame["date"].to_numpy(), frame["y"].to_numpy()
    threshold = frame["threshold"].to_numpy() if "threshold" in frame.columns else None
    return {
        column.removeprefix("pred_"): OutOfSample(
            dates=dates, y=y, pred=frame[column].to_numpy(), threshold=threshold
        )
        for column in frame.columns
        if column.startswith("pred_")
    }


def provenance(builder_fingerprint: str, schema: FeatureSchema, sources: dict[str, str]):
    return Provenance(
        created_at=datetime.now(UTC).isoformat(timespec="seconds"),
        git_sha=_git("rev-parse", "--short", "HEAD"),
        git_dirty=bool(_git("status", "--porcelain")),
        builder_fingerprint=builder_fingerprint,
        feature_schema_digest=schema_digest(schema),
        source_fingerprints=dict(sources),
        packages={name: _version(name) for name in PACKAGES},
    )


def schema_digest(schema: FeatureSchema) -> str:
    """Digest of the declared column set.

    Distinct from `builder_fingerprint`, which hashes the builder's source and so changes
    on a moved comment. This changes only when the columns do, which is what makes it the
    right thing to compare two runs on.
    """
    return blake2b("\n".join(sorted(schema.names)).encode(), digest_size=4).hexdigest()


def _git(*args: str) -> str:
    try:
        done = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        return ""
    return done.stdout.strip()


def _version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return ""


def build(
    spec: RunSpec,
    provenance: Provenance,
    wf: WalkForward,
    schema: FeatureSchema,
    n_rows: int,
    n_rows_before: int,
    coverage_gaps: dict[str, dict[str, int]],
    infoshare: list[InfoShares] | None = None,
) -> RunRecord:
    """Everything the fold sequence produced, scored and intervalled.

    All the bootstrapping happens here rather than inside the report functions, so the
    record is complete before a line is printed and cannot drift from what was shown.
    """
    n_boot = spec.n_boot
    gbt = wf.oos.get("gbt")
    dead_zone = gbt is not None and gbt.threshold is not None

    classes: dict[str, dict[str, Interval]] = {}
    traded: dict[str, float] = {}
    selectivity: list[Selectivity] = []
    if dead_zone:
        for name, oos in wf.oos.items():
            classes[name] = class_confidence_intervals(oos, n_boot)
            traded[name] = float(np.mean(as_classes(oos).pred != 0))
        for coverage in spec.coverages:
            called = as_classes_at_coverage(gbt, coverage).pred
            selectivity.append(
                Selectivity(
                    coverage=coverage,
                    n_called=int(np.sum(called != 0)),
                    metrics=coverage_confidence_intervals(gbt, coverage, n_boot),
                )
            )

    any_oos = next(iter(wf.oos.values()))
    return RunRecord(
        spec=spec,
        provenance=provenance,
        venues=tuple(schema.venues),
        feature_names=_feature_names(wf.splits),
        n_rows=n_rows,
        n_rows_before=n_rows_before,
        n_oos=len(any_oos.y),
        n_test_dates=len(np.unique(any_oos.dates)),
        coverage_gaps=coverage_gaps,
        folds=_folds(wf),
        pooled={name: confidence_intervals(oos, n_boot) for name, oos in wf.oos.items()},
        classes=classes,
        traded=traded,
        base_rates=_base_rates(gbt) if dead_zone else {},
        selectivity=selectivity,
        importance=list(wf.importance),
        infoshare=list(infoshare or ()),
        oos=dict(wf.oos),
    )


def _feature_names(splits: list[Split]) -> tuple[str, ...]:
    """The fitted columns, which every fold shares.

    `split_on_dates` selects them from the whole frame rather than the fold's slice, so a
    disagreement here means a fold saw a different feature set and the pooled series is
    not one series.
    """
    names = {tuple(s.feature_names) for s in splits}
    if len(names) > 1:
        raise ValueError(f"folds disagree on the feature set: {len(names)} distinct lists")
    return names.pop() if names else ()


def _folds(wf: WalkForward) -> list[Fold]:
    return [
        Fold(
            train_dates=tuple(split.train_dates),
            test_dates=tuple(split.test_dates),
            n_train=len(split.y_train),
            n_val=len(split.y_val),
            n_test=len(split.y_test),
            metrics=metrics,
            edge=edge,
            classes=classes,
        )
        for split, metrics, edge, classes in zip(
            wf.splits, wf.fold_metrics, wf.fold_edge, wf.fold_classes, strict=True
        )
    ]


def _base_rates(gbt: OutOfSample) -> dict[str, float]:
    """Share of pooled bars that really moved more than half a spread, by direction.

    The ceiling on recall, and the floor a directional call has to clear to carry any
    information at all. Not the null for precision, though: see `_report_selectivity`.
    """
    truth = dead_zone_classes(gbt.y, gbt.threshold)
    return {"up": float(np.mean(truth == 1)), "down": float(np.mean(truth == -1))}
