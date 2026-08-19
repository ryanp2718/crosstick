"""Bundle a run and its placebo into the layout the notebook repo reads.

The notebook's whole claim is that a stranger with no lake, no Docker and no feature
cache can rerun every headline number. That works because `oos.parquet` is sufficient
(see `research.runs`), so publishing is mostly a copy. What it is not is `cp`, for two
reasons, and they are the reason this module exists.

A placebo only nulls out the run it is paired with if it is the same experiment: same
dates, same horizon, same target, same venue, same feature builder. A placebo run over a
different date range is a different question with a reassuring answer, and once the two
files are sitting side by side in a repo called `data/` nothing downstream can tell. So
the pair is checked here, once, at the moment the two stop being traceable to the runs
that produced them.

The reverse direction is held to the same standard for the same reason. Its whole job is
to be the run that differs from the real one in direction and nothing else, so that a gap
between them cannot be explained by a different window or a different builder.

And a record written from a dirty tree does not have provenance: `git_sha` names a commit
whose contents are not what ran. Fine for a local sweep, not fine for the artifact a
writeup cites, so it is refused rather than warned about.

Files are copied byte for byte rather than read and rewritten, so what gets published is
what the run wrote, not this module's rendering of it.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from research.runs import RunRecord

# What has to match for a placebo to be a null test of the real run rather than a
# different experiment. Deliberately not `n_boot` (resampling the same rows more times
# is still the same experiment) or `placebo_lag_bars` (which is what differs).
EXPERIMENT = (
    "dates",
    "symbol",
    "extra_symbols",
    "horizon",
    "target",
    "spread_col",
    "predict_venue",
    "max_age_ms",
    "min_train",
    "test_size",
    "step",
    "coverages",
    "dead_zone_spreads",
)

# The fields that name a direction. Predicting the other venue moves the target and the
# spread that defines a tradeable move on it, so the reverse run has to differ here and
# nowhere else.
DIRECTION = ("predict_venue", "target", "spread_col")


def check_provenance(tag: str, record: RunRecord, allow_dirty: bool) -> None:
    if record.provenance.git_dirty and not allow_dirty:
        raise ValueError(
            f"the {tag} run was recorded from a dirty tree, so its git_sha "
            f"({record.provenance.git_sha!r}) does not describe the code that produced it. "
            f"Rerun on a clean checkout, or pass --allow-dirty to publish it anyway."
        )


def _differs(real: RunRecord, other: RunRecord, tag: str, fields: tuple[str, ...]) -> list[str]:
    return [
        f"{field}: real={getattr(real.spec, field)!r} {tag}={getattr(other.spec, field)!r}"
        for field in fields
        if getattr(real.spec, field) != getattr(other.spec, field)
    ]


def _check_same_builder(real: RunRecord, other: RunRecord, tag: str) -> None:
    """Same dates and same spec is not enough if the features were built by other code."""
    a, b = real.provenance, other.provenance
    if a.builder_fingerprint != b.builder_fingerprint:
        raise ValueError(
            f"different feature builders: real={a.builder_fingerprint!r} "
            f"{tag}={b.builder_fingerprint!r}"
        )
    if a.feature_schema_digest != b.feature_schema_digest:
        raise ValueError(
            f"different feature schemas: real={a.feature_schema_digest!r} "
            f"{tag}={b.feature_schema_digest!r}"
        )


def check_pair(real: RunRecord, placebo: RunRecord) -> None:
    """The placebo has to be the same experiment, or it nulls out a different one."""
    if real.spec.placebo:
        raise ValueError("the real run was recorded with --placebo set")
    if not placebo.spec.placebo:
        raise ValueError("the placebo run was not recorded with --placebo set")

    differs = _differs(real, placebo, "placebo", EXPERIMENT)
    if differs:
        raise ValueError(
            "the placebo is not the same experiment as the real run:\n  " + "\n  ".join(differs)
        )
    _check_same_builder(real, placebo, "placebo")


def check_reverse(real: RunRecord, reverse: RunRecord) -> None:
    """The other direction has to be the same experiment, pointed the other way.

    Running the forecast backwards does not settle whether a venue discovers price or
    merely tracks it slowly: a coarsely-requoted book produces the same asymmetry as
    genuine leadership, which is why `research.infoshare` exists. What the reverse run
    does rule out is every *symmetric* explanation, a shared latent factor or a leak that
    would inflate both directions alike. It only rules those out if direction is the one
    thing that differs, which is what is checked here.
    """
    if real.spec.placebo or reverse.spec.placebo:
        raise ValueError("a placebo cannot stand in for the reverse direction")
    if real.spec.predict_venue == reverse.spec.predict_venue:
        raise ValueError(
            f"both runs predict {real.spec.predict_venue!r}, so neither reverses the other"
        )

    shared = tuple(field for field in EXPERIMENT if field not in DIRECTION)
    differs = _differs(real, reverse, "reverse", shared)
    if differs:
        raise ValueError(
            "the reverse run is not the same experiment as the real run:\n  " + "\n  ".join(differs)
        )
    _check_same_builder(real, reverse, "reverse")


def export(
    real: Path,
    out: Path,
    placebo: Path | None = None,
    reverse: Path | None = None,
    allow_dirty: bool = False,
) -> list[Path]:
    """Validate, then copy. Returns what was written, in the order it was written."""
    sources = {"real": real, "placebo": placebo, "reverse": reverse}
    records = {tag: RunRecord.read(path) for tag, path in sources.items() if path is not None}

    for tag, record in records.items():
        check_provenance(tag, record, allow_dirty)
    if placebo is not None:
        check_pair(records["real"], records["placebo"])
    if reverse is not None:
        check_reverse(records["real"], records["reverse"])

    out.mkdir(parents=True, exist_ok=True)
    written = []
    for tag, source in sources.items():
        if source is None:
            continue
        for name, stem in (("record.json", "record"), ("oos.parquet", "oos")):
            target = out / f"{stem}_{tag}{Path(name).suffix}"
            shutil.copyfile(source / name, target)
            written.append(target)
    return written


def main() -> None:
    p = argparse.ArgumentParser(description="Publish a run and its placebo for the notebook.")
    p.add_argument("run", type=Path, help="run directory, e.g. runs/20260730T051803-cbd7c6c")
    p.add_argument("--out", type=Path, required=True, help="destination data directory")
    p.add_argument("--placebo", type=Path, help="the matching --placebo run directory")
    p.add_argument(
        "--reverse",
        type=Path,
        help="the same experiment with --predict-venue pointed at the other venue",
    )
    p.add_argument(
        "--allow-dirty",
        action="store_true",
        help="publish a record whose tree was dirty when it ran (its sha is not its code)",
    )
    args = p.parse_args()

    try:
        written = export(args.run, args.out, args.placebo, args.reverse, args.allow_dirty)
    except ValueError as refused:
        raise SystemExit(f"export refused: {refused}") from refused
    for path in written:
        print(f"{path}  {path.stat().st_size / 1e6:.2f} MB")


if __name__ == "__main__":
    main()
