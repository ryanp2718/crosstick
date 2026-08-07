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


def check_provenance(tag: str, record: RunRecord, allow_dirty: bool) -> None:
    if record.provenance.git_dirty and not allow_dirty:
        raise ValueError(
            f"the {tag} run was recorded from a dirty tree, so its git_sha "
            f"({record.provenance.git_sha!r}) does not describe the code that produced it. "
            f"Rerun on a clean checkout, or pass --allow-dirty to publish it anyway."
        )


def check_pair(real: RunRecord, placebo: RunRecord) -> None:
    """The placebo has to be the same experiment, or it nulls out a different one."""
    if real.spec.placebo:
        raise ValueError("the real run was recorded with --placebo set")
    if not placebo.spec.placebo:
        raise ValueError("the placebo run was not recorded with --placebo set")

    differs = [
        f"{field}: real={getattr(real.spec, field)!r} placebo={getattr(placebo.spec, field)!r}"
        for field in EXPERIMENT
        if getattr(real.spec, field) != getattr(placebo.spec, field)
    ]
    if differs:
        raise ValueError(
            "the placebo is not the same experiment as the real run:\n  " + "\n  ".join(differs)
        )

    real_prov, placebo_prov = real.provenance, placebo.provenance
    if real_prov.builder_fingerprint != placebo_prov.builder_fingerprint:
        raise ValueError(
            f"different feature builders: real={real_prov.builder_fingerprint!r} "
            f"placebo={placebo_prov.builder_fingerprint!r}"
        )
    if real_prov.feature_schema_digest != placebo_prov.feature_schema_digest:
        raise ValueError(
            f"different feature schemas: real={real_prov.feature_schema_digest!r} "
            f"placebo={placebo_prov.feature_schema_digest!r}"
        )


def export(
    real: Path, out: Path, placebo: Path | None = None, allow_dirty: bool = False
) -> list[Path]:
    """Validate, then copy. Returns what was written, in the order it was written."""
    records = {"real": RunRecord.read(real)}
    if placebo is not None:
        records["placebo"] = RunRecord.read(placebo)

    for tag, record in records.items():
        check_provenance(tag, record, allow_dirty)
    if placebo is not None:
        check_pair(records["real"], records["placebo"])

    out.mkdir(parents=True, exist_ok=True)
    written = []
    for tag, source in (("real", real), ("placebo", placebo)):
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
        "--allow-dirty",
        action="store_true",
        help="publish a record whose tree was dirty when it ran (its sha is not its code)",
    )
    args = p.parse_args()

    try:
        written = export(args.run, args.out, args.placebo, args.allow_dirty)
    except ValueError as refused:
        raise SystemExit(f"export refused: {refused}") from refused
    for path in written:
        print(f"{path}  {path.stat().st_size / 1e6:.2f} MB")


if __name__ == "__main__":
    main()
