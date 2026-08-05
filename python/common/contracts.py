"""Render the schema tables in docs/data-contracts.md from the dataset registry.

The doc's tables used to be hand-maintained, which made them a third copy of facts
that already live in `common.schemas` and `common.lake.partition_key`, and they had
drifted: seq_gap documented as a bool where it is an int64 count, bronze partition as
int where it is int32, the scorecard `detail` column as json where Arrow has no such
type, and every table silently omitting the `exchange`/`canonical_symbol`/`date`
columns that are physically stored.

Only the tables are generated, and only between explicit markers. The prose around
them is hand-written and load-bearing (the per-exchange sequence-gap policy, the
depth-rung venue-symmetry argument, commit discipline, retention), so it must survive
regeneration untouched.

    python -m common.contracts --check    # exit 1 with a diff if the doc is stale
    python -m common.contracts --write    # rewrite the generated regions in place

`common/tests/test_data_contracts.py` runs --check, so a schema change with a stale
doc fails the ordinary test suite rather than needing its own CI job.
"""

from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path

from common.lake import FRESHNESS_PREFIX, partition_key
from common.schemas import FRESHNESS_SCHEMA, Dataset, dataset, datasets

DOC = Path(__file__).resolve().parents[2] / "docs" / "data-contracts.md"

BEGIN = "<!-- BEGIN GENERATED: {} -->"
END = "<!-- END GENERATED: {} -->"

# Stand-ins for the partition values, so a rendered path reads like the doc's prose.
_PLACEHOLDERS = {"exchange": "{ex}", "symbol": "{canonical}"}


def _path(ds: Dataset) -> str:
    """The object key, built by the same helper that builds it for real."""
    parts = {k: _PLACEHOLDERS[k] for k in ds.partition_by}
    return partition_key(ds.name, date="{d}", filename=ds.filename, **parts)


def _table(header: tuple[str, ...], rows: list[tuple[str, ...]]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def _where_table(items: tuple[Dataset, ...], *, topics: bool = False) -> str:
    """Where each dataset lives and what one row is."""
    header = ("Dataset", "Source topics", "Path", "Row =")
    rows = [(f"`{d.name}`", f"`{d.source_topics}`", f"`{_path(d)}`", d.description) for d in items]
    if not topics:
        header = tuple(h for h in header if h != "Source topics")
        rows = [(name, path, desc) for name, _, path, desc in rows]
    return _table(header, rows)


def _column_table(items: tuple[Dataset, ...]) -> str:
    """One row per column.

    Deliberately not one row per dataset with the columns crammed into a cell: at 15
    columns that cell is 500 characters wide, unreadable, and re-wraps the whole line
    in a diff whenever any single column changes. One row per column stays greppable
    and makes an added column a one-line diff. The dataset name repeats rather than
    being blank for the same reason: grep for a dataset and you get all of it.

    Datasets sharing a schema object collapse to one block; every bronze dataset
    mirrors the log, so bronze renders once rather than nine identical times.
    """
    seen: dict[int, str] = {}
    rows: list[tuple[str, ...]] = []
    for d in items:
        if id(d.schema) in seen:
            continue
        shared = [o.name for o in items if o.schema is d.schema]
        label = f"`{d.name}`" if len(shared) == 1 else "all of the above"
        seen[id(d.schema)] = label
        rows += [(label, f"`{f.name}`", str(f.type)) for f in d.schema]
    return _table(("Dataset", "Column", "Type"), rows)


def _layer(items: tuple[Dataset, ...], *, topics: bool = False) -> str:
    return _where_table(items, topics=topics) + "\n\n" + _column_table(items)


def _freshness_table() -> str:
    """Not in the registry: undated, and written into both the silver and gold
    buckets, so it does not fit the (layer, name, date-partitioned) shape."""
    where = _table(
        ("Dataset", "Path", "Row ="),
        [
            (
                f"`{FRESHNESS_PREFIX}`",
                f"`{FRESHNESS_PREFIX}/<dataset>.parquet`",
                "one dataset's last successful build",
            )
        ],
    )
    columns = _table(("Column", "Type"), [(f"`{f.name}`", str(f.type)) for f in FRESHNESS_SCHEMA])
    return where + "\n\n" + columns


REGIONS: dict[str, object] = {
    "bronze-datasets": lambda: _layer(datasets("bronze"), topics=True),
    "silver-datasets": lambda: _layer(datasets("silver")),
    "gold-scorecard": lambda: _layer((dataset("gold", "scorecard"),)),
    "gold-basis": lambda: _layer((dataset("gold", "basis"), dataset("gold", "basis_summary"))),
    "freshness-markers": _freshness_table,
}


def render(text: str) -> str:
    """Replace the body of every generated region, leaving everything else alone."""
    lines = text.splitlines()
    seen: set[str] = set()
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        name = _region_at(line)
        if name is None:
            i += 1
            continue
        if name not in REGIONS:
            raise ValueError(f"{DOC.name} has a generated region {name!r} with no renderer")
        seen.add(name)
        end = END.format(name)
        try:
            close = next(j for j in range(i + 1, len(lines)) if lines[j].strip() == end)
        except StopIteration:
            raise ValueError(f"region {name!r} is never closed with {end!r}") from None
        out += ["", REGIONS[name](), "", lines[close]]  # type: ignore[operator]
        i = close + 1

    unrendered = set(REGIONS) - seen
    if unrendered:
        raise ValueError(f"no marker in {DOC.name} for region(s) {sorted(unrendered)}")
    return "\n".join(out) + "\n"


def _region_at(line: str) -> str | None:
    stripped = line.strip()
    for name in REGIONS:
        if stripped == BEGIN.format(name):
            return name
    if stripped.startswith("<!-- BEGIN GENERATED:"):
        return stripped.removeprefix("<!-- BEGIN GENERATED:").removesuffix("-->").strip()
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--write", action="store_true", help="rewrite the doc in place")
    ap.add_argument("--doc", type=Path, default=DOC)
    args = ap.parse_args(argv)

    current = args.doc.read_text(encoding="utf-8")
    wanted = render(current)
    if current == wanted:
        print(f"{args.doc.name} is up to date")
        return 0
    if args.write:
        args.doc.write_text(wanted, encoding="utf-8")
        print(f"rewrote {args.doc}")
        return 0
    sys.stdout.writelines(
        difflib.unified_diff(
            current.splitlines(keepends=True),
            wanted.splitlines(keepends=True),
            fromfile=f"{args.doc.name} (on disk)",
            tofile=f"{args.doc.name} (from the registry)",
        )
    )
    print(f"\n{args.doc.name} is stale; run `python -m common.contracts --write`")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
