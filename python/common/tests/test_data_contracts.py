"""docs/data-contracts.md must agree with the registry.

This is the freshness gate. It is a plain test rather than a CI job so that a schema
change with a stale doc fails `pytest` locally, before it is ever pushed, and needs no
change to ci.yml.
"""

from __future__ import annotations

import pytest

from common import contracts
from common.contracts import DOC, render

pytestmark = pytest.mark.skipif(
    not DOC.exists(), reason="docs/ is not present (the service image copies only python/)"
)


@pytest.fixture
def doc() -> str:
    return DOC.read_text(encoding="utf-8")


def test_the_committed_doc_matches_the_registry(doc: str) -> None:
    """If this fails, run `python -m common.contracts --write`. The renderer's own
    --check prints a unified diff; here the assertion message points at it."""
    assert render(doc) == doc, "docs/data-contracts.md is stale"


def test_rendering_is_idempotent(doc: str) -> None:
    """A generator that grows a blank line per run would pass once and churn forever."""
    once = render(doc)
    assert render(once) == once


def test_the_prose_between_regions_survives(doc: str) -> None:
    """The whole reason the tables are marked off rather than the file generated: the
    hand-written arguments around them are not derivable from any schema."""
    for sentence in (
        "kraken's synthesized per-book counter is",  # the per-exchange seq-gap policy
        "would give three venues a feature the",  # the depth-rung venue-symmetry argument
        "turning at-least-once into exactly-once",  # bronze commit discipline
        "Ordering today comes from single-partition topics",  # the topic-key caveat
    ):
        assert sentence in render(doc), sentence


# ── the drifts this replaced ─────────────────────────────────────────────────
# Each of these was wrong in the hand-maintained tables. Pinning them keeps the
# generator honest about the thing it was built to fix.


@pytest.mark.parametrize(
    ("row", "was_documented_as"),
    [
        ("| `book_quality` | `seq_gap` | int64 |", "bool"),
        ("| all of the above | `partition` | int32 |", "int"),
        ("| `scorecard` | `detail` | string |", "json"),
        ("| `scorecard` | `p95_ms` | double |", "omitted from the diagram"),
    ],
)
def test_a_previously_drifted_column_now_renders_from_the_schema(
    doc: str, row: str, was_documented_as: str
) -> None:
    assert row in doc, f"was documented as {was_documented_as}"


def test_the_partition_columns_are_documented_as_the_stored_columns_they_are(doc: str) -> None:
    """exchange/canonical_symbol/date are physically in every silver row, and the old
    hand-written 'Key columns' cells left all three out of every table."""
    for column in ("exchange", "canonical_symbol", "date"):
        assert f"| `quotes` | `{column}` |" in doc, column


def test_the_freshness_dataset_is_documented_at_all(doc: str) -> None:
    """It was written into two buckets and read by the exporter without appearing
    anywhere in the contract doc."""
    assert "_freshness/<dataset>.parquet" in doc
    assert "| `written_at_epoch` | double |" in doc


# ── the renderer's own failure modes ─────────────────────────────────────────


def test_a_region_with_no_renderer_is_rejected() -> None:
    text = "<!-- BEGIN GENERATED: invented -->\n<!-- END GENERATED: invented -->\n"
    with pytest.raises(ValueError, match="no renderer"):
        render(text)


def test_an_unclosed_region_is_rejected() -> None:
    with pytest.raises(ValueError, match="never closed"):
        render("<!-- BEGIN GENERATED: gold-basis -->\nsome prose\n")


def test_a_renderer_with_no_region_in_the_doc_is_rejected(monkeypatch) -> None:
    """Guards the direction the doc cannot catch on its own: register a new dataset
    layer, forget the marker, and its table would silently never appear."""
    monkeypatch.setitem(contracts.REGIONS, "orphan", lambda: "")
    with pytest.raises(ValueError, match="no marker"):
        render(DOC.read_text(encoding="utf-8"))


def test_paths_come_from_the_same_helper_that_builds_them(doc: str) -> None:
    """The rendered path is partition_key's own output, not a re-spelling of it, so a
    layout change cannot leave the doc describing the old one."""
    assert "`quotes/exchange={ex}/symbol={canonical}/date={d}/part.parquet`" in doc
    assert "`scorecard/date={d}/part.parquet`" in doc
    assert "`nbbo/symbol={canonical}/date={d}/part.parquet`" in doc


def test_bronze_renders_its_shared_schema_once(doc: str) -> None:
    """Nine bronze datasets mirror the log, so nine identical column blocks would be
    noise; they collapse to one."""
    assert doc.count("| all of the above | `topic` | string |") == 1
