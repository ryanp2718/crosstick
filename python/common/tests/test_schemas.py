"""The dataset registry's own invariants.

These are cheap, but each one guards a failure that is silent rather than loud: a
name collision that drops half the lake from the registry, a schema that exists but
was never registered (so the generated contract doc quietly omits it), or a layer
alias that has become a copy instead of the registry object.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pyarrow as pa
import pytest

from common import schemas
from common.schemas import (
    BRONZE_SCHEMA,
    DATASETS,
    FRESHNESS_SCHEMA,
    QUOTES_SCHEMA,
    Dataset,
    dataset,
    datasets,
    decimal_columns,
    table_from_rows,
)

LAYERS = ("bronze", "silver", "gold")


def test_every_dataset_is_uniquely_keyed_by_layer_and_name() -> None:
    """The pair, not the name. trades/nbbo/liquidations/mark_price/open_interest each
    exist in two layers with different schemas, so a name-only key would silently
    return whichever happened to be declared last."""
    keys = [(d.layer, d.name) for d in DATASETS]
    assert len(keys) == len(set(keys))
    assert len({d.name for d in DATASETS}) < len(keys), "expected cross-layer name reuse"


def test_the_same_name_in_two_layers_resolves_to_different_schemas() -> None:
    assert dataset("bronze", "trades").schema is BRONZE_SCHEMA
    assert dataset("silver", "trades").schema is not BRONZE_SCHEMA


@pytest.mark.parametrize("layer", LAYERS)
def test_every_layer_has_datasets_and_they_all_claim_it(layer: str) -> None:
    got = datasets(layer)
    assert got
    assert all(d.layer == layer for d in got)


def test_partition_by_only_names_columns_partition_key_accepts() -> None:
    """`partition_by` splats into common.lake.partition_key, which takes exactly
    `exchange` and `symbol` as keywords (it appends `date` itself)."""
    for d in DATASETS:
        assert set(d.partition_by) <= {"exchange", "symbol"}, d.name
        assert "date" not in d.partition_by, d.name


def test_every_declared_schema_is_registered() -> None:
    """A schema that exists but is not in DATASETS would be missing from the generated
    contract tables with nothing to notice. FRESHNESS_SCHEMA is the deliberate
    exception: it has no date partition and lives in two buckets."""
    declared = {
        name
        for name, value in vars(schemas).items()
        if isinstance(value, pa.Schema) and not name.startswith("_")
    }
    registered = {
        name
        for name, value in vars(schemas).items()
        if isinstance(value, pa.Schema) and any(d.schema is value for d in DATASETS)
    }
    assert declared - registered == {"FRESHNESS_SCHEMA"}


def test_bronze_mirrors_the_log_so_every_bronze_dataset_shares_one_schema() -> None:
    assert {id(d.schema) for d in datasets("bronze")} == {id(BRONZE_SCHEMA)}
    assert all(d.source_topics.startswith("md.") for d in datasets("bronze"))


def test_bronze_is_offset_keyed_and_the_derived_layers_are_overwrite_keyed() -> None:
    """Bronze names objects by start offset (exactly-once at the file grain); silver
    and gold write one overwrite-keyed object per partition per date."""
    assert all("start_offset" in d.filename for d in datasets("bronze"))
    assert all(d.filename == "part.parquet" for d in DATASETS if d.layer != "bronze")


def test_an_unknown_dataset_names_both_coordinates() -> None:
    with pytest.raises(KeyError, match=r"nope.*silver"):
        dataset("silver", "nope")
    with pytest.raises(KeyError, match=r"quotes.*gold"):
        dataset("gold", "quotes")


def test_decimal_columns_finds_the_prices_and_nothing_else() -> None:
    got = decimal_columns("silver", "quotes")
    assert set(got) == {f.name for f in QUOTES_SCHEMA if pa.types.is_decimal(f.type)}
    assert "ts_ns" not in got and "exchange" not in got
    assert "best_bid" in got and "bid_depth_10" in got


def test_a_dataset_with_no_decimal_columns_returns_empty() -> None:
    assert decimal_columns("bronze", "trades") == ()


def test_every_dataset_describes_itself() -> None:
    for d in DATASETS:
        assert d.description and not d.description.endswith("."), d.name


def test_datasets_are_frozen() -> None:
    """The registry is module-global, so a mutable record would let one caller's edit
    leak into every other reader of the lake's shape."""
    with pytest.raises(FrozenInstanceError):
        DATASETS[0].name = "other"  # type: ignore[misc]


def test_the_dataclass_defaults_suit_the_derived_layers() -> None:
    d = Dataset("x", "silver", QUOTES_SCHEMA, ("exchange",), "one row")
    assert d.filename == "part.parquet"
    assert d.source_topics == ""


# ── table_from_rows ──────────────────────────────────────────────────────────

_ROW = {
    "dataset": "quotes",
    "date": "2026-07-29",
    "written_at_epoch": 1.0,
    "row_count": 2,
}


def test_a_matching_row_set_builds_the_table() -> None:
    table = table_from_rows([_ROW], FRESHNESS_SCHEMA)
    assert table.schema == FRESHNESS_SCHEMA
    assert table.num_rows == 1


def test_a_missing_key_raises_instead_of_writing_a_null_column() -> None:
    """The whole point. from_pylist would have written row_count as all-null."""
    partial = {k: v for k, v in _ROW.items() if k != "row_count"}
    with pytest.raises(ValueError, match=r"missing=\['row_count'\]"):
        table_from_rows([partial], FRESHNESS_SCHEMA)


def test_an_extra_key_raises_instead_of_being_dropped() -> None:
    with pytest.raises(ValueError, match=r"extra=\['surprise'\]"):
        table_from_rows([{**_ROW, "surprise": 1}], FRESHNESS_SCHEMA)


def test_both_directions_are_reported_together() -> None:
    """One round trip should tell you everything wrong with the row."""
    wrong = {k: v for k, v in _ROW.items() if k != "date"} | {"dt": "2026-07-29"}
    with pytest.raises(ValueError) as excinfo:
        table_from_rows([wrong], FRESHNESS_SCHEMA)
    assert "missing=['date']" in str(excinfo.value)
    assert "extra=['dt']" in str(excinfo.value)


def test_key_order_does_not_matter() -> None:
    """from_pylist keys by name, so order drift is harmless and must not be rejected."""
    reversed_row = dict(reversed(list(_ROW.items())))
    assert table_from_rows([reversed_row], FRESHNESS_SCHEMA).num_rows == 1


def test_no_rows_yields_the_typed_empty_table() -> None:
    """Callers write zero-row partitions; the schema still has to survive."""
    table = table_from_rows([], FRESHNESS_SCHEMA)
    assert table.num_rows == 0
    assert table.schema == FRESHNESS_SCHEMA


def test_only_the_first_row_is_checked() -> None:
    """Pins the documented trade-off rather than leaving it implicit: validation is
    per batch, not per row, because every builder emits a fixed key set and a per-row
    check would cost a set construction per row across a silver date's millions."""
    table = table_from_rows([_ROW, {**_ROW, "surprise": 1}], FRESHNESS_SCHEMA)
    assert table.num_rows == 2
    assert table.column_names == FRESHNESS_SCHEMA.names


# ── the layer aliases ────────────────────────────────────────────────────────


def test_the_layers_re_export_the_registry_objects_not_copies() -> None:
    """`is`, not `==`: two structurally equal pa.Schema objects compare equal, so an
    accidental second declaration would pass an equality check and still mean the
    registry no longer describes what the layer writes."""
    from gold.basis import BASIS_SCHEMA, BASIS_SUMMARY_SCHEMA
    from gold.scorecard import SCORECARD_SCHEMA
    from materializer.bronze import SCHEMA as BRONZE_ALIAS
    from silver.dq import QUOTES_SCHEMA as QUOTES_ALIAS
    from silver.dq import TAPE_SCHEMAS

    assert QUOTES_ALIAS is dataset("silver", "quotes").schema
    assert BASIS_SCHEMA is dataset("gold", "basis").schema
    assert BASIS_SUMMARY_SCHEMA is dataset("gold", "basis_summary").schema
    assert SCORECARD_SCHEMA is dataset("gold", "scorecard").schema
    assert BRONZE_ALIAS is BRONZE_SCHEMA
    for name, schema in TAPE_SCHEMAS.items():
        assert schema is dataset("silver", name).schema, name


def test_the_registry_imports_no_layer() -> None:
    """The import direction is the thing keeping this acyclic: common is the leaf every
    layer depends on, so schemas.py must never reach back up into one."""
    import ast
    from pathlib import Path

    tree = ast.parse(Path(schemas.__file__).read_text(encoding="utf-8"))
    imported = {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    imported |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert imported <= {"__future__", "dataclasses", "pyarrow"}, imported
