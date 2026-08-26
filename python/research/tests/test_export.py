"""What has to be true before a run becomes a published file.

Once `oos_real.parquet` and `oos_placebo.parquet` are sitting next to each other in a
data directory, nothing downstream can tell whether they came from the same experiment.
The notebook will happily plot a placebo run over different dates beside the real one and
report a reassuring null. So the pairing is checked here, at the last moment the two are
still traceable to the runs that produced them.
"""

from __future__ import annotations

import numpy as np
import pytest

from research.export import export
from research.main import COVERAGES
from research.runs import Provenance, RunRecord, RunSpec
from research.validation import OutOfSample

DATES = ("2026-01-01", "2026-01-02", "2026-01-03")


def _spec(**kw) -> RunSpec:
    args = {
        "dates": DATES,
        "symbol": "BTC-USD",
        "extra_symbols": (),
        "horizon": 5,
        "target": "y_kraken_ret_bps_5",
        "spread_col": "kraken_spread_bps",
        "predict_venue": "kraken",
        "max_age_ms": 5_000,
        "min_train": 8,
        "test_size": 3,
        "step": 3,
        "n_boot": 20,
        "placebo": False,
        "placebo_lag_bars": 3600,
        "coverages": COVERAGES,
        "dead_zone_spreads": 0.5,
    }
    return RunSpec(**{**args, **kw})


def _record(sha: str, *, dirty: bool = False, builder: str = "abcd1234", **spec_kw) -> RunRecord:
    """A record with the shape `read` expects and none of the cost of fitting one."""
    dates = np.array(["2026-01-01", "2026-01-02"])
    oos = OutOfSample(dates=dates, y=np.array([1.0, -2.0]), pred=np.array([0.5, -0.5]))
    return RunRecord(
        spec=_spec(**spec_kw),
        provenance=Provenance(
            created_at=f"2026-07-30T05:0{sha[0]}:00+00:00",
            git_sha=sha,
            git_dirty=dirty,
            builder_fingerprint=builder,
            feature_schema_digest="9c0eae5c",
            source_fingerprints={},
            packages={},
        ),
        venues=("coinbase", "kraken"),
        feature_names=("coinbase_ofi",),
        n_rows=2,
        n_rows_before=2,
        n_oos=2,
        n_test_dates=2,
        coverage_gaps={},
        folds=[],
        pooled={"gbt": {"r2_vs_zero": (0.1, 0.09, 0.11)}},
        classes={},
        traded={},
        base_rates={},
        selectivity=[],
        importance=[],
        oos={"gbt": oos},
    )


def _pair(tmp_path, **placebo_kw):
    real = _record("1111111").write(tmp_path / "runs")
    placebo = _record("2222222", placebo=True, **placebo_kw).write(tmp_path / "runs")
    return real, placebo


def test_a_matched_pair_publishes_four_files(tmp_path) -> None:
    real, placebo = _pair(tmp_path)
    written = export(real, tmp_path / "data", placebo)
    assert [p.name for p in written] == [
        "record_real.json",
        "oos_real.parquet",
        "record_placebo.json",
        "oos_placebo.parquet",
    ]


def test_the_published_files_are_byte_for_byte_what_the_run_wrote(tmp_path) -> None:
    """Copied rather than read and rewritten, so the artifact is the run's own output and
    not this module's rendering of it."""
    real, placebo = _pair(tmp_path)
    export(real, tmp_path / "data", placebo)
    for name, tag in (("record.json", "record_real.json"), ("oos.parquet", "oos_real.parquet")):
        assert (real / name).read_bytes() == (tmp_path / "data" / tag).read_bytes()


def test_a_placebo_over_different_dates_is_rejected(tmp_path) -> None:
    """The failure this whole module exists for: a null test of a different experiment,
    indistinguishable once the files are side by side."""
    real, placebo = _pair(tmp_path, dates=("2026-02-01", "2026-02-02"))
    with pytest.raises(ValueError, match="not the same experiment") as caught:
        export(real, tmp_path / "data", placebo)
    assert "dates" in str(caught.value)


def test_a_placebo_at_a_different_horizon_is_rejected(tmp_path) -> None:
    real, placebo = _pair(tmp_path, horizon=20)
    with pytest.raises(ValueError, match="horizon"):
        export(real, tmp_path / "data", placebo)


def test_a_run_that_was_not_a_placebo_cannot_be_published_as_one(tmp_path) -> None:
    real = _record("1111111").write(tmp_path / "runs")
    not_placebo = _record("2222222").write(tmp_path / "runs")
    with pytest.raises(ValueError, match="not recorded with --placebo"):
        export(real, tmp_path / "data", not_placebo)


def test_a_placebo_from_a_different_feature_builder_is_rejected(tmp_path) -> None:
    """Same dates and same spec, different feature code. The null would be measuring a
    pipeline that never produced the real number."""
    real, placebo = _pair(tmp_path)
    real, placebo = (
        real,
        _record("3333333", placebo=True, builder="ffff0000").write(tmp_path / "runs"),
    )
    with pytest.raises(ValueError, match="different feature builders"):
        export(real, tmp_path / "data", placebo)


def test_a_dirty_record_is_refused(tmp_path) -> None:
    """`git_sha` on a dirty record names a commit whose contents are not what ran, which
    is exactly the provenance claim the notebook makes."""
    dirty = _record("1111111", dirty=True).write(tmp_path / "runs")
    with pytest.raises(ValueError, match="dirty tree"):
        export(dirty, tmp_path / "data")


def test_a_dirty_record_can_be_published_deliberately(tmp_path) -> None:
    dirty = _record("1111111", dirty=True).write(tmp_path / "runs")
    written = export(dirty, tmp_path / "data", allow_dirty=True)
    assert [p.name for p in written] == ["record_real.json", "oos_real.parquet"]


def test_the_real_run_can_be_published_without_a_placebo(tmp_path) -> None:
    real = _record("1111111").write(tmp_path / "runs")
    written = export(real, tmp_path / "data")
    assert [p.name for p in written] == ["record_real.json", "oos_real.parquet"]


def _reverse(tmp_path, **kw):
    """The same experiment pointed the other way: predict coinbase from kraken."""
    args = {
        "predict_venue": "coinbase",
        "target": "y_coinbase_ret_bps_5",
        "spread_col": "coinbase_spread_bps",
    }
    return _record("4444444", **{**args, **kw}).write(tmp_path / "runs")


def test_the_reverse_direction_publishes_beside_the_pair(tmp_path) -> None:
    real, placebo = _pair(tmp_path)
    written = export(real, tmp_path / "data", placebo, _reverse(tmp_path))
    assert [p.name for p in written] == [
        "record_real.json",
        "oos_real.parquet",
        "record_placebo.json",
        "oos_placebo.parquet",
        "record_reverse.json",
        "oos_reverse.parquet",
    ]


def test_a_reverse_run_over_different_dates_is_rejected(tmp_path) -> None:
    """An asymmetry between two different windows is not an asymmetry."""
    real = _record("1111111").write(tmp_path / "runs")
    reverse = _reverse(tmp_path, dates=("2026-02-01", "2026-02-02"))
    with pytest.raises(ValueError, match="reverse run is not the same experiment") as caught:
        export(real, tmp_path / "data", reverse=reverse)
    assert "dates" in str(caught.value)


def test_a_reverse_run_in_the_same_direction_is_rejected(tmp_path) -> None:
    real = _record("1111111").write(tmp_path / "runs")
    same = _record("5555555").write(tmp_path / "runs")
    with pytest.raises(ValueError, match="neither reverses the other"):
        export(real, tmp_path / "data", reverse=same)


def test_a_placebo_cannot_be_published_as_the_reverse_direction(tmp_path) -> None:
    real = _record("1111111").write(tmp_path / "runs")
    placebo = _record("2222222", placebo=True).write(tmp_path / "runs")
    with pytest.raises(ValueError, match="placebo cannot stand in"):
        export(real, tmp_path / "data", reverse=placebo)


def test_a_reverse_run_from_a_different_feature_builder_is_rejected(tmp_path) -> None:
    real = _record("1111111").write(tmp_path / "runs")
    reverse = _reverse(tmp_path, builder="ffff0000")
    with pytest.raises(ValueError, match="different feature builders"):
        export(real, tmp_path / "data", reverse=reverse)
