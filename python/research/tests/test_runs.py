"""What a run record has to guarantee to be worth keeping.

Three claims, and all three fail silently if they break. The record has to be complete
(a number on screen that is not on disk makes the file misleading rather than merely
incomplete), it has to be valid JSON for a reader that is not Python, and it has to
carry no fitted matrices, because a record that costs gigabytes will not be kept.
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl
import pytest

from research import infoshare, runs
from research.main import COVERAGES, report
from research.runs import Provenance, RunSpec, oos_frame
from research.schema import FeatureSchema
from research.validation import OutOfSample, evaluate_walk_forward, walk_forward

TARGET = "y_kraken_ret_bps_5"
SPREAD = "kraken_spread_bps"


def _frame(n_dates: int = 14, n_per_date: int = 40) -> pl.DataFrame:
    rows = []
    for d in range(1, n_dates + 1):
        for i in range(n_per_date):
            rows.append(
                {
                    "ts_ns": d * 10_000 + i,
                    "date": f"2026-01-{d:02d}",
                    "coinbase_ofi": float(i),
                    "kraken_ofi": float(-i),
                    "kraken_spread_bps": 1.0,
                    TARGET: (float(i % 7) - 3.0) * 2.0,
                }
            )
    return pl.DataFrame(rows)


def _spec(**kw) -> RunSpec:
    args = {
        "dates": ("2026-01-01", "2026-01-02"),
        "symbol": "BTC-USD",
        "extra_symbols": (),
        "horizon": 5,
        "target": TARGET,
        "spread_col": SPREAD,
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


def _record(spread_col: str | None = SPREAD, **spec_kw):
    df = _frame()
    schema = FeatureSchema.from_columns(df.columns)
    splits = walk_forward(df, TARGET, 5, spread_col=spread_col)
    wf = evaluate_walk_forward(splits, 5, schema.venues)
    return runs.build(
        _spec(**spec_kw),
        runs.provenance("abcd1234", schema, {"2026-01-01": "1785190637"}),
        wf,
        schema,
        n_rows=df.height,
        n_rows_before=df.height + 10,
        coverage_gaps={"2026-01-01": {"kraken/flow": 10}},
    )


# ── completeness ─────────────────────────────────────────────────────────────


def test_the_report_renders_every_section_from_the_record(capsys) -> None:
    """The report only ever runs at the end of a multi-hour job, so a formatting slip
    surfaces as a crash after all the work is done and none of it is saved."""
    report(_record())
    out = capsys.readouterr().out
    for section in (
        "per fold",
        "across folds",
        "pooled out-of-sample",
        "dead-zone classes",
        "cleared half a spread",
        "selectivity",
        "permutation importance",
    ):
        assert section in out, section


def test_the_dead_zone_sections_are_skipped_without_a_threshold(capsys) -> None:
    report(_record(spread_col=None))
    out = capsys.readouterr().out
    assert "pooled out-of-sample" in out
    assert "dead-zone classes" not in out
    assert "selectivity" not in out


def test_the_degenerate_bootstrap_warning_fires_below_the_block_floor(capsys) -> None:
    """Six test dates cannot be resampled into a distribution. The interval still prints,
    because suppressing it would just move the argument, but it prints flagged."""
    record = _record()
    assert record.n_test_dates < 8
    assert record.bootstrap_is_degenerate
    report(record)
    assert "too few to resample" in capsys.readouterr().out


def test_the_record_carries_the_per_fold_edge_and_classes() -> None:
    record = _record()
    assert record.folds[0].edge["gbt"]["n_trades"] > 0
    assert "up_support" in record.folds[0].classes["gbt"]


def test_the_selectivity_curve_covers_every_requested_share() -> None:
    record = _record()
    assert [row.coverage for row in record.selectivity] == list(COVERAGES)
    assert all(
        set(row.metrics) == {"up_precision", "up_recall", "down_precision", "down_recall"}
        for row in record.selectivity
    )


# ── the file on disk ─────────────────────────────────────────────────────────


def test_the_record_is_valid_json_for_a_reader_that_is_not_python(tmp_path) -> None:
    """`json.dumps` emits a bare `NaN` token by default, which is not JSON and which
    every non-Python parser rejects. An undefined metric is routine here: the `zero`
    baseline calls no direction, so its hit rate has no value at all.
    """
    out = _record().write(tmp_path)
    text = (out / "record.json").read_text(encoding="utf-8")
    assert "NaN" not in text and "Infinity" not in text
    parsed = json.loads(text, parse_constant=_reject)
    assert parsed["pooled"]["zero"]["hit_rate"][0] is None


def _reject(token: str):
    raise AssertionError(f"{token} is not valid JSON")


def test_the_record_carries_no_fitted_matrices(tmp_path) -> None:
    """`Split` holds the training matrix, which for a real run is over a million rows by
    27 features per fold. A record that big does not get kept, and the rows it would be
    duplicating are already in oos.parquet."""
    out = _record().write(tmp_path)
    assert (out / "record.json").stat().st_size < 100_000
    parsed = json.loads((out / "record.json").read_text(encoding="utf-8"))
    assert set(parsed["folds"][0]) == {
        "train_dates",
        "test_dates",
        "n_train",
        "n_val",
        "n_test",
        "metrics",
        "edge",
        "classes",
    }
    assert "oos" not in parsed


def test_the_spec_and_provenance_survive_the_round_trip(tmp_path) -> None:
    record = _record()
    out = record.write(tmp_path)
    parsed = json.loads((out / "record.json").read_text(encoding="utf-8"))
    assert parsed["spec"]["target"] == TARGET
    assert parsed["spec"]["predict_venue"] == "kraken"
    assert parsed["provenance"]["builder_fingerprint"] == "abcd1234"
    assert parsed["provenance"]["source_fingerprints"] == {"2026-01-01": "1785190637"}
    assert parsed["provenance"]["packages"]["scikit-learn"]
    assert parsed["pooled"]["gbt"]["r2_vs_zero"][0] == pytest.approx(
        record.pooled["gbt"]["r2_vs_zero"][0]
    )


def test_two_runs_land_in_different_directories(tmp_path) -> None:
    """The directory name is the timestamp plus the commit, so a rerun at the same commit
    a second later does not overwrite the record it should be compared against."""
    record = _record()
    first = record.write(tmp_path)
    object.__setattr__(record.provenance, "created_at", "2099-01-01T00:00:00+00:00")
    assert record.write(tmp_path) != first


# ── the pooled rows ──────────────────────────────────────────────────────────


def test_the_oos_frame_is_one_row_per_test_bar_with_a_column_per_model(tmp_path) -> None:
    """Wide, not long. Every model was scored on the same strided rows, so a long frame
    repeats `date`, `y` and `threshold` once per model and quadruples a file whose whole
    purpose is to be small enough to commit."""
    record = _record()
    frame = pl.read_parquet(record.write(tmp_path) / "oos.parquet")
    assert frame.height == record.n_oos
    assert set(frame.columns) == {
        "date",
        "y",
        "threshold",
        "pred_zero",
        "pred_ridge_ofi",
        "pred_ridge_all",
        "pred_gbt",
    }
    assert frame["pred_zero"].to_list() == [0.0] * frame.height


def test_the_headline_is_recomputable_from_the_pooled_rows_alone(tmp_path) -> None:
    """The reason oos.parquet is the artifact worth keeping: it reproduces the reported
    number with no lake, no silver and no feature cache behind it."""
    record = _record()
    frame = pl.read_parquet(record.write(tmp_path) / "oos.parquet")
    y, pred = frame["y"].to_numpy(), frame["pred_gbt"].to_numpy()
    r2 = 1.0 - float(np.sum((y - pred) ** 2)) / float(np.sum(y**2))
    assert r2 == pytest.approx(record.pooled["gbt"]["r2_vs_zero"][0])


def test_models_scored_on_different_rows_are_rejected() -> None:
    """A wide frame silently misaligns if this is ever untrue, pairing one model's
    prediction with another model's row."""
    dates = np.array(["2026-01-01", "2026-01-01"])
    good = OutOfSample(dates=dates, y=np.array([1.0, 2.0]), pred=np.array([0.1, 0.2]))
    other = OutOfSample(dates=dates, y=np.array([9.0, 9.0]), pred=np.array([0.3, 0.4]))
    with pytest.raises(ValueError, match="different rows"):
        oos_frame({"gbt": good, "ridge_all": other})


# ── the second analysis ──────────────────────────────────────────────────────


def _infoshare_run(stride: int = 1) -> runs.InfoShares:
    """Two cointegrated venues where the first leads, so the estimate is well posed."""
    rng = np.random.default_rng(0)
    n = 6_000
    efficient = np.cumsum(rng.normal(0, 1e-4, n)) + np.log(100.0)
    leader = efficient
    follower = efficient - 0.5 * np.diff(efficient, prepend=efficient[0])
    frame = pl.DataFrame(
        {
            "ts_ns": np.arange(n, dtype=np.int64),
            "date": ["2026-01-01"] * n,
            "coinbase_mid": np.exp(leader),
            "kraken_mid": np.exp(follower),
            "coinbase_age_ms": np.zeros(n),
            "kraken_age_ms": np.zeros(n),
        }
    )
    estimates = infoshare.by_date(frame, ("coinbase", "kraken"), stride=stride, min_rows=1_000)
    return runs.InfoShares(
        venues=("coinbase", "kraken"),
        stride=stride,
        lags=infoshare.DEFAULT_LAGS,
        estimates=estimates,
    )


def test_the_information_share_block_prints_when_asked_for(capsys) -> None:
    """It is the second analysis, not a variant of the first: the model says which venue
    forecasts which, this says which venue the other error-corrects toward."""
    record = _record()
    record.infoshare = [_infoshare_run()]
    report(record)
    out = capsys.readouterr().out
    assert "information shares" in out
    assert "stride 1" in out
    assert "different questions" in out


def test_the_information_share_block_is_absent_by_default(capsys) -> None:
    report(_record())
    assert "information shares" not in capsys.readouterr().out


def test_every_requested_stride_is_its_own_estimate(capsys) -> None:
    """Shares depend on sampling frequency: coarse enough and every venue looks
    simultaneous. A share quoted without its stride is not an answer."""
    record = _record()
    record.infoshare = [_infoshare_run(1), _infoshare_run(5)]
    report(record)
    out = capsys.readouterr().out
    assert "stride 1 " in out and "stride 5 " in out


def test_the_estimates_survive_into_the_json(tmp_path) -> None:
    """`InfoShare` carries numpy arrays (alpha, the Hasbrouck bounds), which json.dumps
    cannot serialise and which would otherwise fail at the very end of a long run."""
    record = _record()
    record.infoshare = [_infoshare_run()]
    out = record.write(tmp_path)
    parsed = json.loads((out / "record.json").read_text(encoding="utf-8"))
    block = parsed["infoshare"][0]
    assert block["venues"] == ["coinbase", "kraken"]
    assert block["stride"] == 1
    lo, hi = block["estimates"][0]["bounds"][0]
    assert 0.0 <= lo <= hi <= 1.0


# ── provenance ───────────────────────────────────────────────────────────────


def test_the_schema_digest_tracks_the_columns_and_not_the_source() -> None:
    """Distinct from `builder_fingerprint`, which hashes the builder's bytes and so moves
    when a comment does. This moves only when the feature set does, which is what makes
    it the right thing to compare two runs on."""
    two = FeatureSchema.from_columns(["ts_ns", "coinbase_ofi", "kraken_ofi"])
    one = FeatureSchema.from_columns(["ts_ns", "coinbase_ofi"])
    same = FeatureSchema.from_columns(["kraken_ofi", "coinbase_ofi", "ts_ns"])
    assert runs.schema_digest(two) != runs.schema_digest(one)
    assert runs.schema_digest(two) == runs.schema_digest(same)


def test_provenance_records_the_libraries_that_can_move_a_number() -> None:
    schema = FeatureSchema.from_columns(["ts_ns", "coinbase_ofi"])
    got = runs.provenance("abcd1234", schema, {})
    assert got.packages["scikit-learn"] and got.packages["numpy"]
    assert got.created_at.endswith("+00:00")


def test_a_missing_git_leaves_provenance_blank_rather_than_failing(monkeypatch) -> None:
    """A record from a tarball with no .git is worth less, but it is worth more than a
    crash at the end of the run that produced it."""
    monkeypatch.setattr(runs, "_git", lambda *_: "")
    got = Provenance(
        created_at="2026-01-01T00:00:00+00:00",
        git_sha=runs._git("rev-parse"),
        git_dirty=False,
        builder_fingerprint="abcd1234",
        feature_schema_digest="0000",
        source_fingerprints={},
        packages={},
    )
    assert got.git_sha == ""


# ── folds ────────────────────────────────────────────────────────────────────


def test_folds_that_disagree_on_the_feature_set_are_an_error() -> None:
    """`split_on_dates` selects features from the whole frame, so every fold sees the
    same list. A disagreement means the pooled series is not one series."""
    df = _frame()
    schema = FeatureSchema.from_columns(df.columns)
    splits = walk_forward(df, TARGET, 5, spread_col=SPREAD)
    wf = evaluate_walk_forward(splits, 5, schema.venues)
    wf.splits[1].feature_names = ["coinbase_ofi"]
    with pytest.raises(ValueError, match="disagree on the feature set"):
        runs.build(
            _spec(),
            runs.provenance("abcd1234", schema, {}),
            wf,
            schema,
            n_rows=1,
            n_rows_before=1,
            coverage_gaps={},
        )


# ── reading it back ──────────────────────────────────────────────────────────


def test_a_written_record_reads_back_to_the_same_record(tmp_path) -> None:
    """Write, read, write again, and compare the bytes.

    Stronger than asserting field by field, which only ever checks the fields someone
    thought to list. Anything `read` drops, defaults or reconstructs as the wrong type
    changes the second serialisation, including the two things JSON cannot carry: the
    tuples and the NaNs.
    """
    record = _record()
    record.infoshare = [_infoshare_run()]
    first = record.write(tmp_path / "a")
    again = runs.RunRecord.read(first).write(tmp_path / "b")

    assert (first / "record.json").read_text(encoding="utf-8") == (again / "record.json").read_text(
        encoding="utf-8"
    )
    assert pl.read_parquet(first / "oos.parquet").equals(pl.read_parquet(again / "oos.parquet"))


def test_an_undefined_metric_reads_back_undefined_rather_than_null(tmp_path) -> None:
    """The `zero` baseline calls no direction, so its hit rate has no value. Left as the
    JSON null it was written as, it formats as a number that was never measured."""
    back = runs.RunRecord.read(_record().write(tmp_path))
    assert np.isnan(back.pooled["zero"]["hit_rate"][0])


def test_the_spec_reads_back_with_tuples_and_not_lists(tmp_path) -> None:
    """`RunSpec` is frozen and its sequences are tuples, which is what lets two specs be
    compared and hashed. JSON has only arrays, so this is the writer's one lossy edge."""
    back = runs.RunRecord.read(_record().write(tmp_path))
    assert isinstance(back.spec.dates, tuple)
    assert isinstance(back.spec.coverages, tuple)
    assert isinstance(back.venues, tuple)
    assert isinstance(back.folds[0].train_dates, tuple)
    assert back.spec.infoshare_venues is None


def test_the_pooled_rows_read_back_and_still_recompute_the_headline(tmp_path) -> None:
    """The point of the whole seam: a reader with the directory and no lake behind it
    gets the same number the run reported."""
    record = _record()
    back = runs.RunRecord.read(record.write(tmp_path))
    assert set(back.oos) == set(record.oos)

    gbt = back.oos["gbt"]
    r2 = 1.0 - float(np.sum((gbt.y - gbt.pred) ** 2)) / float(np.sum(gbt.y**2))
    assert r2 == pytest.approx(record.pooled["gbt"]["r2_vs_zero"][0])
    assert gbt.threshold is not None


def test_the_information_shares_read_back_as_arrays(tmp_path) -> None:
    """`bounds` is indexed as a matrix by the report. A list of lists would print but not
    slice."""
    record = _record()
    record.infoshare = [_infoshare_run()]
    back = runs.RunRecord.read(record.write(tmp_path))
    estimate = back.infoshare[0].estimates[0]
    assert isinstance(estimate.bounds, np.ndarray)
    assert estimate.bounds.shape == record.infoshare[0].estimates[0].bounds.shape
    assert estimate.component_shares.dtype == float


def test_a_record_missing_a_field_is_an_error_rather_than_a_default(tmp_path) -> None:
    """A record written by a version that did not have a field cannot be silently read as
    if the field were absent on purpose: the number it reports would be made up."""
    out = _record().write(tmp_path)
    parsed = json.loads((out / "record.json").read_text(encoding="utf-8"))
    del parsed["base_rates"]
    (out / "record.json").write_text(json.dumps(parsed), encoding="utf-8")
    with pytest.raises(KeyError, match="base_rates"):
        runs.RunRecord.read(out)
