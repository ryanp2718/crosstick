"""What makes two feature matrices the same matrix.

A stale cache hit is the quietest failure in the pipeline: the run reports the feature
count and the venue list it *intended*, trains on whatever the file happens to hold, and
never errors. Each of these pins one thing the key must not ignore.
"""

from __future__ import annotations

from research import features
from research.main import builder_fingerprint, cache_name


def _name(**kw) -> str:
    args = {"date": "2026-06-30", "symbol": "BTC-USD", "extra_symbols": (), "fingerprint": "111"}
    return cache_name(**{**args, **kw})


def test_a_rebuilt_date_gets_a_different_file() -> None:
    """The silver mtime is the data half of the identity."""
    assert _name(fingerprint="111") != _name(fingerprint="222")


def test_extra_legs_are_part_of_the_identity() -> None:
    """The same date with and without Binance legs are different matrices, and serving
    the two-venue frame to a four-venue run would silently answer a different question."""
    assert _name() != _name(extra_symbols=("BTC-USDT",))
    assert _name(extra_symbols=("BTC-USDT",)) != _name(extra_symbols=("BTC-USDT", "BTC-USDT-PERP"))


def test_the_builder_is_part_of_the_identity(tmp_path, monkeypatch) -> None:
    """Adding a feature family changes the frame without touching one silver object, so
    a key made only of data mtimes would keep serving matrices built before the columns
    existed. Nothing downstream can tell that frame from a current one."""
    before = _name()

    fake = tmp_path / "features.py"
    fake.write_text("# a feature builder that computes something else entirely\n")
    monkeypatch.setattr(features, "__file__", str(fake))
    builder_fingerprint.cache_clear()
    after = _name()
    builder_fingerprint.cache_clear()

    assert before != after
