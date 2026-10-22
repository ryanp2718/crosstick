"""What makes two feature matrices the same matrix.

A stale cache hit is the quietest failure in the pipeline: the run reports the feature
count and the venue list it *intended*, trains on whatever the file happens to hold, and
never errors. Each of these pins one thing the key must not ignore.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.fs as pafs

from common.lake import partition_key, write_object
from research import features
from research.main import builder_fingerprint, cache_name, source_fingerprint

DATE = "2026-06-30"


def _name(**kw) -> str:
    args = {"date": DATE, "symbol": "BTC-USD", "extra_symbols": (), "fingerprint": "111"}
    return cache_name(**{**args, **kw})


def _trades(fs, bucket: str, exchange: str, sides: list[str]) -> str:
    key = partition_key("trades", exchange=exchange, symbol="BTC-USD", date=DATE)
    return write_object(fs, bucket, key, pa.table({"ts_ns": [1, 2, 3], "side": sides}))


def _restamp(path: str, seconds: int) -> None:
    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + seconds * 1_000_000_000))


class _CountingFS:
    """A filesystem that counts full-object reads, the cost the digest memo saves."""

    def __init__(self, fs) -> None:
        self._fs, self.reads = fs, 0

    def __getattr__(self, name):
        return getattr(self._fs, name)

    def open_input_stream(self, path):
        self.reads += 1
        return self._fs.open_input_stream(path)


def test_a_rebuilt_date_gets_a_different_file() -> None:
    """The silver fingerprint is the data half of the identity."""
    assert _name(fingerprint="111") != _name(fingerprint="222")


def test_a_copied_lake_keeps_its_fingerprint(tmp_path) -> None:
    """Moving the lake stamps every object with the copy time and changes no byte. A key
    on mtime read that as a rebuild of every date: every cached matrix missed, and every
    published run's fingerprint stopped matching the lake it was read from."""
    fs, bucket, digests = pafs.LocalFileSystem(), tmp_path.as_posix(), tmp_path / "d.json"
    path = _trades(fs, bucket, "coinbase", ["bid", "ask", "bid"])
    before = source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests)

    _restamp(path, 3600)
    assert source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests) == before


def test_a_values_only_rewrite_changes_the_fingerprint(tmp_path) -> None:
    """Flipping every side keeps the row count, the bid/ask range and very likely the
    size, so neither the size nor the Parquet footer would see it. The content does."""
    fs, bucket, digests = pafs.LocalFileSystem(), tmp_path.as_posix(), tmp_path / "d.json"
    path = _trades(fs, bucket, "coinbase", ["bid", "ask", "bid"])
    before = source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests)

    _trades(fs, bucket, "coinbase", ["ask", "bid", "ask"])
    _restamp(path, 1)  # a real rewrite lands at a later mtime; this one may share the old
    assert source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests) != before


def test_a_dropped_object_changes_the_fingerprint(tmp_path) -> None:
    """The newest mtime could not see a deletion while a newer object remained."""
    fs, bucket, digests = pafs.LocalFileSystem(), tmp_path.as_posix(), tmp_path / "d.json"
    kraken = _trades(fs, bucket, "kraken", ["bid", "bid", "ask"])
    _trades(fs, bucket, "coinbase", ["bid", "ask", "bid"])
    both = source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests)

    fs.delete_file(kraken)
    assert source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests) != both


def test_only_new_or_restamped_objects_are_read_again(tmp_path) -> None:
    """A cache hit has to stay a stat per object, or the fingerprint costs a silver read."""
    local, bucket, digests = pafs.LocalFileSystem(), tmp_path.as_posix(), tmp_path / "d.json"
    coinbase = _trades(local, bucket, "coinbase", ["bid", "ask", "bid"])
    _trades(local, bucket, "kraken", ["bid", "bid", "ask"])
    fs = _CountingFS(local)

    source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests)
    assert fs.reads == 2
    source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests)
    assert fs.reads == 2
    _restamp(coinbase, 3600)
    source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests)
    assert fs.reads == 3


def test_an_unreadable_memo_only_costs_reads(tmp_path) -> None:
    fs, bucket, digests = pafs.LocalFileSystem(), tmp_path.as_posix(), tmp_path / "d.json"
    _trades(fs, bucket, "coinbase", ["bid", "ask", "bid"])
    before = source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests)

    digests.write_text("{not json", encoding="utf-8")
    assert source_fingerprint(fs, bucket, DATE, ("BTC-USD",), digests) == before


def test_extra_legs_are_part_of_the_identity() -> None:
    """The same date with and without Binance legs are different matrices, and serving
    the two-venue frame to a four-venue run would silently answer a different question."""
    assert _name() != _name(extra_symbols=("BTC-USDT",))
    assert _name(extra_symbols=("BTC-USDT",)) != _name(extra_symbols=("BTC-USDT", "BTC-USDT-PERP"))


def test_the_builder_is_part_of_the_identity(tmp_path, monkeypatch) -> None:
    """Adding a feature family changes the frame without touching one silver object, so
    a key made only of the data would keep serving matrices built before the columns
    existed. Nothing downstream can tell that frame from a current one."""
    before = _name()

    fake = tmp_path / "features.py"
    fake.write_text("# a feature builder that computes something else entirely\n")
    monkeypatch.setattr(features, "__file__", str(fake))
    builder_fingerprint.cache_clear()
    after = _name()
    builder_fingerprint.cache_clear()

    assert before != after
