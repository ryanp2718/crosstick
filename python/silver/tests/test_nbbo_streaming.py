"""The streaming NBBO build (`_build_nbbo_streaming`) must reorder fold-order quotes
on disk within the lateness window and produce the SAME NBBO as the `_build_nbbo`
oracle, fail loud past the window, and honor down-sentinel eviction. These pin the
adversarial cases the order-clean golden corpus never exercises (test_streaming)."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

import pytest
from pyarrow import fs as pafs

from common.asof import LatenessError
from common.lake import PartitionWriter, partition_key, read_dataset
from silver.dq import QUOTES_SCHEMA, _build_nbbo
from silver.main import _build_nbbo_streaming

DATE = "2026-06-16"
S = 1_000_000_000  # 1s in ns (WINDOW_NS is 60s)


def _q(exchange: str, symbol: str, ts: int, bid: str, ask: str) -> dict:
    return {
        "exchange": exchange,
        "canonical_symbol": symbol,
        "date": DATE,
        "ts_ns": ts,
        "best_bid": Decimal(bid),
        "best_ask": Decimal(ask),
        "bid_sz": Decimal("1"),
        "ask_sz": Decimal("1"),
    }


def _seed_quotes(fs: pafs.FileSystem, bucket: str, rows: list[dict]) -> None:
    """Write quotes to disk in the given (fold) order, one part.parquet per venue."""
    by_part: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_part[(r["exchange"], r["canonical_symbol"])].append(r)
    for (ex, sym), rs in by_part.items():
        with PartitionWriter(
            fs, bucket, partition_key("quotes", exchange=ex, symbol=sym, date=DATE), QUOTES_SCHEMA
        ) as w:
            w.write_rows(rs)


def _norm(rows: list[dict]) -> list[tuple]:
    # compare on values, scale/order-insensitive (the disk round-trip rescales Decimals)
    return sorted(
        (
            r["canonical_symbol"],
            r["ts_ns"],
            float(r["best_bid"]),
            float(r["best_ask"]),
            r["bid_venue"],
            r["ask_venue"],
            r["n_venues"],
        )
        for r in rows
    )


def _run(
    fs: pafs.FileSystem, bucket: str, rows: list[dict], status: list[dict] | None = None
) -> list[dict]:
    _seed_quotes(fs, bucket, rows)
    counts: dict[str, int] = defaultdict(int)
    _build_nbbo_streaming(fs, bucket, DATE, status or [], counts)
    streamed = read_dataset(fs, bucket, "nbbo", DATE)
    return streamed.to_pylist() if streamed is not None else []


def test_seam_straggler_within_window_matches_oracle(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = (tmp_path / "silver").as_posix()
    rows = [
        _q("coinbase", "BTC-USD", 1 * S, "100", "101"),
        _q("coinbase", "BTC-USD", 10 * S, "102", "103"),
        _q("coinbase", "BTC-USD", 5 * S, "99", "100"),  # straggler, 5s behind (< window)
        _q("kraken", "BTC-USD", 2 * S, "100", "102"),
        _q("kraken", "BTC-USD", 8 * S, "101", "103"),
    ]
    assert _norm(_run(fs, bucket, rows)) == _norm(_build_nbbo(rows, []))


def test_equal_ts_ties_match_oracle(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = (tmp_path / "silver").as_posix()
    rows = [
        _q("coinbase", "BTC-USD", 5 * S, "100", "101"),
        _q("coinbase", "BTC-USD", 5 * S, "100", "102"),  # equal ts; fold order -> last wins
        _q("kraken", "BTC-USD", 5 * S, "99", "101"),
    ]
    assert _norm(_run(fs, bucket, rows)) == _norm(_build_nbbo(rows, []))


def test_beyond_window_fails_loud(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = (tmp_path / "silver").as_posix()
    # 100s is emitted once the watermark reaches 200s; a 50s arriving after that is
    # behind an already-emitted ts -> cannot be placed within the 60s window.
    rows = [
        _q("coinbase", "BTC-USD", 100 * S, "100", "101"),
        _q("coinbase", "BTC-USD", 200 * S, "100", "101"),
        _q("coinbase", "BTC-USD", 50 * S, "100", "101"),
    ]
    with pytest.raises((LatenessError, AssertionError)):
        _run(fs, bucket, rows)


def test_down_sentinel_evicts_under_streaming(tmp_path) -> None:
    fs = pafs.LocalFileSystem()
    bucket = (tmp_path / "silver").as_posix()
    rows = [
        _q("coinbase", "BTC-USD", 1 * S, "100", "101"),
        _q("kraken", "BTC-USD", 2 * S, "120", "121"),  # kraken is best, then goes down
        _q("coinbase", "BTC-USD", 5 * S, "100", "101"),
    ]
    status = [
        {
            "exchange": "kraken",
            "date": DATE,
            "ts_ns": 3 * S,
            "state": "down",
            "prev_state": "up",
            "is_transition": True,
            "downtime_ns": None,
        }
    ]
    streamed = _run(fs, bucket, rows, status)
    assert _norm(streamed) == _norm(_build_nbbo(rows, status))
    # after kraken's down at ts=3, the ts=5 nbbo must fall back to coinbase, not a
    # stale kraken leg carried forward.
    last = max(streamed, key=lambda r: r["ts_ns"])
    assert last["bid_venue"] == "coinbase" and last["n_venues"] == 1
