"""Wire-format round-trip for the book-message `epoch` field (the per-connection
generation the gateway keys reconstruction on). Includes the backward-compat
case: bronze captured before the field must still decode."""
from __future__ import annotations

from common.models import BookDelta, BookLevel, BookSnapshot, decode, encode


def test_book_snapshot_epoch_round_trips() -> None:
    snap = BookSnapshot(
        exchange="kraken", symbol="BTC/USD", sequence=0,
        bids=[BookLevel("100", "1")], asks=[BookLevel("101", "2")],
        exchange_ts_ns=1, local_ts_ns=2, epoch=12345,
    )
    out = decode(encode(snap))
    assert isinstance(out, BookSnapshot)
    assert out.epoch == 12345


def test_book_delta_epoch_round_trips() -> None:
    d = BookDelta(
        exchange="kraken", symbol="BTC/USD", sequence=5,
        bids=[], asks=[BookLevel("101", "0")],
        exchange_ts_ns=1, local_ts_ns=2, epoch=999,
    )
    out = decode(encode(d))
    assert isinstance(out, BookDelta)
    assert out.epoch == 999


def test_pre_epoch_payload_decodes_to_zero() -> None:
    """A book message captured before the epoch field (no `epoch` key) must still
    decode — the default keeps existing bronze replayable."""
    legacy = (
        b'{"t":"snap","exchange":"kraken","symbol":"BTC/USD","sequence":0,'
        b'"bids":[["100","1"]],"asks":[["101","2"]],'
        b'"exchange_ts_ns":1,"local_ts_ns":2}'
    )
    out = decode(legacy)
    assert isinstance(out, BookSnapshot)
    assert out.epoch == 0
