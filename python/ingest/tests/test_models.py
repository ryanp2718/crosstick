"""Wire-format round-trips for the optional book-message fields: `epoch` (the
per-connection generation the gateway keys reconstruction on) and `engine_ts_ns`
(the venue's per-message change stamp). Includes the backward-compat cases: bronze
captured before either field must still decode."""

from __future__ import annotations

from common.models import BookDelta, BookLevel, BookSnapshot, decode, encode


def test_book_snapshot_epoch_round_trips() -> None:
    snap = BookSnapshot(
        exchange="kraken",
        symbol="BTC/USD",
        sequence=0,
        bids=[BookLevel("100", "1")],
        asks=[BookLevel("101", "2")],
        exchange_ts_ns=1,
        local_ts_ns=2,
        epoch=12345,
    )
    out = decode(encode(snap))
    assert isinstance(out, BookSnapshot)
    assert out.epoch == 12345


def test_book_delta_epoch_round_trips() -> None:
    d = BookDelta(
        exchange="kraken",
        symbol="BTC/USD",
        sequence=5,
        bids=[],
        asks=[BookLevel("101", "0")],
        exchange_ts_ns=1,
        local_ts_ns=2,
        epoch=999,
    )
    out = decode(encode(d))
    assert isinstance(out, BookDelta)
    assert out.epoch == 999


def test_pre_epoch_payload_decodes_to_zero() -> None:
    """A book message captured before the epoch field (no `epoch` key) must still
    decode - the default keeps existing bronze replayable."""
    legacy = (
        b'{"t":"snap","exchange":"kraken","symbol":"BTC/USD","sequence":0,'
        b'"bids":[["100","1"]],"asks":[["101","2"]],'
        b'"exchange_ts_ns":1,"local_ts_ns":2}'
    )
    out = decode(legacy)
    assert isinstance(out, BookSnapshot)
    assert out.epoch == 0
    assert out.engine_ts_ns == 0


def test_engine_ts_round_trips() -> None:
    common = dict(
        exchange="coinbase",
        symbol="BTC-USD",
        sequence=1,
        bids=[BookLevel("100", "1")],
        asks=[],
        exchange_ts_ns=1_791_761_231_677_099_377,
        local_ts_ns=1_791_761_231_720_000_000,
        engine_ts_ns=1_791_761_231_652_640_000,
    )
    for msg in (BookSnapshot(**common), BookDelta(**common)):
        out = decode(encode(msg))
        assert type(out) is type(msg)
        assert out.engine_ts_ns == 1_791_761_231_652_640_000


def test_pre_engine_time_record_decodes_to_zero() -> None:
    """A real coinbase delta from bronze written before `engine_ts_ns` existed (from
    demo/corpus.jsonl.gz): it carries an epoch but no engine_ts_ns, and decodes as 0."""
    legacy = (
        b'{"t":"delta","exchange":"coinbase","symbol":"BTC-USD","sequence":7,'
        b'"bids":[["61297.3","0"]],"asks":[],"exchange_ts_ns":1783214772129141210,'
        b'"local_ts_ns":1783214772640343764,"epoch":1783214771410629297}'
    )
    out = decode(legacy)
    assert isinstance(out, BookDelta)
    assert out.engine_ts_ns == 0
    assert out.epoch == 1783214771410629297
    assert out.exchange_ts_ns == 1783214772129141210
