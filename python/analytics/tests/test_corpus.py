"""Round-trip tests for the corpus format (no Docker, no network)."""
from __future__ import annotations

from pathlib import Path

from analytics.corpus import CorpusRecord, CorpusWriter, read_corpus, write_corpus
from common.kafka_io import latency_headers, trade_topic
from common.models import Side, Trade, encode


def _sample_records() -> list[CorpusRecord]:
    trade = Trade(
        exchange="coinbase",
        symbol="BTC-USD",
        trade_id="42",
        price="65000.12",
        size="0.5",
        side=Side.BID,
        exchange_ts_ns=1_700_000_000_000_000_000,
        local_ts_ns=1_700_000_000_000_000_100,
    )
    return [
        CorpusRecord(
            topic=trade_topic("coinbase", "BTC-USD"),
            partition=0,
            offset=0,
            timestamp_ms=1_700_000_000_000,
            key=b"coinbase.BTC-USD",
            value=encode(trade),
            headers=latency_headers(local_recv_ts_ns=123, exchange_ts_ns=100),
        ),
        # key=None and empty headers must survive the round-trip.
        CorpusRecord(
            topic="md.status.kraken",
            partition=0,
            offset=1,
            timestamp_ms=1_700_000_000_500,
            key=None,
            value=b'{"t":"status","exchange":"kraken","state":"down","ts_ns":1}',
            headers=[],
        ),
    ]


def test_roundtrip_preserves_records(tmp_path: Path) -> None:
    records = _sample_records()
    path = tmp_path / "corpus.jsonl.gz"

    n = write_corpus(path, records)
    assert n == len(records)

    read_back = list(read_corpus(path))
    assert read_back == records  # frozen Structs compare by value


def test_value_bytes_decode_back_to_model(tmp_path: Path) -> None:
    """The corpus carries real wire bytes; the value must decode unchanged."""
    from common.models import decode

    records = _sample_records()
    path = tmp_path / "corpus.jsonl.gz"
    write_corpus(path, records)

    first = next(read_corpus(path))
    msg = decode(first.value)
    assert isinstance(msg, Trade)
    assert msg.price == "65000.12"


def test_writer_counts_and_streams(tmp_path: Path) -> None:
    records = _sample_records()
    path = tmp_path / "stream.jsonl.gz"
    with CorpusWriter(path) as w:
        for r in records:
            w.write(r)
    assert w.count == len(records)
    assert list(read_corpus(path)) == records


def test_writer_creates_parent_dirs(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deeper" / "corpus.jsonl.gz"
    write_corpus(path, _sample_records())
    assert path.exists()
