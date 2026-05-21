"""Smoke tests for topic naming and header helpers (no network)."""
from __future__ import annotations

import pytest

from common.kafka_io import (
    bbo_topic,
    book_delta_topic,
    book_snapshot_topic,
    brokers_from_env,
    header_value,
    latency_headers,
    normalize_symbol,
    trade_topic,
)


def test_normalize_symbol_strips_slash() -> None:
    assert normalize_symbol("BTC/USD") == "BTC-USD"
    assert normalize_symbol("BTC-USD") == "BTC-USD"
    assert normalize_symbol("BTCUSDT") == "BTCUSDT"


def test_topic_names() -> None:
    assert trade_topic("binance", "BTCUSDT") == "md.trades.binance.BTCUSDT"
    assert trade_topic("kraken", "BTC/USD") == "md.trades.kraken.BTC-USD"
    assert trade_topic("coinbase", "BTC-USD") == "md.trades.coinbase.BTC-USD"

    assert book_snapshot_topic("kraken", "BTC/USD") == "md.book.kraken.BTC-USD.snapshots"
    assert book_delta_topic("kraken", "BTC/USD") == "md.book.kraken.BTC-USD.deltas"
    assert bbo_topic("binance", "BTCUSDT") == "md.bbo.binance.BTCUSDT"


def test_brokers_from_env_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KAFKA_BROKERS", raising=False)
    assert brokers_from_env() == ["localhost:9092"]


def test_brokers_from_env_empty_string_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BROKERS", "")
    with pytest.raises(ValueError, match="empty broker list"):
        brokers_from_env()


def test_brokers_from_env_whitespace_only_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BROKERS", "  ,  ,")
    with pytest.raises(ValueError, match="empty broker list"):
        brokers_from_env()


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("BTC/USD", "BTC-USD"),
        ("BTC-USD", "BTC-USD"),
        ("BTCUSDT", "BTCUSDT"),
        ("BTC*USD", "BTC-USD"),
        ("FOO [X]", "FOO--X-"),
        ("A B", "A-B"),
    ],
)
def test_normalize_symbol_unsafe_chars(raw: str, expected: str) -> None:
    assert normalize_symbol(raw) == expected


def test_latency_headers_roundtrip() -> None:
    hdrs = latency_headers(local_recv_ts_ns=1000, exchange_ts_ns=500)
    assert header_value(hdrs, "local_recv_ts_ns") == b"1000"
    assert header_value(hdrs, "exchange_ts_ns") == b"500"
    assert header_value(hdrs, "missing") is None
    assert header_value(None, "anything") is None
