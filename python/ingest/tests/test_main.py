"""Tests for the ingester entrypoint wiring: exchange factory + config parsing.

The run loop / signal handling in main() is thin I/O glue validated by the e2e
smoke; here we TDD the pure pieces that pick a driver and parse $SYMBOLS.
"""
from __future__ import annotations

import pytest

from ingest.binance import BinanceIngester
from ingest.coinbase import CoinbaseIngester
from ingest.kraken import KrakenIngester
from ingest.main import build_ingester, parse_symbols


class _DummyProducer:
    """build_ingester only stores the producer; it is never called here."""


@pytest.mark.parametrize(
    ("exchange", "cls"),
    [
        ("coinbase", CoinbaseIngester),
        ("binance", BinanceIngester),
        ("kraken", KrakenIngester),
    ],
)
def test_build_ingester_selects_driver(exchange: str, cls: type) -> None:
    ing = build_ingester(exchange, None, _DummyProducer())
    assert isinstance(ing, cls)
    assert ing.exchange == exchange


def test_build_ingester_unknown_exchange_raises() -> None:
    with pytest.raises(ValueError, match="unknown EXCHANGE"):
        build_ingester("ftx", None, _DummyProducer())


def test_build_ingester_passes_symbols_through() -> None:
    ing = build_ingester("kraken", ["BTC/USD"], _DummyProducer())
    assert ing.symbols == ["BTC/USD"]


def test_build_ingester_none_symbols_uses_driver_default() -> None:
    ing = build_ingester("coinbase", None, _DummyProducer())
    assert ing.symbols  # non-empty driver default


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("BTC-USD,ETH-USD", ["BTC-USD", "ETH-USD"]),
        (" BTC/USD , ETH/USD ", ["BTC/USD", "ETH/USD"]),
        ("SOLO", ["SOLO"]),
        ("", None),
        (None, None),
        ("  ,  ", None),
    ],
)
def test_parse_symbols(raw: str | None, expected: list[str] | None) -> None:
    assert parse_symbols(raw) == expected
