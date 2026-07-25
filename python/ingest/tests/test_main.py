"""Tests for the ingester entrypoint wiring: exchange factory + config parsing.

The run loop / signal handling in main() is thin I/O glue validated by the e2e
smoke; here we TDD the pure pieces that pick a driver and parse $SYMBOLS.
"""

from __future__ import annotations

import pytest

from ingest.binance import BinanceIngester
from ingest.binance_futures import BinanceFuturesIngester
from ingest.coinbase import CoinbaseIngester
from ingest.kraken import KrakenIngester
from ingest.main import build_ingesters, parse_symbols


class _DummyProducer:
    """build_ingesters only stores the producer; it is never called here."""


@pytest.mark.parametrize(
    ("exchange", "cls"),
    [
        ("coinbase", CoinbaseIngester),
        ("binance", BinanceIngester),
        ("kraken", KrakenIngester),
    ],
)
def test_build_ingesters_selects_driver(exchange: str, cls: type) -> None:
    [ing] = build_ingesters(exchange, None, _DummyProducer())
    assert isinstance(ing, cls)
    assert ing.exchange == exchange


def test_build_ingesters_binance_futures_is_two_routed_connections() -> None:
    """Binance routes depth (/public) and market streams (/market) to separate
    WS endpoints; the venue therefore needs two instances in one process, with
    exactly one of them owning md.status.binance-futures."""
    ingesters = build_ingesters("binance-futures", None, _DummyProducer())
    assert all(isinstance(i, BinanceFuturesIngester) for i in ingesters)
    assert all(i.exchange == "binance-futures" for i in ingesters)
    paths = sorted(i.ws_url.split("?")[0] for i in ingesters)
    assert paths == [
        "wss://fstream.binance.com/market/stream",
        "wss://fstream.binance.com/public/stream",
    ]
    status_owners = [i for i in ingesters if i.heartbeat_s is not None]
    assert len(status_owners) == 1
    assert "/public/stream" in status_owners[0].ws_url  # the depth instance


def test_build_ingesters_unknown_exchange_raises() -> None:
    with pytest.raises(ValueError, match="unknown EXCHANGE"):
        build_ingesters("ftx", None, _DummyProducer())


def test_build_ingesters_passes_symbols_through() -> None:
    [ing] = build_ingesters("kraken", ["BTC/USD"], _DummyProducer())
    assert ing.symbols == ["BTC/USD"]


def test_build_ingesters_none_symbols_uses_driver_default() -> None:
    [ing] = build_ingesters("coinbase", None, _DummyProducer())
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
