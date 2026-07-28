"""Silver tape datasets: the bronze market-data content lifted verbatim.

These four (trades, liquidations, mark_price, open_interest) are passthroughs, not
reconstructions, so what matters is that nothing is dropped, the decimals survive
as decimals, and every row lands on the same clock as `quotes` - the join key a
downstream feature build uses to align a trade with the book it hit.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from analytics.tests.golden import build_golden_records
from common.kafka_io import header_value
from common.models import decode
from materializer.bronze import CanonicalMap, parse_topic
from silver.dq import TAPE_DATASETS, build_silver

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"


def _facts():
    return build_silver(build_golden_records(), CanonicalMap.from_yaml(INSTRUMENTS_FILE))


def test_every_bronze_tape_record_survives() -> None:
    """A passthrough drops nothing - one silver row per bronze record, per dataset."""
    facts = _facts()
    expected: dict[str, int] = dict.fromkeys(TAPE_DATASETS, 0)
    for rec in build_golden_records():
        dataset = parse_topic(rec.topic).dataset
        if dataset in expected:
            expected[dataset] += 1
    for dataset, n in expected.items():
        assert n > 0, f"golden corpus has no {dataset} to test"
        assert len(getattr(facts, dataset)) == n, dataset


def _by_partition(field):
    """Bronze records keyed the way a silver tape row identifies itself. `offset` is
    per-topic, so the exchange and canonical symbol are part of the key."""
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    out = {}
    for rec in build_golden_records():
        meta = parse_topic(rec.topic)
        if meta.dataset not in TAPE_DATASETS:
            continue
        exchange = meta.exchange or ""
        canon = canonical.resolve(exchange, decode(rec.value).symbol)
        out[(meta.dataset, exchange, canon, rec.offset)] = field(rec)
    return out


def _row_key(dataset: str, row: dict) -> tuple:
    return (dataset, row["exchange"], row["canonical_symbol"], row["offset"])


def test_tape_rows_land_on_the_quotes_clock() -> None:
    """ts_ns is local_recv_ts_ns (one gateway clock), not the venue's exchange_ts_ns
    - cross-venue joins are only valid on the former."""
    recv = _by_partition(lambda r: int(header_value(r.headers, "local_recv_ts_ns")))
    facts = _facts()
    for dataset in TAPE_DATASETS:
        rows = getattr(facts, dataset)
        assert rows, dataset
        for row in rows:
            assert row["ts_ns"] == recv[_row_key(dataset, row)], dataset


def test_trades_carry_taker_side_and_decimal_price() -> None:
    facts = _facts()
    cb = [t for t in facts.trades if t["exchange"] == "coinbase"]
    assert len(cb) == 1
    trade = cb[0]
    assert trade["canonical_symbol"] == "BTC-USD"
    assert trade["trade_id"] == "cb-1"
    assert trade["price"] == Decimal("65000.00")
    assert trade["size"] == Decimal("0.10")
    # taker/aggressor direction, uniform across drivers
    assert trade["side"] == "bid"


def test_trade_side_matches_the_wire_record() -> None:
    """The side written to silver is the side the driver emitted, for every venue."""
    wire = _by_partition(lambda r: decode(r.value))
    rows = _facts().trades
    assert len({r["exchange"] for r in rows}) > 1, "need >1 venue to be worth asserting"
    for row in rows:
        assert row["side"] == str(wire[_row_key("trades", row)].side)


def test_mark_price_carries_the_funding_rate() -> None:
    """The carry rung reads funding from here; it exists nowhere else in silver."""
    facts = _facts()
    assert facts.mark_price
    mark = facts.mark_price[0]
    assert mark["canonical_symbol"] == "BTC-USDT-PERP"
    assert mark["mark_price"] == Decimal("65001.23")
    assert mark["index_price"] == Decimal("64998.50")
    assert mark["funding_rate"] == Decimal("0.00010000")
    assert mark["next_funding_ts_ns"] > 0


def test_open_interest_and_liquidations_carry_their_payloads() -> None:
    facts = _facts()
    assert facts.open_interest[0]["open_interest"] == Decimal("85123.456")
    liq = facts.liquidations[0]
    assert liq["side"] == "ask"  # a long was liquidated
    assert liq["price"] == Decimal("64950.00")
    assert liq["avg_price"] == Decimal("64952.10")
    assert liq["orig_size"] == Decimal("0.500")
    assert liq["status"] == "FILLED"
