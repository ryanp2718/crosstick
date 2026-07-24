"""Silver quotes + reconstructed NBBO, incl. the per-venue BBO oracle.

The quotes/NBBO reconstruction folds bronze book through the same OrderBook the
scorecard uses; the oracle pins it against the captured `bbo` (reconstruction ==
the live gateway's derived BBO) on the golden corpus.
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from analytics.tests.golden import build_golden_records
from common.kafka_io import header_value
from common.models import BBO, decode
from materializer.bronze import CanonicalMap, parse_topic
from silver.dq import _build_nbbo, build_silver

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"


def _facts():
    return build_silver(build_golden_records(), CanonicalMap.from_yaml(INSTRUMENTS_FILE))


# ── quotes reconstruction ──────────────────────────────────────────────────


def test_quotes_skip_crossed_event() -> None:
    facts = _facts()
    bn = [q for q in facts.quotes if q["exchange"] == "binance"]
    # snapshot + delta1001 are valid two-sided; delta1002 crosses -> cleared, no quote.
    assert len(bn) == 2
    assert all(q["best_bid"] < q["best_ask"] for q in bn)


def test_quotes_carry_sizes() -> None:
    facts = _facts()
    cb = [q for q in facts.quotes if q["exchange"] == "coinbase"]
    assert cb and all(q["bid_sz"] is not None and q["ask_sz"] is not None for q in cb)


# ── per-venue BBO oracle ───────────────────────────────────────────────────


def test_reconstructed_quotes_match_captured_bbo() -> None:
    canonical = CanonicalMap.from_yaml(INSTRUMENTS_FILE)
    records = build_golden_records()
    facts = build_silver(records, canonical)

    quotes: dict[tuple[str, str], list[tuple[int, dict]]] = defaultdict(list)
    for q in facts.quotes:
        quotes[(q["exchange"], q["canonical_symbol"])].append((q["ts_ns"], q))
    for series in quotes.values():
        series.sort(key=lambda e: e[0])

    checked = 0
    for rec in records:
        if parse_topic(rec.topic).dataset != "bbo":
            continue
        bbo = decode(rec.value)
        assert isinstance(bbo, BBO)
        canon = canonical.resolve(bbo.exchange, bbo.symbol)
        recv = int(header_value(rec.headers, "local_recv_ts_ns"))
        prior = [q for ts, q in quotes[(bbo.exchange, canon)] if ts <= recv]
        assert prior, f"no reconstructed quote at/before captured bbo ({recv})"
        latest = prior[-1]
        assert latest["best_bid"] == Decimal(bbo.bid_px)
        assert latest["best_ask"] == Decimal(bbo.ask_px)
        checked += 1
    assert checked >= 2  # at least coinbase + kraken bbos are planted


# ── NBBO aggregation ───────────────────────────────────────────────────────


def test_nbbo_picks_max_bid_min_ask_across_venues() -> None:
    quotes = [
        _q("BTC-USD", "coinbase", 10, "100", "102"),
        _q("BTC-USD", "kraken", 11, "101", "103"),
    ]
    last = _build_nbbo(quotes, [])[-1]
    assert last["best_bid"] == Decimal("101")  # kraken's higher bid
    assert last["best_ask"] == Decimal("102")  # coinbase's lower ask
    assert (last["bid_venue"], last["ask_venue"]) == ("kraken", "coinbase")
    assert last["n_venues"] == 2


def test_nbbo_single_venue_passthrough() -> None:
    rows = _build_nbbo([_q("BTC-USDT", "binance", 10, "100", "102")], [])
    assert len(rows) == 1
    assert rows[0]["best_bid"] == Decimal("100")
    assert rows[0]["n_venues"] == 1


def test_nbbo_evicts_down_venue() -> None:
    quotes = [
        _q("BTC-USD", "coinbase", 10, "101", "102"),
        _q("BTC-USD", "kraken", 11, "100", "103"),
    ]
    status = [{"exchange": "coinbase", "ts_ns": 12, "state": "down", "is_transition": True}]
    last = _build_nbbo(quotes, status)[-1]
    assert last["ts_ns"] == 12  # the down transition produces a fresh NBBO tick
    assert last["best_bid"] == Decimal("100")  # coinbase evicted -> kraken only
    assert last["ask_venue"] == "kraken"
    assert last["n_venues"] == 1


def test_nbbo_evicts_stale_quiet_venue() -> None:
    # A multi-venue leg goes quiet and the market moves: its frozen quote must be
    # evicted, not carried into a crossed/wide NBBO (the basis +491bps tail).
    s = 1_000_000_000
    quotes = [
        _q("BTC-USD", "coinbase", 1 * s, "100", "101"),   # coinbase quotes once, then quiet
        _q("BTC-USD", "kraken", 2 * s, "99", "100"),
        _q("BTC-USD", "kraken", 20 * s, "90", "91"),       # market drops ~10%, coinbase frozen
    ]
    last = _build_nbbo(quotes, [], max_age_ns=5 * s)[-1]
    assert last["ts_ns"] == 20 * s
    assert last["n_venues"] == 1 and last["bid_venue"] == "kraken"
    assert last["best_bid"] == Decimal("90")  # not the stale coinbase 100
    # without eviction the frozen coinbase bid (100) crosses the fresh 91 ask.
    no_evict = _build_nbbo(quotes, [], max_age_ns=10**18)[-1]
    assert no_evict["best_bid"] == Decimal("100") and no_evict["n_venues"] == 2


def test_nbbo_keeps_quiet_venue_within_max_age() -> None:
    # A leg quiet but within max_age is still valid - it must be carried forward.
    s = 1_000_000_000
    quotes = [
        _q("BTC-USD", "coinbase", 1 * s, "100", "101"),
        _q("BTC-USD", "kraken", 4 * s, "99", "100"),  # coinbase 3s old here, < 5s max-age
    ]
    last = _build_nbbo(quotes, [], max_age_ns=5 * s)[-1]
    assert last["n_venues"] == 2 and last["best_bid"] == Decimal("100")


def _q(canonical: str, exchange: str, ts: int, bid: str, ask: str) -> dict:
    return {
        "canonical_symbol": canonical,
        "exchange": exchange,
        "ts_ns": ts,
        "best_bid": Decimal(bid),
        "best_ask": Decimal(ask),
    }
