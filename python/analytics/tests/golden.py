"""Deterministic golden-corpus builder for integration + data-quality tests.

We *synthesize* the golden corpus rather than commit a captured binary: a
reviewable Python builder is reproducible (gzip captures carry an mtime and
aren't byte-stable), diffable, and lets us plant the exact hard cases the
harness must exercise. Real captures (via `analytics.capture`) can be dropped in
later for richer corpora.

The corpus mirrors production framing: market-data records are keyed
`"{exchange}:{symbol}"` with `local_recv_ts_ns` / `exchange_ts_ns` headers, and
`md.status.*` records are keyed by exchange with no headers (see
`ingest/base_ingester.py` `_emit` / `_send_status`).

PLANTED HARD EVENTS (Phase 2 data-quality must detect these):
  * sequence gap   - kraken book deltas skip seq 7 (...6, 8...)
  * crossed book   - a binance delta lifts the bid above the ask
  * venue down     - binance emits md.status.binance state="down"

One logical asset (BTC) across three venues, spanning the USD vs USDT quote
split (coinbase/kraken BTC-USD, binance BTCUSDT) so strict per-quote bucketing
is exercised too.

The binance-futures perp segment (BTCUSDT -> canonical BTC-USDT-PERP) covers
the derivatives datasets (markprice / liquidations / openinterest) and plants
a FALSE-POSITIVE TRAP for Phase 2: futures book sequences are update-ids,
monotonic but NOT contiguous on a healthy stream (continuity lives in `pu`,
which doesn't survive normalization) - a naive seq+1 gap detector would flag
a perfectly healthy perp stream.
"""

from __future__ import annotations

from collections.abc import Callable

from analytics.corpus import CorpusRecord, write_corpus
from common.kafka_io import (
    bbo_topic,
    book_delta_topic,
    book_snapshot_topic,
    latency_headers,
    liquidation_topic,
    markprice_topic,
    openinterest_topic,
    status_topic,
    trade_topic,
)
from common.models import (
    BBO,
    BookDelta,
    BookLevel,
    BookSnapshot,
    Liquidation,
    MarkPrice,
    OpenInterest,
    Side,
    Status,
    Trade,
    encode,
)

BASE_NS = 1_700_000_000_000_000_000  # fixed origin so the corpus is deterministic
LAG_NS = 2_000_000  # exchange_ts precedes local_recv by 2ms
STEP_NS = 1_000_000  # 1ms between records
NEXT_FUNDING_NS = BASE_NS + 8 * 3600 * 1_000_000_000  # fixed next-funding instant

# A message factory takes (exchange_ts_ns, local_ts_ns) and stamps the body so
# it stays consistent with the record's latency headers.
MsgFactory = Callable[[int, int], object]

_Levels = list[tuple[str, str]]


def _levels(raw: _Levels) -> list[BookLevel]:
    return [BookLevel(price, size) for price, size in raw]


def _snap(ex: str, sym: str, seq: int, bids: _Levels, asks: _Levels) -> MsgFactory:
    return lambda ex_ts, lo_ts: BookSnapshot(
        exchange=ex,
        symbol=sym,
        sequence=seq,
        bids=_levels(bids),
        asks=_levels(asks),
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
    )


def _delta(ex: str, sym: str, seq: int, bids: _Levels, asks: _Levels) -> MsgFactory:
    return lambda ex_ts, lo_ts: BookDelta(
        exchange=ex,
        symbol=sym,
        sequence=seq,
        bids=_levels(bids),
        asks=_levels(asks),
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
    )


def _trade(ex: str, sym: str, tid: str, px: str, sz: str, side: Side) -> MsgFactory:
    return lambda ex_ts, lo_ts: Trade(
        exchange=ex,
        symbol=sym,
        trade_id=tid,
        price=px,
        size=sz,
        side=side,
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
    )


def _bbo(ex: str, sym: str, bpx: str, bsz: str, apx: str, asz: str) -> MsgFactory:
    return lambda ex_ts, lo_ts: BBO(
        exchange=ex,
        symbol=sym,
        bid_px=bpx,
        bid_sz=bsz,
        ask_px=apx,
        ask_sz=asz,
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
    )


def _mark(ex: str, sym: str, mark: str, index: str, settle: str, funding: str) -> MsgFactory:
    return lambda ex_ts, lo_ts: MarkPrice(
        exchange=ex,
        symbol=sym,
        mark_price=mark,
        index_price=index,
        est_settle_price=settle,
        funding_rate=funding,
        next_funding_ts_ns=NEXT_FUNDING_NS,
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
    )


def _liq(ex: str, sym: str, side: Side, px: str, avg: str, sz: str) -> MsgFactory:
    return lambda ex_ts, lo_ts: Liquidation(
        exchange=ex,
        symbol=sym,
        side=side,
        price=px,
        avg_price=avg,
        orig_size=sz,
        filled_size=sz,
        status="FILLED",
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
    )


def _oi(ex: str, sym: str, oi: str) -> MsgFactory:
    return lambda ex_ts, lo_ts: OpenInterest(
        exchange=ex,
        symbol=sym,
        open_interest=oi,
        exchange_ts_ns=ex_ts,
        local_ts_ns=lo_ts,
    )


class _Log:
    """Accumulates records on a monotonic clock with per-topic offsets."""

    def __init__(self) -> None:
        self.now = BASE_NS
        self.records: list[CorpusRecord] = []
        self._offsets: dict[str, int] = {}

    def _offset(self, topic: str) -> int:
        o = self._offsets.get(topic, 0)
        self._offsets[topic] = o + 1
        return o

    def md(self, topic: str, exchange: str, symbol: str, make: MsgFactory) -> None:
        local, exch = self.now, self.now - LAG_NS
        self.records.append(
            CorpusRecord(
                topic=topic,
                partition=0,
                offset=self._offset(topic),
                timestamp_ms=local // 1_000_000,
                key=f"{exchange}:{symbol}".encode(),
                value=encode(make(exch, local)),
                headers=latency_headers(local, exch),
            )
        )
        self.now += STEP_NS

    def status(self, exchange: str, state: str) -> None:
        topic = status_topic(exchange)
        self.records.append(
            CorpusRecord(
                topic=topic,
                partition=0,
                offset=self._offset(topic),
                timestamp_ms=self.now // 1_000_000,
                key=exchange.encode(),
                value=encode(Status(exchange=exchange, state=state, ts_ns=self.now)),
                headers=[],
            )
        )
        self.now += STEP_NS


def build_golden_records() -> list[CorpusRecord]:
    """Build the deterministic golden corpus (same bytes every call)."""
    log = _Log()

    # All venues come up.
    log.status("coinbase", "up")
    log.status("kraken", "up")
    log.status("binance", "up")
    log.status("binance-futures", "up")

    # ── coinbase BTC-USD: clean snapshot → deltas → trade ──────────────────
    cb, s_cb = "coinbase", "BTC-USD"
    log.md(
        book_snapshot_topic(cb, s_cb),
        cb,
        s_cb,
        _snap(
            cb,
            s_cb,
            100,
            [("64990.00", "1.5"), ("64980.00", "2.0")],
            [("65010.00", "1.2"), ("65020.00", "0.8")],
        ),
    )
    log.md(bbo_topic(cb, s_cb), cb, s_cb, _bbo(cb, s_cb, "64990.00", "1.5", "65010.00", "1.2"))
    log.md(
        book_delta_topic(cb, s_cb),
        cb,
        s_cb,
        _delta(cb, s_cb, 101, [("64990.00", "1.0")], [("65010.00", "0")]),
    )
    log.md(trade_topic(cb, s_cb), cb, s_cb, _trade(cb, s_cb, "cb-1", "65000.00", "0.10", Side.BID))
    log.md(
        book_delta_topic(cb, s_cb),
        cb,
        s_cb,
        _delta(cb, s_cb, 102, [("64995.00", "0.5")], []),
    )

    # ── kraken BTC/USD: PLANTED sequence gap (6 → 8, skips 7) ───────────────
    # (OrderBook won't catch this - 6→8 is still monotonic - which is exactly
    #  why Phase 2 needs explicit gap detection.)
    kr, s_kr = "kraken", "BTC/USD"
    log.md(
        book_snapshot_topic(kr, s_kr),
        kr,
        s_kr,
        _snap(kr, s_kr, 5, [("64985.0", "3.0")], [("65015.0", "2.5")]),
    )
    log.md(bbo_topic(kr, s_kr), kr, s_kr, _bbo(kr, s_kr, "64985.0", "3.0", "65015.0", "2.5"))
    log.md(book_delta_topic(kr, s_kr), kr, s_kr, _delta(kr, s_kr, 6, [], [("65015.0", "1.0")]))
    log.md(book_delta_topic(kr, s_kr), kr, s_kr, _delta(kr, s_kr, 8, [("64986.0", "1.0")], []))
    log.md(trade_topic(kr, s_kr), kr, s_kr, _trade(kr, s_kr, "kr-1", "65000.0", "0.05", Side.ASK))

    # ── binance BTCUSDT (USDT quote): PLANTED crossed book ──────────────────
    # delta 1002 lifts the bid to 65040 ≥ ask 65030 → OrderBook.apply_delta raises.
    bn, s_bn = "binance", "BTCUSDT"
    log.md(
        book_snapshot_topic(bn, s_bn),
        bn,
        s_bn,
        _snap(bn, s_bn, 1000, [("64970.00", "5.0")], [("65030.00", "4.0")]),
    )
    log.md(book_delta_topic(bn, s_bn), bn, s_bn, _delta(bn, s_bn, 1001, [], [("65030.00", "3.0")]))
    log.md(book_delta_topic(bn, s_bn), bn, s_bn, _delta(bn, s_bn, 1002, [("65040.00", "1.0")], []))
    log.md(trade_topic(bn, s_bn), bn, s_bn, _trade(bn, s_bn, "bn-1", "65035.00", "0.20", Side.BID))

    # ── binance-futures BTCUSDT (perp → BTC-USDT-PERP) ──────────────────────
    # Delta sequences are futures update-ids: monotonic but NOT contiguous on a
    # healthy stream (PLANTED false-positive trap for Phase 2 gap detection).
    bf, s_bf = "binance-futures", "BTCUSDT"
    log.md(
        book_snapshot_topic(bf, s_bf),
        bf,
        s_bf,
        _snap(
            bf,
            s_bf,
            5000,
            [("64965.00", "8.0"), ("64960.00", "3.0")],
            [("65035.00", "7.0"), ("65040.00", "2.5")],
        ),
    )
    log.md(book_delta_topic(bf, s_bf), bf, s_bf, _delta(bf, s_bf, 5004, [("64965.00", "6.5")], []))
    log.md(
        trade_topic(bf, s_bf),
        bf,
        s_bf,
        _trade(bf, s_bf, "210001", "65001.00", "0.25", Side.ASK),
    )
    log.md(book_delta_topic(bf, s_bf), bf, s_bf, _delta(bf, s_bf, 5009, [], [("65035.00", "0")]))
    log.md(
        markprice_topic(bf, s_bf),
        bf,
        s_bf,
        _mark(bf, s_bf, "65001.23", "64998.50", "65000.00", "0.00010000"),
    )
    log.md(
        liquidation_topic(bf, s_bf),
        bf,
        s_bf,
        _liq(bf, s_bf, Side.ASK, "64950.00", "64952.10", "0.500"),
    )
    log.md(openinterest_topic(bf, s_bf), bf, s_bf, _oi(bf, s_bf, "85123.456"))

    # ── PLANTED venue down, then a surviving venue keeps quoting ────────────
    # (Spot binance only: binance-futures is a distinct exchange id and its
    #  perp legs must survive this eviction.)
    log.status("binance", "down")
    log.md(bbo_topic(cb, s_cb), cb, s_cb, _bbo(cb, s_cb, "64995.00", "0.5", "65020.00", "0.8"))

    return log.records


def write_golden(path: str) -> int:
    """Write the golden corpus to `path`; return the record count."""
    return write_corpus(path, build_golden_records())


if __name__ == "__main__":
    import sys

    out = sys.argv[1] if len(sys.argv) > 1 else "golden.jsonl.gz"
    print(f"wrote {write_golden(out)} records to {out}")
