"""Every lake schema, declared once.

The layers used to each own their own `pa.Schema` constants, which meant `_PRICE` was
copy-pasted between silver and gold and nothing could enumerate the lake's shape. They
live here instead, and the layers re-export them, so existing imports
(`from silver.dq import QUOTES_SCHEMA`) keep working unchanged.

The direction matters: this module imports **only pyarrow**, never a layer. `common` is
the leaf every layer already depends on, so a registry that reached back up into
`silver.dq` or `gold.basis` would invert that and cycle.

DECIMAL(38,18) is the portable canonical scale; ns timestamps and offsets are int64.
"""

from __future__ import annotations

import pyarrow as pa

_PRICE = pa.decimal128(38, 18)


# ── bronze ────────────────────────────────────────────────────────────────────
# Verbatim log records. `partition` is int32 (Kafka's own width); everything else
# that counts is int64.

BRONZE_SCHEMA = pa.schema(
    [
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("timestamp_ms", pa.int64()),
        ("key", pa.binary()),
        ("value", pa.binary()),
        ("headers", pa.list_(pa.struct([("key", pa.string()), ("value", pa.binary())]))),
    ]
)


# ── silver ────────────────────────────────────────────────────────────────────

# Depth rungs carried into `quotes` beyond the touch, as cumulative size over the
# best N levels a side.
#
# Ten is not a tuning choice, it is the floor across venues: Kraken's v2 book
# channel is subscribed at depth 10 (ingest/kraken.py DEFAULT_DEPTH) and hard-trims
# past it, so ten is the deepest rung EVERY venue can supply. Going deeper - or
# using price-relative windows ("size within 25bps"), which coinbase and binance
# could fill and kraken structurally could not - would hand three venues a feature
# the fourth cannot have, and a cross-venue lead-lag model reads that asymmetry as
# venue skill. Symmetry beats resolution here.
MAX_DEPTH = 10
DEPTH_LEVELS = (5, 10)

BOOK_QUALITY_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("kind", pa.string()),
        ("offset", pa.int64()),
        ("sequence", pa.int64()),
        ("epoch", pa.int64()),
        ("exchange_ts_ns", pa.int64()),
        ("local_ts_ns", pa.int64()),
        ("local_recv_ts_ns", pa.int64()),
        ("best_bid", _PRICE),
        ("best_ask", _PRICE),
        # A COUNT of monotonic-but-missing deltas, not a flag.
        ("seq_gap", pa.int64()),
        ("crossed", pa.bool_()),
        ("invariant_kind", pa.string()),
    ]
)

LATENCY_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("dataset", pa.string()),
        ("offset", pa.int64()),
        ("exchange_ts_ns", pa.int64()),
        ("exchange_to_recv_ns", pa.int64()),
        ("exchange_to_emit_ns", pa.int64()),
    ]
)

STATUS_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("state", pa.string()),
        ("prev_state", pa.string()),
        ("is_transition", pa.bool_()),
        ("downtime_ns", pa.int64()),
    ]
)

QUOTES_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("best_bid", _PRICE),
        ("best_ask", _PRICE),
        ("bid_sz", _PRICE),
        ("ask_sz", _PRICE),
        # Depth beyond the touch: cumulative size at each DEPTH_LEVELS rung, and the
        # worst price the deepest rung reaches (size + distance = book slope).
        *[(f"{side}_depth_{n}", _PRICE) for n in DEPTH_LEVELS for side in ("bid", "ask")],
        ("bid_px_10", _PRICE),
        ("ask_px_10", _PRICE),
    ]
)

NBBO_SCHEMA = pa.schema(
    [
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("best_bid", _PRICE),
        ("best_ask", _PRICE),
        ("bid_venue", pa.string()),
        ("ask_venue", pa.string()),
        ("n_venues", pa.int64()),
    ]
)

# Tape schemas. The leading seven columns are `_tape_base` and are identical across
# all four, so a feature build can join any tape to `quotes` on (exchange, symbol, ts_ns).
_TAPE_BASE_FIELDS = [
    ("exchange", pa.string()),
    ("canonical_symbol", pa.string()),
    ("date", pa.string()),
    ("ts_ns", pa.int64()),
    ("offset", pa.int64()),
    ("exchange_ts_ns", pa.int64()),
    ("local_ts_ns", pa.int64()),
]

TRADES_SCHEMA = pa.schema(
    [
        *_TAPE_BASE_FIELDS,
        ("trade_id", pa.string()),
        ("price", _PRICE),
        ("size", _PRICE),
        ("side", pa.string()),
    ]
)

MARK_PRICE_SCHEMA = pa.schema(
    [
        *_TAPE_BASE_FIELDS,
        ("mark_price", _PRICE),
        ("index_price", _PRICE),
        ("est_settle_price", _PRICE),
        ("funding_rate", _PRICE),
        ("next_funding_ts_ns", pa.int64()),
    ]
)

OPEN_INTEREST_SCHEMA = pa.schema([*_TAPE_BASE_FIELDS, ("open_interest", _PRICE)])

LIQUIDATIONS_SCHEMA = pa.schema(
    [
        *_TAPE_BASE_FIELDS,
        ("side", pa.string()),
        ("price", _PRICE),
        ("avg_price", _PRICE),
        ("orig_size", _PRICE),
        ("filled_size", _PRICE),
        ("status", pa.string()),
    ]
)


# ── gold ──────────────────────────────────────────────────────────────────────

BASIS_SCHEMA = pa.schema(
    [
        ("base", pa.string()),
        ("date", pa.string()),
        ("ts_ns", pa.int64()),
        ("usd_mid", _PRICE),
        ("usdt_mid", _PRICE),
        ("basis_abs", _PRICE),
        ("basis_bps", pa.float64()),
        ("usd_bid", _PRICE),
        ("usd_ask", _PRICE),
        ("usdt_bid", _PRICE),
        ("usdt_ask", _PRICE),
    ]
)

BASIS_SUMMARY_SCHEMA = pa.schema(
    [
        ("base", pa.string()),
        ("date", pa.string()),
        ("n_obs", pa.int64()),
        ("basis_bps_mean", pa.float64()),
        ("basis_bps_std", pa.float64()),
        ("basis_bps_median", pa.float64()),
        ("basis_bps_min", pa.float64()),
        ("basis_bps_max", pa.float64()),
        ("basis_bps_p1", pa.float64()),
        ("basis_bps_p99", pa.float64()),
        ("coverage_ns", pa.int64()),
    ]
)

SCORECARD_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("canonical_symbol", pa.string()),
        ("date", pa.string()),
        ("check", pa.string()),
        ("n_records", pa.int64()),
        ("n_violations", pa.int64()),
        ("p50_ms", pa.float64()),
        ("p95_ms", pa.float64()),
        ("p99_ms", pa.float64()),
        # Compact JSON, stored as a string: Arrow has no json type.
        ("detail", pa.string()),
    ]
)


# ── freshness markers ─────────────────────────────────────────────────────────
# Written into both the silver and gold buckets under `_freshness/`, one object per
# dataset, undated. See common.lake.write_freshness_marker.

FRESHNESS_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("date", pa.string()),
        ("written_at_epoch", pa.float64()),
        ("row_count", pa.int64()),
    ]
)
