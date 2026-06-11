// Wire types for the msgspec-tagged JSON the ingesters publish to Redpanda.
// Mirrors python/common/models.py: the tag field is "t", levels are [price,size]
// string arrays (BookLevel is array_like), prices/sizes are strings.

export type WireLevel = [string, string]; // [price, size]

export interface BookSnapshotMsg {
  t: "snap";
  exchange: string;
  symbol: string;
  sequence: number;
  bids: WireLevel[];
  asks: WireLevel[];
  exchange_ts_ns: number;
  local_ts_ns: number;
}

export interface BookDeltaMsg {
  t: "delta";
  exchange: string;
  symbol: string;
  sequence: number;
  bids: WireLevel[];
  asks: WireLevel[];
  exchange_ts_ns: number;
  local_ts_ns: number;
}

export interface TradeMsg {
  t: "trade";
  exchange: string;
  symbol: string;
  trade_id: string;
  price: string;
  size: string;
  side: "bid" | "ask";
  exchange_ts_ns: number;
  local_ts_ns: number;
}

// Published by this gateway to md.bbo.* — shape matches models.py BBO so the
// Python streaming decoder round-trips it.
export interface BBOMsg {
  t: "bbo";
  exchange: string;
  symbol: string;
  bid_px: string;
  bid_sz: string;
  ask_px: string;
  ask_sz: string;
  exchange_ts_ns: number;
  local_ts_ns: number;
}

// Cross-exchange NBBO, published to md.nbbo.<canonical_id> and broadcast on WS.
// Per-leg ts/age surface staleness for consumers to filter at their own
// threshold (see docs/DESIGN_nbbo.md "Per-leg staleness is the consumer's
// call"). px/sz are the exact source decimal strings, passed through verbatim
// (lossless) so the md.nbbo log feeds downstream exact-decimal arithmetic.
// local_ts_ns and leg_age_ms are stream time — the max input event-time at
// compute, not wall clock — so md.nbbo.* replays deterministically (D1). In
// live operation stream time tracks wall clock within consumer lag (ms).
export interface NBBOLeg {
  px: string;
  sz: string;
  exchange: string;
  leg_ts_ns: number;
  leg_age_ms: number;
}

export interface NBBOMsg {
  t: "nbbo";
  canonical_id: string;
  base: string;
  quote: string;
  best_bid: NBBOLeg;
  best_ask: NBBOLeg;
  // Exact cmpDecimal(ask,bid) < 0 — not the float spread (epsilon false-positives).
  crossed: boolean;
  spread: number;
  mid: number;
  constituents: string[];
  local_ts_ns: number;
}

// Per-exchange venue health from the ingester (md.status.<exchange>). Drives
// connection-state leg eviction in the NBBO aggregator — see DESIGN_nbbo.md.
export interface StatusMsg {
  t: "status";
  exchange: string;
  state: "up" | "down";
  ts_ns: number;
}

export type StreamMsg = BookSnapshotMsg | BookDeltaMsg | TradeMsg | BBOMsg | NBBOMsg | StatusMsg;

// NOTE: *_ts_ns are epoch nanoseconds (~1.7e18), past Number.MAX_SAFE_INTEGER
// (~9.0e15). JSON.parse rounds them to ~200ns granularity — negligible for the
// ms-scale hop-latency we track, but do not treat these as exact ns. A
// BigInt-aware parse is the hardening if exact ns ever matter here.
export function decodeMsg(buf: Buffer): StreamMsg {
  return JSON.parse(buf.toString("utf8")) as StreamMsg;
}

// Mirror of normalize_symbol() in python/common/kafka_io.py: anything outside
// [a-zA-Z0-9._-] becomes '-'. Idempotent, so safe whether the message carries a
// native ("BTC/USD") or already-normalized ("BTC-USD") symbol.
const UNSAFE = /[^a-zA-Z0-9._-]/g;

export function normalizeSymbol(symbol: string): string {
  return symbol.replace(UNSAFE, "-");
}

export function bboTopic(exchange: string, symbol: string): string {
  return `md.bbo.${exchange}.${normalizeSymbol(symbol)}`;
}

// canonical_id is constrained to <BASE>-<QUOTE> (see ops/instruments.yml),
// all topic-safe characters — no normalization required.
export function nbboTopic(canonical_id: string): string {
  return `md.nbbo.${canonical_id}`;
}

// Per-exchange venue-health topic. Mirror of status_topic() in
// python/common/kafka_io.py.
export function statusTopic(exchange: string): string {
  return `md.status.${exchange}`;
}
