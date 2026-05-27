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

export type StreamMsg = BookSnapshotMsg | BookDeltaMsg | TradeMsg | BBOMsg;

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
