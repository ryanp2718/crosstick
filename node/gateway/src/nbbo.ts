import type { CanonicalInstrument } from "./canonical.js";
import type { BBOMsg, NBBOLeg, NBBOMsg } from "./messages.js";

// Per-canonical_id NBBO aggregation. Caller is responsible for resolving an
// incoming BBO to its CanonicalInstrument (via CanonicalMap.lookup) before
// calling onBBO — this class knows nothing about the venue→canonical map.
//
// Semantics:
//   - leg storage: latest BBOMsg per (canonical_id, exchange); a leg never
//     expires here (per-leg staleness is the consumer's call — they read
//     leg_age_ms and filter at their own threshold).
//   - winner selection: highest bid_px, lowest ask_px; tie-break by larger
//     size, then alphabetical exchange.
//   - onBBO emit-gate: L1 tuple (bid_px|bid_sz|ask_px|ask_sz) as strings
//     (exact source values). Unchanged → null. NBBO is a surface stream;
//     routing-grade venue-switch resolution lives on md.bbo.* per-exchange
//     topics.
//   - snapshot() always recomputes from current legs (reflects in-flight leg
//     switches that onBBO dedup'd, so new WS clients see the true current
//     winner, not whichever one was emitted last on the wire).

interface CanonicalState {
  canonical: CanonicalInstrument;
  legs: Map<string, BBOMsg>;
  lastTuple: string | null;
}

export class NBBOAggregator {
  private readonly state = new Map<string, CanonicalState>();

  onBBO(canonical: CanonicalInstrument, msg: BBOMsg, nowMs: number = Date.now()): NBBOMsg | null {
    let st = this.state.get(canonical.canonical_id);
    if (!st) {
      st = { canonical, legs: new Map(), lastTuple: null };
      this.state.set(canonical.canonical_id, st);
    }
    st.legs.set(msg.exchange, msg);
    const computed = compute(st, nowMs);
    if (!computed) return null;
    if (st.lastTuple === computed.tuple) return null;
    st.lastTuple = computed.tuple;
    return computed.msg;
  }

  snapshot(nowMs: number = Date.now()): NBBOMsg[] {
    const out: NBBOMsg[] = [];
    for (const st of this.state.values()) {
      const computed = compute(st, nowMs);
      if (computed) out.push(computed.msg);
    }
    return out;
  }
}

function compute(st: CanonicalState, nowMs: number): { msg: NBBOMsg; tuple: string } | null {
  const exchanges = [...st.legs.keys()].sort();
  let bestBidLeg: BBOMsg | null = null;
  let bestAskLeg: BBOMsg | null = null;
  for (const ex of exchanges) {
    const leg = st.legs.get(ex)!;
    if (bestBidLeg === null || beatsBid(leg, bestBidLeg)) bestBidLeg = leg;
    if (bestAskLeg === null || beatsAsk(leg, bestAskLeg)) bestAskLeg = leg;
  }
  if (!bestBidLeg || !bestAskLeg) return null;

  const tuple = `${bestBidLeg.bid_px}|${bestBidLeg.bid_sz}|${bestAskLeg.ask_px}|${bestAskLeg.ask_sz}`;
  const bid_px = Number(bestBidLeg.bid_px);
  const ask_px = Number(bestAskLeg.ask_px);
  const msg: NBBOMsg = {
    t: "nbbo",
    canonical_id: st.canonical.canonical_id,
    base: st.canonical.base,
    quote: st.canonical.quote,
    best_bid: legFor(bestBidLeg, "bid", nowMs),
    best_ask: legFor(bestAskLeg, "ask", nowMs),
    spread: ask_px - bid_px,
    mid: (ask_px + bid_px) / 2,
    constituents: exchanges,
    local_ts_ns: nowMs * 1e6,
  };
  return { msg, tuple };
}

// Bid winner: higher price; on tie, larger size; on full tie, alphabetical
// exchange. Caller iterates exchanges already sorted alphabetically and only
// replaces on strict win, so alphabetical fallback is implicit (first wins).
function beatsBid(candidate: BBOMsg, incumbent: BBOMsg): boolean {
  const cp = Number(candidate.bid_px);
  const ip = Number(incumbent.bid_px);
  if (cp !== ip) return cp > ip;
  return Number(candidate.bid_sz) > Number(incumbent.bid_sz);
}

function beatsAsk(candidate: BBOMsg, incumbent: BBOMsg): boolean {
  const cp = Number(candidate.ask_px);
  const ip = Number(incumbent.ask_px);
  if (cp !== ip) return cp < ip;
  return Number(candidate.ask_sz) > Number(incumbent.ask_sz);
}

function legFor(src: BBOMsg, side: "bid" | "ask", nowMs: number): NBBOLeg {
  const px = side === "bid" ? Number(src.bid_px) : Number(src.ask_px);
  const sz = side === "bid" ? Number(src.bid_sz) : Number(src.ask_sz);
  return {
    px,
    sz,
    exchange: src.exchange,
    leg_ts_ns: src.local_ts_ns,
    leg_age_ms: Math.max(0, nowMs - src.local_ts_ns / 1e6),
  };
}
