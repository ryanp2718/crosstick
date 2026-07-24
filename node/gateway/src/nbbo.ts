import type { CanonicalInstrument } from "./canonical.js";
import { cmpDecimal } from "./decimal.js";
import type { BBOMsg, NBBOLeg, NBBOMsg } from "./messages.js";

// Per-canonical_id NBBO aggregation. Caller is responsible for resolving an
// incoming BBO to its CanonicalInstrument (via CanonicalMap.lookup) before
// calling onBBO - this class knows nothing about the venue→canonical map.
//
// nowMs is always caller-supplied (no Date.now() default): the server passes
// stream time - the max event-time across consumed messages - so every NBBO
// timestamp and leg age is a pure function of the log and replays byte-for-
// byte (D1 in ARCHITECTURE.md).
//
// Semantics:
//   - leg storage: latest BBOMsg per (canonical_id, exchange); a leg never
//     expires here (per-leg staleness is the consumer's call - they read
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
  // Venues evicted from NBBO computation on a connection-state signal (not a
  // quote-age threshold). Legs stay in st.legs so a venue rejoins on recovery.
  private readonly downVenues = new Set<string>();

  onBBO(canonical: CanonicalInstrument, msg: BBOMsg, nowMs: number): NBBOMsg | null {
    let st = this.state.get(canonical.canonical_id);
    if (!st) {
      st = { canonical, legs: new Map(), lastTuple: null };
      this.state.set(canonical.canonical_id, st);
    }
    st.legs.set(msg.exchange, msg);
    const computed = compute(st, this.downVenues, nowMs);
    if (!computed) return null;
    if (st.lastTuple === computed.tuple) return null;
    st.lastTuple = computed.tuple;
    return computed.msg;
  }

  // Mark a venue up/down. On an actual transition, recomputes every canonical
  // holding a leg from that venue and returns the NBBOs to (re)publish - a
  // constituents change is worth emitting even if the winning L1 is unchanged.
  setVenueDown(exchange: string, down: boolean, nowMs: number): NBBOMsg[] {
    if (down === this.downVenues.has(exchange)) return [];
    if (down) this.downVenues.add(exchange);
    else this.downVenues.delete(exchange);

    const out: NBBOMsg[] = [];
    for (const st of this.state.values()) {
      if (!st.legs.has(exchange)) continue;
      const computed = compute(st, this.downVenues, nowMs);
      if (!computed) {
        st.lastTuple = null; // no live legs left; reset so recovery re-emits
        continue;
      }
      st.lastTuple = computed.tuple;
      out.push(computed.msg);
    }
    return out;
  }

  snapshot(nowMs: number): NBBOMsg[] {
    const out: NBBOMsg[] = [];
    for (const st of this.state.values()) {
      const computed = compute(st, this.downVenues, nowMs);
      if (computed) out.push(computed.msg);
    }
    return out;
  }
}

function compute(
  st: CanonicalState,
  downVenues: Set<string>,
  nowMs: number,
): { msg: NBBOMsg; tuple: string } | null {
  const exchanges = [...st.legs.keys()].filter((ex) => !downVenues.has(ex)).sort();
  let bestBidLeg: BBOMsg | null = null;
  let bestAskLeg: BBOMsg | null = null;
  for (const ex of exchanges) {
    const leg = st.legs.get(ex)!;
    if (bestBidLeg === null || beatsBid(leg, bestBidLeg)) bestBidLeg = leg;
    if (bestAskLeg === null || beatsAsk(leg, bestAskLeg)) bestAskLeg = leg;
  }
  if (!bestBidLeg || !bestAskLeg) return null;

  const tuple = `${bestBidLeg.bid_px}|${bestBidLeg.bid_sz}|${bestAskLeg.ask_px}|${bestAskLeg.ask_sz}`;
  // Exact decimal compare, not Number(ask)-Number(bid) < 0 (float epsilon false-cross).
  const crossed = cmpDecimal(bestAskLeg.ask_px, bestBidLeg.bid_px) < 0;
  const bid_px = Number(bestBidLeg.bid_px);
  const ask_px = Number(bestAskLeg.ask_px);
  const msg: NBBOMsg = {
    t: "nbbo",
    canonical_id: st.canonical.canonical_id,
    base: st.canonical.base,
    quote: st.canonical.quote,
    best_bid: legFor(bestBidLeg, "bid", nowMs),
    best_ask: legFor(bestAskLeg, "ask", nowMs),
    crossed,
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
  const c = cmpDecimal(candidate.bid_px, incumbent.bid_px);
  if (c !== 0) return c > 0;
  return cmpDecimal(candidate.bid_sz, incumbent.bid_sz) > 0;
}

function beatsAsk(candidate: BBOMsg, incumbent: BBOMsg): boolean {
  const c = cmpDecimal(candidate.ask_px, incumbent.ask_px);
  if (c !== 0) return c < 0;
  return cmpDecimal(candidate.ask_sz, incumbent.ask_sz) > 0;
}

// A crossed NBBO is only worth counting when both winning legs are fresh. A
// cross carried by a stale leg (one venue quoted, the market moved, its old
// quote now crosses a live one) is a benign artifact of not age-evicting legs,
// not a reconstruction defect. The wire `crossed` flag stays faithful either
// way -- consumers still filter on leg_age_ms at their own threshold.
export function isFreshCross(nbbo: NBBOMsg, maxLegAgeMs: number): boolean {
  return (
    nbbo.crossed &&
    nbbo.best_bid.leg_age_ms <= maxLegAgeMs &&
    nbbo.best_ask.leg_age_ms <= maxLegAgeMs
  );
}

// Cross depth in basis points: how far the best bid exceeds the best ask,
// relative to mid. Positive only when crossed (spread < 0). A materiality floor
// on this separates a benign tick-scale venue lock/cross from a real inversion.
export function crossBps(nbbo: NBBOMsg): number {
  return nbbo.mid > 0 ? (-nbbo.spread / nbbo.mid) * 1e4 : 0;
}

function legFor(src: BBOMsg, side: "bid" | "ask", nowMs: number): NBBOLeg {
  const px = side === "bid" ? src.bid_px : src.ask_px;
  const sz = side === "bid" ? src.bid_sz : src.ask_sz;
  return {
    px,
    sz,
    exchange: src.exchange,
    leg_ts_ns: src.local_ts_ns,
    leg_age_ms: Math.max(0, nowMs - src.local_ts_ns / 1e6),
  };
}
