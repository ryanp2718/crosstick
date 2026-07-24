import type { Aggregator } from "./aggregator.js";
import type { CanonicalMap } from "./canonical.js";
import type { BBOMsg, NBBOMsg, StreamMsg } from "./messages.js";
import type { NBBOAggregator } from "./nbbo.js";

export interface RouteResult {
  publish: BBOMsg | null; // -> md.bbo.* on Kafka
  broadcast: StreamMsg | null; // -> WS clients (per-exchange BBO or trade relay)
  nbboPublish: NBBOMsg | null; // -> md.nbbo.* on Kafka
  nbboBroadcast: NBBOMsg | null; // -> WS clients (cross-exchange NBBO)
}

const EMPTY: RouteResult = {
  publish: null,
  broadcast: null,
  nbboPublish: null,
  nbboBroadcast: null,
};

// Pure decision layer between the Kafka consumer and the producer/fan-out.
// Book messages drive per-exchange BBO derivation; if the (exchange, symbol)
// is mapped to a canonical instrument, the BBO also feeds NBBO aggregation.
// Trades are relayed to clients only. nowMs is the caller's stream time (max
// input event-time), not wall clock - see D1 in ARCHITECTURE.md.
export function routeMessage(
  msg: StreamMsg,
  agg: Aggregator,
  canonicalMap: CanonicalMap,
  nbboAgg: NBBOAggregator,
  nowMs: number,
): RouteResult {
  if (msg.t === "snap" || msg.t === "delta") {
    const bbo = agg.applyBook(msg);
    if (!bbo) return EMPTY;
    const canonical = canonicalMap.lookup(bbo.exchange, bbo.symbol);
    const nbbo = canonical ? nbboAgg.onBBO(canonical, bbo, nowMs) : null;
    return { publish: bbo, broadcast: bbo, nbboPublish: nbbo, nbboBroadcast: nbbo };
  }
  if (msg.t === "trade") {
    return { publish: null, broadcast: msg, nbboPublish: null, nbboBroadcast: null };
  }
  // "bbo"/"nbbo" are gateway outputs, not inputs; ignore if seen on the wire.
  return EMPTY;
}
