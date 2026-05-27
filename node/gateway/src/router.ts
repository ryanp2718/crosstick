import type { Aggregator } from "./aggregator.js";
import type { BBOMsg, StreamMsg } from "./messages.js";

export interface RouteResult {
  publish: BBOMsg | null; // -> md.bbo.* on Kafka
  broadcast: StreamMsg | null; // -> WS clients
}

// Pure decision layer between the Kafka consumer and the producer/fan-out:
// book messages drive BBO derivation (published + broadcast when L1 moves);
// trades are relayed to clients only. Keeps the kafkajs/ws glue trivial.
export function routeMessage(msg: StreamMsg, agg: Aggregator): RouteResult {
  if (msg.t === "snap" || msg.t === "delta") {
    const bbo = agg.applyBook(msg);
    return { publish: bbo, broadcast: bbo };
  }
  if (msg.t === "trade") {
    return { publish: null, broadcast: msg };
  }
  // "bbo" is something this gateway produces, not consumes; ignore if seen.
  return { publish: null, broadcast: null };
}
