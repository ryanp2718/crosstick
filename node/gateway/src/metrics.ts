import { Counter, Gauge, Registry, collectDefaultMetrics } from "prom-client";

// Single process-wide registry. Modules increment named metrics directly;
// /metrics scrapes via registry.metrics(). collectDefaultMetrics adds the
// stock node process metrics (rss, gc, event-loop lag, fd count).
export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: "gateway_" });

export const messagesConsumed = new Counter({
  name: "gateway_messages_consumed_total",
  help: "Kafka messages consumed by the gateway",
  labelNames: ["topic", "result"] as const,
  registers: [registry],
});

export const bboProduced = new Counter({
  name: "gateway_bbo_produced_total",
  help: "BBO messages produced to md.bbo.* (counted on send completion)",
  labelNames: ["result"] as const,
  registers: [registry],
});

export const bboInflight = new Gauge({
  name: "gateway_bbo_inflight_sends",
  help: "In-flight md.bbo producer sends awaiting ack",
  registers: [registry],
});

export const wsClients = new Gauge({
  name: "gateway_ws_clients",
  help: "Currently connected WS clients",
  registers: [registry],
});

export const wsBroadcasts = new Counter({
  name: "gateway_ws_broadcasts_total",
  help: "Messages broadcast (one increment per broadcast call, regardless of client count)",
  registers: [registry],
});

export const wsSlowDrops = new Counter({
  name: "gateway_ws_slow_drops_total",
  help: "WS clients closed for exceeding the bufferedAmount ceiling",
  registers: [registry],
});

export const consumerLag = new Gauge({
  name: "gateway_consumer_lag_messages",
  help: "HWM - lastConsumedOffset per topic/partition, refreshed periodically",
  labelNames: ["topic", "partition"] as const,
  registers: [registry],
});

export const aggregatorStreams = new Gauge({
  name: "gateway_aggregator_streams",
  help: "Number of (exchange, symbol) streams tracked in the aggregator",
  registers: [registry],
});

export const nbboProduced = new Counter({
  name: "gateway_nbbo_produced_total",
  help: "NBBO messages produced to md.nbbo.* (counted on send completion)",
  labelNames: ["canonical_id", "result"] as const,
  registers: [registry],
});

export const nbboConstituents = new Gauge({
  name: "gateway_nbbo_constituents",
  help: "Number of venues currently contributing a leg to each canonical NBBO",
  labelNames: ["canonical_id"] as const,
  registers: [registry],
});

export const nbboCrossed = new Counter({
  name: "gateway_nbbo_crossed_total",
  help: "Crossed NBBO emissions where both winning legs are fresh (stale-leg crosses excluded; see NBBO_CROSS_MAX_LEG_AGE_MS)",
  labelNames: ["canonical_id"] as const,
  registers: [registry],
});

// Per-venue INTERNAL crossed top-of-book (ask < bid within one exchange's book).
// Distinct from nbboCrossed (cross-venue): this catches book-reconstruction
// corruption — the 06-12 warm-start failure that was entirely unobserved.
export const bboCrossed = new Counter({
  name: "gateway_bbo_crossed_total",
  help: "Per-exchange BBO emissions with a crossed book (ask < bid within the venue)",
  labelNames: ["exchange"] as const,
  registers: [registry],
});

export const venueUp = new Gauge({
  name: "gateway_venue_up",
  help: "Venue health from md.status.* / liveness timeout (1 up, 0 down)",
  labelNames: ["exchange"] as const,
  registers: [registry],
});
