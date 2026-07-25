import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";

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
  help: "Derived messages enqueued or awaiting producer ack",
  registers: [registry],
});

export const produceFlushes = new Counter({
  name: "gateway_produce_flushes_total",
  help: "Producer sendBatch flushes; produced totals / flushes = realized batch factor",
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

// Fresh crosses that also clear the materiality floor (NBBO_CROSS_MIN_BPS). The
// raw counter above still fires on the benign sub-bp venue lock/cross baseline;
// this is the subset worth paging on (a real tens-of-bps inversion).
export const nbboCrossedMaterial = new Counter({
  name: "gateway_nbbo_crossed_material_total",
  help: "Fresh crossed NBBO emissions with cross depth >= NBBO_CROSS_MIN_BPS bps (benign tick-scale venue locks excluded)",
  labelNames: ["canonical_id"] as const,
  registers: [registry],
});

// Per-venue INTERNAL crossed top-of-book (ask < bid within one exchange's book).
// Distinct from nbboCrossed (cross-venue): this catches book-reconstruction
// corruption - the 06-12 warm-start failure that was entirely unobserved.
export const bboCrossed = new Counter({
  name: "gateway_bbo_crossed_total",
  help: "Per-exchange BBO emissions with a crossed book (ask < bid within the venue)",
  labelNames: ["exchange"] as const,
  registers: [registry],
});

// Same-epoch re-snapshots skipped because the book already passed their
// sequence. The ingester stamps a periodic re-snapshot at the seq captured
// before an in-flight delta was applied; in arrival order that delta lands
// first, so resetting to the older snapshot would rewind and resurrect deleted
// levels (the residual crossed-book source after the 06-12 epoch fix).
export const bookSnapshotStale = new Counter({
  name: "gateway_book_snapshot_stale_total",
  help: "Stale same-epoch re-snapshots skipped to avoid rewinding the book",
  labelNames: ["exchange"] as const,
  registers: [registry],
});

// Stale same-epoch re-snapshots applied ANYWAY because the book was crossed
// (corrupt): the snapshot resyncs it instead of the guard skipping it, healing a
// warm-start-stranded cross within one re-snapshot interval. A steady stream here
// means a book is repeatedly corrupting (investigate the onset); a few after a
// restart is the warm-start heal working as intended.
export const bookResnapshotHeal = new Counter({
  name: "gateway_book_resnapshot_heal_total",
  help: "Crossed books healed by applying a same-epoch re-snapshot the guard would otherwise skip",
  labelNames: ["exchange"] as const,
  registers: [registry],
});

// Deltas replayed from the applied tail on top of a heal re-snapshot: the
// depth of the rewind the heal performed. Depth 0-2 is the live cross-topic
// race (snapshot consumed just behind a delta it predates); hundreds+ means
// warm-start batch skew. See aggregator.ts MAX_APPLIED_TAIL.
export const bookHealReplayDepth = new Histogram({
  name: "gateway_book_heal_replay_depth",
  help: "Applied deltas replayed over a heal re-snapshot (the tail its rewind would otherwise discard)",
  labelNames: ["exchange"] as const,
  buckets: [0, 1, 2, 5, 10, 25, 100, 500, 2500, 10000],
  registers: [registry],
});

// Heals whose replay tail had lost entries to overflow eviction that the
// snapshot's seq still needed (evicted seq > snapshot seq): the replay may be
// incomplete and the book can carry a stale level until the next re-snapshot.
// Nonzero here means MAX_APPLIED_TAIL is undersized for the observed rewinds.
export const bookHealReplayUnderrun = new Counter({
  name: "gateway_book_heal_replay_underrun_total",
  help: "Heal replays that could not reach back to the snapshot seq (tail entries evicted)",
  labelNames: ["exchange"] as const,
  registers: [registry],
});

// Flips to 1 only after the warm-start seeks have been issued (server.ts).
// Replay tooling (demo/) gates producing on this: a record produced before the
// plan lands carries an old timestamp and would be seeked past.
export const warmstartPlanned = new Gauge({
  name: "gateway_warmstart_planned",
  help: "1 once the warm-start seek plan has been applied (0 during startup)",
  registers: [registry],
});

export const venueUp = new Gauge({
  name: "gateway_venue_up",
  help: "Venue health from md.status.* / liveness timeout (1 up, 0 down)",
  labelNames: ["exchange"] as const,
  registers: [registry],
});
