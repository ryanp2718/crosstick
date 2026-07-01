import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import http from "node:http";
import path from "node:path";

import { Kafka, logLevel } from "kafkajs";
import { WebSocketServer } from "ws";

import { Aggregator } from "./aggregator.js";
import { Broadcaster } from "./broadcaster.js";
import { loadCanonicalMap } from "./canonical.js";
import { bboTopic, decodeMsg, nbboTopic, statusTopic } from "./messages.js";
import type { NBBOMsg, StatusMsg } from "./messages.js";
import {
  aggregatorStreams,
  bboInflight,
  bboProduced,
  consumerLag,
  messagesConsumed,
  nbboConstituents,
  nbboCrossed,
  nbboProduced,
  registry,
  venueUp,
  wsClients,
} from "./metrics.js";
import { isFreshCross, NBBOAggregator } from "./nbbo.js";
import { routeMessage, type RouteResult } from "./router.js";
import { DrainGate, planWarmStart } from "./warmstart.js";

// ── config (env, with dev-friendly defaults) ────────────────────────────────
const BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:19092")
  .split(",")
  .map((b) => b.trim())
  .filter(Boolean);
const WS_PORT = Number(process.env.WS_PORT ?? 8080);
const GROUP_ID = process.env.KAFKA_GROUP_ID ?? "gateway";
// Per-client socket-buffer ceiling before we close+resync the slow consumer.
const MAX_BUFFERED_BYTES = Number(process.env.WS_MAX_BUFFER_BYTES ?? 1_000_000);
// Architectural ceiling for an individual Kafka message, mirroring
// MAX_MESSAGE_BYTES in python/common/kafka_io.py. Coinbase's full L2 snapshot
// is ~1.1 MiB on the wire, over kafkajs's 1 MiB default maxBytesPerPartition.
const MAX_MESSAGE_BYTES = 8 * 1024 * 1024;
// Resolve dashboard/ from CWD (container: /app/dashboard) with a dev fallback
// (gateway run from node/gateway/ → ../../dashboard).
const DASHBOARD_DIR = path.resolve(
  process.env.DASHBOARD_DIR ??
    (existsSync(path.resolve(process.cwd(), "dashboard"))
      ? path.resolve(process.cwd(), "dashboard")
      : path.resolve(process.cwd(), "..", "..", "dashboard")),
);
const INSTRUMENTS_FILE = path.resolve(
  process.env.INSTRUMENTS_FILE ??
    (existsSync(path.resolve(process.cwd(), "ops", "instruments.yml"))
      ? path.resolve(process.cwd(), "ops", "instruments.yml")
      : path.resolve(process.cwd(), "..", "..", "ops", "instruments.yml")),
);
const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

// Regex subscription. NOTE: kafkajs matches these against topics that exist at
// subscribe time; topics created later need a gateway restart to be picked up.
// Fine for the compose stack (ingesters produce on startup); a metadata-refresh
// resubscribe is the production hardening.
const TOPIC_PATTERNS = [/^md\.book\..+/, /^md\.trades\..+/, /^md\.status\..+/];
// A venue is evicted from NBBO when the stream clock moves more than this far
// past its last md.status heartbeat ts_ns (covers an ingester crash/kill that
// sends no graceful "down"). Measured in log time, not wall clock, so eviction
// replays deterministically (D1). Ingesters heartbeat every 2s.
const LIVENESS_TIMEOUT_MS = Number(process.env.NBBO_LIVENESS_TIMEOUT_MS ?? 5000);
// A crossed NBBO only increments gateway_nbbo_crossed_total when both winning
// legs are no older than this (stream time). Older than this and the cross is a
// benign stale-leg artifact, not corruption, so it must not page. Well under the
// 5s liveness eviction: a leg that survives eviction can still be too stale to
// trust as a live cross. The wire `crossed` flag is unaffected (stays faithful).
const NBBO_CROSS_MAX_LEG_AGE_MS = Number(process.env.NBBO_CROSS_MAX_LEG_AGE_MS ?? 1000);
// Warm-start lookback (D2b): how far back to search for a book snapshot on
// restart. Must exceed the ingesters' snapshot_interval_s (300s) so a healthy
// stream always has one inside the window; 2x gives slack for a venue that
// missed an interval. See src/warmstart.ts for the per-topic-class seek rules.
const WARMSTART_LOOKBACK_MS = Number(process.env.WARMSTART_LOOKBACK_MS ?? 600_000);
// Consumer group session timeout. The kafkajs default (30s) is too tight for the
// warm-start replay against the single-shard (smp=1) redpanda: the replay's
// fetches plus the ingesters' produce firehose serialize on one shard, so
// heartbeats queue behind them and miss the window — the coordinator evicts the
// member ("not aware of this member") and the drain livelocks (see warmstart.ts).
// 90s lets a delayed heartbeat land in the load gaps before eviction; well within
// redpanda's 300s group_max_session_timeout_ms. A multi-shard/managed broker
// removes the contention, so this is just a conservative default there.
const KAFKA_SESSION_TIMEOUT_MS = Number(process.env.KAFKA_SESSION_TIMEOUT_MS ?? 90_000);
// Must be >= sessionTimeout (kafkajs default 60s), so it is raised alongside.
const KAFKA_REBALANCE_TIMEOUT_MS = Number(process.env.KAFKA_REBALANCE_TIMEOUT_MS ?? 90_000);
// Total fetch ceiling. Left at the kafkajs default; exposed only as an escape
// hatch — a smaller cap eases shard saturation if the timeout raise alone doesn't
// stop warm-start evictions, but it slows the drain and would hurt replay over a
// future high-latency remote/tiered log, so it stays default until proven needed.
const KAFKA_MAX_BYTES = Number(process.env.KAFKA_MAX_BYTES ?? 10 * 1024 * 1024);
// Hand control back to the event loop every N consumed messages. kafkajs runs
// eachMessage for every message in a fetched batch; a synchronous handler drains
// the whole batch's microtasks before the loop services I/O, so a large bursty
// batch (warm-start replay or catch-up after lag) freezes the HTTP /metrics
// endpoint and WS broadcasts for seconds and flaps the healthcheck. Yielding
// every N messages bounds that stall; N is large enough that the per-batch
// steady-state adds ~no overhead.
const CONSUME_YIELD_EVERY = Number(process.env.CONSUME_YIELD_EVERY ?? 500);

async function serveStatic(rel: string, res: http.ServerResponse): Promise<void> {
  const full = path.resolve(DASHBOARD_DIR, rel);
  // Path-traversal guard: resolved file must stay inside DASHBOARD_DIR.
  if (full !== DASHBOARD_DIR && !full.startsWith(DASHBOARD_DIR + path.sep)) {
    res.writeHead(403);
    res.end();
    return;
  }
  try {
    const buf = await readFile(full);
    res.writeHead(200, { "content-type": MIME[path.extname(full)] ?? "application/octet-stream" });
    res.end(buf);
  } catch {
    res.writeHead(404);
    res.end();
  }
}

async function main(): Promise<void> {
  // Load canonical map first — fail fast on a malformed instruments.yml
  // instead of discovering it only when the first BBO routes through.
  const canonicalMap = loadCanonicalMap(INSTRUMENTS_FILE);
  console.log(
    `[gateway] loaded ${canonicalMap.all().length} canonical instruments from ${INSTRUMENTS_FILE}`,
  );
  const agg = new Aggregator();
  const nbboAgg = new NBBOAggregator();
  const broadcaster = new Broadcaster(MAX_BUFFERED_BYTES);

  // Stream clock: max event-time (ns→ms) across consumed messages. Every NBBO
  // timestamp, leg age, and liveness-eviction decision derives from it instead
  // of Date.now(), so md.nbbo.* is a pure function of the log (D1). Tradeoff:
  // if ALL ingesters go silent the clock freezes and nothing is evicted — but
  // then nothing is emitted either; per-leg ages are the consumer's staleness
  // signal, and ingester liveness is alerted on independently via Prometheus.
  let streamNowMs = 0;

  // Warm-start output gate (warmstart.ts → DrainGate). Null until the warm-start
  // plan is applied; while `gate.warming`, derived md.bbo/md.nbbo are held so an
  // uneven backlog drain can't emit a stale-leg phantom cross. A clean start
  // plans no backlog and never arms it.
  let gate: DrainGate | null = null;

  // ── WS server (+ tiny HTTP for health, dashboard, metrics, upgrade) ──────
  const httpServer = http.createServer((req, res) => {
    if (req.url === "/healthz") {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("ok");
      return;
    }
    if (req.url === "/metrics") {
      void registry.metrics().then((body) => {
        res.writeHead(200, { "content-type": registry.contentType });
        res.end(body);
      });
      return;
    }
    if (req.url === "/" || req.url === "/dashboard" || req.url === "/dashboard/") {
      void serveStatic("index.html", res);
      return;
    }
    if (req.url?.startsWith("/dashboard/")) {
      void serveStatic(req.url.slice("/dashboard/".length), res);
      return;
    }
    res.writeHead(404);
    res.end();
  });
  const wss = new WebSocketServer({ server: httpServer, path: "/ws" });
  wss.on("connection", (ws) => {
    // Snapshot-on-connect before adding to broadcaster: dashboard renders
    // immediately during quiet periods. Tiny window where broadcasts firing
    // between these two calls are missed; self-heals on the next L1 move.
    for (const bbo of agg.snapshot()) ws.send(JSON.stringify(bbo));
    for (const nbbo of nbboAgg.snapshot(streamNowMs)) ws.send(JSON.stringify(nbbo));
    broadcaster.add(ws);
    wsClients.inc();
    // "close" is guaranteed to fire exactly once (also after "error"); dec here.
    ws.once("close", () => {
      broadcaster.remove(ws);
      wsClients.dec();
    });
    ws.on("error", () => broadcaster.remove(ws));
  });
  httpServer.listen(WS_PORT, () => console.log(`[gateway] ws listening on :${WS_PORT}/ws`));

  // ── Kafka in (book/trades) and out (md.bbo) ───────────────────────────────
  const kafka = new Kafka({ clientId: "gateway", brokers: BROKERS, logLevel: logLevel.WARN });
  const producer = kafka.producer({ allowAutoTopicCreation: true });
  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    // Fetch ceiling per partition — must clear the largest message we expect
    // to consume (Coinbase L2 snapshot ~1.1 MiB), else the broker truncates
    // and the fetch loops without progress.
    maxBytesPerPartition: MAX_MESSAGE_BYTES,
    maxBytes: KAFKA_MAX_BYTES,
    // Raised from the 30s default so warm-start replay can't get the member
    // evicted off the single-shard broker (see KAFKA_SESSION_TIMEOUT_MS).
    sessionTimeout: KAFKA_SESSION_TIMEOUT_MS,
    rebalanceTimeout: KAFKA_REBALANCE_TIMEOUT_MS,
  });
  const admin = kafka.admin();
  await producer.connect();
  await consumer.connect();
  await admin.connect();

  // Pre-create md.nbbo.* topics as compacted so late-joining consumers can
  // bootstrap from the latest message per canonical_id. Idempotent — kafkajs
  // returns false (not an error) when the topic already exists.
  const compacted = [{ name: "cleanup.policy", value: "compact" }];
  const nbboTopics = canonicalMap.all().map((c) => ({
    topic: nbboTopic(c.canonical_id),
    numPartitions: 1,
    configEntries: compacted,
  }));
  // One status topic per distinct venue across all instruments (compacted so a
  // late/ restarted consumer reads the latest liveness per exchange).
  const exchanges = [
    ...new Set(canonicalMap.all().flatMap((c) => c.venues.map((v) => v.exchange))),
  ];
  const statusTopics = exchanges.map((ex) => ({
    topic: statusTopic(ex),
    numPartitions: 1,
    configEntries: compacted,
  }));
  const toCreate = [...nbboTopics, ...statusTopics];
  if (toCreate.length > 0) {
    await admin.createTopics({ topics: toCreate, waitForLeaders: true });
    console.log(
      `[gateway] ensured ${nbboTopics.length} compacted md.nbbo.* + ` +
        `${statusTopics.length} md.status.* topics`,
    );
  }

  await consumer.subscribe({ topics: TOPIC_PATTERNS, fromBeginning: false });
  console.log(`[gateway] consuming md.book.* / md.trades.* from ${BROKERS.join(",")}`);

  // In-flight send promises (post fix #1: fire-and-forget). Tracked so
  // shutdown can drain them before producer.disconnect() — kafkajs's disconnect
  // aborts pending sends rather than flushing (verified in
  // node_modules/kafkajs/src/producer/index.js:230).
  const inFlight = new Set<Promise<unknown>>();
  const SHUTDOWN_DRAIN_MS = 5000;

  // Last consumed offset per (topic, partition); used by the lag poll.
  const lastOffsets = new Map<string, Map<number, bigint>>();

  // Publish an NBBO to its compacted topic (fire-and-forget) + broadcast to WS.
  // Reused by the live book path and by status-driven recompute.
  const emitNbbo = (nbbo: NBBOMsg): void => {
    if (gate?.warming) return; // held through the warm-start drain
    nbboConstituents.set({ canonical_id: nbbo.canonical_id }, nbbo.constituents.length);
    if (isFreshCross(nbbo, NBBO_CROSS_MAX_LEG_AGE_MS)) {
      nbboCrossed.inc({ canonical_id: nbbo.canonical_id });
    }
    bboInflight.inc();
    const p: Promise<unknown> = producer
      .send({
        topic: nbboTopic(nbbo.canonical_id),
        messages: [{ key: nbbo.canonical_id, value: JSON.stringify(nbbo) }],
      })
      .then(() => nbboProduced.inc({ canonical_id: nbbo.canonical_id, result: "ok" }))
      .catch((err) => {
        nbboProduced.inc({ canonical_id: nbbo.canonical_id, result: "error" });
        console.error("[gateway] kafka nbbo produce failed:", err);
      })
      .finally(() => {
        inFlight.delete(p);
        bboInflight.dec();
      });
    inFlight.add(p);
    broadcaster.broadcast(nbbo);
  };

  // Venue health: last md.status heartbeat per exchange, in heartbeat log time
  // (ts_ns, not consume time). A status transition evicts/re-adds that venue's
  // legs and re-emits the affected NBBOs so a crossed book from a dead leg
  // clears immediately, not on the next tick.
  const lastSeenMs = new Map<string, number>();
  const handleStatus = (msg: StatusMsg): void => {
    lastSeenMs.set(msg.exchange, msg.ts_ns / 1e6);
    const up = msg.state === "up";
    venueUp.set({ exchange: msg.exchange }, up ? 1 : 0);
    for (const nbbo of nbboAgg.setVenueDown(msg.exchange, !up, streamNowMs)) emitNbbo(nbbo);
  };

  // Missed-heartbeat eviction, evaluated as the stream clock advances (every
  // consumed message) rather than on a wall-clock interval: a replayed log
  // reproduces the same evictions at the same points in the stream.
  // setVenueDown is idempotent, so re-checking an already-down venue is free.
  const evictSilentVenues = (): void => {
    for (const [exchange, seenMs] of lastSeenMs) {
      if (streamNowMs - seenMs > LIVENESS_TIMEOUT_MS) {
        venueUp.set({ exchange }, 0);
        for (const nbbo of nbboAgg.setVenueDown(exchange, true, streamNowMs)) emitNbbo(nbbo);
      }
    }
  };

  // Rolling count since the last event-loop yield (see CONSUME_YIELD_EVERY).
  let sinceYield = 0;
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) return;
      // Periodically yield so a large bursty batch can't starve I/O (HTTP
      // /metrics, WS writes) by draining its whole microtask chain first.
      if (++sinceYield >= CONSUME_YIELD_EVERY) {
        sinceYield = 0;
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      const tp = lastOffsets.get(topic) ?? new Map<number, bigint>();
      tp.set(partition, BigInt(message.offset));
      lastOffsets.set(topic, tp);

      let result: RouteResult;
      try {
        const decoded = decodeMsg(message.value);
        // Advance the stream clock, then evict before routing so the NBBO
        // computed for this message already excludes newly-silent venues.
        const eventTimeNs = decoded.t === "status" ? decoded.ts_ns : decoded.local_ts_ns;
        streamNowMs = Math.max(streamNowMs, eventTimeNs / 1e6);
        evictSilentVenues();
        if (decoded.t === "status") {
          messagesConsumed.inc({ topic, result: "ok" });
          handleStatus(decoded);
          return;
        }
        result = routeMessage(decoded, agg, canonicalMap, nbboAgg, streamNowMs);
      } catch (err) {
        messagesConsumed.inc({ topic, result: "error" });
        console.error("[gateway] bad message, skipping:", err);
        return;
      }
      messagesConsumed.inc({ topic, result: "ok" });
      // Hold derived output while the warm-start backlog drains; the book is
      // still rebuilt above (routeMessage), just not (re)published yet.
      if (!gate?.warming) {
        if (result.publish) {
          const bbo = result.publish;
          bboInflight.inc();
          // Fire-and-forget: awaiting producer.send here would cap publish rate
          // at 1/broker-RTT per partition (same antipattern the python ingester
          // removed in a6d1fae). The gateway is stateless — a dropped md.bbo
          // publish is recovered by the next L1 move; no resync needed.
          const p: Promise<unknown> = producer
            .send({
              topic: bboTopic(bbo.exchange, bbo.symbol),
              messages: [{ key: `${bbo.exchange}:${bbo.symbol}`, value: JSON.stringify(bbo) }],
            })
            .then(() => bboProduced.inc({ result: "ok" }))
            .catch((err) => {
              bboProduced.inc({ result: "error" });
              console.error("[gateway] kafka produce failed:", err);
            })
            .finally(() => {
              inFlight.delete(p);
              bboInflight.dec();
            });
          inFlight.add(p);
        }
        if (result.broadcast) broadcaster.broadcast(result.broadcast);

        // nbboPublish and nbboBroadcast are the same object (see router.ts);
        // emitNbbo does both the compacted-topic publish and the WS broadcast.
        if (result.nbboPublish) emitNbbo(result.nbboPublish);
      }

      // The message that drains the last backlogged book partition opens the
      // gate; flush a snapshot so the compacted md.nbbo carries the true
      // current state, not whatever the pre-restart edge left there.
      if (gate?.observe(topic, partition, message.offset)) {
        console.log("[gateway] warm-start drain complete — resuming derived output");
        for (const nbbo of nbboAgg.snapshot(streamNowMs)) emitNbbo(nbbo);
      }
    },
  });

  // Warm-start seeks (D2b): re-derive in-memory state from the log instead of
  // resuming at committed group offsets with empty books. Must run after
  // consumer.run() — kafkajs only accepts seek() on a running consumer. A few
  // messages may consume from the old position before the seeks land; the
  // aggregator's order-insensitivity (D2a) makes that prefix harmless.
  const warmTopics = (await admin.listTopics()).filter((t) =>
    TOPIC_PATTERNS.some((re) => re.test(t)),
  );
  const seeks = await planWarmStart(admin, warmTopics, Date.now(), WARMSTART_LOOKBACK_MS);
  for (const s of seeks) consumer.seek(s);
  // Arm the gate only after the seeks land (no await between): the pre-seek
  // prefix consumed from the committed edge is the existing harmless replay
  // (D2a), but its offsets sit at/after the backlog targets and would open the
  // gate spuriously — so observe() must run only against the post-seek backlog.
  gate = new DrainGate(seeks);
  console.log(
    `[gateway] warm-start: planned ${seeks.length} seeks across ${warmTopics.length} topics ` +
      `(lookback ${WARMSTART_LOOKBACK_MS}ms)` +
      (gate.warming ? "; holding derived output until the book backlog drains" : ""),
  );

  // Periodic lag + aggregator-size refresh. fetchTopicOffsets returns the HWM
  // (= offset of next message that *would* be written), so a fully caught-up
  // consumer with lastConsumed = HWM - 1 yields lag 0.
  const LAG_POLL_MS = 5000;
  const lagPoll = setInterval(() => {
    aggregatorStreams.set(agg.snapshot().length);
    for (const [topic, partitions] of lastOffsets) {
      admin
        .fetchTopicOffsets(topic)
        .then((offsets) => {
          for (const { partition, offset } of offsets) {
            const consumed = partitions.get(partition);
            if (consumed === undefined) continue;
            const lag = Number(BigInt(offset) - 1n - consumed);
            consumerLag.set({ topic, partition: String(partition) }, Math.max(0, lag));
          }
        })
        .catch((err) => console.error(`[gateway] lag poll failed for ${topic}:`, err));
    }
  }, LAG_POLL_MS);
  lagPoll.unref();

  // ── graceful shutdown ─────────────────────────────────────────────────────
  const shutdown = async (sig: string): Promise<void> => {
    console.log(`[gateway] ${sig} → shutting down`);
    try {
      clearInterval(lagPoll);
      // Stop accepting new messages (so no new sends are issued), then drain
      // any in-flight sends before disconnecting the producer — disconnect
      // aborts pending sends rather than flushing them.
      await consumer.disconnect();
      if (inFlight.size > 0) {
        console.log(`[gateway] draining ${inFlight.size} in-flight sends`);
        await Promise.race([
          Promise.allSettled([...inFlight]),
          new Promise((r) => setTimeout(r, SHUTDOWN_DRAIN_MS)),
        ]);
      }
      await producer.disconnect();
      await admin.disconnect();
      wss.close();
      httpServer.close();
    } finally {
      process.exit(0);
    }
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((err) => {
  console.error("[gateway] fatal:", err);
  process.exit(1);
});
