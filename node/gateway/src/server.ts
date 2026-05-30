import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import http from "node:http";
import path from "node:path";

import { Kafka, logLevel } from "kafkajs";
import { WebSocketServer } from "ws";

import { Aggregator } from "./aggregator.js";
import { Broadcaster } from "./broadcaster.js";
import { loadCanonicalMap } from "./canonical.js";
import { bboTopic, decodeMsg, nbboTopic } from "./messages.js";
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
  wsClients,
} from "./metrics.js";
import { NBBOAggregator } from "./nbbo.js";
import { routeMessage, type RouteResult } from "./router.js";

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
const TOPIC_PATTERNS = [/^md\.book\..+/, /^md\.trades\..+/];

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
    for (const nbbo of nbboAgg.snapshot()) ws.send(JSON.stringify(nbbo));
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
  });
  const admin = kafka.admin();
  await producer.connect();
  await consumer.connect();
  await admin.connect();

  // Pre-create md.nbbo.* topics as compacted so late-joining consumers can
  // bootstrap from the latest message per canonical_id. Idempotent — kafkajs
  // returns false (not an error) when the topic already exists.
  const nbboTopics = canonicalMap.all().map((c) => ({
    topic: nbboTopic(c.canonical_id),
    numPartitions: 1,
    configEntries: [{ name: "cleanup.policy", value: "compact" }],
  }));
  if (nbboTopics.length > 0) {
    await admin.createTopics({ topics: nbboTopics, waitForLeaders: true });
    console.log(
      `[gateway] ensured ${nbboTopics.length} compacted md.nbbo.* topics: ` +
        nbboTopics.map((t) => t.topic).join(", "),
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

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) return;
      const tp = lastOffsets.get(topic) ?? new Map<number, bigint>();
      tp.set(partition, BigInt(message.offset));
      lastOffsets.set(topic, tp);

      let result: RouteResult;
      try {
        result = routeMessage(decodeMsg(message.value), agg, canonicalMap, nbboAgg);
      } catch (err) {
        messagesConsumed.inc({ topic, result: "error" });
        console.error("[gateway] bad message, skipping:", err);
        return;
      }
      messagesConsumed.inc({ topic, result: "ok" });
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

      if (result.nbboPublish) {
        const nbbo = result.nbboPublish;
        nbboConstituents.set({ canonical_id: nbbo.canonical_id }, nbbo.constituents.length);
        if (nbbo.spread < 0) nbboCrossed.inc({ canonical_id: nbbo.canonical_id });
        bboInflight.inc();
        // Same fire-and-forget pattern as md.bbo above. Compacted topic keyed
        // by canonical_id so late consumers can bootstrap from the latest.
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
      }
      if (result.nbboBroadcast) broadcaster.broadcast(result.nbboBroadcast);
    },
  });

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
