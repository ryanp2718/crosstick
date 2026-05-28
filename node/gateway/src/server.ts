import http from "node:http";

import { Kafka, logLevel } from "kafkajs";
import { WebSocketServer } from "ws";

import { Aggregator } from "./aggregator.js";
import { Broadcaster } from "./broadcaster.js";
import { bboTopic, decodeMsg } from "./messages.js";
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

// Regex subscription. NOTE: kafkajs matches these against topics that exist at
// subscribe time; topics created later need a gateway restart to be picked up.
// Fine for the compose stack (ingesters produce on startup); a metadata-refresh
// resubscribe is the production hardening.
const TOPIC_PATTERNS = [/^md\.book\..+/, /^md\.trades\..+/];

async function main(): Promise<void> {
  const agg = new Aggregator();
  const broadcaster = new Broadcaster(MAX_BUFFERED_BYTES);

  // ── WS server (+ tiny HTTP for health/upgrade) ────────────────────────────
  const httpServer = http.createServer((req, res) => {
    if (req.url === "/healthz") {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("ok");
      return;
    }
    res.writeHead(404);
    res.end();
  });
  const wss = new WebSocketServer({ server: httpServer, path: "/ws" });
  wss.on("connection", (ws) => {
    broadcaster.add(ws);
    ws.on("close", () => broadcaster.remove(ws));
    ws.on("error", () => broadcaster.remove(ws));
  });
  httpServer.listen(WS_PORT, () => console.log(`[gateway] ws listening on :${WS_PORT}/ws`));

  // ── Kafka in (book/trades) and out (md.bbo) ───────────────────────────────
  const kafka = new Kafka({ clientId: "gateway", brokers: BROKERS, logLevel: logLevel.WARN });
  const producer = kafka.producer({ allowAutoTopicCreation: true });
  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topics: TOPIC_PATTERNS, fromBeginning: false });
  console.log(`[gateway] consuming md.book.* / md.trades.* from ${BROKERS.join(",")}`);

  // In-flight send promises (post fix #1: fire-and-forget). Tracked so
  // shutdown can drain them before producer.disconnect() — kafkajs's disconnect
  // aborts pending sends rather than flushing (verified in
  // node_modules/kafkajs/src/producer/index.js:230).
  const inFlight = new Set<Promise<unknown>>();
  const SHUTDOWN_DRAIN_MS = 5000;

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      let result: RouteResult;
      try {
        result = routeMessage(decodeMsg(message.value), agg);
      } catch (err) {
        console.error("[gateway] bad message, skipping:", err);
        return;
      }
      if (result.publish) {
        const bbo = result.publish;
        // Fire-and-forget: awaiting producer.send here would cap publish rate
        // at 1/broker-RTT per partition (same antipattern the python ingester
        // removed in a6d1fae). The gateway is stateless — a dropped md.bbo
        // publish is recovered by the next L1 move; no resync needed.
        const p: Promise<unknown> = producer
          .send({
            topic: bboTopic(bbo.exchange, bbo.symbol),
            messages: [{ key: `${bbo.exchange}:${bbo.symbol}`, value: JSON.stringify(bbo) }],
          })
          .catch((err) => console.error("[gateway] kafka produce failed:", err))
          .finally(() => inFlight.delete(p));
        inFlight.add(p);
      }
      if (result.broadcast) broadcaster.broadcast(result.broadcast);
    },
  });

  // ── graceful shutdown ─────────────────────────────────────────────────────
  const shutdown = async (sig: string): Promise<void> => {
    console.log(`[gateway] ${sig} → shutting down`);
    try {
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
