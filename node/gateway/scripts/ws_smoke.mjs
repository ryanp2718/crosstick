// Smoke: connect to the gateway WS twice - second connection should receive a
// snapshot-on-connect (latest BBO per stream the gateway has seen). Then both
// should keep receiving live broadcasts.

import { WebSocket } from "ws";

const url = process.env.WS_URL ?? "ws://localhost:8080/ws";
const ws = new WebSocket(url);
const t0 = Date.now();
const seen = [];

ws.on("open", () => console.log(`[t+${Date.now() - t0}ms] open`));
ws.on("message", (data) => {
  const msg = JSON.parse(data.toString());
  seen.push({ at: Date.now() - t0, msg });
  if (seen.length === 5) {
    console.log(`first ${seen.length} messages:`);
    for (const { at, msg } of seen) {
      console.log(
        `  +${at}ms ${msg.t} ${msg.exchange}/${msg.symbol} bid=${msg.bid_px ?? "-"} ask=${msg.ask_px ?? "-"}`,
      );
    }
    ws.close();
    process.exit(0);
  }
});
ws.on("close", () => console.log("close"));
ws.on("error", (e) => {
  console.error("error:", e.message);
  process.exit(1);
});

setTimeout(() => {
  console.error("timeout");
  process.exit(2);
}, 15000);
