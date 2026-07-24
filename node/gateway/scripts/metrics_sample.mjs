// Scrapes the gateway /metrics endpoint every 10s for 60s and prints per-
// segment deltas (to see throughput evolve) plus final gauge values.
// Run while the gateway is live: `node scripts/metrics_sample.mjs`

const URL = process.env.METRICS_URL ?? "http://localhost:8080/metrics";
const N = 7;
const INTERVAL_MS = 10_000;

async function scrape() {
  const text = await fetch(URL).then((r) => r.text());
  const m = {};
  for (const line of text.split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const match = line.match(/^([\w]+)(\{[^}]*\})?\s+(\S+)/);
    if (!match) continue;
    const [, name, labels = "", value] = match;
    m[name + labels] = Number(value);
  }
  return m;
}

function sumByTopicSuffix(m, prefix, suffix) {
  let s = 0;
  for (const [k, v] of Object.entries(m)) {
    if (!k.startsWith(prefix + "{")) continue;
    const topic = k.match(/topic="([^"]+)"/)?.[1];
    if (topic && topic.includes(suffix)) s += v;
  }
  return s;
}

const samples = [];
console.log(`sampling ${URL} every ${INTERVAL_MS / 1000}s, ${N} samples`);
for (let i = 0; i < N; i++) {
  if (i > 0) await new Promise((r) => setTimeout(r, INTERVAL_MS));
  samples.push(await scrape());
  process.stdout.write(`  t+${(i * INTERVAL_MS) / 1000}s ok\n`);
}

console.log("\n=== per-segment deltas (each ~10s) ===");
console.log("seg | deltas | bbo | bbo/del% | trades | bcast | sum_ok");
for (let i = 1; i < N; i++) {
  const prev = samples[i - 1];
  const cur = samples[i];
  const dDeltas =
    sumByTopicSuffix(cur, "gateway_messages_consumed_total", ".deltas") -
    sumByTopicSuffix(prev, "gateway_messages_consumed_total", ".deltas");
  const dTrades =
    sumByTopicSuffix(cur, "gateway_messages_consumed_total", "md.trades") -
    sumByTopicSuffix(prev, "gateway_messages_consumed_total", "md.trades");
  const dBbo =
    (cur['gateway_bbo_produced_total{result="ok"}'] ?? 0) -
    (prev['gateway_bbo_produced_total{result="ok"}'] ?? 0);
  const dBcast =
    (cur["gateway_ws_broadcasts_total"] ?? 0) - (prev["gateway_ws_broadcasts_total"] ?? 0);
  const ratio = dDeltas ? ((dBbo / dDeltas) * 100).toFixed(1) : "n/a";
  const sumOk = dBcast === dBbo + dTrades ? "ok" : "MISMATCH";
  console.log(
    `${String(i).padStart(3)} |` +
      ` ${String(dDeltas).padStart(6)} |` +
      ` ${String(dBbo).padStart(3)} |` +
      ` ${String(ratio).padStart(8)} |` +
      ` ${String(dTrades).padStart(6)} |` +
      ` ${String(dBcast).padStart(5)} |` +
      ` ${sumOk}`,
  );
}

console.log("\n=== final gauges ===");
const last = samples[N - 1];
const first = samples[0];

for (const [k, v] of Object.entries(last).sort()) {
  if (k.startsWith("gateway_consumer_lag_messages")) console.log(`  ${k} = ${v}`);
}

const evMs = (suffix) => (last[`gateway_nodejs_eventloop_lag_${suffix}_seconds`] * 1000).toFixed(2);
console.log(`  eventloop_lag_mean = ${evMs("mean")} ms`);
console.log(`  eventloop_lag_p99  = ${evMs("p99")} ms`);
console.log(`  eventloop_lag_max  = ${evMs("max")} ms`);

const mib = (b) => (b / 1024 / 1024).toFixed(1);
const rss0 = first["gateway_process_resident_memory_bytes"];
const rss1 = last["gateway_process_resident_memory_bytes"];
console.log(
  `  rss        = ${mib(rss1)} MiB (start ${mib(rss0)}, Δ ${((rss1 - rss0) / 1024 / 1024).toFixed(2)} MiB)`,
);
console.log(`  heap_used  = ${mib(last["gateway_nodejs_heap_size_used_bytes"])} MiB`);
console.log(`  heap_total = ${mib(last["gateway_nodejs_heap_size_total_bytes"])} MiB`);

const gcKinds = new Set();
for (const k of Object.keys(last)) {
  const m = k.match(/^gateway_nodejs_gc_duration_seconds_sum\{kind="([^"]+)"\}/);
  if (m) gcKinds.add(m[1]);
}
for (const kind of gcKinds) {
  const sumKey = `gateway_nodejs_gc_duration_seconds_sum{kind="${kind}"}`;
  const cntKey = `gateway_nodejs_gc_duration_seconds_count{kind="${kind}"}`;
  const dSum = (last[sumKey] ?? 0) - (first[sumKey] ?? 0);
  const dCnt = (last[cntKey] ?? 0) - (first[cntKey] ?? 0);
  if (dCnt > 0)
    console.log(`  gc[${kind}] over 60s: ${dCnt} pauses, ${(dSum * 1000).toFixed(1)} ms total`);
}
