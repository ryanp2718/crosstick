// One-shot: send each dashboard panel's PromQL through the Grafana datasource
// proxy and report status, so we catch dashboard-JSON typos that the UI would
// otherwise silently render as empty panels.

const DASH = "http://localhost:3000/api/dashboards/uid/gateway-overview";
const QUERY = "http://localhost:3000/api/datasources/proxy/uid/prometheus/api/v1/query";

const dash = await fetch(DASH).then((r) => r.json());
for (const p of dash.dashboard.panels) {
  for (const t of p.targets ?? []) {
    if (!t.expr) continue;
    const url = `${QUERY}?query=${encodeURIComponent(t.expr)}`;
    const res = await fetch(url).then((r) => r.json());
    const ok = res.status === "success";
    const n = ok ? res.data.result.length : 0;
    const tag = ok ? `ok n=${n}` : `ERR ${res.errorType}: ${res.error}`;
    console.log(`panel ${p.id} [${p.title.slice(0, 40)}] ${tag}`);
  }
}
