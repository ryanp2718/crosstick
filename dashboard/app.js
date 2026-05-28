// Minimal WS client for the gateway BBO firehose. Renders one row per
// (exchange, symbol) keyed off the latest BBO. Spread/mid are display-only —
// Number() is fine for crypto-scale prices (<< 2^53).

const status = document.getElementById("status");
const meta = document.getElementById("meta");
const rows = document.getElementById("rows");

const state = new Map(); // key -> { bbo, rowEl, prevBid, prevAsk }
let msgCount = 0;

function key(b) { return `${b.exchange}|${b.symbol}`; }

function fmt(s, places = 2) {
  const n = Number(s);
  if (!Number.isFinite(n)) return s;
  return n.toLocaleString(undefined, { minimumFractionDigits: places, maximumFractionDigits: places });
}

function ageMs(local_ts_ns) {
  // local_ts_ns is JSON-parsed to a Number (rounded to ~200ns granularity past
  // 2^53). Plenty precise for ms-scale UI.
  return Math.max(0, Date.now() - local_ts_ns / 1e6);
}

function ensureRow(k, bbo) {
  let st = state.get(k);
  if (st) return st;
  const tr = document.createElement("tr");
  for (let i = 0; i < 9; i++) tr.appendChild(document.createElement("td"));
  tr.children[0].textContent = bbo.exchange;
  tr.children[1].textContent = bbo.symbol;
  tr.children[2].className = "num bid";
  tr.children[3].className = "num";
  tr.children[4].className = "num ask";
  tr.children[5].className = "num";
  tr.children[6].className = "num";
  tr.children[7].className = "num";
  tr.children[8].className = "num";
  // Insert sorted by exchange,symbol.
  const k2 = `${bbo.exchange}|${bbo.symbol}`;
  let inserted = false;
  for (const existing of rows.children) {
    const ek = `${existing.children[0].textContent}|${existing.children[1].textContent}`;
    if (k2 < ek) { rows.insertBefore(tr, existing); inserted = true; break; }
  }
  if (!inserted) rows.appendChild(tr);
  st = { bbo: null, rowEl: tr, prevBid: null, prevAsk: null };
  state.set(k, st);
  return st;
}

function applyBbo(bbo) {
  msgCount++;
  const k = key(bbo);
  const st = ensureRow(k, bbo);
  const bid = Number(bbo.bid_px);
  const ask = Number(bbo.ask_px);
  const spread = ask - bid;
  const mid = (ask + bid) / 2;

  st.rowEl.children[2].textContent = fmt(bbo.bid_px);
  st.rowEl.children[3].textContent = fmt(bbo.bid_sz, 4);
  st.rowEl.children[4].textContent = fmt(bbo.ask_px);
  st.rowEl.children[5].textContent = fmt(bbo.ask_sz, 4);
  st.rowEl.children[6].textContent = fmt(spread.toString(), 4);
  st.rowEl.children[7].textContent = fmt(mid.toString());

  // Flash cells when L1 moved.
  st.rowEl.classList.remove("flash-up", "flash-down");
  if (st.prevBid !== null && bid > st.prevBid) {
    void st.rowEl.offsetWidth;
    st.rowEl.classList.add("flash-up");
  } else if (st.prevAsk !== null && ask < st.prevAsk) {
    void st.rowEl.offsetWidth;
    st.rowEl.classList.add("flash-down");
  }
  st.prevBid = bid;
  st.prevAsk = ask;
  st.bbo = bbo;
}

// Age-column tick. Always-ms format keeps the cell width stable (fixed col in
// HTML); ages >9999ms overflow visibly, which is the right signal that
// something upstream is actually slow.
setInterval(() => {
  for (const st of state.values()) {
    if (!st.bbo) continue;
    st.rowEl.children[8].textContent = `${ageMs(st.bbo.local_ts_ns).toFixed(0)}ms`;
  }
  meta.textContent = `${state.size} streams · ${msgCount} msgs`;
}, 100);

function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws`);
  ws.onopen = () => { status.textContent = "live"; status.className = "up"; };
  ws.onclose = () => {
    status.textContent = "disconnected"; status.className = "down";
    setTimeout(connect, 1000);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = (ev) => {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }
    if (msg && msg.t === "bbo") applyBbo(msg);
  };
}

connect();
