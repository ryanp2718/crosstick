// Minimal WS client for the gateway BBO firehose. Renders one row per
// (exchange, symbol) keyed off the latest BBO. Spread/mid are display-only —
// Number() is fine for crypto-scale prices (<< 2^53).

const status = document.getElementById("status");
const meta = document.getElementById("meta");
const rows = document.getElementById("rows");
const nbboRows = document.getElementById("nbbo-rows");

const state = new Map(); // key -> { bbo, rowEl, prevBid, prevAsk }
const nbboState = new Map(); // canonical_id -> { nbbo, rowEl }
let msgCount = 0;

// Grey an NBBO leg whose winning quote is older than this — a consumer-side
// staleness call (the gateway never drops a stale leg; see DESIGN_nbbo.md).
const STALE_MS = 3000;

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

function ensureNbboRow(canonical_id) {
  let st = nbboState.get(canonical_id);
  if (st) return st;
  const tr = document.createElement("tr");
  for (let i = 0; i < 10; i++) tr.appendChild(document.createElement("td"));
  tr.children[0].textContent = canonical_id;
  for (let i = 1; i < 10; i++) tr.children[i].className = "num";
  // Insert sorted by canonical_id.
  let inserted = false;
  for (const existing of nbboRows.children) {
    if (canonical_id < existing.children[0].textContent) {
      nbboRows.insertBefore(tr, existing);
      inserted = true;
      break;
    }
  }
  if (!inserted) nbboRows.appendChild(tr);
  st = { nbbo: null, rowEl: tr };
  nbboState.set(canonical_id, st);
  return st;
}

function applyNbbo(nbbo) {
  msgCount++;
  const st = ensureNbboRow(nbbo.canonical_id);
  st.rowEl.children[1].textContent = fmt(nbbo.best_bid.px);
  st.rowEl.children[2].textContent = fmt(nbbo.best_bid.sz, 4);
  st.rowEl.children[3].textContent = nbbo.best_bid.exchange;
  st.rowEl.children[4].textContent = fmt(nbbo.best_ask.px);
  st.rowEl.children[5].textContent = fmt(nbbo.best_ask.sz, 4);
  st.rowEl.children[6].textContent = nbbo.best_ask.exchange;
  const spreadCell = st.rowEl.children[7];
  spreadCell.textContent = fmt(nbbo.spread.toString(), 4);
  spreadCell.className = `num ${nbbo.spread < 0 ? "spread-neg" : "spread-pos"}`;
  spreadCell.title = nbbo.spread < 0 ? "crossed — check leg age (possible stale venue)" : "";
  st.rowEl.children[8].textContent = fmt(nbbo.mid.toString());
  st.rowEl.children[9].textContent = nbbo.constituents.join(",");
  st.nbbo = nbbo;
}

// Age-column tick. Always-ms format keeps the cell width stable (fixed col in
// HTML); ages >9999ms overflow visibly, which is the right signal that
// something upstream is actually slow.
function setStale(cells, stale) {
  for (const c of cells) c.classList.toggle("stale", stale);
}

setInterval(() => {
  for (const st of state.values()) {
    if (!st.bbo) continue;
    st.rowEl.children[8].textContent = `${ageMs(st.bbo.local_ts_ns).toFixed(0)}ms`;
  }
  // Grey each NBBO leg live as its winning quote ages, even with no new NBBO.
  for (const st of nbboState.values()) {
    if (!st.nbbo) continue;
    const c = st.rowEl.children;
    setStale([c[1], c[2], c[3]], ageMs(st.nbbo.best_bid.leg_ts_ns) > STALE_MS);
    setStale([c[4], c[5], c[6]], ageMs(st.nbbo.best_ask.leg_ts_ns) > STALE_MS);
  }
  meta.textContent = `${state.size} bbo · ${nbboState.size} nbbo · ${msgCount} msgs`;
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
    if (!msg) return;
    if (msg.t === "bbo") applyBbo(msg);
    else if (msg.t === "nbbo") applyNbbo(msg);
  };
}

connect();
