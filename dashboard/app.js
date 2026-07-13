// WS client for the gateway feed. One shared state renders two views: the
// ledger tables (NBBO + per-exchange BBO) and a matrix pivot (instrument rows
// by venue columns). Spread/mid are display-only - Number() is fine for
// crypto-scale prices (<< 2^53).

const dateline = document.getElementById("dateline");
const conn = document.getElementById("conn");
const clockEl = document.getElementById("clock");
const stallEl = document.getElementById("stall");
const statMsgs = document.getElementById("stat-msgs");
const statRate = document.getElementById("stat-rate");
const statStreams = document.getElementById("stat-streams");
const rows = document.getElementById("rows");
const nbboRows = document.getElementById("nbbo-rows");
const matrixTable = document.getElementById("matrix");
const venueRows = document.getElementById("venue-rows");
const tapeRows = document.getElementById("tape-rows");

const state = new Map(); // "exchange|symbol" -> { bbo, rowEl, prevBid, prevAsk }
const pairIndex = new Map(); // "exchange|BASEQUOTE" -> state entry, for matrix cell lookup
const nbboState = new Map(); // canonical_id -> { nbbo, rowEl, pair, venues: Set }
const venueState = new Map(); // exchange -> { lastNs, msgs, rowEl, ledEl, ageEl, msgsEl }
let msgCount = 0;

// Grey a quote whose winning leg trails the stream clock by more than this:
// a consumer-side staleness call (the gateway never drops a stale leg).
const STALE_MS = 3000;
// Flag the whole feed when no message arrives for this long. Wall clock on
// purpose: a stalled feed freezes the stream clock, so stream ages can't see it.
const STALL_MS = 3000;
const TAPE_MAX = 12;
const RATE_WINDOW_MS = 5000;

// Stream clock: max event time seen. Ages compare data to data, never
// Date.now(), so a replayed corpus renders exactly like a live feed.
let streamNowNs = 0;
let lastMsgWallMs = 0;
const rateSamples = []; // { t: wallMs, c: msgCount } over the last RATE_WINDOW_MS

function key(b) { return `${b.exchange}|${b.symbol}`; }

// "BTC/USD" -> "BTCUSD", "BTCUSDT" -> "BTCUSDT": matches canonical base+quote.
function pairOf(symbol) { return symbol.replace(/[^a-zA-Z0-9]/g, "").toUpperCase(); }

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function fmt(s, places = 2) {
  const n = Number(s);
  if (!Number.isFinite(n)) return s;
  // Nonzero dust that would round to 0.0000 shows significant digits instead.
  if (n !== 0 && Math.abs(n) < 0.5 * 10 ** -places) {
    return n.toLocaleString(undefined, { maximumSignificantDigits: 2 });
  }
  return n.toLocaleString(undefined, { minimumFractionDigits: places, maximumFractionDigits: places });
}

function clearEmpty(tbody) {
  const ph = tbody.querySelector(".empty");
  if (ph) ph.remove();
}

function ageMs(ts_ns) {
  // ts fields are JSON-parsed Numbers (~200ns granularity past 2^53): fine for ms UI.
  return Math.max(0, (streamNowNs - ts_ns) / 1e6);
}

function markVenue(exchange, ts_ns) {
  let v = venueState.get(exchange);
  if (!v) {
    const tr = el("tr");
    tr.dataset.exchange = exchange;
    const nameTd = el("td", "l");
    const led = el("span", "led", "●");
    nameTd.appendChild(led);
    nameTd.appendChild(document.createTextNode(` ${exchange}`));
    const ageTd = el("td");
    const msgsTd = el("td");
    tr.appendChild(nameTd); tr.appendChild(ageTd); tr.appendChild(msgsTd);
    let inserted = false;
    for (const existing of venueRows.children) {
      if (exchange < existing.dataset.exchange) { venueRows.insertBefore(tr, existing); inserted = true; break; }
    }
    if (!inserted) venueRows.appendChild(tr);
    v = { lastNs: 0, msgs: 0, rowEl: tr, ledEl: led, ageEl: ageTd, msgsEl: msgsTd };
    venueState.set(exchange, v);
  }
  if (ts_ns > v.lastNs) v.lastNs = ts_ns;
  v.msgs++;
}

function ensureRow(k, bbo) {
  let st = state.get(k);
  if (st) return st;
  clearEmpty(rows);
  const tr = document.createElement("tr");
  const classes = ["l venue", "l name", "bid", "", "ask", "", "sp", "", ""];
  for (const c of classes) tr.appendChild(el("td", c));
  tr.children[0].textContent = bbo.exchange;
  tr.children[1].textContent = bbo.symbol;
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
  pairIndex.set(`${bbo.exchange}|${pairOf(bbo.symbol)}`, st);
  return st;
}

function applyBbo(bbo) {
  msgCount++;
  if (bbo.local_ts_ns > streamNowNs) streamNowNs = bbo.local_ts_ns;
  markVenue(bbo.exchange, bbo.local_ts_ns);
  const k = key(bbo);
  const st = ensureRow(k, bbo);
  const bid = Number(bbo.bid_px);
  const ask = Number(bbo.ask_px);
  const spread = ask - bid;
  const mid = (ask + bid) / 2;

  const c = st.rowEl.children;
  c[2].textContent = fmt(bbo.bid_px);
  c[3].textContent = fmt(bbo.bid_sz, 4);
  c[4].textContent = fmt(bbo.ask_px);
  c[5].textContent = fmt(bbo.ask_sz, 4);
  c[6].textContent = (spread < 0 ? "× " : "") + fmt(spread.toString(), 4);
  c[6].className = spread < 0 ? "sp x" : "sp"; // per-venue crossed book
  c[7].textContent = fmt(mid.toString());

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
  clearEmpty(nbboRows);
  const tr = document.createElement("tr");
  const classes = ["l name", "", "", "l venue", "", "", "l venue", "sp", "", "l venue wrap"];
  for (const c of classes) tr.appendChild(el("td", c));
  tr.children[0].textContent = canonical_id;
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
  st = { nbbo: null, rowEl: tr, pair: "", venues: new Set() };
  nbboState.set(canonical_id, st);
  return st;
}

function applyNbbo(nbbo) {
  msgCount++;
  streamNowNs = Math.max(streamNowNs, nbbo.best_bid.leg_ts_ns, nbbo.best_ask.leg_ts_ns);
  const st = ensureNbboRow(nbbo.canonical_id);
  st.pair = `${nbbo.base}${nbbo.quote}`.toUpperCase();
  for (const v of nbbo.constituents) st.venues.add(v);
  const c = st.rowEl.children;
  c[1].textContent = fmt(nbbo.best_bid.px);
  c[2].textContent = fmt(nbbo.best_bid.sz, 4);
  c[3].textContent = nbbo.best_bid.exchange;
  c[4].textContent = fmt(nbbo.best_ask.px);
  c[5].textContent = fmt(nbbo.best_ask.sz, 4);
  c[6].textContent = nbbo.best_ask.exchange;
  c[7].textContent = (nbbo.crossed ? "× " : "") + fmt(nbbo.spread.toString(), 4);
  c[7].className = nbbo.crossed ? "sp x" : "sp";
  c[7].title = nbbo.crossed ? "crossed: check leg age (possible stale venue)" : "";
  c[8].textContent = fmt(nbbo.mid.toString());
  c[9].textContent = nbbo.constituents.join(", ");
  st.nbbo = nbbo;
}

function applyTrade(t) {
  msgCount++;
  if (t.local_ts_ns > streamNowNs) streamNowNs = t.local_ts_ns;
  markVenue(t.exchange, t.local_ts_ns);
  const tr = el("tr");
  const iso = new Date(t.local_ts_ns / 1e6).toISOString();
  tr.appendChild(el("td", "l", iso.slice(11, 22)));
  tr.appendChild(el("td", "l venue", t.exchange));
  tr.appendChild(el("td", "l venue", t.symbol));
  // Ingest convention: side bid = taker buy, side ask = taker sell.
  const buy = t.side === "bid";
  tr.appendChild(el("td", buy ? "buy" : "sell", buy ? "B" : "S"));
  tr.appendChild(el("td", buy ? "buy" : "sell", fmt(t.price)));
  tr.appendChild(el("td", "", fmt(t.size, 4)));
  tapeRows.insertBefore(tr, tapeRows.firstChild);
  while (tapeRows.children.length > TAPE_MAX) tapeRows.removeChild(tapeRows.lastChild);
}

// Matrix view: one row per canonical instrument, one column per venue. The
// venue cell for a canonical is the BBO stream whose (exchange, base+quote)
// matches, gated on the exchange having ever appeared in the canonical's
// constituents (disambiguates binance vs binance_futures, both "BTCUSDT").
// Rebuilt wholesale each tick it is visible: the grid is tiny.
function renderMatrix() {
  const venues = [...venueState.keys()].sort();
  const canonicals = [...nbboState.keys()].sort();
  matrixTable.textContent = "";

  const colgroup = el("colgroup");
  const fixed = [13, 12, 12, 7];
  const venueW = venues.length ? (100 - 44) / venues.length : 56;
  for (const w of fixed) {
    const col = el("col");
    col.style.width = `${w}%`;
    colgroup.appendChild(col);
  }
  for (let i = 0; i < venues.length; i++) {
    const col = el("col");
    col.style.width = `${venueW}%`;
    colgroup.appendChild(col);
  }
  matrixTable.appendChild(colgroup);

  const thead = el("thead");
  const hr = el("tr");
  hr.appendChild(el("th", "l", "Instrument"));
  hr.appendChild(el("th", "nb", "NBBO bid"));
  hr.appendChild(el("th", "nb", "NBBO ask"));
  hr.appendChild(el("th", "nb", "Sprd"));
  for (const v of venues) hr.appendChild(el("th", "", v));
  thead.appendChild(hr);
  matrixTable.appendChild(thead);

  const tbody = el("tbody");
  if (canonicals.length === 0) {
    const tr = el("tr");
    const td = el("td", "dotcell", "waiting for feed");
    td.colSpan = 4 + venues.length;
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
  let prevBase = null;
  for (const id of canonicals) {
    const c = nbboState.get(id);
    if (!c.nbbo) continue;
    // Rule off each base-asset block (BTC rows, then ETH rows, ...).
    const tr = el("tr", prevBase !== null && c.nbbo.base !== prevBase ? "grp" : "");
    prevBase = c.nbbo.base;
    tr.appendChild(el("td", "l name", id));

    // Price over venue, matching the venue cells' two-line shape.
    const bAge = ageMs(c.nbbo.best_bid.leg_ts_ns);
    const bidTd = el("td", bAge > STALE_MS ? "nb stalecell" : "nb");
    bidTd.appendChild(el("span", "b", fmt(c.nbbo.best_bid.px)));
    bidTd.appendChild(el("br"));
    bidTd.appendChild(el("span", "age", c.nbbo.best_bid.exchange));
    tr.appendChild(bidTd);

    const aAge = ageMs(c.nbbo.best_ask.leg_ts_ns);
    const askTd = el("td", aAge > STALE_MS ? "nb stalecell" : "nb");
    askTd.appendChild(el("span", "a", fmt(c.nbbo.best_ask.px)));
    askTd.appendChild(el("br"));
    askTd.appendChild(el("span", "age", c.nbbo.best_ask.exchange));
    tr.appendChild(askTd);

    tr.appendChild(el("td", c.nbbo.crossed ? "nb sp x" : "nb sp", (c.nbbo.crossed ? "× " : "") + fmt(c.nbbo.spread.toString(), 4)));

    for (const v of venues) {
      const st = c.venues.has(v) ? pairIndex.get(`${v}|${c.pair}`) : undefined;
      if (!st || !st.bbo) {
        tr.appendChild(el("td", "dotcell", "·"));
        continue;
      }
      const age = ageMs(st.bbo.local_ts_ns);
      const td = el("td", age > STALE_MS ? "stalecell" : "");
      td.appendChild(el("span", "b", fmt(st.bbo.bid_px)));
      td.appendChild(el("br"));
      td.appendChild(el("span", "a", fmt(st.bbo.ask_px)));
      td.appendChild(document.createTextNode(" "));
      td.appendChild(el("span", "age", `${age.toFixed(0)}ms`));
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  matrixTable.appendChild(tbody);
}

const btnLedger = document.getElementById("view-ledger");
const btnMatrix = document.getElementById("view-matrix");

function setView(v) {
  document.body.classList.toggle("matrix", v === "matrix");
  btnLedger.classList.toggle("on", v !== "matrix");
  btnMatrix.classList.toggle("on", v === "matrix");
  try { localStorage.setItem("crosstick-view", v); } catch { /* private mode */ }
  if (v === "matrix") renderMatrix();
}
btnLedger.onclick = () => setView("ledger");
btnMatrix.onclick = () => setView("matrix");
try { if (localStorage.getItem("crosstick-view") === "matrix") setView("matrix"); } catch { /* private mode */ }

function setStale(cells, stale) {
  for (const c of cells) c.classList.toggle("stale", stale);
}

// Display tick. Always-ms ages keep cell widths stable (fixed table layout);
// ages >9999ms overflow visibly, which is the right signal that something
// upstream is actually slow.
setInterval(() => {
  for (const st of state.values()) {
    if (!st.bbo) continue;
    st.rowEl.children[8].textContent = `${ageMs(st.bbo.local_ts_ns).toFixed(0)}ms`;
  }
  // Grey each NBBO leg live as its winning quote ages, even with no new NBBO;
  // the venue cell names the staleness so the grey is self-explanatory.
  for (const st of nbboState.values()) {
    if (!st.nbbo) continue;
    const c = st.rowEl.children;
    const bAge = ageMs(st.nbbo.best_bid.leg_ts_ns);
    const bStale = bAge > STALE_MS;
    c[3].textContent = bStale
      ? `${st.nbbo.best_bid.exchange} · stale ${(bAge / 1000).toFixed(1)}s`
      : st.nbbo.best_bid.exchange;
    setStale([c[1], c[2], c[3]], bStale);
    const aAge = ageMs(st.nbbo.best_ask.leg_ts_ns);
    const aStale = aAge > STALE_MS;
    c[6].textContent = aStale
      ? `${st.nbbo.best_ask.exchange} · stale ${(aAge / 1000).toFixed(1)}s`
      : st.nbbo.best_ask.exchange;
    setStale([c[4], c[5], c[6]], aStale);
  }
  for (const v of venueState.values()) {
    if (!v.lastNs) continue;
    const age = ageMs(v.lastNs);
    v.ageEl.textContent = `${age.toFixed(0)}ms`;
    v.msgsEl.textContent = v.msgs.toLocaleString();
    v.ledEl.classList.toggle("warn", age > STALE_MS);
  }

  const now = Date.now();
  rateSamples.push({ t: now, c: msgCount });
  while (rateSamples.length > 1 && now - rateSamples[0].t > RATE_WINDOW_MS) rateSamples.shift();
  const base = rateSamples[0];
  const dt = (now - base.t) / 1000;
  statRate.textContent = dt > 0.5 ? `${Math.round((msgCount - base.c) / dt)}/s` : "0/s";
  statMsgs.textContent = msgCount.toLocaleString();
  statStreams.textContent = `${state.size} bbo · ${nbboState.size} nbbo`;

  if (streamNowNs > 0) {
    const iso = new Date(streamNowNs / 1e6).toISOString();
    clockEl.textContent = `${iso.slice(0, 10)} · ${iso.slice(11, 19)} UTC`;
  }
  const stallMs = lastMsgWallMs ? Date.now() - lastMsgWallMs : 0;
  stallEl.textContent = stallMs > STALL_MS ? `feed stalled ${(stallMs / 1000).toFixed(0)}s` : "";

  if (document.body.classList.contains("matrix")) renderMatrix();
}, 100);

function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws`);
  ws.onopen = () => { conn.textContent = "Live"; dateline.classList.add("up"); };
  ws.onclose = () => {
    conn.textContent = "Disconnected"; dateline.classList.remove("up");
    setTimeout(connect, 1000);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = (ev) => {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }
    if (!msg) return;
    lastMsgWallMs = Date.now();
    if (msg.t === "bbo") applyBbo(msg);
    else if (msg.t === "nbbo") applyNbbo(msg);
    else if (msg.t === "trade") applyTrade(msg);
  };
}

connect();
