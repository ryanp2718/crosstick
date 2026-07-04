import { Book } from "./book.js";
import { cmpDecimal } from "./decimal.js";
import { bboCrossed, bookResnapshotHeal, bookSnapshotStale } from "./metrics.js";
import type { BBOMsg, BookDeltaMsg, BookSnapshotMsg } from "./messages.js";

// Deltas retained per stream while waiting for its snapshot (drop-oldest on
// overflow — the oldest buffered deltas are the lowest-seq ones a snapshot's
// sequence supersedes, so shedding them never gaps the book above the snapshot).
export const MAX_PENDING_DELTAS = 10_000;

// Opt-in diagnostic: on an uncrossed->crossed transition, log the last N consumed
// messages for that stream (kind, seq, epoch, disposition) to pin the exact
// consume interleave that crosses the book. Zero cost unless enabled.
const CROSS_ONSET_LOG = process.env.GATEWAY_CROSS_ONSET_LOG === "1";
const ONSET_RING = 250;

// Maintains one Book per (exchange, symbol) and derives the per-exchange BBO.
// Returns a BBOMsg only when top-of-book actually changed (dedup), so md.bbo
// volume tracks L1 changes rather than every depth update.
//
// Order-insensitive across the snapshot/delta topic pair (D2): a delta that
// arrives before its stream's snapshot is buffered, not dropped, and drained
// in order once the snapshot lands (the monotonic sequence guard discards the
// ones the snapshot already covers). Cross-topic consumption order — replay,
// warm restart, live race — therefore converges to the same book; only the
// emitted BBO *sequence* coalesces when deltas drain in a batch.
//
// Epoch-keyed (see messages.ts BookSnapshotMsg.epoch): coinbase/kraken reset
// their per-connection sequence counter on each reconnect, so a prior
// connection's high-seq delta can out-rank a fresh snapshot's low seq. A delta
// is only applied to a book of its OWN connection epoch; deltas of any other
// epoch are buffered (never applied to the live book) until a matching-epoch
// snapshot drains them. This is what stops the warm-start crossed-book
// corruption — by equality only, so it is immune to the clock skew that can
// reorder epoch values.
export class Aggregator {
  private readonly books = new Map<string, Book>();
  private readonly lastBbo = new Map<string, BBOMsg>();
  private readonly pending = new Map<string, BookDeltaMsg[]>();
  // Onset diagnostics (only populated when CROSS_ONSET_LOG): per-stream ring of
  // recent consumed-message descriptors, and last-seen crossed state.
  private readonly recent = new Map<string, string[]>();
  private readonly wasCrossed = new Map<string, boolean>();

  private rec(key: string, desc: string): void {
    const r = this.recent.get(key) ?? [];
    r.push(desc);
    if (r.length > ONSET_RING) r.shift();
    this.recent.set(key, r);
  }

  applyBook(msg: BookSnapshotMsg | BookDeltaMsg): BBOMsg | null {
    const key = `${msg.exchange} ${msg.symbol}`;
    const epoch = msg.epoch ?? 0;

    if (msg.t === "delta") {
      const book = this.books.get(key);
      // No book yet, or a different connection epoch than the current book →
      // buffer for a matching-epoch snapshot drain (see class header).
      if (!book || book.seq < 0 || epoch !== book.epoch) {
        const queue = this.pending.get(key) ?? [];
        if (queue.length >= MAX_PENDING_DELTAS) queue.shift();
        queue.push(msg);
        this.pending.set(key, queue);
        if (CROSS_ONSET_LOG) this.rec(key, `d${msg.sequence}:buf`);
        return null;
      }
      const applied = book.applyDelta(msg.sequence, msg.bids, msg.asks);
      if (CROSS_ONSET_LOG) {
        if (applied) {
          const bb = book.bestBid();
          const ba = book.bestAsk();
          this.rec(key, `d${msg.sequence}->${bb?.[0] ?? "-"}/${ba?.[0] ?? "-"}`);
        } else {
          this.rec(key, `d${msg.sequence}:SKIP<=seq`);
        }
      }
      if (!applied) return null;
      return this.deriveBbo(key, book, msg);
    }

    let book = this.books.get(key);
    if (!book) {
      book = new Book();
      this.books.set(key, book);
    }
    // A same-epoch re-snapshot the book already passed is a rewind, normally
    // skipped — unless the book is crossed, when applySnapshot applies it as a
    // resync that heals the corruption (bounds a cross to one interval).
    const staleReSnapshot = epoch === book.epoch && msg.sequence <= book.seq;
    const snapApplied = book.applySnapshot(msg.sequence, epoch, msg.bids, msg.asks);
    if (CROSS_ONSET_LOG)
      this.rec(key, `S${msg.sequence}:${snapApplied ? "applied" : "GUARD-SKIP"}`);
    if (!snapApplied) {
      // Stale same-epoch re-snapshot the book already passed: skip the rewind.
      // Live book unchanged, same-epoch deltas already applied → nothing to drain.
      bookSnapshotStale.inc({ exchange: msg.exchange });
      return null;
    }
    // Applied despite being stale ⇒ the guard let it through to heal a crossed book.
    if (staleReSnapshot) bookResnapshotHeal.inc({ exchange: msg.exchange });
    // Drain only this snapshot's own epoch (seq guard drops what it supersedes);
    // retain other epochs for their own snapshot. ts from the last applied msg.
    let last: BookSnapshotMsg | BookDeltaMsg = msg;
    const retained: BookDeltaMsg[] = [];
    for (const delta of this.pending.get(key) ?? []) {
      if ((delta.epoch ?? 0) !== epoch) {
        retained.push(delta);
        continue;
      }
      if (book.applyDelta(delta.sequence, delta.bids, delta.asks)) last = delta;
    }
    if (retained.length > 0) this.pending.set(key, retained);
    else this.pending.delete(key);
    return this.deriveBbo(key, book, last);
  }

  // Last known BBO per (exchange, symbol) — used for snapshot-on-connect so
  // new WS clients don't sit blank during quiet periods.
  snapshot(): BBOMsg[] {
    return [...this.lastBbo.values()];
  }

  private deriveBbo(
    key: string,
    book: Book,
    msg: BookSnapshotMsg | BookDeltaMsg,
  ): BBOMsg | null {
    const bid = book.bestBid();
    const ask = book.bestAsk();
    if (!bid || !ask) return null; // one-sided book has no BBO

    // Crossed within-venue book (ask < bid): count it but still emit — a
    // faithful projection, not a silent fixer. The re-snapshot rewind that drove
    // this is now guarded (book.ts), so a residual cross is upstream corruption.
    const crossed = cmpDecimal(ask[0], bid[0]) < 0;
    if (crossed) bboCrossed.inc({ exchange: msg.exchange });
    if (CROSS_ONSET_LOG) {
      if (crossed && !(this.wasCrossed.get(key) ?? false)) {
        const ring = (this.recent.get(key) ?? []).join(" | ");
        console.error(
          `[cross-onset] ${key} bid=${bid[0]} ask=${ask[0]} @seq=${book.seq} recent: ${ring}`,
        );
      }
      this.wasCrossed.set(key, crossed);
    }

    const prev = this.lastBbo.get(key);
    if (
      prev &&
      prev.bid_px === bid[0] && prev.bid_sz === bid[1] &&
      prev.ask_px === ask[0] && prev.ask_sz === ask[1]
    ) {
      return null;
    }

    const bbo: BBOMsg = {
      t: "bbo",
      exchange: msg.exchange,
      symbol: msg.symbol,
      bid_px: bid[0],
      bid_sz: bid[1],
      ask_px: ask[0],
      ask_sz: ask[1],
      exchange_ts_ns: msg.exchange_ts_ns,
      local_ts_ns: msg.local_ts_ns,
    };
    this.lastBbo.set(key, bbo);
    return bbo;
  }
}
