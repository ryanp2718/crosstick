import { Book } from "./book.js";
import { cmpDecimal } from "./decimal.js";
import {
  bboCrossed,
  bookHealReplayDepth,
  bookHealReplayUnderrun,
  bookResnapshotHeal,
  bookSnapshotStale,
} from "./metrics.js";
import type { BBOMsg, BookDeltaMsg, BookSnapshotMsg } from "./messages.js";

// Deltas retained per stream while waiting for its snapshot (drop-oldest on
// overflow — the oldest buffered deltas are the lowest-seq ones a snapshot's
// sequence supersedes, so shedding them never gaps the book above the snapshot).
export const MAX_PENDING_DELTAS = 10_000;

// Deltas retained per stream AFTER application, so an accepted rewind (the
// crossed-book heal in book.ts applySnapshot) can replay the tail it rewinds
// past instead of resurrecting since-deleted levels: the snapshot and its
// superseding deltas ride separate topics with no cross-topic order, and Kafka
// is already past a delta the rewind discards. Pruned to seq > the last
// accepted snapshot, since the snapshot topic is consumed in order, so no
// later heal can need entries an earlier accepted snapshot already covers.
export const MAX_APPLIED_TAIL = 10_000;

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
  // Applied-delta tail per stream (see MAX_APPLIED_TAIL) and the newest entry
  // each stream has evicted to overflow: a heal snapshot older than that seq
  // lost part of its replay tail (counted as underrun).
  private readonly appliedTail = new Map<string, BookDeltaMsg[]>();
  private readonly tailEvicted = new Map<string, { epoch: number; seq: number }>();
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

  private recordApplied(key: string, msg: BookDeltaMsg): void {
    const tail = this.appliedTail.get(key) ?? [];
    if (tail.length >= MAX_APPLIED_TAIL) {
      // Shed the oldest half in one splice: amortized O(1) per applied delta,
      // unlike a per-push shift of a full tail on the hot path.
      const dropped = tail.splice(0, MAX_APPLIED_TAIL >> 1);
      const newest = dropped[dropped.length - 1];
      this.tailEvicted.set(key, { epoch: newest.epoch ?? 0, seq: newest.sequence });
    }
    tail.push(msg);
    this.appliedTail.set(key, tail);
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
      if (applied) this.recordApplied(key, msg);
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
    let last: BookSnapshotMsg | BookDeltaMsg = msg;
    // Replay the applied tail over the snapshot. Only a heal rewind has tail
    // entries above the snapshot's seq; for a forward snapshot the seq guard
    // rejects every entry (and the epoch filter drops prior-connection tails,
    // whose reset counters could out-rank a fresh snapshot's low seq). Level
    // ops are absolute set/delete, so replay converges to snapshot + tail
    // regardless of what the snapshot already reflected.
    const tail = this.appliedTail.get(key) ?? [];
    let replayed = 0;
    for (const d of tail) {
      if ((d.epoch ?? 0) !== epoch) continue;
      if (book.applyDelta(d.sequence, d.bids, d.asks)) {
        last = d;
        replayed++;
      }
    }
    if (CROSS_ONSET_LOG && replayed > 0) this.rec(key, `replay:${replayed}`);
    const kept = tail.filter((d) => (d.epoch ?? 0) === epoch && d.sequence > msg.sequence);
    if (kept.length > 0) this.appliedTail.set(key, kept);
    else this.appliedTail.delete(key);
    const evicted = this.tailEvicted.get(key);
    if (evicted && (evicted.epoch !== epoch || evicted.seq <= msg.sequence)) {
      this.tailEvicted.delete(key);
    }
    if (staleReSnapshot) {
      bookHealReplayDepth.observe({ exchange: msg.exchange }, replayed);
      if (evicted && evicted.epoch === epoch && evicted.seq > msg.sequence) {
        bookHealReplayUnderrun.inc({ exchange: msg.exchange });
      }
    }
    // Drain only this snapshot's own epoch (seq guard drops what it supersedes);
    // retain other epochs for their own snapshot. ts from the last applied msg.
    const retained: BookDeltaMsg[] = [];
    for (const delta of this.pending.get(key) ?? []) {
      if ((delta.epoch ?? 0) !== epoch) {
        retained.push(delta);
        continue;
      }
      if (book.applyDelta(delta.sequence, delta.bids, delta.asks)) {
        last = delta;
        this.recordApplied(key, delta);
      }
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
