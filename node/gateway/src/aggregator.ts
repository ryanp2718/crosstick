import { Book } from "./book.js";
import { cmpDecimal } from "./decimal.js";
import { bboCrossed, bookSnapshotStale } from "./metrics.js";
import type { BBOMsg, BookDeltaMsg, BookSnapshotMsg } from "./messages.js";

// Deltas retained per stream while waiting for its snapshot (drop-oldest on
// overflow — older deltas are the ones a snapshot's sequence supersedes).
export const MAX_PENDING_DELTAS = 10_000;

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
        return null;
      }
      if (!book.applyDelta(msg.sequence, msg.bids, msg.asks)) return null;
      return this.deriveBbo(key, book, msg);
    }

    let book = this.books.get(key);
    if (!book) {
      book = new Book();
      this.books.set(key, book);
    }
    if (!book.applySnapshot(msg.sequence, epoch, msg.bids, msg.asks)) {
      // Stale same-epoch re-snapshot the book already passed: skip the rewind.
      // Live book unchanged, same-epoch deltas already applied → nothing to drain.
      bookSnapshotStale.inc({ exchange: msg.exchange });
      return null;
    }
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
    if (cmpDecimal(ask[0], bid[0]) < 0) bboCrossed.inc({ exchange: msg.exchange });

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
