// sorted-btree's CJS module ships the class as both `module.exports` and
// `.default`; the ESNext + esModuleInterop default-import lands on the wrapper
// at runtime under tsx (vitest's transform papers over it). Unwrap explicitly.
import BTreeImport from "sorted-btree";
const BTree = ((BTreeImport as unknown) as { default?: typeof BTreeImport }).default
  ?? BTreeImport;

import { cmpDecimal, isZeroSize } from "./decimal.js";
import type { WireLevel } from "./messages.js";

export type Level = [string, string]; // [price, size]

// A depth book per (exchange, symbol), kept only to derive top-of-book. It is a
// lightweight port of the relevant subset of python/ingest/book.py: enough state
// to know the next-best level when the current best is deleted. No CRC, no
// crossed-book guard, no full sequence gap-detection — the ingester already
// validated all of that; here a monotonic-sequence guard on deltas is enough,
// and a fresh snapshot resets the book, except a stale same-epoch re-snapshot
// the book has already advanced past (see applySnapshot).
export class Book {
  private readonly bids = new BTree<string, string>(undefined, cmpDecimal);
  private readonly asks = new BTree<string, string>(undefined, cmpDecimal);
  seq = -1;
  // Connection generation of the snapshot this book was built from. The
  // aggregator only applies deltas of the same epoch, so a prior connection's
  // deltas (whose reset sequence counter could out-rank this snapshot) can't
  // corrupt it. See aggregator.ts.
  epoch = 0;

  // Returns false (mutates nothing) for a stale same-epoch re-snapshot whose seq
  // the book has already passed — resetting to it would rewind and resurrect
  // since-deleted levels, crossing newer quotes. A genuine resync is a new epoch.
  applySnapshot(seq: number, epoch: number, bids: WireLevel[], asks: WireLevel[]): boolean {
    if (epoch === this.epoch && seq <= this.seq) return false;
    this.bids.clear();
    this.asks.clear();
    for (const [px, sz] of bids) if (!isZeroSize(sz)) this.bids.set(px, sz);
    for (const [px, sz] of asks) if (!isZeroSize(sz)) this.asks.set(px, sz);
    this.seq = seq;
    this.epoch = epoch;
    return true;
  }

  // Returns false (and mutates nothing) for a stale/duplicate delta.
  applyDelta(seq: number, bids: WireLevel[], asks: WireLevel[]): boolean {
    if (seq <= this.seq) return false;
    for (const [px, sz] of bids) {
      if (isZeroSize(sz)) this.bids.delete(px);
      else this.bids.set(px, sz);
    }
    for (const [px, sz] of asks) {
      if (isZeroSize(sz)) this.asks.delete(px);
      else this.asks.set(px, sz);
    }
    this.seq = seq;
    return true;
  }

  bestBid(): Level | null {
    const px = this.bids.maxKey();
    return px === undefined ? null : [px, this.bids.get(px)!];
  }

  bestAsk(): Level | null {
    const px = this.asks.minKey();
    return px === undefined ? null : [px, this.asks.get(px)!];
  }
}
