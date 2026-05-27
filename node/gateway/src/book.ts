import BTree from "sorted-btree";

import { cmpDecimal, isZeroSize } from "./decimal.js";
import type { WireLevel } from "./messages.js";

export type Level = [string, string]; // [price, size]

// A depth book per (exchange, symbol), kept only to derive top-of-book. It is a
// lightweight port of the relevant subset of python/ingest/book.py: enough state
// to know the next-best level when the current best is deleted. No CRC, no
// crossed-book guard, no full sequence gap-detection — the ingester already
// validated all of that; here a monotonic-sequence guard on deltas is enough,
// and a fresh snapshot is authoritative (it resets the book unconditionally).
export class Book {
  private readonly bids = new BTree<string, string>(undefined, cmpDecimal);
  private readonly asks = new BTree<string, string>(undefined, cmpDecimal);
  seq = -1;

  applySnapshot(seq: number, bids: WireLevel[], asks: WireLevel[]): void {
    this.bids.clear();
    this.asks.clear();
    for (const [px, sz] of bids) if (!isZeroSize(sz)) this.bids.set(px, sz);
    for (const [px, sz] of asks) if (!isZeroSize(sz)) this.asks.set(px, sz);
    this.seq = seq;
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
