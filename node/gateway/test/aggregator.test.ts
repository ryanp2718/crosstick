import { describe, expect, it } from "vitest";

import { Aggregator, MAX_PENDING_DELTAS } from "../src/aggregator.js";
import type { BookDeltaMsg, BookSnapshotMsg, WireLevel } from "../src/messages.js";

function snap(seq: number, bids: WireLevel[], asks: WireLevel[]): BookSnapshotMsg {
  return {
    t: "snap", exchange: "kraken", symbol: "BTC/USD", sequence: seq,
    bids, asks, exchange_ts_ns: 1, local_ts_ns: 2,
  };
}

function delta(seq: number, bids: WireLevel[], asks: WireLevel[]): BookDeltaMsg {
  return {
    t: "delta", exchange: "kraken", symbol: "BTC/USD", sequence: seq,
    bids, asks, exchange_ts_ns: 1, local_ts_ns: 2,
  };
}

describe("Aggregator", () => {
  it("emits a BBO after a two-sided snapshot", () => {
    const a = new Aggregator();
    const bbo = a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
    expect(bbo).toMatchObject({
      t: "bbo", exchange: "kraken", symbol: "BTC/USD",
      bid_px: "100", bid_sz: "1", ask_px: "101", ask_sz: "2",
    });
  });

  it("withholds a BBO until both sides exist", () => {
    const a = new Aggregator();
    expect(a.applyBook(snap(0, [["100", "1"]], []))).toBeNull();
  });

  it("buffers a delta that arrives before any snapshot (no BBO yet)", () => {
    const a = new Aggregator();
    expect(a.applyBook(delta(1, [["100", "1"]], [["101", "1"]]))).toBeNull();
    expect(a.snapshot()).toEqual([]);
  });

  describe("pre-snapshot delta buffering (D2)", () => {
    it("drains buffered deltas on snapshot arrival, in order", () => {
      const a = new Aggregator();
      // Deltas arrive first (cross-topic race / unordered replay).
      a.applyBook(delta(1, [["100.5", "3"]], []));
      a.applyBook(delta(2, [], [["100.8", "2"]]));
      const bbo = a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
      // Snapshot state + both deltas, one coalesced BBO from the final book.
      expect(bbo).toMatchObject({ bid_px: "100.5", bid_sz: "3", ask_px: "100.8", ask_sz: "2" });
    });

    it("seq guard drops buffered deltas the snapshot supersedes", () => {
      const a = new Aggregator();
      a.applyBook(delta(3, [["99", "9"]], [])); // covered by snapshot seq 5
      a.applyBook(delta(7, [["100.5", "3"]], []));
      const bbo = a.applyBook(snap(5, [["100", "1"]], [["101", "2"]]));
      expect(bbo).toMatchObject({ bid_px: "100.5", ask_px: "101" });
      // The superseded delta's level must not leak into the book.
      expect(a.snapshot()[0].bid_px).toBe("100.5");
    });

    it("stamps the coalesced BBO from the last applied message", () => {
      const a = new Aggregator();
      const late: BookDeltaMsg = { ...delta(7, [["100.5", "3"]], []), local_ts_ns: 99 };
      a.applyBook(late);
      const bbo = a.applyBook(snap(5, [["100", "1"]], [["101", "2"]]));
      expect(bbo!.local_ts_ns).toBe(99);
    });

    it("uses the snapshot's ts when every buffered delta is stale", () => {
      const a = new Aggregator();
      a.applyBook({ ...delta(3, [["99", "9"]], []), local_ts_ns: 99 });
      const bbo = a.applyBook(snap(5, [["100", "1"]], [["101", "2"]]));
      expect(bbo!.local_ts_ns).toBe(2); // snap() helper's local_ts_ns
    });

    it("drops oldest on buffer overflow", () => {
      const a = new Aggregator();
      // Delta seq 1 sets the bid we expect to LOSE to overflow.
      a.applyBook(delta(1, [["50", "1"]], []));
      for (let i = 2; i <= MAX_PENDING_DELTAS + 1; i++) {
        a.applyBook(delta(i, [], [[`${200 + i}`, "1"]]));
      }
      const bbo = a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
      // seq-1 delta was evicted, so its bid never applies.
      expect(bbo!.bid_px).toBe("100");
    });

    it("buffers per stream independently", () => {
      const a = new Aggregator();
      const other: BookDeltaMsg = {
        t: "delta", exchange: "coinbase", symbol: "BTC-USD", sequence: 1,
        bids: [["999", "1"]], asks: [], exchange_ts_ns: 1, local_ts_ns: 2,
      };
      a.applyBook(other); // buffered under coinbase, must not affect kraken
      const bbo = a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
      expect(bbo).toMatchObject({ exchange: "kraken", bid_px: "100" });
    });
  });

  it("dedupes: a deep delta that doesn't move L1 emits nothing", () => {
    const a = new Aggregator();
    a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
    expect(a.applyBook(delta(1, [["98", "5"]], []))).toBeNull();
  });

  it("emits again when the top-of-book changes", () => {
    const a = new Aggregator();
    a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
    const bbo = a.applyBook(delta(1, [["100.5", "3"]], []));
    expect(bbo?.bid_px).toBe("100.5");
  });

  it("keeps books separate per (exchange, symbol)", () => {
    const a = new Aggregator();
    a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
    const other: BookSnapshotMsg = {
      t: "snap", exchange: "coinbase", symbol: "BTC-USD", sequence: 0,
      bids: [["200", "1"]], asks: [["201", "2"]], exchange_ts_ns: 1, local_ts_ns: 2,
    };
    const bbo = a.applyBook(other);
    expect(bbo).toMatchObject({ exchange: "coinbase", bid_px: "200" });
  });

  it("snapshot() returns the latest BBO per stream", () => {
    const a = new Aggregator();
    expect(a.snapshot()).toEqual([]);
    a.applyBook(snap(0, [["100", "1"]], [["101", "2"]]));
    a.applyBook(delta(1, [["100.5", "3"]], []));
    const snap1 = a.snapshot();
    expect(snap1).toHaveLength(1);
    expect(snap1[0]).toMatchObject({ exchange: "kraken", bid_px: "100.5" });
  });
});
