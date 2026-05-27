import { describe, expect, it } from "vitest";

import { Aggregator } from "../src/aggregator.js";
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

  it("drops deltas that arrive before any snapshot", () => {
    const a = new Aggregator();
    expect(a.applyBook(delta(1, [["100", "1"]], [["101", "1"]]))).toBeNull();
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
});
