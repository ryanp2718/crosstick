import { describe, expect, it } from "vitest";

import { Book } from "../src/book.js";
import { cmpDecimal } from "../src/decimal.js";

describe("cmpDecimal", () => {
  it("orders by magnitude, not lexically", () => {
    expect(cmpDecimal("1000", "999")).toBeGreaterThan(0);
    expect(cmpDecimal("99.5", "100")).toBeLessThan(0);
  });

  it("compares fractions of differing length", () => {
    expect(cmpDecimal("0.05005", "0.0501")).toBeLessThan(0);
    expect(cmpDecimal("0.5", "0.05")).toBeGreaterThan(0);
  });

  it("treats trailing zeros as equal value", () => {
    expect(cmpDecimal("45285.20", "45285.2")).toBe(0);
    expect(cmpDecimal("100", "100.000")).toBe(0);
  });

  it("handles sub-1 values with empty integer part", () => {
    expect(cmpDecimal("0.1", "0.2")).toBeLessThan(0);
  });
});

describe("Book", () => {
  it("tracks best bid/ask from a snapshot", () => {
    const b = new Book();
    b.applySnapshot(1, [["100", "1"], ["99", "2"]], [["101", "0.5"], ["102", "3"]]);
    expect(b.bestBid()).toEqual(["100", "1"]);
    expect(b.bestAsk()).toEqual(["101", "0.5"]);
  });

  it("exposes next-best after a zero-size delete of the top", () => {
    const b = new Book();
    b.applySnapshot(1, [["100", "1"], ["99", "2"]], [["101", "1"]]);
    expect(b.applyDelta(2, [["100", "0"]], [])).toBe(true);
    expect(b.bestBid()).toEqual(["99", "2"]);
  });

  it("updates a level in place", () => {
    const b = new Book();
    b.applySnapshot(1, [["100", "1"]], [["101", "1"]]);
    b.applyDelta(2, [["100", "5"]], []);
    expect(b.bestBid()).toEqual(["100", "5"]);
  });

  it("drops a stale delta (seq <= current) without mutating", () => {
    const b = new Book();
    b.applySnapshot(5, [["100", "1"]], [["101", "1"]]);
    expect(b.applyDelta(5, [["100", "9"]], [])).toBe(false);
    expect(b.bestBid()).toEqual(["100", "1"]);
  });

  it("treats padded-zero sizes as removals", () => {
    const b = new Book();
    b.applySnapshot(1, [["100", "1"]], [["101", "1"]]);
    b.applyDelta(2, [], [["101", "0.00000000"]]);
    expect(b.bestAsk()).toBeNull();
  });

  it("resets unconditionally on a snapshot (reconnect with lower seq)", () => {
    const b = new Book();
    b.applySnapshot(50, [["100", "1"]], [["101", "1"]]);
    b.applySnapshot(0, [["200", "1"]], [["201", "1"]]);
    expect(b.bestBid()).toEqual(["200", "1"]);
    expect(b.seq).toBe(0);
  });
});
