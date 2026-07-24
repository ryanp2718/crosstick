import { describe, expect, it } from "vitest";

import type { CanonicalInstrument } from "../src/canonical.js";
import type { BBOMsg, NBBOMsg } from "../src/messages.js";
import { crossBps, isFreshCross, NBBOAggregator } from "../src/nbbo.js";

const BTC_USD: CanonicalInstrument = {
  canonical_id: "BTC-USD",
  base: "BTC",
  quote: "USD",
  venues: [
    { exchange: "coinbase", symbol: "BTC-USD" },
    { exchange: "kraken", symbol: "BTC/USD" },
  ],
};

function bbo(
  exchange: string,
  symbol: string,
  bid_px: string,
  bid_sz: string,
  ask_px: string,
  ask_sz: string,
  local_ts_ns = 1_700_000_000_000_000_000,
): BBOMsg {
  return {
    t: "bbo",
    exchange,
    symbol,
    bid_px,
    bid_sz,
    ask_px,
    ask_sz,
    exchange_ts_ns: local_ts_ns,
    local_ts_ns,
  };
}

describe("NBBOAggregator", () => {
  describe("single leg", () => {
    it("emits an NBBO equal to the single source BBO", () => {
      const agg = new NBBOAggregator();
      const nbbo = agg.onBBO(
        BTC_USD,
        bbo("coinbase", "BTC-USD", "100", "1", "101", "2"),
        1_700_000_000_000,
      );
      expect(nbbo).not.toBeNull();
      expect(nbbo!).toMatchObject({
        t: "nbbo",
        canonical_id: "BTC-USD",
        base: "BTC",
        quote: "USD",
        constituents: ["coinbase"],
        spread: 1,
        mid: 100.5,
      });
      expect(nbbo!.best_bid).toMatchObject({ px: "100", sz: "1", exchange: "coinbase" });
      expect(nbbo!.best_ask).toMatchObject({ px: "101", sz: "2", exchange: "coinbase" });
    });

    it("dedupes when the same BBO is fed twice", () => {
      const agg = new NBBOAggregator();
      const first = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "2"), 1);
      const second = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "2"), 2);
      expect(first).not.toBeNull();
      expect(second).toBeNull();
    });

    it("emits again when L1 changes", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "2"), 1);
      const next = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "3", "101", "2"), 2);
      expect(next).not.toBeNull();
      expect(next!.best_bid.sz).toBe("3");
    });
  });

  describe("two legs", () => {
    it("picks higher bid and lower ask across venues", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100.0", "1", "101.0", "1"), 1);
      const nbbo = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "100.5", "1", "100.9", "1"), 2);
      expect(nbbo).not.toBeNull();
      expect(nbbo!.best_bid).toMatchObject({ px: "100.5", exchange: "kraken" });
      expect(nbbo!.best_ask).toMatchObject({ px: "100.9", exchange: "kraken" });
      expect(nbbo!.constituents.sort()).toEqual(["coinbase", "kraken"]);
    });

    it("populates leg_age_ms relative to caller-supplied now", () => {
      const agg = new NBBOAggregator();
      const tsNs = 1_700_000_000_000_000_000;
      const tsMs = tsNs / 1e6;
      const nbbo = agg.onBBO(
        BTC_USD,
        bbo("coinbase", "BTC-USD", "100", "1", "101", "1", tsNs),
        tsMs + 50,
      );
      expect(nbbo!.best_bid.leg_age_ms).toBe(50);
      expect(nbbo!.best_ask.leg_age_ms).toBe(50);
    });
  });

  describe("tie-break", () => {
    it("larger size wins on same price (bid)", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "200", "1"), 1);
      const nbbo = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "100", "5", "200", "1"), 2);
      expect(nbbo!.best_bid).toMatchObject({ exchange: "kraken", sz: "5" });
    });

    it("alphabetical exchange wins on same price and size (visible via snapshot)", () => {
      const agg = new NBBOAggregator();
      // Order matters: kraken first emits as sole leg, then coinbase joins
      // with identical L1 → dedup'd (no emit), but the winner internally
      // switches to coinbase by alpha. snapshot() surfaces the current state.
      agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "100", "1", "200", "1"), 1);
      const dedup = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "200", "1"), 2);
      expect(dedup).toBeNull();
      const snap = agg.snapshot(2);
      expect(snap).toHaveLength(1);
      expect(snap[0].best_bid.exchange).toBe("coinbase");
      expect(snap[0].best_ask.exchange).toBe("coinbase");
    });

    it("does not emit when a leg switch leaves the L1 tuple unchanged", () => {
      const agg = new NBBOAggregator();
      // venueA wins (only leg) at (100/1, 200/1)
      const first = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "100", "1", "200", "1"), 1);
      expect(first).not.toBeNull();
      // venueB joins with identical L1 (same px, same sz). coinbase wins
      // alphabetically - but L1 tuple is unchanged, so no emit.
      const second = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "200", "1"), 2);
      expect(second).toBeNull();
    });
  });

  describe("snapshot", () => {
    it("returns one NBBO per canonical with refreshed leg_age_ms", () => {
      const agg = new NBBOAggregator();
      const tsNs = 1_700_000_000_000_000_000;
      const tsMs = tsNs / 1e6;
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1", tsNs), tsMs);
      const snap = agg.snapshot(tsMs + 500);
      expect(snap).toHaveLength(1);
      expect(snap[0].best_bid.leg_age_ms).toBe(500);
    });

    it("returns empty when no legs have been received", () => {
      const agg = new NBBOAggregator();
      expect(agg.snapshot(0)).toEqual([]);
    });
  });

  describe("crossed market", () => {
    it("still emits with negative spread when bid > ask", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      const crossed = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "102", "1", "103", "1"), 2);
      expect(crossed).not.toBeNull();
      // best_bid = kraken@102, best_ask = coinbase@101 → spread = -1
      expect(crossed!.spread).toBe(-1);
      expect(crossed!.crossed).toBe(true);
      expect(crossed!.best_bid.exchange).toBe("kraken");
      expect(crossed!.best_ask.exchange).toBe("coinbase");
    });
  });

  describe("exact decimal handling", () => {
    it("passes leg px/sz through as lossless strings (no Number() rounding)", () => {
      const agg = new NBBOAggregator();
      // 1e8 + 1e-8 exceeds float64's ~15-16 sig digits; Number() drops the
      // trailing 1. Verbatim strings keep it exact for the downstream warehouse.
      const px = "100000000.00000001";
      const nbbo = agg.onBBO(
        BTC_USD,
        bbo("coinbase", "BTC-USD", px, "0.10000000", "100000000.00000002", "0.20000000"),
        1,
      );
      expect(nbbo!.best_bid.px).toBe(px);
      expect(nbbo!.best_bid.sz).toBe("0.10000000");
      expect(nbbo!.crossed).toBe(false);
    });

    it("flags crossed via exact comparison and clears it when uncrossed", () => {
      const agg = new NBBOAggregator();
      const uncrossed = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      expect(uncrossed!.crossed).toBe(false);
      const crossed = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "102", "1", "103", "1"), 2);
      expect(crossed!.crossed).toBe(true);
    });
  });

  describe("venue-down eviction", () => {
    it("evicts the down venue's leg, uncrossing a stale-leg cross", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      // kraken joins crossed: bid 102 > coinbase ask 101
      const crossed = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "102", "1", "103", "1"), 2);
      expect(crossed!.spread).toBe(-1);
      expect(crossed!.best_bid.exchange).toBe("kraken");

      const out = agg.setVenueDown("kraken", true, 3);
      expect(out).toHaveLength(1);
      expect(out[0].constituents).toEqual(["coinbase"]);
      expect(out[0].best_bid.exchange).toBe("coinbase");
      expect(out[0].best_ask.exchange).toBe("coinbase");
      expect(out[0].spread).toBe(1); // 101 - 100, uncrossed
    });

    it("snapshot excludes down venues", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "102", "1", "103", "1"), 2);
      agg.setVenueDown("kraken", true, 3);
      const snap = agg.snapshot(4);
      expect(snap).toHaveLength(1);
      expect(snap[0].constituents).toEqual(["coinbase"]);
      expect(snap[0].best_bid.exchange).toBe("coinbase");
    });

    it("setVenueDown(false) re-includes the venue's leg", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "102", "1", "103", "1"), 2);
      agg.setVenueDown("kraken", true, 3);
      const back = agg.setVenueDown("kraken", false, 4);
      expect(back).toHaveLength(1);
      expect(back[0].constituents.sort()).toEqual(["coinbase", "kraken"]);
      expect(back[0].best_bid.exchange).toBe("kraken");
      expect(back[0].spread).toBe(-1); // crossed again
    });

    it("returns [] for a venue with no legs", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      expect(agg.setVenueDown("binance", true, 2)).toEqual([]);
    });

    it("is idempotent - re-marking the current state returns []", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "102", "1", "103", "1"), 2);
      expect(agg.setVenueDown("kraken", true, 3)).toHaveLength(1);
      expect(agg.setVenueDown("kraken", true, 4)).toEqual([]); // already down
    });

    it("onBBO keeps a down venue excluded until it is marked back up", () => {
      const agg = new NBBOAggregator();
      agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
      agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "102", "1", "103", "1"), 2);
      agg.setVenueDown("kraken", true, 3);
      // fresh kraken quote arrives while still marked down → not in NBBO
      const live = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", "104", "1", "105", "1"), 4);
      if (live) expect(live.constituents).toEqual(["coinbase"]);
      expect(agg.snapshot(5)[0].constituents).toEqual(["coinbase"]);
    });
  });
});

describe("isFreshCross", () => {
  // Build a crossed NBBO with controllable per-leg ages. Each leg is quoted at
  // an explicit ms instant (local_ts_ns = ms * 1e6, exact in float64) and read
  // back at nowMs, so leg_age_ms = nowMs - quoted-ms is exact. coinbase carries
  // the lower ask; kraken carries the higher, crossing bid.
  function crossedNbbo(bidLegMs: number, askLegMs: number, nowMs: number): NBBOMsg {
    const agg = new NBBOAggregator();
    agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1", askLegMs * 1e6), nowMs);
    const nbbo = agg.onBBO(
      BTC_USD,
      bbo("kraken", "BTC/USD", "102", "1", "103", "1", bidLegMs * 1e6),
      nowMs,
    );
    if (!nbbo || !nbbo.crossed) throw new Error("expected a crossed NBBO");
    return nbbo;
  }

  it("counts a cross when both winning legs are fresh", () => {
    expect(isFreshCross(crossedNbbo(2900, 2900, 3000), 1000)).toBe(true); // both 100ms old
  });

  it("ignores a cross carried by a stale winning leg", () => {
    const nbbo = crossedNbbo(1000, 3000, 3000); // bid leg 2000ms old, ask fresh
    expect(nbbo.best_bid.leg_age_ms).toBe(2000);
    expect(nbbo.best_ask.leg_age_ms).toBe(0);
    expect(isFreshCross(nbbo, 1000)).toBe(false);
  });

  it("treats the age threshold as inclusive on both legs", () => {
    const nbbo = crossedNbbo(2000, 2000, 3000); // both exactly 1000ms old
    expect(isFreshCross(nbbo, 1000)).toBe(true);
    expect(isFreshCross(nbbo, 999)).toBe(false);
  });

  it("never counts an uncrossed NBBO, however fresh", () => {
    const agg = new NBBOAggregator();
    const nbbo = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
    expect(nbbo!.crossed).toBe(false);
    expect(isFreshCross(nbbo!, 1_000_000)).toBe(false);
  });
});

describe("crossBps", () => {
  // A cross where kraken's bid crosses coinbase's (lower) ask; kraken's own ask is
  // parked far away so coinbase always wins the ask. Legs default to a real ns
  // timestamp read back at nowMs≈0, so both are age 0 (fresh).
  function cross(bidPx: string, askPx: string): NBBOMsg {
    const agg = new NBBOAggregator();
    agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "1", "1", askPx, "1"), 1);
    const nbbo = agg.onBBO(BTC_USD, bbo("kraken", "BTC/USD", bidPx, "1", "9999999", "1"), 2);
    if (!nbbo?.crossed) throw new Error("expected a crossed NBBO");
    return nbbo;
  }

  it("reports the cross depth in bps, positive when crossed", () => {
    expect(crossBps(cross("101", "100"))).toBeCloseTo(99.5, 1); // (1/100.5)*1e4
  });

  it("stays ~1 bp for a benign tick-scale venue lock", () => {
    expect(crossBps(cross("100.01", "100.00"))).toBeCloseTo(1.0, 1);
  });

  it("is <= 0 for an uncrossed book (never counted anyway)", () => {
    const agg = new NBBOAggregator();
    const nbbo = agg.onBBO(BTC_USD, bbo("coinbase", "BTC-USD", "100", "1", "101", "1"), 1);
    expect(nbbo!.crossed).toBe(false);
    expect(crossBps(nbbo!)).toBeLessThanOrEqual(0);
  });

  it("gates the material counter as the server does: fresh AND >= floor", () => {
    const FLOOR = 10;
    const material = (n: NBBOMsg) => isFreshCross(n, 1000) && crossBps(n) >= FLOOR;
    expect(material(cross("101", "100"))).toBe(true); // ~100 bps inversion
    expect(material(cross("100.01", "100.00"))).toBe(false); // ~1 bp, below floor
  });
});
