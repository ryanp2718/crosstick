import { describe, expect, it } from "vitest";

import { Aggregator } from "../src/aggregator.js";
import { CanonicalMap } from "../src/canonical.js";
import type { BookSnapshotMsg, TradeMsg } from "../src/messages.js";
import { NBBOAggregator } from "../src/nbbo.js";
import { routeMessage } from "../src/router.js";

const mappedCanonicalMap = new CanonicalMap([
  {
    canonical_id: "BTC-USD",
    base: "BTC",
    quote: "USD",
    venues: [{ exchange: "kraken", symbol: "BTC/USD" }],
  },
]);

const emptyCanonicalMap = new CanonicalMap([]);

describe("routeMessage", () => {
  it("publishes and broadcasts the same BBO from a two-sided snapshot", () => {
    const agg = new Aggregator();
    const nbboAgg = new NBBOAggregator();
    const snap: BookSnapshotMsg = {
      t: "snap",
      exchange: "kraken",
      symbol: "BTC/USD",
      sequence: 0,
      bids: [["100", "1"]],
      asks: [["101", "2"]],
      exchange_ts_ns: 1,
      local_ts_ns: 2,
    };
    const r = routeMessage(snap, agg, mappedCanonicalMap, nbboAgg, 2);
    expect(r.publish).toMatchObject({ t: "bbo", bid_px: "100", ask_px: "101" });
    expect(r.broadcast).toBe(r.publish);
  });

  it("neither publishes nor broadcasts when there is no BBO change", () => {
    const agg = new Aggregator();
    const nbboAgg = new NBBOAggregator();
    const oneSided: BookSnapshotMsg = {
      t: "snap",
      exchange: "kraken",
      symbol: "BTC/USD",
      sequence: 0,
      bids: [["100", "1"]],
      asks: [],
      exchange_ts_ns: 1,
      local_ts_ns: 2,
    };
    const r = routeMessage(oneSided, agg, mappedCanonicalMap, nbboAgg, 2);
    expect(r.publish).toBeNull();
    expect(r.broadcast).toBeNull();
    expect(r.nbboPublish).toBeNull();
    expect(r.nbboBroadcast).toBeNull();
  });

  it("relays trades to clients but never publishes them", () => {
    const agg = new Aggregator();
    const nbboAgg = new NBBOAggregator();
    const trade: TradeMsg = {
      t: "trade",
      exchange: "kraken",
      symbol: "BTC/USD",
      trade_id: "1",
      price: "100",
      size: "0.5",
      side: "bid",
      exchange_ts_ns: 1,
      local_ts_ns: 2,
    };
    const r = routeMessage(trade, agg, mappedCanonicalMap, nbboAgg, 2);
    expect(r.publish).toBeNull();
    expect(r.broadcast).toBe(trade);
    expect(r.nbboPublish).toBeNull();
  });

  it("emits NBBO alongside BBO when the venue is in the canonical map", () => {
    const agg = new Aggregator();
    const nbboAgg = new NBBOAggregator();
    const snap: BookSnapshotMsg = {
      t: "snap",
      exchange: "kraken",
      symbol: "BTC/USD",
      sequence: 0,
      bids: [["100", "1"]],
      asks: [["101", "2"]],
      exchange_ts_ns: 1,
      local_ts_ns: 2,
    };
    const r = routeMessage(snap, agg, mappedCanonicalMap, nbboAgg, 2);
    expect(r.nbboPublish).toMatchObject({ t: "nbbo", canonical_id: "BTC-USD" });
    expect(r.nbboBroadcast).toBe(r.nbboPublish);
  });

  it("stamps NBBO from the caller's stream time, not wall clock (D1)", () => {
    const agg = new Aggregator();
    const nbboAgg = new NBBOAggregator();
    const tsNs = 1_700_000_000_000_000_000;
    const snap: BookSnapshotMsg = {
      t: "snap",
      exchange: "kraken",
      symbol: "BTC/USD",
      sequence: 0,
      bids: [["100", "1"]],
      asks: [["101", "2"]],
      exchange_ts_ns: tsNs,
      local_ts_ns: tsNs,
    };
    const nowMs = tsNs / 1e6 + 25;
    const r = routeMessage(snap, agg, mappedCanonicalMap, nbboAgg, nowMs);
    expect(r.nbboPublish!.local_ts_ns).toBe(nowMs * 1e6);
    expect(r.nbboPublish!.best_bid.leg_age_ms).toBe(25);
  });

  it("skips NBBO when the venue is not mapped", () => {
    const agg = new Aggregator();
    const nbboAgg = new NBBOAggregator();
    const snap: BookSnapshotMsg = {
      t: "snap",
      exchange: "kraken",
      symbol: "BTC/USD",
      sequence: 0,
      bids: [["100", "1"]],
      asks: [["101", "2"]],
      exchange_ts_ns: 1,
      local_ts_ns: 2,
    };
    const r = routeMessage(snap, agg, emptyCanonicalMap, nbboAgg, 2);
    expect(r.publish).not.toBeNull();
    expect(r.nbboPublish).toBeNull();
  });
});
