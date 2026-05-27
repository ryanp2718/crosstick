import { describe, expect, it } from "vitest";

import { Aggregator } from "../src/aggregator.js";
import type { BookSnapshotMsg, TradeMsg } from "../src/messages.js";
import { routeMessage } from "../src/router.js";

describe("routeMessage", () => {
  it("publishes and broadcasts the same BBO from a two-sided snapshot", () => {
    const agg = new Aggregator();
    const snap: BookSnapshotMsg = {
      t: "snap", exchange: "kraken", symbol: "BTC/USD", sequence: 0,
      bids: [["100", "1"]], asks: [["101", "2"]], exchange_ts_ns: 1, local_ts_ns: 2,
    };
    const r = routeMessage(snap, agg);
    expect(r.publish).toMatchObject({ t: "bbo", bid_px: "100", ask_px: "101" });
    expect(r.broadcast).toBe(r.publish); // browsers get the derived BBO, not raw book
  });

  it("neither publishes nor broadcasts when there is no BBO change", () => {
    const agg = new Aggregator();
    const oneSided: BookSnapshotMsg = {
      t: "snap", exchange: "kraken", symbol: "BTC/USD", sequence: 0,
      bids: [["100", "1"]], asks: [], exchange_ts_ns: 1, local_ts_ns: 2,
    };
    const r = routeMessage(oneSided, agg);
    expect(r.publish).toBeNull();
    expect(r.broadcast).toBeNull();
  });

  it("relays trades to clients but never publishes them", () => {
    const agg = new Aggregator();
    const trade: TradeMsg = {
      t: "trade", exchange: "kraken", symbol: "BTC/USD", trade_id: "1",
      price: "100", size: "0.5", side: "bid", exchange_ts_ns: 1, local_ts_ns: 2,
    };
    const r = routeMessage(trade, agg);
    expect(r.publish).toBeNull();
    expect(r.broadcast).toBe(trade);
  });
});
