import { describe, expect, it } from "vitest";

import { classifyTopic, DrainGate, planWarmStart, type AdminLike } from "../src/warmstart.js";

// Stub admin: per-topic partition offsets plus the offset-for-timestamp answer
// the broker would give for the warm-start cutoff.
function stubAdmin(data: {
  [topic: string]: {
    partitions: Array<{ partition: number; high: string; low: string }>;
    atCutoff?: Array<{ partition: number; offset: string }>;
  };
}): AdminLike {
  return {
    fetchTopicOffsets: (topic) =>
      Promise.resolve(
        data[topic].partitions.map((p) => ({ ...p, offset: p.high })),
      ),
    fetchTopicOffsetsByTimestamp: (topic) =>
      Promise.resolve(data[topic].atCutoff ?? []),
  };
}

const NOW = 1_700_000_000_000;
const LOOKBACK = 600_000;

describe("classifyTopic", () => {
  it("classifies the gateway's consumed topic shapes", () => {
    expect(classifyTopic("md.book.kraken.BTC-USD.snapshots")).toBe("snapshots");
    expect(classifyTopic("md.book.kraken.BTC-USD.deltas")).toBe("deltas");
    expect(classifyTopic("md.trades.kraken.BTC-USD")).toBe("trades");
    expect(classifyTopic("md.status.kraken")).toBe("status");
    expect(classifyTopic("md.bbo.kraken.BTC-USD")).toBe("other");
    expect(classifyTopic("md.nbbo.BTC-USD")).toBe("other");
  });
});

describe("planWarmStart", () => {
  it("seeks snapshots to HWM-1 when a snapshot exists inside the lookback", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.snapshots": {
        partitions: [{ partition: 0, high: "12", low: "0" }],
        atCutoff: [{ partition: 0, offset: "10" }], // ≥1 snapshot after cutoff
      },
    });
    const seeks = await planWarmStart(admin, ["md.book.kraken.BTC-USD.snapshots"], NOW, LOOKBACK);
    expect(seeks).toEqual([
      { topic: "md.book.kraken.BTC-USD.snapshots", partition: 0, offset: "11", drainTo: "11" },
    ]);
  });

  it("seeks snapshots to the live edge when none are inside the lookback", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.snapshots": {
        partitions: [{ partition: 0, high: "12", low: "0" }],
        atCutoff: [{ partition: 0, offset: "-1" }], // broker: no message ≥ cutoff
      },
    });
    const seeks = await planWarmStart(admin, ["md.book.kraken.BTC-USD.snapshots"], NOW, LOOKBACK);
    expect(seeks[0].offset).toBe("12");
  });

  it("treats a missing partition in the timestamp answer as not-within-lookback", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.snapshots": {
        partitions: [{ partition: 0, high: "12", low: "0" }],
        atCutoff: [],
      },
    });
    const seeks = await planWarmStart(admin, ["md.book.kraken.BTC-USD.snapshots"], NOW, LOOKBACK);
    expect(seeks[0].offset).toBe("12");
  });

  it("seeks deltas to the first offset at/after the cutoff", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.deltas": {
        partitions: [{ partition: 0, high: "5000", low: "0" }],
        atCutoff: [{ partition: 0, offset: "4200" }],
      },
    });
    const seeks = await planWarmStart(admin, ["md.book.kraken.BTC-USD.deltas"], NOW, LOOKBACK);
    expect(seeks[0].offset).toBe("4200");
  });

  it("seeks deltas to the live edge when the whole topic predates the cutoff", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.deltas": {
        partitions: [{ partition: 0, high: "5000", low: "0" }],
        atCutoff: [{ partition: 0, offset: "-1" }],
      },
    });
    const seeks = await planWarmStart(admin, ["md.book.kraken.BTC-USD.deltas"], NOW, LOOKBACK);
    expect(seeks[0].offset).toBe("5000");
  });

  it("seeks trades to the live edge and status to the first offset at/after the cutoff", async () => {
    const admin = stubAdmin({
      "md.trades.kraken.BTC-USD": {
        partitions: [{ partition: 0, high: "900", low: "100" }],
      },
      "md.status.kraken": {
        partitions: [{ partition: 0, high: "40", low: "3" }],
        atCutoff: [{ partition: 0, offset: "31" }], // recent heartbeat inside lookback
      },
    });
    const seeks = await planWarmStart(
      admin, ["md.trades.kraken.BTC-USD", "md.status.kraken"], NOW, LOOKBACK,
    );
    expect(seeks).toEqual([
      { topic: "md.trades.kraken.BTC-USD", partition: 0, offset: "900" },
      { topic: "md.status.kraken", partition: 0, offset: "31" }, // seeded, no drainTo
    ]);
  });

  it("seeks status to the live edge when no heartbeat is inside the lookback", async () => {
    const admin = stubAdmin({
      "md.status.kraken": {
        partitions: [{ partition: 0, high: "40", low: "3" }],
        atCutoff: [{ partition: 0, offset: "-1" }], // broker: no message ≥ cutoff
      },
    });
    const seeks = await planWarmStart(admin, ["md.status.kraken"], NOW, LOOKBACK);
    expect(seeks[0].offset).toBe("40");
  });

  it("skips topics the gateway doesn't warm from", async () => {
    const admin = stubAdmin({});
    const seeks = await planWarmStart(admin, ["md.nbbo.BTC-USD"], NOW, LOOKBACK);
    expect(seeks).toEqual([]);
  });

  it("handles an empty topic (low == high) without going negative", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.snapshots": {
        partitions: [{ partition: 0, high: "0", low: "0" }],
        atCutoff: [{ partition: 0, offset: "-1" }],
      },
    });
    const seeks = await planWarmStart(admin, ["md.book.kraken.BTC-USD.snapshots"], NOW, LOOKBACK);
    expect(seeks).toEqual([
      { topic: "md.book.kraken.BTC-USD.snapshots", partition: 0, offset: "0" },
    ]);
  });

  it("plans every partition of a multi-partition topic", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.deltas": {
        partitions: [
          { partition: 0, high: "100", low: "0" },
          { partition: 1, high: "200", low: "0" },
        ],
        atCutoff: [
          { partition: 0, offset: "90" },
          { partition: 1, offset: "-1" },
        ],
      },
    });
    const seeks = await planWarmStart(admin, ["md.book.kraken.BTC-USD.deltas"], NOW, LOOKBACK);
    expect(seeks).toEqual([
      { topic: "md.book.kraken.BTC-USD.deltas", partition: 0, offset: "90", drainTo: "99" },
      { topic: "md.book.kraken.BTC-USD.deltas", partition: 1, offset: "200" },
    ]);
  });

  it("records drainTo on deltas sought below the live edge, but not at it", async () => {
    const admin = stubAdmin({
      "md.book.kraken.BTC-USD.deltas": {
        partitions: [{ partition: 0, high: "5000", low: "0" }],
        atCutoff: [{ partition: 0, offset: "4200" }],
      },
    });
    const [backlogged] = await planWarmStart(
      admin, ["md.book.kraken.BTC-USD.deltas"], NOW, LOOKBACK,
    );
    expect(backlogged.drainTo).toBe("4999"); // hwm - 1

    const liveEdge = stubAdmin({
      "md.book.kraken.BTC-USD.deltas": {
        partitions: [{ partition: 0, high: "5000", low: "0" }],
        atCutoff: [{ partition: 0, offset: "-1" }], // nothing recent → seek to edge
      },
    });
    const [atEdge] = await planWarmStart(liveEdge, ["md.book.kraken.BTC-USD.deltas"], NOW, LOOKBACK);
    expect(atEdge.drainTo).toBeUndefined();
  });

  it("leaves trades and status without a drainTo (no backlog to gate)", async () => {
    const admin = stubAdmin({
      "md.trades.kraken.BTC-USD": { partitions: [{ partition: 0, high: "900", low: "100" }] },
      "md.status.kraken": { partitions: [{ partition: 0, high: "40", low: "3" }] },
    });
    const seeks = await planWarmStart(
      admin, ["md.trades.kraken.BTC-USD", "md.status.kraken"], NOW, LOOKBACK,
    );
    expect(seeks.every((s) => s.drainTo === undefined)).toBe(true);
  });
});

describe("DrainGate", () => {
  const SNAP = "md.book.kraken.BTC-USD.snapshots";
  const DELTA = "md.book.coinbase.BTC-USD.deltas";

  it("is never warming when the plan has no backlog (clean start / replay test)", () => {
    const gate = new DrainGate([
      { topic: "md.trades.kraken.BTC-USD", partition: 0, offset: "900" },
      { topic: SNAP, partition: 0, offset: "12" }, // live-edge seek, no drainTo
    ]);
    expect(gate.warming).toBe(false);
    expect(gate.observe(SNAP, 0, "12")).toBe(false);
  });

  it("holds until a single backlogged partition reaches its drain target", () => {
    const gate = new DrainGate([{ topic: DELTA, partition: 0, offset: "90", drainTo: "99" }]);
    expect(gate.warming).toBe(true);
    expect(gate.observe(DELTA, 0, "95")).toBe(false); // mid-backlog
    expect(gate.warming).toBe(true);
    expect(gate.observe(DELTA, 0, "99")).toBe(true); // reaches drainTo → opens
    expect(gate.warming).toBe(false);
  });

  it("opens only once every backlogged partition has drained", () => {
    const gate = new DrainGate([
      { topic: SNAP, partition: 0, offset: "11", drainTo: "11" },
      { topic: DELTA, partition: 0, offset: "4200", drainTo: "4999" },
    ]);
    expect(gate.observe(SNAP, 0, "11")).toBe(false); // one of two drained
    expect(gate.warming).toBe(true);
    expect(gate.observe(DELTA, 0, "4999")).toBe(true); // second → opens
    expect(gate.warming).toBe(false);
  });

  it("ignores a pre-seek straggler already past the backlog", () => {
    const gate = new DrainGate([{ topic: DELTA, partition: 0, offset: "4200", drainTo: "4999" }]);
    // A committed-edge message (offset 5200 > drainTo) arrives before the
    // re-sought backlog: it must not open the gate.
    expect(gate.observe(DELTA, 0, "5200")).toBe(false);
    expect(gate.warming).toBe(true);
    // The replay then drains the backlog for real.
    expect(gate.observe(DELTA, 0, "4999")).toBe(true);
  });

  it("ignores offsets for partitions it isn't gating", () => {
    const gate = new DrainGate([{ topic: DELTA, partition: 0, offset: "90", drainTo: "99" }]);
    expect(gate.observe("md.book.binance.BTCUSDT.deltas", 0, "99")).toBe(false);
    expect(gate.observe(DELTA, 1, "99")).toBe(false); // different partition
    expect(gate.warming).toBe(true);
  });
});
