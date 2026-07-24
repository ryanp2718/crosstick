import { describe, expect, it } from "vitest";

import { Aggregator, MAX_APPLIED_TAIL, MAX_PENDING_DELTAS } from "../src/aggregator.js";
import {
  bboCrossed,
  bookHealReplayDepth,
  bookHealReplayUnderrun,
  bookResnapshotHeal,
  bookSnapshotStale,
} from "../src/metrics.js";
import type { BookDeltaMsg, BookSnapshotMsg, WireLevel } from "../src/messages.js";

function snap(
  seq: number, bids: WireLevel[], asks: WireLevel[], epoch?: number,
): BookSnapshotMsg {
  return {
    t: "snap", exchange: "kraken", symbol: "BTC/USD", sequence: seq,
    bids, asks, exchange_ts_ns: 1, local_ts_ns: 2, epoch,
  };
}

function delta(
  seq: number, bids: WireLevel[], asks: WireLevel[], epoch?: number,
): BookDeltaMsg {
  return {
    t: "delta", exchange: "kraken", symbol: "BTC/USD", sequence: seq,
    bids, asks, exchange_ts_ns: 1, local_ts_ns: 2, epoch,
  };
}

// Read the gateway_bbo_crossed_total value for one exchange (0 if unseen).
async function crossedFor(exchange: string): Promise<number> {
  const m = await bboCrossed.get();
  return m.values.find((v) => v.labels.exchange === exchange)?.value ?? 0;
}

// Read the gateway_book_snapshot_stale_total value for one exchange (0 if unseen).
async function staleFor(exchange: string): Promise<number> {
  const m = await bookSnapshotStale.get();
  return m.values.find((v) => v.labels.exchange === exchange)?.value ?? 0;
}

// Read the gateway_book_resnapshot_heal_total value for one exchange (0 if unseen).
async function healFor(exchange: string): Promise<number> {
  const m = await bookResnapshotHeal.get();
  return m.values.find((v) => v.labels.exchange === exchange)?.value ?? 0;
}

// Read the gateway_book_heal_replay_depth histogram's _count and _sum for one exchange.
async function replayDepthFor(exchange: string): Promise<{ count: number; sum: number }> {
  const m = await bookHealReplayDepth.get();
  const pick = (suffix: string) =>
    m.values.find(
      (v) => v.metricName === `gateway_book_heal_replay_depth_${suffix}` &&
        v.labels.exchange === exchange,
    )?.value ?? 0;
  return { count: pick("count"), sum: pick("sum") };
}

// Read the gateway_book_heal_replay_underrun_total value for one exchange (0 if unseen).
async function underrunFor(exchange: string): Promise<number> {
  const m = await bookHealReplayUnderrun.get();
  return m.values.find((v) => v.labels.exchange === exchange)?.value ?? 0;
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

  describe("epoch-keyed reconstruction (warm-start cross fix)", () => {
    it("drops a prior-epoch high-seq delta buffered before the snapshot", () => {
      const a = new Aggregator();
      // Prior connection (epoch 1) left a high-seq delta whose bid sits ABOVE
      // the fresh snapshot's ask - applying it would cross the book.
      a.applyBook(delta(5000, [["200", "1"]], [], 1));
      // Current connection (epoch 2) snapshot resets the counter to 0.
      const bbo = a.applyBook(snap(0, [["100", "1"]], [["101", "1"]], 2));
      // The stale-epoch delta is dropped, not drained → clean book, no cross.
      expect(bbo).toMatchObject({ bid_px: "100", ask_px: "101" });
    });

    it("buffers (never applies) a prior-epoch delta arriving after the snapshot", () => {
      const a = new Aggregator();
      a.applyBook(snap(0, [["100", "1"]], [["101", "1"]], 2));
      // A straggler from epoch 1 with a crossing bid and a higher seq.
      expect(a.applyBook(delta(5000, [["200", "1"]], [], 1))).toBeNull();
      // Live book is untouched - still the epoch-2 snapshot's top-of-book.
      expect(a.snapshot()[0]).toMatchObject({ bid_px: "100", ask_px: "101" });
    });

    it("buffers a newer-epoch delta until its snapshot drains it", () => {
      const a = new Aggregator();
      a.applyBook(snap(0, [["100", "1"]], [["101", "1"]], 2));
      // A delta from the next connection (epoch 3) races ahead of its snapshot.
      expect(a.applyBook(delta(1, [["100.5", "3"]], [], 3))).toBeNull();
      // Its snapshot lands and drains the buffered epoch-3 delta.
      const bbo = a.applyBook(snap(0, [["100", "1"]], [["101", "1"]], 3));
      expect(bbo).toMatchObject({ bid_px: "100.5", ask_px: "101" });
    });

    it("applies same-epoch deltas under the seq guard", () => {
      const a = new Aggregator();
      a.applyBook(snap(5, [["100", "1"]], [["101", "1"]], 9));
      // Same epoch, newer seq → applies.
      expect(a.applyBook(delta(6, [["100.5", "3"]], [], 9))?.bid_px).toBe("100.5");
      // Same epoch, stale seq → dropped.
      expect(a.applyBook(delta(6, [["100.7", "3"]], [], 9))).toBeNull();
    });

    it("skips a stale same-epoch re-snapshot instead of rewinding the book", async () => {
      const a = new Aggregator();
      a.applyBook(snap(5, [["100", "1"]], [["101", "1"]], 9));
      // A live delta deletes the old top and advances the book to seq 7.
      a.applyBook(delta(7, [["100", "0"], ["100.6", "2"]], [], 9));
      // A periodic re-snapshot stamped at the OLD seq 6 (captured before delta 7
      // applied) arrives late; resetting to it would resurrect the deleted 100
      // bid and rewind the 100.6 top.
      const before = await staleFor("kraken");
      expect(a.applyBook(snap(6, [["100", "1"]], [["101", "1"]], 9))).toBeNull();
      expect(a.snapshot()[0]).toMatchObject({ bid_px: "100.6", ask_px: "101" });
      expect(await staleFor("kraken")).toBe(before + 1);
    });

    it("accepts a stale re-snapshot on a crossed book without dropping newer deltas", async () => {
      const a = new Aggregator();
      a.applyBook(snap(5, [["100", "1"]], [["101", "1"]], 9));
      // A delta strands a bid above the ask → the book is crossed (corrupt).
      const crossed = a.applyBook(delta(7, [["200", "1"]], [], 9));
      expect(crossed).toMatchObject({ bid_px: "200", ask_px: "101" });
      // The periodic re-snapshot at the OLD seq 6 would normally be skipped as a
      // rewind, but because the book is crossed it is applied as a resync.
      const staleBefore = await staleFor("kraken");
      const healBefore = await healFor("kraken");
      const healed = a.applyBook(snap(6, [["100", "1"]], [["101", "1"]], 9));
      // Delta 7 outranks the seq-6 snapshot and replays on top (a snapshot only
      // attests state as of its own seq), so this cross persists faithfully:
      // top-of-book is unchanged and the dedup emits nothing new.
      expect(healed).toBeNull();
      expect(a.snapshot()[0]).toMatchObject({ bid_px: "200", ask_px: "101" });
      expect(await healFor("kraken")).toBe(healBefore + 1);
      expect(await staleFor("kraken")).toBe(staleBefore); // not counted as a skip
      // The next re-snapshot outranks delta 7 and fully heals the book.
      const after = a.applyBook(snap(8, [["100", "1"]], [["101", "1"]], 9));
      expect(after).toMatchObject({ bid_px: "100", ask_px: "101" });
    });
  });

  describe("heal replay (applied-delta tail)", () => {
    // The 2026-07-14 live episode shape: the venue stalls, the delete of its
    // stale top bid exists only in an in-band recovery snapshot, and that
    // snapshot is consumed BEHIND a delta it predates (separate topics, no
    // cross-topic order). The heal must not resurrect what the delta deleted.
    it("replays the tail over a heal instead of resurrecting a deleted level", async () => {
      const a = new Aggregator();
      a.applyBook(snap(5, [["100", "1"]], [["101", "1"]], 9));
      // Falling ask crosses the stale bid 100 (its delete never arrives as a delta).
      a.applyBook(delta(7, [], [["99", "1"]], 9));
      // The venue deletes bid 98; consumed AHEAD of the snapshot that lists it.
      a.applyBook({ ...delta(9, [["98", "0"]], [], 9), local_ts_ns: 99 });
      const healBefore = await healFor("kraken");
      const depthBefore = await replayDepthFor("kraken");
      // In-band recovery snapshot: bid 100 gone (implicit delete), bid 98 present.
      const healed = a.applyBook(snap(8, [["98", "1"], ["97", "1"]], [["99", "1"]], 9));
      // Without replay this reads 98/99, resurrecting the deleted 98 for a full
      // re-snapshot interval (the delta that removes it is behind the consumer).
      expect(healed).toMatchObject({ bid_px: "97", ask_px: "99" });
      expect(healed!.local_ts_ns).toBe(99); // stamped from the replayed delta
      expect(await healFor("kraken")).toBe(healBefore + 1);
      const depth = await replayDepthFor("kraken");
      expect(depth.count).toBe(depthBefore.count + 1);
      expect(depth.sum).toBe(depthBefore.sum + 1); // exactly the one rewound delta
    });

    it("forward snapshots ignore the tail (replay is a seq-guard no-op)", async () => {
      const a = new Aggregator();
      a.applyBook(snap(5, [["100", "1"]], [["101", "1"]], 9));
      a.applyBook(delta(6, [["100.5", "1"]], [], 9));
      const depthBefore = await replayDepthFor("kraken");
      const bbo = a.applyBook(snap(10, [["102", "1"]], [["103", "1"]], 9));
      // The superseded 100.5 bid must not replay over the newer snapshot.
      expect(bbo).toMatchObject({ bid_px: "102", ask_px: "103" });
      expect((await replayDepthFor("kraken")).count).toBe(depthBefore.count);
    });

    it("a new-epoch snapshot never replays the prior connection's tail", () => {
      const a = new Aggregator();
      a.applyBook(snap(5, [["100", "1"]], [["101", "1"]], 1));
      // Prior connection's applied high-seq delta: its counter would out-rank
      // the fresh epoch's reset seq if the epoch filter were missing.
      a.applyBook(delta(5000, [["200", "1"]], [], 1));
      const bbo = a.applyBook(snap(0, [["100", "1"]], [["101", "1"]], 2));
      expect(bbo).toMatchObject({ bid_px: "100", ask_px: "101" });
    });

    it("counts an underrun when eviction lost part of the replay tail", async () => {
      const a = new Aggregator();
      a.applyBook(snap(0, [["100", "1"]], [["101", "1"]], 9));
      // The crossing delta lands first, then a flood evicts it from the tail.
      a.applyBook(delta(1, [["200", "1"]], [], 9));
      for (let i = 2; i <= MAX_APPLIED_TAIL + 2; i++) {
        a.applyBook(delta(i, [], [[`${1000 + i}`, "1"]], 9));
      }
      const before = await underrunFor("kraken");
      const healed = a.applyBook(snap(0, [["100", "1"]], [["101", "1"]], 9));
      // The evicted crossing delta cannot replay; the shortfall is counted.
      expect(healed).toMatchObject({ bid_px: "100", ask_px: "101" });
      expect(await underrunFor("kraken")).toBe(before + 1);
    });
  });

  it("counts a crossed book but still emits the BBO", async () => {
    const a = new Aggregator();
    const before = await crossedFor("kraken");
    // A snapshot whose bid sits above its ask (planted upstream corruption).
    const bbo = a.applyBook(snap(0, [["200", "1"]], [["101", "1"]], 1));
    expect(bbo).toMatchObject({ bid_px: "200", ask_px: "101" }); // still emitted
    expect(await crossedFor("kraken")).toBe(before + 1); // and counted
  });
});
