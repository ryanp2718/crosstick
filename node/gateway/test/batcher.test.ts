import { describe, expect, it, vi } from "vitest";

import { EmitBatcher, type FlushSettle, type TopicMessages } from "../src/batcher.js";

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

function harness(opts: { maxQueued?: number; fail?: boolean } = {}) {
  const calls: TopicMessages[][] = [];
  const settles: Array<{ topic: string; count: number; keys: string[]; ok: boolean }> = [];
  const producer = {
    sendBatch: vi.fn(({ topicMessages }: { topicMessages: TopicMessages[] }) => {
      calls.push(topicMessages);
      return opts.fail ? Promise.reject(new Error("broker down")) : Promise.resolve();
    }),
  };
  const onSettle: FlushSettle = (topic, messages, ok) =>
    settles.push({ topic, count: messages.length, keys: messages.map((m) => m.key), ok });
  const b = new EmitBatcher(producer, onSettle, opts.maxQueued ?? 1000);
  return { b, producer, calls, settles };
}

// Like harness, but each sendBatch stays pending until settleNext() releases it,
// so a test can hold one flush in flight while it enqueues more. Tracks peak
// concurrency to assert the batcher never issues overlapping sends.
function deferredHarness(opts: { maxQueued?: number } = {}) {
  const batches: string[][] = []; // flushed values, flattened, one entry per sendBatch
  const release: Array<() => void> = [];
  let inFlight = 0;
  let peak = 0;
  const producer = {
    sendBatch: vi.fn(({ topicMessages }: { topicMessages: TopicMessages[] }) => {
      batches.push(topicMessages.flatMap((tm) => tm.messages.map((m) => m.value)));
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      return new Promise<void>((resolve) => {
        release.push(() => {
          inFlight -= 1;
          resolve();
        });
      });
    }),
  };
  const b = new EmitBatcher(producer, () => {}, opts.maxQueued ?? 1000);
  // Release the oldest in-flight send and let its settle re-flush any backlog.
  const settleNext = async () => {
    release.shift()?.();
    await tick();
  };
  return { b, batches, settleNext, peak: () => peak };
}

describe("EmitBatcher", () => {
  it("coalesces a tick's enqueues into one sendBatch, grouped per topic in FIFO order", async () => {
    const { b, producer, calls } = harness();
    b.enqueue("md.bbo.kraken.BTC-USD", "kraken:BTC/USD", "b1");
    b.enqueue("md.nbbo.BTC-USD", "BTC-USD", "n1");
    b.enqueue("md.bbo.kraken.BTC-USD", "kraken:BTC/USD", "b2");
    expect(producer.sendBatch).not.toHaveBeenCalled();
    await tick();
    expect(producer.sendBatch).toHaveBeenCalledTimes(1);
    const byTopic = new Map(calls[0].map((tm) => [tm.topic, tm.messages]));
    expect(byTopic.get("md.bbo.kraken.BTC-USD")?.map((m) => m.value)).toEqual(["b1", "b2"]);
    expect(byTopic.get("md.nbbo.BTC-USD")?.map((m) => m.value)).toEqual(["n1"]);
    expect(b.pending).toBe(0);
  });

  it("flushes immediately at the size cap instead of waiting for the tick", () => {
    const { b, producer } = harness({ maxQueued: 3 });
    b.enqueue("t", "k", "1");
    b.enqueue("t", "k", "2");
    expect(producer.sendBatch).not.toHaveBeenCalled();
    b.enqueue("t", "k", "3");
    expect(producer.sendBatch).toHaveBeenCalledTimes(1);
  });

  it("settles ok per topic with the flushed messages", async () => {
    const { b, settles } = harness();
    b.enqueue("md.nbbo.BTC-USD", "BTC-USD", "n1");
    b.enqueue("md.nbbo.BTC-USDT", "BTC-USDT", "n2");
    await tick();
    await tick();
    expect(settles).toHaveLength(2);
    expect(settles.every((s) => s.ok)).toBe(true);
    expect(
      settles
        .map((s) => s.keys)
        .flat()
        .sort(),
    ).toEqual(["BTC-USD", "BTC-USDT"]);
  });

  it("settles every topic of a failed flush with ok=false", async () => {
    const { b, settles } = harness({ fail: true });
    b.enqueue("a", "k1", "1");
    b.enqueue("b", "k2", "2");
    await tick();
    await tick();
    expect(settles).toHaveLength(2);
    expect(settles.every((s) => !s.ok)).toBe(true);
  });

  it("keeps later enqueues out of an already-dispatched flush", async () => {
    const { b, calls } = harness({ maxQueued: 1 });
    b.enqueue("t", "k", "1");
    b.enqueue("t", "k", "2");
    await tick();
    expect(calls).toHaveLength(2);
    expect(calls[0][0].messages.map((m) => m.value)).toEqual(["1"]);
    expect(calls[1][0].messages.map((m) => m.value)).toEqual(["2"]);
  });

  it("drain flushes the queue and resolves after in-flight sends settle", async () => {
    const { b, producer, settles } = harness();
    b.enqueue("t", "k", "1");
    await b.drain();
    expect(producer.sendBatch).toHaveBeenCalledTimes(1);
    expect(settles).toHaveLength(1);
    expect(b.pending).toBe(0);
    expect(b.inFlightCount).toBe(0);
  });

  it("drain is a no-op on an empty batcher", async () => {
    const { b, producer } = harness();
    await b.drain();
    expect(producer.sendBatch).not.toHaveBeenCalled();
  });

  // Regression for #86: with only one send in flight at a time, two adjacent
  // updates that land in separate flushes still reach the partition in enqueue
  // order, because the second batch is not issued until the first has settled.
  it("keeps one send in flight and preserves order across flushes", async () => {
    const { b, batches, settleNext, peak } = deferredHarness();
    b.enqueue("md.bbo.binance.BTCUSDT", "binance:BTCUSDT", "13");
    await tick();
    expect(batches).toEqual([["13"]]);
    expect(b.inFlightCount).toBe(1);

    // Enqueued while the first send is still in flight: must not start a second.
    b.enqueue("md.bbo.binance.BTCUSDT", "binance:BTCUSDT", "14");
    await tick();
    expect(batches).toEqual([["13"]]);
    expect(b.pending).toBe(1);

    await settleNext();
    expect(batches).toEqual([["13"], ["14"]]);
    expect(peak()).toBe(1);
  });

  it("splits a backlog built during an in-flight send into maxQueued-capped batches", async () => {
    const { b, batches, settleNext, peak } = deferredHarness({ maxQueued: 2 });
    b.enqueue("t", "k", "1");
    b.enqueue("t", "k", "2"); // hits the cap, dispatches [1,2]
    expect(batches).toEqual([["1", "2"]]);

    // Pile up behind the in-flight send; the batcher may not issue more yet.
    b.enqueue("t", "k", "3");
    b.enqueue("t", "k", "4");
    b.enqueue("t", "k", "5");
    await tick();
    expect(batches).toEqual([["1", "2"]]);

    await settleNext(); // releases [1,2] -> flushes [3,4] (capped at 2)
    expect(batches).toEqual([
      ["1", "2"],
      ["3", "4"],
    ]);
    await settleNext(); // releases [3,4] -> flushes [5]
    expect(batches).toEqual([["1", "2"], ["3", "4"], ["5"]]);
    expect(peak()).toBe(1);
    expect(batches.every((batch) => batch.length <= 2)).toBe(true);
  });

  it("drain resolves after a backlog queued behind an in-flight send fully flushes", async () => {
    const { b, batches, settleNext } = deferredHarness();
    b.enqueue("t", "k", "1");
    await tick();
    b.enqueue("t", "k", "2"); // queued behind the in-flight [1]
    const drained = b.drain();
    await settleNext(); // let [1] settle so drain can issue and await [2]
    await settleNext(); // let [2] settle
    await drained;
    expect(batches).toEqual([["1"], ["2"]]);
    expect(b.pending).toBe(0);
    expect(b.inFlightCount).toBe(0);
  });
});
