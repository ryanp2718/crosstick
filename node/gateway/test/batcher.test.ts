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
    expect(settles.map((s) => s.keys).flat().sort()).toEqual(["BTC-USD", "BTC-USDT"]);
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
});
