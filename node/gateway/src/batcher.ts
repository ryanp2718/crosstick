import { produceFlushes } from "./metrics.js";

// The minimal slice of kafkajs Producer the batcher depends on, so the logic
// is unit-testable with plain fakes instead of a broker.
export interface BatchProducer {
  sendBatch(batch: { topicMessages: TopicMessages[] }): Promise<unknown>;
}

export interface KeyedMessage {
  key: string;
  value: string;
}

export interface TopicMessages {
  topic: string;
  messages: KeyedMessage[];
}

// Called once per topic per settled flush, with the flush's outcome; the
// server maps this back onto the per-message result counters.
export type FlushSettle = (
  topic: string,
  messages: KeyedMessage[],
  ok: boolean,
  err?: unknown,
) => void;

// Emit coalescing (G7): accumulates emitted messages per topic and flushes
// them through one producer.sendBatch per event-loop tick, replacing one
// producer.send (one produce request, one socket write) per emitted message.
// Per-topic FIFO preserves INV-1 ordering; message bytes and keys are
// untouched, so the byte-identical replay is unaffected. Flushes are
// fire-and-forget like the per-message sends they replace: a dropped batch is
// recovered by the next L1 move, and cross-request ordering under retry is
// unchanged from the previous concurrent per-message sends.
export class EmitBatcher {
  private readonly queues = new Map<string, KeyedMessage[]>();
  private queued = 0;
  private flushScheduled = false;
  private readonly inFlight = new Set<Promise<unknown>>();

  // maxQueued bounds accumulator memory and keeps a flush's request well under
  // the 8 MiB message/request ceiling (INV-4): ~1000 L1-sized messages is
  // roughly half a MiB.
  constructor(
    private readonly producer: BatchProducer,
    private readonly onSettle: FlushSettle,
    private readonly maxQueued = 1000,
  ) {}

  get pending(): number {
    return this.queued;
  }

  get inFlightCount(): number {
    return this.inFlight.size;
  }

  enqueue(topic: string, key: string, value: string): void {
    let q = this.queues.get(topic);
    if (!q) {
      q = [];
      this.queues.set(topic, q);
    }
    q.push({ key, value });
    this.queued += 1;
    if (this.queued >= this.maxQueued) {
      this.flush();
      return;
    }
    if (!this.flushScheduled) {
      this.flushScheduled = true;
      setImmediate(() => {
        this.flushScheduled = false;
        this.flush();
      });
    }
  }

  flush(): void {
    if (this.queued === 0) return;
    const topicMessages: TopicMessages[] = [];
    for (const [topic, messages] of this.queues) topicMessages.push({ topic, messages });
    this.queues.clear();
    this.queued = 0;
    produceFlushes.inc();
    const p: Promise<unknown> = this.producer
      .sendBatch({ topicMessages })
      .then(() => {
        for (const tm of topicMessages) this.onSettle(tm.topic, tm.messages, true);
      })
      .catch((err) => {
        for (const tm of topicMessages) this.onSettle(tm.topic, tm.messages, false, err);
      })
      .finally(() => {
        this.inFlight.delete(p);
      });
    this.inFlight.add(p);
  }

  // Flush anything queued and wait for every in-flight send to settle; the
  // caller bounds the wait (shutdown races this against a timeout).
  async drain(): Promise<void> {
    this.flush();
    await Promise.allSettled([...this.inFlight]);
  }
}
