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
// them through producer.sendBatch, replacing one producer.send (one produce
// request, one socket write) per emitted message.
//
// At most one flush is in flight at a time: a flush that settles re-flushes
// whatever accumulated while it was in flight. That single-in-flight chain is
// what makes md.bbo/md.nbbo per-partition order deterministic (INV-1). The
// broker only guarantees per-partition append order for produce requests it
// receives in order; concurrent fire-and-forget sends race in their pre-write
// async work, so two adjacent updates split across two ticks could reach the
// partition out of order (issue #86). Serializing the requests removes that
// race, and its natural backpressure coalesces harder under load. Message
// bytes and keys are untouched, so the byte-identical replay holds.
//
// Sends stay fire-and-forget for the consumer: a dropped batch is recovered by
// the next L1 move, and no consume-path await gates on the broker RTT.
export class EmitBatcher {
  private readonly queues = new Map<string, KeyedMessage[]>();
  private queued = 0;
  private flushScheduled = false;
  private sending = false;
  private current: Promise<void> | null = null;

  // maxQueued caps a single flush's message count so its request stays well
  // under the 8 MiB message/request ceiling (INV-4): ~1000 L1-sized messages is
  // roughly half a MiB. A backlog that accumulates past it during an in-flight
  // send is split across successive flushes rather than sent as one request.
  constructor(
    private readonly producer: BatchProducer,
    private readonly onSettle: FlushSettle,
    private readonly maxQueued = 1000,
  ) {}

  get pending(): number {
    return this.queued;
  }

  get inFlightCount(): number {
    return this.sending ? 1 : 0;
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

  // Pull up to maxQueued messages into one batch, per topic in FIFO; any
  // remainder stays queued for the next flush.
  private takeBatch(): TopicMessages[] {
    const out: TopicMessages[] = [];
    let budget = this.maxQueued;
    for (const [topic, messages] of this.queues) {
      if (budget <= 0) break;
      if (messages.length <= budget) {
        out.push({ topic, messages });
        budget -= messages.length;
        this.queued -= messages.length;
        this.queues.delete(topic);
      } else {
        out.push({ topic, messages: messages.splice(0, budget) });
        this.queued -= budget;
        budget = 0;
      }
    }
    return out;
  }

  flush(): void {
    if (this.sending || this.queued === 0) return;
    const topicMessages = this.takeBatch();
    this.sending = true;
    produceFlushes.inc();
    this.current = this.producer
      .sendBatch({ topicMessages })
      .then(() => {
        for (const tm of topicMessages) this.onSettle(tm.topic, tm.messages, true);
      })
      .catch((err) => {
        for (const tm of topicMessages) this.onSettle(tm.topic, tm.messages, false, err);
      })
      .finally(() => {
        this.sending = false;
        // Drain whatever queued while this batch was in flight, in order.
        this.flush();
      });
  }

  // Flush anything queued and wait for every in-flight send to settle; the
  // caller bounds the wait (shutdown races this against a timeout).
  async drain(): Promise<void> {
    this.flush();
    while (this.current) {
      const c = this.current;
      await c;
      // If settling this send started the next one, keep waiting; otherwise
      // the chain is idle.
      if (c === this.current) this.current = null;
    }
  }
}
