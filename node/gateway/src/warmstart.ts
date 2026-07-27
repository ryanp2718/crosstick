// Warm-start seek planning.
//
// The gateway's books and NBBO state live nowhere but memory. On restart,
// committed group offsets resume at the live edge, so every book would sit
// empty until its venue's next snapshot. Instead we re-derive state from the
// log: one seek per (topic, partition), chosen by topic class.
//
//   *.snapshots - latest snapshot (HWM-1) when one exists inside the lookback,
//                 else the live edge (the ingester's periodic re-snapshot
//                 bounds that wait to one interval).
//   *.deltas    - first offset at/after (now - lookback). A snapshot inside
//                 the lookback is by definition newer than the cutoff, so this
//                 covers every delta past it; the surplus prefix is absorbed
//                 by the aggregator's pre-snapshot buffering + sequence guard.
//                 No snapshot inside the lookback degrades to waiting
//                 for the next periodic one, exactly like a cold start.
//   md.trades.* - live edge; trades are events, not state to rebuild.
//   md.status.* - first offset at/after (now - lookback), same as deltas:
//                 recent heartbeats seed liveness + the stream clock, cheap and
//                 independent of cleanup policy (an earliest seek replays a
//                 delete-policy status topic's full history).
//
// Replayed inputs would re-publish md.bbo/md.nbbo already in the log. That is
// NOT harmless when the per-topic backlogs drain unevenly: the stream clock is
// the max event-time across ALL topics, so a leg from a slow-draining book is
// aged (and cross-checked) against a clock a fast-draining topic already pushed
// ahead - a stale top-of-book then wins the NBBO and prints a phantom cross
// until its own replay catches up. md.status drains far faster than md.book, so
// liveness eviction doesn't suppress it. DrainGate (below) holds derived output
// until every backlogged book partition reaches its startup HWM.

export type TopicClass = "snapshots" | "deltas" | "trades" | "status" | "other";

export function classifyTopic(topic: string): TopicClass {
  if (topic.startsWith("md.status.")) return "status";
  if (topic.startsWith("md.trades.")) return "trades";
  if (topic.startsWith("md.book.")) {
    if (topic.endsWith(".snapshots")) return "snapshots";
    if (topic.endsWith(".deltas")) return "deltas";
  }
  return "other";
}

// The slice of kafkajs's Admin the planner needs - kept minimal so tests can
// stub it. fetchTopicOffsets returns the HWM as both `offset` and `high`;
// fetchTopicOffsetsByTimestamp returns the earliest offset whose timestamp is
// >= the argument, or "-1" when no such message exists.
export interface AdminLike {
  fetchTopicOffsets(
    topic: string,
  ): Promise<Array<{ partition: number; offset: string; high: string; low: string }>>;
  fetchTopicOffsetsByTimestamp(
    topic: string,
    timestamp: number,
  ): Promise<Array<{ partition: number; offset: string }>>;
}

export interface Seek {
  topic: string;
  partition: number;
  offset: string;
  // Set only for book partitions sought strictly below the live edge: the last
  // offset of the startup backlog (HWM-1) the server must replay before its
  // book is rebuilt. Absent when the seek lands at the live edge (empty topic,
  // or no snapshot/delta inside the lookback) - those carry no backlog to gate.
  drainTo?: string;
}

export async function planWarmStart(
  admin: AdminLike,
  topics: string[],
  nowMs: number,
  lookbackMs: number,
): Promise<Seek[]> {
  const cutoffMs = nowMs - lookbackMs;
  const seeks: Seek[] = [];
  for (const topic of topics) {
    const cls = classifyTopic(topic);
    if (cls === "other") continue;
    const offsets = await admin.fetchTopicOffsets(topic);
    const byTs =
      cls === "snapshots" || cls === "deltas" || cls === "status"
        ? new Map(
            (await admin.fetchTopicOffsetsByTimestamp(topic, cutoffMs)).map((o) => [
              o.partition,
              o.offset,
            ]),
          )
        : new Map<number, string>();

    for (const { partition, high } of offsets) {
      const hwm = BigInt(high);
      let offset: string;
      switch (cls) {
        case "trades":
          offset = high; // live edge
          break;
        case "snapshots":
        case "deltas":
        case "status": {
          const ts = byTs.get(partition);
          const tsOff = ts === undefined ? -1n : BigInt(ts);
          const withinLookback = tsOff >= 0n && tsOff < hwm;
          if (!withinLookback) {
            offset = high; // nothing recent enough - start at the live edge
          } else {
            offset = cls === "snapshots" ? (hwm - 1n).toString() : ts!;
          }
          break;
        }
      }
      const seek: Seek = { topic, partition, offset };
      // A book seek below the live edge has a backlog [offset, hwm-1] to replay;
      // record its end so the server can gate output until it's drained.
      if ((cls === "snapshots" || cls === "deltas") && BigInt(offset) < hwm) {
        seek.drainTo = (hwm - 1n).toString();
      }
      seeks.push(seek);
    }
  }
  return seeks;
}

// Holds the gateway's derived output (md.bbo/md.nbbo) through the warm-start
// replay and opens it once every backlogged book partition has drained - see
// the module header for why an ungated uneven drain emits phantom crosses. A
// clean start (and the byte-identical replay test) plans no drainTo, so the
// gate is never armed and emission is unchanged.
interface DrainState {
  drainTo: bigint;
  // True once we've seen an offset within the backlog (<= drainTo). Guards
  // against a pre-seek straggler from the committed edge - an offset already
  // past the backlog - spuriously opening the gate before the replay runs.
  engaged: boolean;
}

function drainKey(topic: string, partition: number): string {
  return `${topic} ${partition}`;
}

export class DrainGate {
  private readonly pending = new Map<string, DrainState>();

  constructor(seeks: Seek[]) {
    for (const s of seeks) {
      if (s.drainTo !== undefined) {
        this.pending.set(drainKey(s.topic, s.partition), {
          drainTo: BigInt(s.drainTo),
          engaged: false,
        });
      }
    }
  }

  // True while any backlogged book partition is still draining - output held.
  get warming(): boolean {
    return this.pending.size > 0;
  }

  // Record a consumed offset; returns true exactly once - on the message that
  // drains the final pending partition (the instant the gate opens).
  observe(topic: string, partition: number, offset: string): boolean {
    const st = this.pending.get(drainKey(topic, partition));
    if (st === undefined) return false;
    const off = BigInt(offset);
    if (off <= st.drainTo) st.engaged = true;
    if (!st.engaged || off < st.drainTo) return false;
    this.pending.delete(drainKey(topic, partition));
    return this.pending.size === 0;
  }
}
