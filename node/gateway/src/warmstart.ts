// Warm-start seek planning (D2b in ARCHITECTURE.md).
//
// The gateway's books and NBBO state live nowhere but memory. On restart,
// committed group offsets resume at the live edge, so every book would sit
// empty until its venue's next snapshot. Instead we re-derive state from the
// log: one seek per (topic, partition), chosen by topic class.
//
//   *.snapshots — latest snapshot (HWM-1) when one exists inside the lookback,
//                 else the live edge (the ingester's periodic re-snapshot
//                 bounds that wait to one interval).
//   *.deltas    — first offset at/after (now - lookback). A snapshot inside
//                 the lookback is by definition newer than the cutoff, so this
//                 covers every delta past it; the surplus prefix is absorbed
//                 by the aggregator's pre-snapshot buffering + sequence guard
//                 (D2a). No snapshot inside the lookback degrades to waiting
//                 for the next periodic one, exactly like a cold start.
//   md.trades.* — live edge; trades are events, not state to rebuild.
//   md.status.* — earliest; compacted, so this replays the latest liveness
//                 per venue into the stream clock and eviction state.
//
// Replayed inputs re-publish md.bbo/md.nbbo messages already in the log —
// harmless: same keys into compacted topics, and WS clients dedup nothing
// worse than a startup burst.

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

// The slice of kafkajs's Admin the planner needs — kept minimal so tests can
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
      cls === "snapshots" || cls === "deltas"
        ? new Map(
            (await admin.fetchTopicOffsetsByTimestamp(topic, cutoffMs)).map((o) => [
              o.partition,
              o.offset,
            ]),
          )
        : new Map<number, string>();

    for (const { partition, high, low } of offsets) {
      const hwm = BigInt(high);
      let offset: string;
      switch (cls) {
        case "status":
          offset = low;
          break;
        case "trades":
          offset = high; // live edge
          break;
        case "snapshots":
        case "deltas": {
          const ts = byTs.get(partition);
          const tsOff = ts === undefined ? -1n : BigInt(ts);
          const withinLookback = tsOff >= 0n && tsOff < hwm;
          if (!withinLookback) {
            offset = high; // nothing recent enough — start at the live edge
          } else {
            offset = cls === "snapshots" ? (hwm - 1n).toString() : ts!;
          }
          break;
        }
      }
      seeks.push({ topic, partition, offset });
    }
  }
  return seeks;
}
