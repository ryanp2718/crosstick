# ADR-0001: Byte-identical replay is a first-class guarantee

Status: Accepted

## Context

The platform's core promise is that its derived streams are a pure function of
the input log. Replaying a fixed capture through the gateway must produce the
same `md.bbo.*` and `md.nbbo.*` output every time, byte for byte. Three things
rest on this: reproducible debugging (a captured incident replays into the exact
same output, so a fix can be proven against it offline), a defensible
correctness story (the README carries a determinism badge and the guarantee is a
headline of the design), and the offline demo (a recorded corpus replays
identically on any machine).

The guarantee is not free. The gateway consumes asynchronously, derives BBO and
NBBO in memory, and produces to Redpanda. A dependence on wall-clock time, a
nondeterministic ordering in the emit path, or a reliance on cross-topic input
order would each break it. It held in practice for a long time by luck rather
than by construction, and issue #86 exposed the gap: two adjacent BBO updates on
one partition reordered run to run, because concurrent fire-and-forget produce
requests raced in their pre-write async work.

## Decision

Byte-identical replay is a maintained invariant, not a happy accident. Every
derived output stream is a deterministic function of the input log, independent
of wall clock and of async scheduling. Changes to the gateway's consume, derive,
or emit paths must preserve it, and CI enforces it. Three mechanisms carry it:

- **Stream clock, not wall clock.** Every emitted timestamp, leg age, and
  liveness-eviction decision derives from the maximum event-time across consumed
  messages, never `Date.now()`, so a replay reproduces the same timestamps and
  the same eviction points. (Stream-time notes in `data-contracts.md` and
  `DESIGN_nbbo.md`.)
- **Order-insensitive reconstruction.** The book fold converges to the same
  state regardless of the order deltas and snapshots arrive, so cross-topic
  input order need not be imposed; an adversarial all-deltas-first replay reaches
  the same end state.
- **In-order production.** Output topics are single-partition, and the emit
  batcher keeps at most one produce request in flight, so requests reach a
  partition strictly in enqueue order. This is the hole issue #86 closed: the
  broker preserves per-partition order only for requests it receives in order,
  and concurrent sends raced.

## Consequences

- A captured incident replays into identical output, so a regression can be
  reproduced and a fix proven without a live venue connection.
- One produce request in flight caps pipelining. Under load the batcher
  coalesces more messages per request, so message throughput holds, but a
  high-latency remote broker would feel the serialization. The correctness of
  the ordering guarantee is judged to outrank peak produce concurrency for this
  workload.
- New code inherits a standing constraint: a wall-clock read in the emit path,
  or a newly concurrent send, is a determinism regression. The invariant is a
  review checklist item, not a hope.
- If every ingester goes silent the stream clock freezes and nothing is evicted,
  but nothing is emitted either; per-leg ages remain the consumer's staleness
  signal, and ingester liveness is alerted on independently.

## Alternatives considered

- **Idempotent producer, or max-in-flight-of-one at the client layer.** Also
  yields per-partition order, but it depends on how the client assigns sequence
  numbers across concurrently issued sends and cannot be verified without a live
  broker. Serializing inside the batcher is deterministic by construction and is
  covered by fast unit tests with a fake producer.
- **Sort each flush by `(exchange_ts_ns, sequence)` before sending.** Does not
  address the failure: the reorder was across separate produce requests, not
  within one batch, and nanosecond timestamps are already rounded at the JSON
  boundary, so they are not a safe sort key.
- **Assert set-equality only, dropping the byte-identity check.** Rejected.
  Set-equality would have hidden the exact reorder that reproducible debugging
  depends on catching.

## Enforcement

The byte-identical assertion runs on every pull request inside the integration
job (`test_replayed_nbbo_is_byte_identical_across_runs`, which replays a fixed
corpus twice and compares the derived streams) and on every push to `main` via
the determinism workflow that feeds the README badge. The test is never papered
over with an auto-retry: catching this class of nondeterminism is its entire
purpose.

## References

- Issue #86 and its fix, PR #87 (serialize gateway produce batches)
- `docs/data-contracts.md` (replay determinism, stream time, single-partition ordering)
- `docs/DESIGN_nbbo.md` (stream clock, connection-state eviction)
- `node/gateway/src/batcher.ts` (single-in-flight produce chain)
- `python/analytics/tests/test_gateway_integration.py` (byte-identical assertion)
- `.github/workflows/determinism.yml` (badge on `main`)
