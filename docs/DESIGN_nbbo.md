# NBBO design (v1)

Cross-exchange National Best Bid & Offer aggregation in the gateway. This doc
captures the decisions made in design discussion; the implementation slots into
the gateway between the existing `Aggregator` and the producer/broadcaster
fanout.

## Goal

For each canonical instrument (e.g. `BTC-USD`), maintain a live best bid and
best ask drawn from all venues that quote it, and publish dedup'd state-change
messages to `md.nbbo.<canonical_id>`. The single-exchange BBO topics
(`md.bbo.<exchange>.<symbol>`) remain in parallel — NBBO is a derived view, not
a replacement.

## Scope decisions

### Strict per-quote-asset bucketing

`BTC-USDT` (Binance) and `BTC-USD` (Coinbase) are **separate** canonical
instruments. They do not get mixed into a single "USD-class" NBBO.

**Why.** USDT and USD are different assets. The USDT/USD premium is itself a
tradeable signal; collapsing them would discard it. During a stablecoin depeg
event, a loose NBBO would surface false "arbitrage" that is actually the peg
breaking. Strict bucketing never lies, even if v1 NBBO is narrower than it
could be.

**When to revisit.** If a consumer materializes a concrete need for a
cross-stablecoin view, add `md.nbbo.usd-class.<base>` as a separate stream
alongside the strict per-quote topics. Don't widen the existing topic.

### Canonical map in repo YAML, resolved gateway-side

`ops/instruments.yml` declares the canonical instruments and which
`(exchange, symbol)` pairs feed each one. The gateway loads it at startup and
resolves on receive.

**Why YAML in repo.** Versioned with code, reviewed in PRs, simpler than a
compacted metadata topic for static data that changes ~weekly. Overkill
alternatives ruled out: no admin service, no DB table, no derived parsing
rules.

**Why gateway-side (not at the ingester).** The ingester is intentionally dumb
(raw bytes → Redpanda). Pushing canonical resolution down to it would force
every ingester to load and reload the same map. Single source of truth at the
gateway is simpler; we can move resolution earlier if a non-gateway consumer
ever needs the same view.

**Schema:**

```yaml
# ops/instruments.yml
instruments:
  BTC-USD:
    base: BTC
    quote: USD
    venues:
      - { exchange: coinbase, symbol: BTC-USD }
      - { exchange: kraken,   symbol: BTC/USD }
  BTC-USDT:
    base: BTC
    quote: USDT
    venues:
      - { exchange: binance, symbol: BTCUSDT }
```

`canonical_id` format = `<BASE>-<QUOTE>`. Matches Coinbase's convention,
human-readable, no escaping needed in topic names.

### Dedup on L1 tuple only — `(bid_px, bid_sz, ask_px, ask_sz)`

Same dedup grain as single-exchange BBO. If the source venue switches (Coinbase
pulls, Kraken matches the same price+size), **no NBBO emit**.

**Why.** NBBO is an analytics/surface stream. Consumers who need
routing-grade precision (where to send the order) subscribe to the per-exchange
BBO topics, which still carry venue-switch resolution. Routing-aware
NBBO-only consumption is the wrong abstraction for that job.

**Cost.** A NBBO consumer reading `best_bid.exchange` sees the new venue only
on the next price/size change, not at the switch moment. Acceptable for
surface/dashboard semantics.

### Per-leg staleness is the consumer's call

The gateway never drops a leg for being "too stale." If a venue's WS dropped,
its last-known BBO continues to be considered for NBBO computation. The
emitted message includes `leg_age_ms` per side so the consumer can filter at
whatever threshold fits their use case.

**Why.** Gateway-side staleness gating means an arbitrary threshold baked in,
silent leg disappearance from downstream's perspective, and surprise when a
consumer expects different behavior. Surfacing the age is honest; consumers
with different tolerances (1s for HFT, 30s for dashboards) coexist without
gateway changes.

### Tie-break: larger size wins; alphabetical fallback

When two venues quote the same best price on the same side, the venue with the
larger size at that price takes the `best_bid`/`best_ask` slot. If sizes are
also tied, exchanges sort alphabetically.

**Why.** Single venue per side keeps the schema simple (one `exchange` string,
not an array). The "real liquidity at top of book" question is a separate
analytic — could become `md.nbbo.liquidity.<id>` later if asked for. Not in
v1.

### Compacted topics

`md.nbbo.<canonical_id>` topics are configured `cleanup.policy=compact`, keyed
by `canonical_id`. Redpanda retains the latest message per key forever.

**Why.** NBBO is a stateful surface. Late-joining consumers (analytics started
mid-session, dashboards reconnecting) get the current state on subscribe
without waiting for the next change. Standard Kafka pattern for state streams;
the small ops cost (one line in `redpanda-init`) buys real consumer
convenience.

### Always emit, even with partial state

If only one venue's BBO has arrived for `BTC-USD` (Coinbase up, Kraken WS not
yet connected), the NBBO is emitted with `constituents: ["coinbase"]` and a
single-leg `best_bid`/`best_ask`. Self-healing: as more venues come online,
they join the NBBO automatically.

**Why.** Down-venue handling becomes "the consumer reads `constituents` and
decides." No special degraded-mode flag in the schema.

### Snapshot replay on WS connect

Same pattern as the existing `Aggregator`: on browser WS connect, replay one
NBBOMsg per canonical_id from `nbboAgg.snapshot()`. Skip canonical_ids that
have zero constituents (avoid emitting empty NBBOs).

## Message schema

```ts
interface NBBOMsg {
  type: 'nbbo';
  canonical_id: string;          // 'BTC-USD'
  base: string;                  // 'BTC'
  quote: string;                 // 'USD'
  best_bid: {
    px: number;
    sz: number;
    exchange: string;            // venue currently holding the slot
    leg_ts_ns: bigint;           // local_ts_ns of the source BBOMsg
    leg_age_ms: number;          // (local_ts_ns_now - leg_ts_ns) / 1e6
  };
  best_ask: { /* same shape */ };
  spread: number;                // best_ask.px - best_bid.px (signed; negative = crossed)
  mid: number;                   // (best_bid.px + best_ask.px) / 2
  constituents: string[];        // exchanges currently contributing a quote
  local_ts_ns: bigint;           // gateway emit time
}
```

## Code architecture

```
node/gateway/src/
  canonical.ts     // load ops/instruments.yml → CanonicalMap with:
                   //   - exchangeSymbolToCanonical: Map<(exchange,symbol), CanonicalInstrument>
                   //   - canonicalById: Map<canonical_id, CanonicalInstrument>
  nbbo.ts          // NBBOAggregator:
                   //   - per-canonical_id state: legs Map<exchange, BBOMsg>
                   //   - onBBO(msg): update leg, recompute, dedup, return NBBOMsg | null
                   //   - snapshot(): NBBOMsg[] for WS replay
  aggregator.ts    // existing; add onBBO callback hook for in-process wiring
  server.ts        // wire: aggregator emits BBO → nbboAgg.onBBO → if emit:
                   //                                 producer.send + broadcaster.broadcast
  metrics.ts       // add: nbboProduced{canonical_id,result},
                   //      nbboConstituents{canonical_id},
                   //      nbboCrossed_total{canonical_id}
```

In-process wiring (aggregator → nbbo via callback, not Kafka round-trip) keeps
NBBO latency equal to BBO latency — no extra hop.

## Implementation phases

### Phase 1 — single-constituent NBBO end-to-end

- `ops/instruments.yml` with `BTC-USD` mapping Coinbase only initially
- `CanonicalMap` loader + lookup
- `NBBOAggregator` with leg storage, recompute, L1-tuple dedup
- Topic creation in `redpanda-init` (compacted)
- Producer wiring + broadcaster fanout
- Metrics
- Dashboard: stacked NBBO table above existing BBO table; spread cell colored
  green/red on sign

Validation: NBBO `BTC-USD` should match Coinbase BBO `BTC-USD` exactly (same
px, sz, ts; `constituents: ["coinbase"]`). Compare on the dashboard side by
side.

### Phase 2 — bring up Kraken, validate cross-venue

- Add Kraken venue entry to `ops/instruments.yml` for `BTC-USD`
- Start the Kraken ingester (already in docker-compose)
- Verify two-leg NBBO behavior:
  - both venues contributing → `constituents.length == 2`
  - tie-break working when prices match
  - dedup behavior when venues switch at same px/sz
  - `leg_age_ms` populated correctly per side
  - crossed-market events occasionally fire (verify red dashboard cell)

### Phase 3 — operational tests

- Drop Kraken ingester mid-session → NBBO `leg_age_ms` for Kraken grows
- Restart gateway → snapshot from compacted topic populates pre-startup state
- Browser reconnect → NBBO snapshot replay works

## Out of scope (v1)

- Cross-stablecoin "USD-class" NBBO topic — add later as parallel stream if
  asked for
- Liquidity-aggregated NBBO (`sum(sz)` across venues at top price) — separate
  analytic stream if asked for
- Gateway-side staleness gating — surfacing `leg_age_ms` covers this
- Routing-grade NBBO (dedup on venue switches) — per-exchange BBO topics cover
  this consumer
- Hot-reload of `instruments.yml` — gateway restart handles map changes for now
- Cross-exchange trade tape / VWAP — separate concept, separate stream
- NBBO for instruments with only one venue declared in the map — works
  correctly (NBBO == single-exchange BBO), but consider whether the topic is
  worth emitting; v1 emits it anyway for uniformity

## Open questions

- **Cross-exchange clock skew.** Gateway uses its own `local_ts_ns` for each
  leg's freshness measure (when *we* received the BBO). For consumers doing
  precise spread analysis where venue-side timestamps matter, we might want to
  surface both `leg_ts_exchange` (from the original venue payload) and
  `leg_ts_gateway`. Defer until a consumer asks.

- **Compaction tombstones.** If a `canonical_id` is removed from
  `instruments.yml`, the existing compacted topic retains its last value
  forever. We may want a cleanup path (emit tombstone, or topic deletion).
  Defer; not a v1 concern.
