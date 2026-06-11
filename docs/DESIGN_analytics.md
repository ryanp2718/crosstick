# Analytics design (v1) — lake, replay, feature store

Design for the **analytics half** of crosstick: the batch/research side that the
streaming spine (ingest → Redpanda → gateway → NBBO) feeds. Companion to
`ARCHITECTURE.md` (→ "The analytics seam", D1–D7), `DESIGN_nbbo.md` (NBBO
semantics, strict per-quote bucketing, `instruments.yml`), and `scale-out.md`.

Status: **Phases 0–1 built; Phase 3's determinism core landed (2026-06).**
Phase 0 (golden corpus + capture/replay + testcontainers harness) and Phase 1
(`python/materializer` → bronze Parquet on MinIO, exactly-once via start-offset
object keys) are implemented and integration-tested; the lake/topic contract
lives in `data-contracts.md`. The D1/D2 fixes shipped in the gateway-replay
refactor, and Phase 3's headline acceptance test — replay twice → byte-identical
`md.bbo.*`/`md.nbbo.*` — passes (`analytics/tests/test_gateway_integration.py`).
The seekable research replay engine (replay-to-time-T over the reused
`OrderBook`) remains. Phases 2, 4, 5 remain design. This doc records the
decisions + open concerns so the "why" survives.

## Goal

Turn the durable Redpanda log into a research surface that can answer
microstructure questions, replay history deterministically, and serve features
to models consistently offline and online — to an industry-grade bar, with
ground truth and reproducibility as first-class design goals (not afterthoughts).

The chosen spine is **research + replay + feature-store** (the "eventually act on
signals" path), selected over the "trustworthy data utility" path
(data-quality + TCA + BI). The data-quality floor is still built first — it is
the prerequisite that tells us whether anything downstream is trustworthy — but
the replay engine is the keystone the research and feature work sit on.

## Scope

- **End-state: broad multi-asset.** 50+ instruments (incl. long-tail) across
  many venues. The current `BTC/ETH × {coinbase, kraken, binance}` is the
  **build-first** subset, not the target.
- **Design for the end-state, build incrementally.** Schema, symbology,
  partitioning, DECIMAL scale, and SQL portability all anticipate 50+ from the
  first row; we ingest BTC-first and widen.
- **Quote currencies kept strictly distinct** (USD ≠ USDT ≠ USDC). Already canon
  for NBBO (`DESIGN_nbbo.md` → "Strict per-quote-asset bucketing"); the
  cross-quote **basis** is a derived research signal, computed in the warehouse,
  never a merged book.

---

## Decision register

Recommendations locked unless flagged in **Open concerns**. Each is multi-asset-
and reproducibility-aware.

### Medallion layering: `lake` (bronze) → `silver` → `gold`

Reuse the buckets compose already provisions. `lake` = bronze (the repo names the
bronze bucket `lake`).

- **`lake` / bronze** — verbatim, immutable, append-only projection of each
  topic to Parquet. The ground-truth / insurance layer: capture full fidelity
  now, query later. You cannot backfill what you didn't record.
- **`silver`** — cleaned + *reconstructed*: validated (gaps/checksums),
  decimal-cast, canonical-resolved, point-in-time book state and as-of-joined
  enriched events.
- **`gold`** — purpose-built marts (features, basis, TCA-style market-quality
  metrics, data-quality scorecard).

### Storage format: raw Parquet for `lake`, Iceberg for `silver`/`gold`

- **`lake` stays raw Parquet — catalog-free on purpose.** The insurance layer
  must have zero dependency on a metadata service: if an Iceberg catalog is lost
  or corrupted, everything downstream is still rebuildable from plain Parquet
  with any tool. This robustness argument gets *stronger* at scale, not weaker.
- **`silver`/`gold` use Iceberg** for compaction, time-travel, and schema
  evolution — exactly what 50+ assets and "reproduce any past result" need.

**Why not Iceberg for bronze too.** Compaction is tempting against small-files
(below), but an append-only immutable layer gains little from ACID/upserts and
loses the catalog-free property. Keep bronze dumb.

### Exactly-once into the lake via deterministic object keys

`make_consumer` already sets `enable_auto_commit=False` (commit offsets only
*after* the Parquet PUT). That gives **at-least-once**: a crash between PUT and
offset-commit re-reads and re-PUTs the same batch.

- **Decision (refined at build time):** name each object deterministically by
  its **start offset only** — `…/{partition:03d}-{start_offset:012d}.parquet` —
  so a re-PUT **overwrites the identical key** rather than appending a
  duplicate. The originally sketched `{start}-{end}` range key is *not* safely
  deterministic: a chunk's end offset depends on the wall-clock age flush, so a
  crash-retry could cut at a different end and leave an overlapping stale
  object. The start offset is always the committed consumer offset (and the
  materializer keeps at most one PUT un-committed), so start-only keys make the
  overwrite exact — the Kafka Connect S3-sink convention. End offset + count
  live in the Parquet footer metadata. That turns at-least-once into
  effectively exactly-once *at the file grain*, with no downstream dedup needed
  for bronze. Layout details: `data-contracts.md` → "Bronze lake".
- Silver transforms additionally dedup on a natural key
  (`exchange, symbol, sequence` for book events; `exchange, trade_id` for trades)
  as defence in depth.

### Query engine: DuckDB + dbt now; ClickHouse is the scale target

Compose already says "DuckDB + dbt." Keep it for the build — zero-ops, embedded,
native `ASOF JOIN`, ideal at the BTC-first scale. But 50+ × full-L2 is genuinely
ClickHouse-scale, so:

- **Treat the engine as swappable over the engine-agnostic lake.** DuckDB and
  ClickHouse both read Parquet/Iceberg and both have `ASOF JOIN`; dbt has
  adapters for both.
- **Discipline (the price of reversibility): keep SQL to the portable subset** —
  standard `ASOF JOIN`, no engine-specific functions — so the eventual cutover is
  operational, not a logic rewrite.

### Symbology: canonical resolution extended to the lake

The lake must partition by **canonical instrument** (`<BASE>-<QUOTE>`), not by
each venue's native topic symbol (`BTCUSDT` vs `BTC-USD` vs `BTC/USD`). Topic
symbol ≠ canonical_id: `md.trades.binance.BTCUSDT` resolves to canonical
`BTC-USDT`. So the analytics half needs the same `(exchange, symbol) →
canonical_id` resolution the gateway already does from `ops/instruments.yml`.

- **Decision:** reuse `instruments.yml` as the source of truth; the materializer
  resolves native → canonical on write. Quote currencies stay strictly distinct.
- **Point-in-time reference data:** record per-`(venue, instrument)`
  **first-seen / last-seen** from day one. At 50+ with long-tail listings and
  delistings, skipping this bakes in **survivorship/listing bias** — a textbook
  research unknown-unknown. (Curated-vs-discovered symbology at 50+ is an
  **open concern** below.)

### DECIMAL scale: `DECIMAL(38,18)` canonical

Prices/sizes are strings on the wire (`models.py`: "Convert to Decimal at the
boundary where math is needed"). The warehouse type must cover the long tail:

- `(18,8)` (the earlier D3 placeholder) **breaks on long-tail assets** — sub-cent
  prices need more fractional digits; meme-coin-scale sizes need more integer
  digits.
- **`DECIMAL(38,18)`**: 38 is Iceberg's max precision *and* the shared ceiling of
  DuckDB and ClickHouse (Decimal128) — so it is the one scale **portable across
  all three engines**. Covers `1e-18` prices and `1e20` sizes; we will not
  outgrow it.
- **Verify against the corpus to confirm fit** (not to discover the bound).
  Supersedes the `(18,8)` note in `ARCHITECTURE.md` D3.

### Partitioning: `exchange/canonical_symbol/date` (+`hour` for hot)

- Bronze/silver Parquet layout `exchange/canonical_symbol/date[/hour]`.
- **Hot/cold skew** across 50+ symbols (BTC firehose vs long-tail trickle) means
  hour-partitioning cold symbols produces tiny files. Hour-partition hot symbols;
  date-only for cold; lean on **Iceberg compaction** at silver. The materializer
  flush should be **size-dominant** (`FLUSH_BYTES`, already 16 MiB) so cold
  symbols accumulate before flushing instead of emitting a tiny file every
  `FLUSH_INTERVAL_SEC` (currently 60s — see Open concerns).

### Time semantics: keep every clock; choose per use

Every message carries `exchange_ts_ns` (venue) and `local_ts_ns` (ingester
emit); Kafka headers add `local_recv_ts_ns` (wire receipt) and `exchange_ts_ns`.

- **Preserve all of them in bronze — never drop a clock.**
- **As-of joins use `local_recv_ts_ns`** ("what was *knowable* to us at the
  time" — the no-lookahead-correct clock); **`exchange_ts_ns` answers "what
  actually happened at the venue."** Cross-venue `exchange_ts_ns` comparisons
  inherit the clock-domain caveat in `ARCHITECTURE.md` D4.

---

## The layers in detail

### `lake` / bronze — what gets projected

All raw topics, one Parquet dataset per logical stream:

| Source topic | Bronze dataset | Notes |
|---|---|---|
| `md.book.*.deltas` | `book_deltas` | the high-cardinality firehose; full fidelity |
| `md.book.*.snapshots` | `book_snapshots` | replay/seek anchors |
| `md.trades.*` | `trades` | the trade tape |
| `md.status.*` | `status` | venue liveness (data-quality + replay eviction) |
| `md.bbo.*`, `md.nbbo.*` | `bbo`, `nbbo` | derived; captured **for validation** (golden) |

Derived streams (`bbo`/`nbbo`) are re-derivable, but capturing them lets silver
assert reconstruction == the live gateway (the oracle below).

### `silver` — reconstruction + enrichment

- **Book state (hybrid grain).** Bronze keeps the *full* delta stream, so:
  materialize **cadence depth snapshots** (top-N every fixed interval) in silver
  for everyday queryable depth, and **reconstruct event-grain on demand via the
  replay engine** for the specific signals that need it (queue dynamics,
  micro-timing). Pay event-grain cost only for windows actually researched, not
  for all 50+ assets continuously. (Cadence value is an **open concern**.)
- **Enriched trades.** `trades` as-of-joined to prevailing NBBO/BBO at
  `local_recv_ts_ns` → the substrate for effective spread, price improvement,
  trade-through (market-quality TCA; we have no orders of our own).
- **Data-quality facts.** Sequence-gap flags, Kraken book-checksum results,
  crossed/locked frequency, per-hop latency — see the scorecard.

### `gold` — marts

Features (offline, point-in-time), cross-quote **basis** (same-base
different-quote as-of join), market-quality metrics, and the **data-quality
scorecard**. `VWAP` already exists as a batch type in `models.py` ("computed by
dbt after the fact") — the precedent for gold marts.

---

## Integration-first testing (the harness the repo is missing)

`ARCHITECTURE.md` names the integration harness the biggest test gap — "every
shipped bug was caught by smoke, not the unit suite." This design makes
integration the *spine*, via one realization:

**The replay engine and the integration harness are the same investment.** Both
need a recorded slice of the log + deterministic replay producing reproducible
output. So a **golden corpus** + the replay engine *is* the integration fixture;
integration becomes a byproduct of the keystone, not a separate chore.

Testing layers:

| Layer | What | Where |
|---|---|---|
| Unit (TDD) | pure transforms, reconstruction logic | every phase |
| Component | one process vs ephemeral infra (testcontainers: Redpanda + MinIO) | Phase 1+ |
| Integration | corpus replay → assert golden end-state | Phase 0 spine |
| **Property** | idempotency under crash; replay determinism; train/serve skew | Phases 1, 3, 5 |
| Data-quality-as-tests | gaps, checksums, no-lookahead | Phase 2 (CI + runtime) |

The **golden corpus**: a real ~10-min slice of `trades` + `book.*` +
`status` across all current venues, *including* at least one planted hard event
(venue drop / crossed book / sequence gap), stored as a fixture. It is the
deterministic input for every downstream test, the replay fodder, and the
artifact we measure DECIMAL fit and engine choice against.

---

## Phased plan

**Phase 0 — Harness foundation + golden corpus** *(enabler; pays down existing debt)*
Capture the corpus; stand up testcontainers (Redpanda + MinIO); first test =
replay corpus → existing ingest/gateway path → assert recorded NBBO. Retroactively
closes the integration gap for the already-shipped streaming half. Decision-light
(no engine/format lock needed) → cleanest place to start.

**Phase 1 — Materializer → `lake`/bronze** *(floor, part 1)*
`python/materializer`: consume each topic → Parquet on MinIO, canonical-resolved,
deterministic offset-keyed objects, size-dominant flush. Tests: bronze content ==
input (no loss/dupe); **crash-recovery property** (kill mid-batch, restart, assert
exactly-once via key overwrite). Proves the idempotency claim instead of asserting
it. *Needs: DECIMAL + partition + format locked.*

**Phase 2 — Data-quality scorecard** *(floor, part 2 — ground truth)*
Validation over bronze: sequence gaps, Kraken checksum verification, crossed/locked
frequency, per-hop latency, snapshot/delta ratios, venue uptime. These *are* tests
(CI against the corpus's planted gap) *and* live monitors (Grafana). Tech: pandera
+ dbt tests.

**Phase 3 — Replay engine + D1/D2 fixes** *(keystone — determinism core done)*
Deterministic, seekable replay: seek to last snapshot offset before T (**D2 fix**),
replay deltas via the reused `OrderBook`, drive all timing off log-time `ts_ns`
not wall-clock (**D1 fix**); optionally re-emit on the gateway WS protocol for
dashboard scrubbing. Headline test: replay corpus twice → **byte-identical**
output (the only proof D1/D2 are fixed). Provides the canonical fixture for
everything else. *Done (2026-06): D1/D2 shipped in the gateway-replay refactor
(stream clock, order-insensitive aggregator, periodic snapshots, warm-start
seek — see `ARCHITECTURE.md`), and the byte-identical test plus an adversarial
all-deltas-first convergence test pass in
`analytics/tests/test_gateway_integration.py`. Remaining: the seekable
research-side replay engine (replay-to-T, dashboard scrubbing).*

**Phase 4 — Silver: reconstructed book state + enriched events**
Cadence book snapshots + as-of-joined enriched trades. Tests: **3-way
reconstruction oracle** (silver == replay engine == live gateway, elevating
`ARCHITECTURE.md` D5); no-lookahead property tests on the as-of joins.

**Phase 5 — Research surface + feature store (DIY-first)**
Offline features (Parquet, point-in-time) + online (Redis), DIY before adopting
Feast (Feast's value is organizational; the technical core — train/serve skew +
PIT — is what matters solo). Backtest harness on replay + silver. Tests:
train/serve skew (offline path == online path), PIT-correctness, backtest
determinism.

---

## Open concerns

Genuinely unresolved — flagged for verification / decision, not yet locked.

1. **Symbology at 50+: curated YAML vs discovered.** `DESIGN_nbbo.md` chose
   curated `instruments.yml` for "static data that changes ~weekly" and
   explicitly ruled out "derived parsing rules" — correct for the gateway's
   marquee NBBO at small scale. Hand-curating every `(exchange, symbol) →
   canonical` for 50+ long-tail with listing churn is infeasible. Likely
   resolution: keep curated YAML for the gateway's live NBBO instruments, but add
   **deterministic per-exchange normalization rules** + auto-registration for the
   lake's long tail, with YAML as override. **This revisits a shipped decision —
   needs the user's call.** Note the hard case: Binance concatenates
   (`BTCUSDT`, no separator) so base/quote splitting needs a known quote-suffix
   table (USDT/USD/USDC/BTC/…); Coinbase/Kraken use separators and are trivial.

2. **Compute/orchestration: the pencilled Spark + Airflow vs DuckDB/dbt + a light
   orchestrator.** Compose's header says DuckDB+dbt; its footer pencils in
   `airflow + spark`. At 50+, heavy silver book-reconstruction *could* justify
   distributed Spark — or a big single box with DuckDB/ClickHouse may suffice.
   Lean: **defer Spark until a named bottleneck**, prefer a lighter orchestrator
   (Dagster/Prefect) unless Airflow is specifically wanted. Unresolved.

3. **Exact DECIMAL fit.** `(38,18)` is the safe portable ceiling, but confirm no
   intended asset's quoted precision exceeds it — measure from the corpus before
   writing rows.

4. **Engine cutover trigger.** What concretely moves us DuckDB → ClickHouse?
   (single-node query latency? lake size? ingestion rate?) Define the metric so
   the call isn't vibes-based.

5. **Silver book-state cadence.** What interval for cadence depth snapshots
   (100ms? 1s?) and what depth (top-N)? Drives silver volume; depends on which
   research signals we commit to. Open.

6. **`lake` small-files vs catalog-free.** Keeping bronze raw Parquet forgoes
   Iceberg compaction; size-dominant flush + coarse partitioning for cold symbols
   mitigates but doesn't fully solve. Revisit if cold-symbol file counts bite.

7. **Resolved:** `data-contracts.md` now exists and is the home for the
   topic/payload/bronze-lake/corpus contract (`ARCHITECTURE.md` D7).

8. **Resolved (2026-06):** replay determinism depended on the D1/D2 refactor;
   both shipped (gateway-replay), and the Phase-3 byte-identical acceptance
   test now passes against the real gateway.

9. **Ingestion fan-out (adjacent).** 50+ × many venues exceeds one-connection-
   per-exchange (`scale-out.md`): WS bandwidth (the ~1.1 MiB re-encoded / ~5 MiB
   raw snapshot × N symbols) and reconstruction CPU. Out of the analytics half's
   scope but the end-state vision depends on it; tracked in `scale-out.md`.

---

## Out of scope (v1)

- Your-own-order TCA (we have no orders; market-quality TCA only).
- Streaming OLAP / real-time materialized views (the Kappa-vs-lambda fork) — batch
  medallion first; revisit if sub-second gold freshness is ever needed.
- Cross-stablecoin "USD-class" NBBO topic — the basis mart covers the research
  need without widening any live topic (`DESIGN_nbbo.md` "When to revisit").
- Feast adoption — DIY feature store first; adopt deliberately later.

## Cross-references

- `ARCHITECTURE.md` — D1 (replay-time eviction), D2 (snapshot-offset seek), D3
  (decimal endpoints), D4 (clock domains), D5 (reconstruction oracle), D7
  (missing data-contracts.md), "The analytics seam".
- `DESIGN_nbbo.md` — strict per-quote bucketing, `instruments.yml`, canonical_id.
- `scale-out.md` — connection topology, per-symbol resync, snapshot bandwidth.
