# Data contracts

The wire contract for everything on the Redpanda log, plus the corpus file
format used by the replay harness. Code is authoritative; this doc is the
cross-language map:

- **Payload types:** `python/common/models.py` (msgspec Structs), mirrored by
  `node/gateway/src/messages.ts`.
- **Topic naming:** `python/common/kafka_io.py`, mirrored by
  `node/gateway/src/messages.ts`.
- **Corpus format:** `python/analytics/corpus.py`.

A divergence between the Python and Node mirrors is a bug. A cross-language
oracle that would catch it mechanically is a known gap, not yet built.

The log is the source of truth; bronze mirrors it verbatim, and every silver and
gold table is a batch projection of bronze:

The entities and their derivation edges; the column-level schemas are the
generated tables further down.

```mermaid
erDiagram
    bronze ||--o{ book_quality : "OrderBook fold"
    bronze ||--o{ latency : "header decode"
    bronze ||--o{ status_events : "status fold"
    bronze ||--o{ quotes : "OrderBook fold, top-of-1"
    quotes ||--o{ nbbo : "cross-venue max-bid min-ask"
    status_events ||--o{ nbbo : "connection-state eviction"
    book_quality }o--|| scorecard : "aggregate per check"
    latency }o--|| scorecard : "latency percentiles"
    status_events }o--|| scorecard : "venue_uptime"
    nbbo ||--o{ basis : "as-of join USD x USDT"
    basis }o--|| basis_summary : "daily summary"

    %% Columns are deliberately not listed: mermaid's ER types cannot express
    %% decimal128(38,18) vs int32 vs int64, so an attribute block here would be a
    %% second, lossy copy of the generated tables below.
```

## Topics

| Topic | Producer | Payload (tag) | Key | Cleanup |
|---|---|---|---|---|
| `md.book.{exchange}.{symbol}.snapshots` | ingester | `BookSnapshot` (`snap`) | `{exchange}:{symbol}` | delete |
| `md.book.{exchange}.{symbol}.deltas` | ingester | `BookDelta` (`delta`) | `{exchange}:{symbol}` | delete |
| `md.trades.{exchange}.{symbol}` | ingester | `Trade` (`trade`) | `{exchange}:{symbol}` | delete |
| `md.status.{exchange}` | ingester | `Status` (`status`) | `{exchange}` | compact |
| `md.bbo.{exchange}.{symbol}` | gateway | `BBO` (`bbo`) | `{exchange}:{symbol}` | delete |
| `md.liquidations.{exchange}.{symbol}` | ingester (derivatives) | `Liquidation` (`liq`) | `{exchange}:{symbol}` | delete |
| `md.markprice.{exchange}.{symbol}` | ingester (derivatives) | `MarkPrice` (`mark`) | `{exchange}:{symbol}` | delete |
| `md.openinterest.{exchange}.{symbol}` | ingester (derivatives, REST poll) | `OpenInterest` (`oi`) | `{exchange}:{symbol}` | delete |
| `md.nbbo.{canonical_id}` | gateway | `NBBOMsg` (`nbbo`, messages.ts only) | `{canonical_id}` | compact |

- `{symbol}` in a **topic name** is the normalized form (`normalize_symbol`:
  any character outside `[a-zA-Z0-9._-]` becomes `-`; Kraken's `BTC/USD` →
  `BTC-USD`). The native symbol is preserved in the message body (`symbol`
  field); it is **not** in a record header. Record **keys** carry the native
  form (e.g. `kraken:BTC/USD`).
- `{canonical_id}` is `<BASE>-<QUOTE>` from `ops/instruments.yml`. Quote
  currencies are strictly bucketed (USD ≠ USDT); see `DESIGN_nbbo.md`.
- The gateway pre-creates the compacted `md.nbbo.*` / `md.status.*` topics at
  startup; firehose topics are auto-created on first produce and inherit the
  cluster default cleanup (delete).
- Keys exist for log semantics (compaction identity, future partition
  affinity). Ordering today comes from single-partition topics, not keys.
- `NBBOMsg` is gateway-emitted JSON defined only in `messages.ts`; the Python
  streaming decoder (`models.decode`) does not cover it. `Spread` and `VWAP`
  exist in `models.py` but are not published on any live topic.
- `md.book.*.snapshots` carries bootstrap/resync snapshots **and** periodic
  re-emissions of the live local book (every `snapshot_interval_s`, default
  300 s). Re-emitted snapshots are shape-identical, with `sequence` = the
  book's current sequence and `exchange_ts_ns = 0` (locally generated, no
  exchange event behind them); they bound the delta tail a warm-starting
  consumer must replay to one interval.
- `BookSnapshot` / `BookDelta` carry an `epoch` (per-WS-connection generation,
  default `0`). coinbase/kraken reset `sequence` on each reconnect, so the
  gateway only applies a delta to a book of the same `epoch`, so a prior
  connection's deltas can't corrupt a fresh snapshot. Compared by equality only
  (never as a clock); pre-epoch records decode as `0`.
- `NBBOMsg.local_ts_ns` and per-leg `leg_age_ms` are **stream time** (the max
  event-time across the gateway's consumed messages at compute, not wall
  clock), so `md.nbbo.*` replays byte-for-byte (see ADR-0001). In
  live operation stream time tracks wall clock within consumer lag (ms).

## Retention

Delete-policy topics inherit the cluster `log_retention_ms`, set to **30 days**
by `redpanda-init` in `docker-compose.yml` (Redpanda's default is 7 days).
Until the materializer (Phase 1) projects topics to the lake, the log is the
only store, so retention is the data-loss horizon. Compacted topics keep the
latest record per key indefinitely.

**Bronze lake lifecycle.** The materializer now persists bronze to the `lake`
bucket, so the log is just the replay buffer. To stop the lake growing unbounded,
`minio-init` sets a MinIO lifecycle rule expiring raw `lake` objects after **30
days** (matching `log_retention_ms`); verify with `mc ilm rule ls local/lake`.
On the dev box this is generous (~200 GB free is months of headroom), and it
deletes nothing until objects age past 30 days. The horizon is the
raw-history-vs-disk tradeoff; the durable layer's real fix is off-box object
storage with tiered lifecycle, where the identical S3 lifecycle
API applies unchanged.

## Payload encoding

- msgspec-tagged JSON; the tag field is `t` (`snap` / `delta` / `trade` /
  `bbo` / `spread` / `status` / `nbbo` / `liq` / `mark` / `oi`).
- Prices and sizes are decimal **strings** end-to-end (no float drift);
  convert at the boundary where math is needed.
- Book levels are `[price, size]` string arrays (`BookLevel` is `array_like`).
- Timestamps are epoch **nanoseconds** (`*_ts_ns`). They exceed 2^53, so
  Node's `JSON.parse` rounds them to ~200ns granularity (see the NOTE in
  `messages.ts`); do not treat gateway-side values as exact.
- Hard ceiling: **8 MiB** per message pre-compression (`MAX_MESSAGE_BYTES`),
  mirrored at the producer, the broker (`kafka_batch_max_bytes`), and the
  gateway consumer. Producers use gzip; the broker stores whatever codec the
  producer used, so non-gzip producers are a contract violation (the gateway
  only decodes gzip).

## Record headers

Firehose records (book, trades, liquidations, markprice, openinterest) carry
latency-tracking headers (`latency_headers()` in `kafka_io.py`):

| Header | Value |
|---|---|
| `local_recv_ts_ns` | ingester receive time, epoch ns (ASCII integer) |
| `exchange_ts_ns` | exchange-reported event time, epoch ns (ASCII integer) |

Status and gateway-derived records (bbo/nbbo) carry no headers.

## Bronze lake (materializer)

The materializer (`python/materializer`) projects every `md.*` topic verbatim
to Parquet on the `lake` bucket, the insurance layer of the medallion plan.
Object paths are Hive-partitioned, canonical-resolved
via `ops/instruments.yml`:

<!-- BEGIN GENERATED: bronze-datasets -->

| Dataset | Source topics | Path | Row = |
|---|---|---|---|
| `book_snapshots` | `md.book.*.snapshots` | `book_snapshots/exchange={ex}/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one bootstrap or re-emitted book snapshot |
| `book_deltas` | `md.book.*.deltas` | `book_deltas/exchange={ex}/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one book delta |
| `trades` | `md.trades.*` | `trades/exchange={ex}/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one trade |
| `bbo` | `md.bbo.*` | `bbo/exchange={ex}/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one gateway-derived best bid/offer |
| `liquidations` | `md.liquidations.*` | `liquidations/exchange={ex}/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one forced order |
| `mark_price` | `md.markprice.*` | `mark_price/exchange={ex}/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one mark/funding tick |
| `open_interest` | `md.openinterest.*` | `open_interest/exchange={ex}/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one open-interest poll |
| `status` | `md.status.*` | `status/exchange={ex}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one venue connection-state record |
| `nbbo` | `md.nbbo.*` | `nbbo/symbol={canonical}/date={d}/{partition:03d}-{start_offset:012d}.parquet` | one gateway-derived NBBO tick |

| Dataset | Column | Type |
|---|---|---|
| all of the above | `topic` | string |
| all of the above | `partition` | int32 |
| all of the above | `offset` | int64 |
| all of the above | `timestamp_ms` | int64 |
| all of the above | `key` | binary |
| all of the above | `value` | binary |
| all of the above | `headers` | list<item: struct<key: string, value: binary>> |

<!-- END GENERATED: bronze-datasets -->

- **Object naming:** `{partition:03d}-{start_offset:012d}.parquet`, keyed by
  the chunk's **start offset only** (Kafka Connect S3-sink convention). The
  end offset of a chunk is wall-clock-dependent (the age flush), but the start
  is always the committed consumer offset, so a crash-retry rewrites the
  *identical* key, turning at-least-once into exactly-once at the file grain.
  End offset and record count live in the Parquet footer metadata
  (`crosstick:start_offset` / `end_offset` / `record_count` /
  `format = bronze-v1`). Chunks are contiguous per topic-partition, so a
  sorted listing implies coverage.
- **Rows are corpus-shaped:** the columns are exactly the `CorpusRecord`
  fields below (`topic`, `partition`, `offset`, `timestamp_ms`, `key`,
  `value`, `headers`); any bronze slice reads back as replay fodder.
- **Chunk cuts:** size-dominant (`FLUSH_BYTES`, 16 MiB), plus a UTC
  date-boundary cut (keeps `date=` partitions honest) and an age sweep
  (`FLUSH_INTERVAL_SEC`, the bronze-lag bound for cold topics, not the
  primary trigger). Compression is zstd.
- **Commit discipline:** offsets are committed only *after* the Parquet PUT,
  one chunk at a time, so at most one PUT is ever un-committed, which is what
  makes the start-offset overwrite argument airtight. The consumer group
  (`materializer`) is the durable "how far has bronze got" cursor.
- Unmapped `(exchange, symbol)` pairs partition by the normalized native
  symbol (warn-once); bronze never drops data over missing reference data.

## Silver datasets

`python -m silver.main <date>` reads bronze and writes the silver datasets (the
data-quality, basis, and tape slices) to the `silver` bucket. Values are decoded once (`common.models.decode`) and books reconstructed
through the real `ingest.book.OrderBook`, the same engine as live ingest, so the
facts agree with the gateway's live metrics by construction. Hive-partitioned,
canonical-resolved; **one overwrite-keyed `part.parquet` per partition** (a layer
aggregates a whole date, so a recompute rewrites the object: idempotent, the same
discipline as bronze's start-offset keys, but at date grain).

<!-- BEGIN GENERATED: silver-datasets -->

| Dataset | Path | Row = |
|---|---|---|
| `book_quality` | `book_quality/exchange={ex}/symbol={canonical}/date={d}/part.parquet` | one book event, with crossed/invariant flags and a sequence-gap count |
| `latency` | `latency/exchange={ex}/symbol={canonical}/date={d}/part.parquet` | one firehose record's per-hop latency |
| `status_events` | `status_events/exchange={ex}/date={d}/part.parquet` | one typed venue up/down transition, with downtime |
| `quotes` | `quotes/exchange={ex}/symbol={canonical}/date={d}/part.parquet` | one reconstructed top-of-book, at each event with a valid two-sided book |
| `nbbo` | `nbbo/symbol={canonical}/date={d}/part.parquet` | one reconstructed cross-venue NBBO tick |
| `trades` | `trades/exchange={ex}/symbol={canonical}/date={d}/part.parquet` | one trade, taker side measured |
| `liquidations` | `liquidations/exchange={ex}/symbol={canonical}/date={d}/part.parquet` | one forced order |
| `mark_price` | `mark_price/exchange={ex}/symbol={canonical}/date={d}/part.parquet` | one mark/funding tick |
| `open_interest` | `open_interest/exchange={ex}/symbol={canonical}/date={d}/part.parquet` | one open-interest poll |

| Dataset | Column | Type |
|---|---|---|
| `book_quality` | `exchange` | string |
| `book_quality` | `canonical_symbol` | string |
| `book_quality` | `date` | string |
| `book_quality` | `kind` | string |
| `book_quality` | `offset` | int64 |
| `book_quality` | `sequence` | int64 |
| `book_quality` | `epoch` | int64 |
| `book_quality` | `exchange_ts_ns` | int64 |
| `book_quality` | `local_ts_ns` | int64 |
| `book_quality` | `local_recv_ts_ns` | int64 |
| `book_quality` | `best_bid` | decimal128(38, 18) |
| `book_quality` | `best_ask` | decimal128(38, 18) |
| `book_quality` | `seq_gap` | int64 |
| `book_quality` | `crossed` | bool |
| `book_quality` | `invariant_kind` | string |
| `latency` | `exchange` | string |
| `latency` | `canonical_symbol` | string |
| `latency` | `date` | string |
| `latency` | `dataset` | string |
| `latency` | `offset` | int64 |
| `latency` | `exchange_ts_ns` | int64 |
| `latency` | `exchange_to_recv_ns` | int64 |
| `latency` | `exchange_to_emit_ns` | int64 |
| `status_events` | `exchange` | string |
| `status_events` | `date` | string |
| `status_events` | `ts_ns` | int64 |
| `status_events` | `state` | string |
| `status_events` | `prev_state` | string |
| `status_events` | `is_transition` | bool |
| `status_events` | `downtime_ns` | int64 |
| `quotes` | `exchange` | string |
| `quotes` | `canonical_symbol` | string |
| `quotes` | `date` | string |
| `quotes` | `ts_ns` | int64 |
| `quotes` | `best_bid` | decimal128(38, 18) |
| `quotes` | `best_ask` | decimal128(38, 18) |
| `quotes` | `bid_sz` | decimal128(38, 18) |
| `quotes` | `ask_sz` | decimal128(38, 18) |
| `quotes` | `bid_depth_5` | decimal128(38, 18) |
| `quotes` | `ask_depth_5` | decimal128(38, 18) |
| `quotes` | `bid_depth_10` | decimal128(38, 18) |
| `quotes` | `ask_depth_10` | decimal128(38, 18) |
| `quotes` | `bid_px_10` | decimal128(38, 18) |
| `quotes` | `ask_px_10` | decimal128(38, 18) |
| `nbbo` | `canonical_symbol` | string |
| `nbbo` | `date` | string |
| `nbbo` | `ts_ns` | int64 |
| `nbbo` | `best_bid` | decimal128(38, 18) |
| `nbbo` | `best_ask` | decimal128(38, 18) |
| `nbbo` | `bid_venue` | string |
| `nbbo` | `ask_venue` | string |
| `nbbo` | `n_venues` | int64 |
| `trades` | `exchange` | string |
| `trades` | `canonical_symbol` | string |
| `trades` | `date` | string |
| `trades` | `ts_ns` | int64 |
| `trades` | `offset` | int64 |
| `trades` | `exchange_ts_ns` | int64 |
| `trades` | `local_ts_ns` | int64 |
| `trades` | `trade_id` | string |
| `trades` | `price` | decimal128(38, 18) |
| `trades` | `size` | decimal128(38, 18) |
| `trades` | `side` | string |
| `liquidations` | `exchange` | string |
| `liquidations` | `canonical_symbol` | string |
| `liquidations` | `date` | string |
| `liquidations` | `ts_ns` | int64 |
| `liquidations` | `offset` | int64 |
| `liquidations` | `exchange_ts_ns` | int64 |
| `liquidations` | `local_ts_ns` | int64 |
| `liquidations` | `side` | string |
| `liquidations` | `price` | decimal128(38, 18) |
| `liquidations` | `avg_price` | decimal128(38, 18) |
| `liquidations` | `orig_size` | decimal128(38, 18) |
| `liquidations` | `filled_size` | decimal128(38, 18) |
| `liquidations` | `status` | string |
| `mark_price` | `exchange` | string |
| `mark_price` | `canonical_symbol` | string |
| `mark_price` | `date` | string |
| `mark_price` | `ts_ns` | int64 |
| `mark_price` | `offset` | int64 |
| `mark_price` | `exchange_ts_ns` | int64 |
| `mark_price` | `local_ts_ns` | int64 |
| `mark_price` | `mark_price` | decimal128(38, 18) |
| `mark_price` | `index_price` | decimal128(38, 18) |
| `mark_price` | `est_settle_price` | decimal128(38, 18) |
| `mark_price` | `funding_rate` | decimal128(38, 18) |
| `mark_price` | `next_funding_ts_ns` | int64 |
| `open_interest` | `exchange` | string |
| `open_interest` | `canonical_symbol` | string |
| `open_interest` | `date` | string |
| `open_interest` | `ts_ns` | int64 |
| `open_interest` | `offset` | int64 |
| `open_interest` | `exchange_ts_ns` | int64 |
| `open_interest` | `local_ts_ns` | int64 |
| `open_interest` | `open_interest` | decimal128(38, 18) |

<!-- END GENERATED: silver-datasets -->

- **`crossed`/`invariant_kind`** come from the OrderBook fold per
  `(exchange, symbol, epoch)`; a violation resyncs (clear) like the ingester. The
  book is reconstructed to **top-of-1**; full-depth cadence book-state + checksum
  verification are Phase 4 and extend this layer.
- **`seq_gap`** counts monotonic-but-missing deltas under a per-exchange policy:
  kraken's synthesized per-book counter is **contiguous** (a hole = ingest→bronze
  loss); coinbase (connection-wide counter shared across channels) and
  binance/binance-futures (update-ids) are **monotonic-only** (forward jumps are
  normal). Non-monotonic regressions are caught for all venues by the OrderBook
  fold (`non_monotonic_seq`).
- **`latency`** skips locally-generated records (`exchange_ts_ns == 0`:
  re-emitted snapshots, binance(-futures) snapshots, with no exchange clock behind
  them). Cross-venue `exchange_ts_ns` comparisons inherit the clock-domain caveat
  (ingest and gateway share one clock). `bbo`/`nbbo` carry no headers and are validated live by
  the gateway, so they are not re-checked here.
- **`quotes` depth** carries cumulative resting size over the best 5 and 10 levels a
  side, plus the worst price the 10th rung reaches, so size *and* distance are both
  available (book slope). Ten levels is a venue-symmetry floor, not a tuning knob:
  Kraken's v2 book channel is subscribed at depth 10 and hard-trims past it, so a
  deeper rung (or a price-relative window like "size within 25bps", which coinbase
  and binance could fill and kraken could not) would give three venues a feature the
  fourth structurally cannot have, and a cross-venue lead-lag model would read that
  asymmetry as venue skill. Rungs truncate to the levels a side actually holds, so a
  thin book reports its whole side rather than a null.
- **`quotes`/`nbbo`** are the basis-slice additions.
  `quotes` is per-venue top-of-1 from the *same* OrderBook fold as `book_quality`
  (crossed/one-sided events skipped); `nbbo` is the per-canonical max-bid/min-ask
  across a canonical's venues (strict per-quote bucketing), evicting a venue's leg
  while its `status` is down (`DESIGN_nbbo.md` connection-state eviction). The
  per-venue BBO oracle checks reconstructed `quotes` against the captured bronze
  `bbo`.
- **`trades`/`liquidations`/`mark_price`/`open_interest`** are the *tape* datasets:
  bronze content carried over verbatim rather than derived, so they are the one part
  of silver that is a copy and not a reconstruction. They exist to outlive bronze,
  which expires on a lifecycle rule (`docker-compose.yml`, `BRONZE_EXPIRE_DAYS`)
  while silver has no expiry. Each shares a common seven-column head
  (`exchange`, `canonical_symbol`, `date`, `ts_ns`, `offset`, `exchange_ts_ns`,
  `local_ts_ns`), and `ts_ns` is the same `local_recv_ts_ns` clock `quotes` uses, so
  a tape joins to the book on `(exchange, canonical_symbol, ts_ns)` without a clock
  conversion. `trades.side` is the **taker/aggressor** direction in every driver
  (`bid` = buyer-initiated), so order-flow sign needs no Lee-Ready inference.
  `liquidations` is sampled, not a tape (`DESIGN_perp_capture.md`), and
  `open_interest` is REST-polled at the ingester's interval.

## Gold scorecard (data-quality mart)

`python -m gold.main <date>` aggregates the silver facts into the scorecard mart
on the `gold` bucket (a gold mart reads silver, never bronze). One overwrite-keyed
object per date: `scorecard/date={d}/part.parquet`. Plain Parquet (DuckDB-queryable
ad-hoc; dbt formalizes gold marts at Phase 4/5, over typed silver).

Fact table keyed `(exchange, canonical_symbol, date, check)`:

<!-- BEGIN GENERATED: gold-scorecard -->

| Dataset | Path | Row = |
|---|---|---|
| `scorecard` | `scorecard/date={d}/part.parquet` | one (exchange, symbol, date, check) data-quality fact |

| Dataset | Column | Type |
|---|---|---|
| `scorecard` | `exchange` | string |
| `scorecard` | `canonical_symbol` | string |
| `scorecard` | `date` | string |
| `scorecard` | `check` | string |
| `scorecard` | `n_records` | int64 |
| `scorecard` | `n_violations` | int64 |
| `scorecard` | `p50_ms` | double |
| `scorecard` | `p95_ms` | double |
| `scorecard` | `p99_ms` | double |
| `scorecard` | `detail` | string |

<!-- END GENERATED: gold-scorecard -->

What the non-obvious columns mean (the schema can carry the type, not the intent):

| Column | Meaning |
|---|---|
| `n_records`, `n_violations` | denominator + the headline pass/fail count for the check |
| `p50_ms` / `p95_ms` / `p99_ms` | latency percentiles (latency checks only; null otherwise) |
| `detail` | compact JSON breakdown (by-kind invariant counts, `total_missing`, `downtime_sec`, ...) |

Checks: `sequence_gap`, `book_invariant`, `coverage` (per book symbol);
`latency.{dataset}` (per firehose dataset); `venue_uptime` (per exchange,
`canonical_symbol` null). `--fail-on-violation` makes `gold.main` exit non-zero
for ops/CI gating.

## Gold basis mart (stablecoin USDT/USD basis)

`python -m gold.main <date>` also builds the stablecoin-basis mart from the silver
`nbbo` dataset (a gold mart reads silver, never bronze). For each base quoted in
both USD and USDT (e.g. `BTC-USD` vs `BTC-USDT`; perp `-PERP` canonicals
excluded), it as-of joins the two NBBO series (backward-only, so each observation
is point-in-time correct) and emits the basis where **both** legs have a valid
two-sided NBBO. Two overwrite-keyed objects per date:

<!-- BEGIN GENERATED: gold-basis -->

| Dataset | Path | Row = |
|---|---|---|
| `basis` | `basis/date={d}/part.parquet` | one USD/USDT basis tick (either leg moved) |
| `basis_summary` | `basis_summary/date={d}/part.parquet` | one base per day |

| Dataset | Column | Type |
|---|---|---|
| `basis` | `base` | string |
| `basis` | `date` | string |
| `basis` | `ts_ns` | int64 |
| `basis` | `usd_mid` | decimal128(38, 18) |
| `basis` | `usdt_mid` | decimal128(38, 18) |
| `basis` | `basis_abs` | decimal128(38, 18) |
| `basis` | `basis_bps` | double |
| `basis` | `usd_bid` | decimal128(38, 18) |
| `basis` | `usd_ask` | decimal128(38, 18) |
| `basis` | `usdt_bid` | decimal128(38, 18) |
| `basis` | `usdt_ask` | decimal128(38, 18) |
| `basis_summary` | `base` | string |
| `basis_summary` | `date` | string |
| `basis_summary` | `n_obs` | int64 |
| `basis_summary` | `basis_bps_mean` | double |
| `basis_summary` | `basis_bps_std` | double |
| `basis_summary` | `basis_bps_median` | double |
| `basis_summary` | `basis_bps_min` | double |
| `basis_summary` | `basis_bps_max` | double |
| `basis_summary` | `basis_bps_p1` | double |
| `basis_summary` | `basis_bps_p99` | double |
| `basis_summary` | `coverage_ns` | int64 |

<!-- END GENERATED: gold-basis -->

`basis_abs = usd_mid - usdt_mid`; `basis_bps = basis_abs / usd_mid * 1e4`. This is
the first signal driven through the full research spine;
the later ladder rungs (price-discovery, carry, OFI) reuse the same as-of join.

## Freshness markers

A tiny overwrite-keyed object per dataset under `_freshness/`, written into both
the `silver` and `gold` buckets **after** that dataset's data objects, so a
partial or aborted build can never read back as fresh. It lets the lake-exporter
read a derived layer's freshness in O(1) (one GET per marker) instead of a full
LIST walk whose Class A cost grows with venues x securities, which is the
difference between free and metered on R2. A once-daily audit still walks the
layer to cross-check the markers. A zero-row dataset writes no object, so its
freshness stays undefined, exactly as the LIST walk reported it.

<!-- BEGIN GENERATED: freshness-markers -->

| Dataset | Path | Row = |
|---|---|---|
| `_freshness` | `_freshness/<dataset>.parquet` | one dataset's last successful build |

| Column | Type |
|---|---|
| `dataset` | string |
| `date` | string |
| `written_at_epoch` | double |
| `row_count` | int64 |

<!-- END GENERATED: freshness-markers -->

## Corpus format (replay harness)

A corpus is a gzipped JSON-lines file (`.jsonl.gz`), one `CorpusRecord` per
line, in capture order:

| Field | Meaning |
|---|---|
| `topic`, `partition`, `offset` | provenance from the source log |
| `timestamp_ms` | Kafka record timestamp (ms) |
| `key`, `value` | raw bytes, base64-encoded by msgspec's JSON codec |
| `headers` | `[name, base64-bytes]` pairs (including the latency headers) |

Replay (`analytics/replay.py`) sends every record to **partition 0** of its
original topic; the broker assigns fresh offsets `0..N-1`. Per-topic order is
preserved exactly; **cross-topic order is not**, but the gateway no longer
needs it imposed: a delta arriving before its stream's snapshot is buffered
and drained in order (see ADR-0001). Replay determinism is asserted
end-to-end in `analytics/tests/test_gateway_integration.py`: an adversarial
all-deltas-first replay converges to the same end state, and the same replay
run twice produces byte-identical `md.bbo.*` / `md.nbbo.*` streams.
