# Data contracts

The wire contract for everything on the Redpanda log, plus the corpus file
format used by the replay harness. Code is authoritative; this doc is the
cross-language map:

- **Payload types:** `python/common/models.py` (msgspec Structs), mirrored by
  `node/gateway/src/messages.ts`.
- **Topic naming:** `python/common/kafka_io.py`, mirrored by
  `node/gateway/src/messages.ts`.
- **Corpus format:** `python/analytics/corpus.py`.

A divergence between the Python and Node mirrors is a bug — D5 in
`ARCHITECTURE.md` tracks the missing cross-language oracle that would catch it
mechanically.

## Topics

| Topic | Producer | Payload (tag) | Key | Cleanup |
|---|---|---|---|---|
| `md.book.{exchange}.{symbol}.snapshots` | ingester | `BookSnapshot` (`snap`) | `{exchange}:{symbol}` | delete |
| `md.book.{exchange}.{symbol}.deltas` | ingester | `BookDelta` (`delta`) | `{exchange}:{symbol}` | delete |
| `md.trades.{exchange}.{symbol}` | ingester | `Trade` (`trade`) | `{exchange}:{symbol}` | delete |
| `md.status.{exchange}` | ingester | `Status` (`status`) | `{exchange}` | compact |
| `md.bbo.{exchange}.{symbol}` | gateway | `BBO` (`bbo`) | `{exchange}:{symbol}` | delete |
| `md.nbbo.{canonical_id}` | gateway | `NBBOMsg` (`nbbo`, messages.ts only) | `{canonical_id}` | compact |

- `{symbol}` in a **topic name** is the normalized form (`normalize_symbol`:
  any character outside `[a-zA-Z0-9._-]` becomes `-`; Kraken's `BTC/USD` →
  `BTC-USD`). The native symbol is preserved in the message body (`symbol`
  field) — it is **not** in a record header. Record **keys** carry the native
  form (e.g. `kraken:BTC/USD`).
- `{canonical_id}` is `<BASE>-<QUOTE>` from `ops/instruments.yml`. Quote
  currencies are strictly bucketed (USD ≠ USDT) — see `DESIGN_nbbo.md`.
- The gateway pre-creates the compacted `md.nbbo.*` / `md.status.*` topics at
  startup; firehose topics are auto-created on first produce and inherit the
  cluster default cleanup (delete).
- Keys exist for log semantics (compaction identity, future partition
  affinity). Ordering today comes from single-partition topics, not keys.
- `NBBOMsg` is gateway-emitted JSON defined only in `messages.ts`; the Python
  streaming decoder (`models.decode`) does not cover it. `Spread` and `VWAP`
  exist in `models.py` but are not published on any live topic.

## Retention

Delete-policy topics inherit the cluster `log_retention_ms`, set to **30 days**
by `redpanda-init` in `docker-compose.yml` (Redpanda's default is 7 days).
Until the materializer (Phase 1) projects topics to the lake, the log is the
only store — retention is the data-loss horizon. Compacted topics keep the
latest record per key indefinitely.

## Payload encoding

- msgspec-tagged JSON; the tag field is `t` (`snap` / `delta` / `trade` /
  `bbo` / `spread` / `status` / `nbbo`).
- Prices and sizes are decimal **strings** end-to-end (no float drift);
  convert at the boundary where math is needed (D3 in `ARCHITECTURE.md`).
- Book levels are `[price, size]` string arrays (`BookLevel` is `array_like`).
- Timestamps are epoch **nanoseconds** (`*_ts_ns`). They exceed 2^53, so
  Node's `JSON.parse` rounds them to ~200ns granularity (see the NOTE in
  `messages.ts`) — do not treat gateway-side values as exact.
- Hard ceiling: **8 MiB** per message pre-compression (`MAX_MESSAGE_BYTES`),
  mirrored at the producer, the broker (`kafka_batch_max_bytes`), and the
  gateway consumer. Producers use gzip; the broker stores whatever codec the
  producer used, so non-gzip producers are a contract violation (the gateway
  only decodes gzip).

## Record headers

Firehose records (book + trades) carry latency-tracking headers
(`latency_headers()` in `kafka_io.py`):

| Header | Value |
|---|---|
| `local_recv_ts_ns` | ingester receive time, epoch ns (ASCII integer) |
| `exchange_ts_ns` | exchange-reported event time, epoch ns (ASCII integer) |

Status and gateway-derived records (bbo/nbbo) carry no headers.

## Bronze lake (materializer)

The materializer (`python/materializer`) projects every `md.*` topic verbatim
to Parquet on the `lake` bucket — the insurance layer of the medallion plan
(`DESIGN_analytics.md`). Object paths are Hive-partitioned, canonical-resolved
via `ops/instruments.yml`:

| Dataset | Source topics | Path |
|---|---|---|
| `book_snapshots`, `book_deltas` | `md.book.*` | `{dataset}/exchange={ex}/symbol={canonical}/date={YYYY-MM-DD}/` |
| `trades` | `md.trades.*` | same |
| `bbo` | `md.bbo.*` | same |
| `status` | `md.status.*` | `status/exchange={ex}/date=…/` |
| `nbbo` | `md.nbbo.*` | `nbbo/symbol={canonical}/date=…/` |

- **Object naming:** `{partition:03d}-{start_offset:012d}.parquet` — keyed by
  the chunk's **start offset only** (Kafka Connect S3-sink convention). The
  end offset of a chunk is wall-clock-dependent (the age flush), but the start
  is always the committed consumer offset — so a crash-retry rewrites the
  *identical* key, turning at-least-once into exactly-once at the file grain.
  End offset and record count live in the Parquet footer metadata
  (`crosstick:start_offset` / `end_offset` / `record_count` /
  `format = bronze-v1`). Chunks are contiguous per topic-partition, so a
  sorted listing implies coverage.
- **Rows are corpus-shaped:** the columns are exactly the `CorpusRecord`
  fields below (`topic`, `partition`, `offset`, `timestamp_ms`, `key`,
  `value`, `headers`) — any bronze slice reads back as replay fodder.
- **Chunk cuts:** size-dominant (`FLUSH_BYTES`, 16 MiB), plus a UTC
  date-boundary cut (keeps `date=` partitions honest) and an age sweep
  (`FLUSH_INTERVAL_SEC` — the bronze-lag bound for cold topics, not the
  primary trigger). Compression is zstd.
- **Commit discipline:** offsets are committed only *after* the Parquet PUT,
  one chunk at a time — at most one PUT is ever un-committed, which is what
  makes the start-offset overwrite argument airtight. The consumer group
  (`materializer`) is the durable "how far has bronze got" cursor.
- Unmapped `(exchange, symbol)` pairs partition by the normalized native
  symbol (warn-once) — bronze never drops data over missing reference data.

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
preserved exactly; **cross-topic order is not** — a consumer that needs the
live snapshot-before-delta ordering must impose it (see the two-phase barrier
in `analytics/tests/test_gateway_integration.py`; making the gateway
order-insensitive is the D2 fix, Phase 3).
