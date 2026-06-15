# crosstick

**Real-time, cross-exchange cryptocurrency market-data platform.** Reconstructs
full L2 order books from Coinbase, Binance, and Kraken, derives a live
cross-exchange NBBO, on a Kappa-shaped architecture with Redpanda as the single
durable log and source of truth.

> Status: the real-time half (ingest → transport → gateway → NBBO → dashboard) is
> built and validated against live exchange data. The analytics half (lake,
> warehouse, batch roll-ups) is on the roadmap below.

## Architecture

```
        ┌─ Coinbase ─┐  ┌─ Binance ─┐  ┌─ Kraken ─┐     WebSocket L2 + trades
        └─────┬──────┘  └─────┬─────┘  └────┬─────┘
              └───────────────┼─────────────┘
                              ▼
              Python ingesters  (asyncio · uvloop)
        L2 book reconstruction · CRC32 gap detection
        per-symbol sequence state machine · msgspec
                              ▼
              ┌──────────────────────────────┐
              │     Redpanda  (Kafka API)     │   source of truth — a replayable log
              │  md.book.*   md.trades.*      │
              │  md.bbo.*    md.nbbo.*        │
              │         md.status.*           │
              └───────────────┬──────────────┘
          ┌───────────────────┴────────────────────┐
          ▼                                          ▼
  Node / TS gateway (kafkajs · ws)        Stream materializer
  live BBO + spread                         → bronze Parquet on MinIO
  cross-exchange NBBO                        → DuckDB + dbt medallion (roadmap)
  per-client backpressure                   → PySpark daily roll-ups (roadmap)
  venue-health leg eviction                 → Grafana board #2 (roadmap)
  WS fan-out + snapshot-on-connect
          │
          ▼
  Browser dashboard (WS)   +   Prometheus / Grafana board #1 (ops)
```

## Why it's interesting

- **Kappa-shaped, replayable topics.** Redpanda is the single source of truth, and
  every derived stream is a pure function of the raw log: `md.bbo.*` derives from
  `md.book.*`, and `md.nbbo.*` is stamped and evicted in **stream time** (max
  event-time across consumed messages, never `Date.now()`), so a replay reproduces
  it byte-for-byte. On restart the gateway re-derives its books from the log —
  seeking each topic by class (latest snapshot, deltas from a bounded lookback,
  status from earliest) rather than re-warming from the live edge. The history of
  these two gaps (D1, D2) and how they were closed is in
  [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
- **Real L2 order-book reconstruction.** Bounded-depth books per venue
  (`SortedDict`), CRC32 checksum validation against the exchange, and
  sequence-gap detection with buffering + resync — driven by a
  `BOOTSTRAP → BUFFERING → LIVE → STALE` state machine per symbol.
- **Cross-exchange NBBO with strict bucketing.** Best bid/offer across venues per
  canonical instrument — and `BTC-USD` (Coinbase/Kraken) is *never* merged with
  `BTC-USDT` (Binance), because USDT≠USD is a real FX/credit basis, not noise.
- **Connection-state venue eviction.** A dead ingester used to leave a frozen leg
  that could win the NBBO and print a phantom crossed book. Venues now heartbeat
  liveness on `md.status.*`; the gateway evicts a dead venue's legs on explicit
  shutdown *or* missed-heartbeat timeout — connection state, not naive quote-age
  gating.
- **Backpressure that protects the hot path.** Per-client WebSocket send buffers
  are bounded; a slow consumer is dropped rather than allowed to stall fan-out
  for everyone.
- **An honest size ceiling.** An 8 MiB limit is enforced end-to-end (producer →
  broker → consumer); Coinbase's re-encoded L2 snapshot message runs ~1.1 MiB at
  current depth (the raw full-depth WS frame is larger, ~5 MiB), so oversize fails
  loudly instead of silently truncating.
- **Observability first-class.** Prometheus metrics on every component, with a
  provisioned Grafana ops dashboard.

## Status & roadmap

**Built and validated against live data**

- [x] Python ingesters — Coinbase, Binance, Kraken (L2 + trades, CRC, resync)
- [x] Redpanda transport + topic/schema contracts
- [x] Node/TS gateway — live BBO/spread, cross-exchange NBBO, backpressure,
      snapshot-on-connect
- [x] Venue-health liveness + NBBO leg eviction
- [x] Browser dashboard — staleness-aware greying + crossed-book warnings
- [x] Prometheus + Grafana ops dashboard
- [x] CI gate (GitHub Actions) — vitest, pytest, ruff, tsc on push/PR
- [x] Stream materializer — bronze Parquet lake on MinIO, exactly-once at the
      file grain
- [x] Cross-process integration harness — corpus replay through the real
      gateway over ephemeral Redpanda, incl. byte-identical NBBO determinism
- [x] Perp capture (Binance USDⓈ-M) — L2 book + aggTrade tape, liquidations,
      mark/funding, open-interest poll, on two routed WS connections; validated
      live end-to-end (book → NBBO `BTC-USDT-PERP` → bronze)

**Roadmap (the analytics half of the diagram)**

- [ ] DuckDB + dbt silver/gold marts (medallion over the event log — see
      `docs/DESIGN_analytics.md`; the schema is an event/tick store, not a star)
- [ ] PySpark daily long-horizon roll-ups
- [ ] Grafana board #2 (business metrics)
- [ ] Airflow orchestration

## Tech stack

| Layer | Tools |
|---|---|
| Ingest | Python (asyncio, uvloop), aiokafka, msgspec, sortedcontainers — managed by `uv` |
| Transport | Redpanda (Kafka API) |
| Gateway | Node 22 + TypeScript, kafkajs, `ws`, prom-client — tested with vitest |
| Observability | Prometheus, Grafana |
| Analytics (roadmap) | MinIO (S3), DuckDB, dbt, PySpark, Airflow |

## Repo layout

```
python/            ingesters + analytics (uv-managed)
  common/          msgspec models, kafka_io, metrics, backoff, ratelimit
  ingest/          base_ingester state machine + per-exchange drivers (+ tests)
  analytics/       analytics modules (+ tests)
node/gateway/      kafkajs → ws gateway: BBO, NBBO, backpressure (src/, test/)
dashboard/         static WS client, served by the gateway
ops/               instruments.yml, prometheus/ (config + alerts), grafana provisioning, smoke.py
docs/              design docs (ARCHITECTURE.md, DESIGN_nbbo.md, scale-out.md)
docker-compose.yml redpanda, prometheus, grafana, ingesters, materializer, gateway
```

## Quickstart

Prerequisites: Docker Desktop, [`uv`](https://docs.astral.sh/uv/) (Python),
`pnpm` (Node 22+). Commands below are PowerShell (Windows); translate `$env:X=...`
to `X=...` on a POSIX shell.

```powershell
# 1. Infrastructure
docker compose up -d redpanda prometheus grafana

# 2. Gateway (dev mode, on the host)
cd node/gateway; pnpm install
$env:KAFKA_BROKERS="localhost:19092"; pnpm exec tsx src/server.ts

# 3. An ingester (separate shell) — give each a distinct METRICS_PORT
cd python; uv sync
$env:EXCHANGE="kraken"; $env:SYMBOLS="BTC/USD"
$env:KAFKA_BROKERS="localhost:19092"; $env:METRICS_PORT="9103"
.venv\Scripts\python.exe -m ingest.main

# 4. Dashboard → http://localhost:8080/
```

Dev mode runs the gateway and ingesters on the host against Redpanda's external
listener (`localhost:19092`); ports per exchange are binance `9101`, coinbase
`9102`, kraken `9103`. Full container mode is via `docker compose`.

Prometheus/Grafana read their mounted config (`ops/prometheus/`, `ops/grafana/`)
only at container creation. After editing it, recreate rather than restart, then
smoke-check that it actually loaded:

```powershell
docker compose up -d --force-recreate prometheus grafana
python ops/smoke.py   # asserts rules loaded + dashboards provisioned
```

## Testing

```powershell
cd node/gateway; pnpm test                            # vitest — 50 tests
cd python; .venv\Scripts\python.exe -m pytest -q      # pytest — 120 tests
```

## Design docs

The *why* behind the architecture lives in [`docs/`](docs/):

- [`ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system-wide stock-take: the
  load-bearing decisions, the honest Kappa caveats, and the analytics-half spec
- [`DESIGN_nbbo.md`](docs/DESIGN_nbbo.md) — cross-exchange NBBO: strict quote
  bucketing, tie-breaks, per-leg staleness, and connection-state venue eviction
- [`scale-out.md`](docs/scale-out.md) — scaling the pipeline horizontally
