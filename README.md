# crypto-md

A polyglot crypto market-data platform built as a portfolio piece for a backend + data engineering role. **Kappa architecture**: Redpanda topics are the source of truth, MinIO + Parquet projects the stream to a queryable lake, DuckDB + dbt models the warehouse, and a Node.js gateway fans the live firehose to WebSocket clients with per-client backpressure.

## Quickstart

```bash
docker compose up --build
```

- Dashboard:        <http://localhost:8080/dashboard/>
- Gateway WS:       `ws://localhost:8080/ws`
- Redpanda Console: <http://localhost:8090>
- MinIO Console:    <http://localhost:9001>  (user: `minio` / pw: `minio12345`)
- Prometheus:       <http://localhost:9090>
- Grafana:          <http://localhost:3000>

## Architecture

See the [full plan](../.claude/plans/i-want-to-build-mellow-crayon.md) for design rationale, including the defensibility of each choice from first principles.

```
Binance / Coinbase / Kraken WSS
              │
              ▼
   Python ingesters (asyncio, uvloop, msgspec, SortedDict L2 book)
              │ produce
              ▼
   ┌────────────────────────────────────┐
   │  REDPANDA topics = source of truth  │
   └──┬───────────────────────┬──────────┘
      │                       │
      ▼                       ▼
 Node gateway          Stream materializer (Python, aiokafka)
 (kafkajs, ws)              │
 - WS fanout                ▼
 - per-client backpressure  Parquet on MinIO (S3-compatible)
 - live BBO + spread          │
 - dashboard                  ▼
                       DuckDB + dbt (star schema, tests, lineage)
                              │
                       PySpark daily long-horizon aggregations
                              │
                       Grafana board #2 (business KPIs)
                              ▼
                       Airflow orchestrates dbt + Spark + freshness DAGs
```

## Development

Python (uv-managed):
```bash
cd python
uv sync
uv run pytest
```

Node gateway:
```bash
cd node/gateway
pnpm install
pnpm test
pnpm dev
```

dbt:
```bash
cd warehouse/dbt
dbt deps && dbt seed && dbt run && dbt test
```

## Design notes

- **Kappa, not Lambda.** One metric definition (in dbt SQL) is the source of truth. Live UI bypasses the warehouse for sub-second derived signals (computed in the Node gateway from the firehose); everything else queries the warehouse.
- **Redpanda, not Redis or Apache Kafka.** Kafka wire protocol, no Zookeeper/JVM. Production swap to Apache Kafka / MSK is a config line.
- **DuckDB + dbt, no Polars.** One transformation paradigm (SQL). Adding Polars would be a second tool that doesn't earn its keep.
- **PySpark for the daily long-horizon job only.** Working set grows with retention; API ports unchanged to EMR/Glue.
- **Decimal** prices everywhere — never float. 8+ decimal places, precision is non-negotiable.
- **Order book reconstruction** validates Kraken CRC32 / Binance U-u sequence / Coinbase sequence_num. On mismatch: STALE → snapshot resync. Never patch silently.
- **Backpressure**: bounded `asyncio.Queue`; on overflow we close the WS and resnapshot — dropping deltas corrupts the book permanently. Same logic in the Node gateway: `bufferedAmount > 1MiB` → `1008` disconnect.
- **uvloop** is Linux-only; the ingesters fall back to default asyncio on Windows hosts. Containers always use uvloop.
