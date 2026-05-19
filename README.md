# crypto-md

Polyglot (Python + Node.js) crypto market-data platform.

Ingests live L2 order books and trades from **Binance**, **Coinbase Advanced Trade**, and **Kraken v2** over WebSocket, normalizes them, computes cross-exchange analytics (spread, VWAP, latency), and republishes through a unified Node.js WebSocket gateway with a live dashboard.

## Quickstart

```bash
docker compose up --build
```

- Dashboard: <http://localhost:8080/dashboard/>
- Gateway WS: `ws://localhost:8080/ws`
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000>

## Architecture

See [plan](../.claude/plans/i-want-to-build-mellow-crayon.md) for the full design rationale.

```
Binance  WSS ─┐                                ┌─ Redis Streams ─┐
Coinbase WSS ─┼─► 3× Python ingesters ─────────┤  (book+trades)  ├─► Node gateway ─► clients
Kraken   WSS ─┘   uvloop, msgspec, SortedDict  │                 │   ws, ioredis    + dashboard
                       │                       └─ Redis Pub/Sub ─┘
                       ▼                          (BBO, spread, latency)
              Python analytics
              VWAP, spread, latency p50/p99
                       │
                       └─► Prometheus ◄── Grafana
```

## Development

Python:
```bash
cd python
uv sync
uv run pytest
```

Node:
```bash
cd node/gateway
pnpm install
pnpm test
pnpm dev
```

## Notes

- **uvloop** is Linux-only. On Windows hosts the ingesters fall back to default asyncio (still works). Containers always use uvloop.
- **Decimal** prices everywhere — never float. Crypto precision is too tight.
- **Order book reconstruction** validates checksums (Kraken) / sequence numbers (Binance, Coinbase); on mismatch the book is marked STALE and a full resync runs.
- **Backpressure**: bounded asyncio queues; on overflow we disconnect and resnapshot rather than drop deltas (dropping deltas corrupts the book permanently).
