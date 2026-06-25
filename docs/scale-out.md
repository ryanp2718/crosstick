# Scale-out tradeoffs

Decisions deferred while shipping the first exchange driver (Coinbase Advanced
Trade). Each is correct at current scale (a handful of symbols, one connection per
exchange) and has a documented trigger for revisiting. Recorded here so the
"why not" survives past the commit that made the call.

## Connection topology: one connection per exchange (not per symbol)

**Today.** Each ingester opens a single WS connection and subscribes all its
symbols on it. Simple, few sockets, one backoff/token-bucket to reason about.

**The cost.** Blast radius. Any fault that tears down the connection — a
sequence gap, a queue overflow, a staleness timeout, a transient close — resyncs
*every* symbol on that socket, not just the one that faulted. With BTC-USD and
ETH-USD sharing a connection, a gap on either forces both to re-snapshot.

**When to shard (one connection per symbol, or per small group).** When a single
connection carries enough symbols that resync churn from one bad symbol degrades
the others, or when a high-volume symbol (BTC, ETH) deserves fault isolation from
the long tail. Sharding trades socket count and more concurrent backoff state for
independent failure domains. Not worth it at two symbols; reconsider past ~10–20
per connection, or sooner for the marquee pairs.

## Resync granularity: whole-connection (not per-symbol)

This is downstream of Coinbase's wire contract, not an arbitrary choice.

Coinbase Advanced Trade stamps a **single per-connection `sequence_num`** on every
envelope across all channels and products (verified live: 500 msgs, 4 channels,
2 products, 328 product/channel switches, zero non-`+1` steps). A gap in that
counter means we may have missed data for *any* symbol on the socket — so the only
correct recovery is to resync the whole connection. The base ingester already does
exactly this (`ResyncRequired` aborts the connection; the connect-loop
re-bootstraps), so whole-connection resync is **correct and free** for Coinbase.

**Where it gets coarse: Binance and Kraken.** Those feeds sequence
**per-symbol** — Binance via `U`/`u` update IDs, Kraken via a per-book CRC32. A
gap there implicates exactly one book, so a connection-wide resync is heavier than
necessary: it throws away good books to recover one bad one. The base supports
per-symbol state (`SymbolContext`, `last_seq`, the STALE state) but the recovery
path currently aborts the whole connection regardless.

**Deferred:** per-symbol resync isolation — mark only the faulted symbol STALE and
re-snapshot just that book (REST for Binance, re-subscribe for Kraken) while the
rest keep streaming. Worth building when Binance/Kraken land *and* connections
carry enough symbols that whole-connection resync visibly drops good data. For
Coinbase alone it would be pure complexity with no correctness gain.

## Snapshot bandwidth: the ~5 MiB full-depth frame

Coinbase's `l2_data` snapshot is the **full** book, not top-N. The BTC-USD
snapshot measured **4.92 MiB** live — which is what forced `ws_max_size` to be
configurable (the old hardcoded 4 MiB cap closed the connection with `1009`;
Coinbase now passes 16 MiB).

Every resync re-downloads that full snapshot. So resync frequency is a real
bandwidth (and decode-CPU) cost, and it compounds with the two decisions above:
coarse whole-connection resync means one symbol's gap re-pulls *all* symbols'
snapshots on the socket. This is the concrete cost that makes per-symbol resync
isolation and connection sharding worth it once symbol counts grow — until then
the snapshot is large but infrequent.

## Liveness: data-staleness watchdog vs. ping/pong

WS ping/pong only proves the *socket* is alive; a feed can hold the socket open
(heartbeats, pong frames) while market *data* goes silent. The base now has an
optional `stale_timeout` watchdog that reconnects when no frame arrives within the
window. Coinbase sets ~15s, safe because the `heartbeats` channel emits ~1/s. It
defaults to `None` (disabled) so exchanges without a steady heartbeat aren't
falsely reconnected — set it per-driver only where a known heartbeat cadence makes
silence unambiguously a fault.

## Not generalized: connection-level sequence tracking

Coinbase's per-connection counter lives in the driver (`_expected_seq` +
a `_reset_contexts` override), **not** in the base class. A connection-scoped
counter only fits Coinbase; Binance and Kraken are per-symbol with entirely
different gap semantics. Lifting it into the abstract base now would be
generalizing from one example. Promote it only if a second exchange needs the same
shape (rule of three).

## Durable storage: single-node MinIO on the box (not separated storage/compute)

**Today.** Bronze/silver/gold live in single-node MinIO on one local volume
(`docker-compose.yml`), and batch compute (silver/gold) runs on the same box. All
lake access goes through `filesystem_from_env() -> S3FileSystem` with endpoint and
credentials from env (`common/lake.py`, `materializer/main.py`), so the code is
already storage-location-agnostic. A 30-day `lake` lifecycle bounds growth
(`data-contracts.md`).

**The cost.** The durable tier *is* the box: one disk, no tiering, no cross-node
durability, and remote-read economics don't apply — so the per-file read
amplification and whole-day Python materialization that are tolerable over
localhost MinIO get expensive over a network, and raw history is capped at what
one disk holds.

**When to separate (the endpoint).** Move durable bronze to remote/cloud
S3-compatible object storage (AWS S3 / Cloudflare R2 / Backblaze B2 / a larger
remote MinIO) with lifecycle tiering (hot→warm→cold→expire); the box becomes
compute + a hot cache. Because lake access is endpoint-agnostic, the cutover is an
**env change** (`S3_ENDPOINT` / keys / region / scheme), not a rewrite — the real
work is provider/cost selection, lifecycle config, and keeping reads
remote-friendly: compaction into larger objects, and relational aggregation pushed
to DuckDB over Parquet rather than materialized in Python. Trigger: raw-history
needs exceed the box's disk, or batch compute contends with live capture.

## Local stack startup: one memory hog, not a thundering herd

**Today.** The whole stack — 12 long-running containers plus two init jobs — runs
inside a single Docker Desktop / WSL2 utility VM. With the WSL2 backend every
container is a process in that one VM sharing one Linux kernel; the base images
(alpine/slim/node) are only each container's userland, so there is no
host-process-vs-container split — the VM's RAM is the entire budget. WSL2 by default
caps that VM at ~50 % of host RAM (~7.4 GiB on a 15 GB box). `depends_on`
health-gates the data-plane on `redpanda`/`minio`, and every app service carries a
healthcheck so `docker compose up -d --wait` blocks on a real ready signal and
`docker ps` shows which container is still coming up.

**The cost — measured.** A settled stack (`docker stats`) shows the budget is not
shared evenly — one container dominates. **Redpanda holds ~5.7 GiB, 77 % of the
7.4 GiB VM**, while every other container combined (MinIO, gateway, Grafana,
Prometheus, materializer, the four ingesters, the consoles, lake-exporter) totals
~0.9 GiB. The VM sits at ~80 MiB available with swap exhausted. The cause is
Redpanda's Seastar allocator, which reserves a large fraction of *detected* memory
unless given an explicit `--memory`; `--mode dev-container` relaxes the startup
minimum check but does not bound the reservation. So the VM is steadily near-full,
and that — not a transient spike — is why a new container start (an `up`, a recreate,
or the reboot herd below) finds no headroom and the Docker engine returns 500/502 on
*new-container start* (`_ping` 500, `containers/<id>/start` 502 "unexpected EOF")
while already-running containers keep serving (see the engine-flap memory).

**The reboot multiplier.** `depends_on` orders **`docker compose up` only**. On a
host reboot or a Docker Desktop restart the daemon restarts every
`restart: unless-stopped` container *simultaneously*, ignoring `depends_on`. With the
VM already near-full from Redpanda, that simultaneous cold-start is what tips it
over, and the Kafka clients also retry-storm `NotLeaderForPartition` until Redpanda
elects partition leaders — self-healing, but load piled on exactly when the box is
most starved.

**The levers, by leverage.**

1. **Cap Redpanda's memory (done).** `--memory=1536M` in the redpanda command bounds
   the Seastar reservation. The workload (a handful of single-partition topics) runs
   comfortably within it, and the cap frees ~4 GiB back to the VM — the single change
   that removes the steady-state starvation behind the flap. The official Redpanda
   dev quickstart uses `--memory 1G`; 1.5 GiB adds margin for continuous operation.
2. **Per-service `mem_limit` (compose) — optional backstop.** With the one hog
   bounded, the remaining containers are small (≤ ~350 MiB each) and a blanket limit
   adds little. Reach for `mem_limit` only on a service that `docker stats` shows
   growing unbounded, sized at ~1.5–2× observed RSS; too-low limits OOM-kill into a
   restart loop worse than the herd.
3. **WSL2 VM envelope (`%USERPROFILE%\.wslconfig`).** `[wsl2] memory=`/`swap=` set
   the VM's total budget, applied with `wsl --shutdown` (then restart Docker Desktop).
   It *redistributes* host RAM rather than adding it, so it cannot create headroom the
   host does not have. With Redpanda capped the VM has room and no `.wslconfig` change
   is required; it remains the knob if the container set grows.
4. **Healthchecks (done).** Don't change memory, but make stabilization observable
   and give `up --wait` a real gate.

**The durable fix** is the same cloud cutover as the storage section above:
off-box compute + managed object store removes the single-VM RAM ceiling, and a
real orchestrator (or provider-managed restarts) gives dependency-ordered,
resource-bounded startup that Docker Desktop's reboot path does not.
