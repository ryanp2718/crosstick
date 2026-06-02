# Architecture stock-take (2026-06)

A big-picture review taken after the real-time half (ingest → Redpanda → gateway
→ NBBO → dashboard) reached feature-complete and the analytics half is still
unbuilt. Records the load-bearing decisions and the gaps worth naming, so the
"why" survives and the next sessions (CI, data model, refactor) have a spec.

Companion docs: `DESIGN_nbbo.md` (NBBO semantics), `scale-out.md` (deferred
scaling calls).

## Honest framing: a strong streaming spine, a Kappa story to qualify

What's built is solid: real L2 reconstruction with a disciplined per-symbol state
machine, a stateless derivation gateway, cross-exchange NBBO with connection-state
venue eviction, and first-class observability. But the headline Kappa claim —
"every derived stream is a pure function of the raw log" — is **stronger than the
implementation delivers** in two specific places (D1, D2). Naming the gap is the
point of this doc; overclaiming it is the failure mode.

---

## D1 — NBBO is not yet a replayable function of the log

**Finding.** `md.bbo.*` is a pure function of `md.book.*`. `md.nbbo.*` is **not**,
because three wall-clock inputs leak into the computation:

- `nbbo.ts` — `local_ts_ns` and `leg_age_ms` are stamped from compute-time
  `Date.now()`.
- The decisive one — the liveness sweep (`server.ts`) evicts a venue when
  `Date.now() - lastSeen > LIVENESS_TIMEOUT_MS`, where `lastSeen` is the
  **consume** wall-clock time, **not** the heartbeat's `ts_ns`. The
  `Status.ts_ns` field exists but is ignored for timing.

On replay, every `up` heartbeat arrives in a burst, the 5-second real-time gap
never reproduces, the sweep never fires, and a crashed venue is never evicted —
reconstructing exactly the crossed-book phantom-arb bug the feature was built to
kill. Graceful `down` is replayable (a real log message); missed-heartbeat
eviction is not.

**Decision.** Make eviction replayable: drive the liveness timeout off **log-time
gaps between consecutive heartbeat `ts_ns`**, not consume-time `Date.now()`. The
heartbeats already carry the timestamp. Treat NBBO compute timestamps the same way
(derive from input message time, not wall clock) so a replay reproduces byte-for-
byte. *Refactor session.*

**Until then,** state it plainly in the README: `md.nbbo.*` carries wall-clock
eviction and is not bit-reproducible under replay.

## D2 — Restart re-warms from the live edge, not log replay

**Finding.** The gateway claims to "re-derive state from the log on restart." As
built it subscribes `fromBeginning: false` (`server.ts`); `Aggregator` /
`NBBOAggregator` start empty and fill only from messages produced *after* it
rejoins. It waits for the **ingester** to reconnect and re-emit a snapshot (the
"cold-start race"). True re-derivation would **seek to the last `.snapshots`
offset** per book topic and replay forward.

**Decision.** Honest framing now: "Kappa-shaped, replayable topics; replay-on-
restart re-warms from the live edge today, snapshot-offset seek is a roadmap
item." The seek-and-replay mechanism is a concrete future task, not a claim to
make in the present tense. *README now; mechanism is a tracked roadmap item.*

## D3 — NBBO numeric correctness: exact comparison in-process, exact arithmetic downstream

**Finding.** `nbbo.ts` runs winner selection, crossed-detection, `spread`, and
`mid` through `Number(...)` (float64). The live defect is crossed-detection:
`spread = ask − bid; if (spread < 0)` — float subtraction of two near-equal large
numbers yields a false negative epsilon that trips `nbboCrossed` and the dashboard
crossed-book warning. (The dedup tuple already uses exact strings; `book.ts`
already orders levels exactly via `cmpDecimal`.)

**The key realization: this is a *comparison* bug, not an *arithmetic* bug.** It is
fixed with the exact comparator we already have, no arithmetic and no new
dependency:

- Crossed-book: `cmpDecimal(bestAsk.px, bestBid.px) < 0` — no subtraction.
- Winner selection + size tie-break: `cmpDecimal`.
- Leg `px`/`sz`: pass the source strings through verbatim (stop `Number()`-ing
  them) so downstream inputs are lossless.

**Standard.** Numeric representation is layered by where the value lives, not one
type everywhere:

| Layer | Standard |
|---|---|
| Transport / wire | decimal **strings** (JSON) or **scaled integers** (binary: ITCH/SBE/FIX) |
| Compute, lowest latency | **scaled int64** (price × 10⁸) — matching engines, HFT |
| Compute, correctness first | **arbitrary-precision decimal** — `Decimal`/`BigDecimal`/`decimal`/`big.js` |
| Storage / warehouse | `DECIMAL(p,s)` — **never `DOUBLE`** |
| Last mile (charts, ratios) | float is fine |

Two near-universal rules: never float for price/money in compute or storage; float
only at the last mile.

**Decision — decimal at the endpoints; the gateway stays thin.**

- **In the gateway (live serving):** exact *comparison* (`cmpDecimal`) and exact
  string *pass-through* only. Emitted `spread` / `mid` are last-mile display
  conveniences — float is acceptable for the dashboard number.
- **No decimal-arithmetic library in the gateway, and no hand-rolled
  `subDecimal`/`halve`.** There is no live-path consumer that needs an exact
  *computed* (not compared, not passed-through) decimal value. Hand-rolling signed
  subtraction/division to fix a money-math bug is backwards, and the bug doesn't
  need it.
- **All exact decimal *arithmetic* is computed downstream in the warehouse** over
  `DECIMAL(18,8)` columns, where SQL gives exact decimal math for free: exact
  spread-as-a-fact, VWAP, notional, USD/USDT basis, spread distributions. This is
  the right Kappa seam — thin real-time serving, aggregation in the batch layer.
- **`decimal.ts` stays as-is.** `cmpDecimal` is the `book.ts` BTree comparator on
  the per-delta hot path; swapping it for `Big(a).cmp(Big(b))` would add two
  allocations per comparison there for zero correctness gain.
- **`big.js` is the boring-standard fallback**, pulled in *only* when a live-path
  feature first needs exact in-process arithmetic (e.g. a gateway-side VWAP or
  notional walk). That is the named-consumer trigger; nothing meets it today.
- **Warehouse:** all price/size/spread columns `DECIMAL(18,8)` (never `DOUBLE`).
  *Exact scale decided in the data-model session.*

**When to revisit.** The first live-serving feature that needs an exact computed
decimal value in the gateway — at which point add `big.js` for that path.

*Refactor session (in-gateway fixes); data-model session (warehouse scale +
downstream arithmetic).*

## D4 — One clock domain; keep the out-of-order window as a net

**Finding.** All-in-compose runs on one VM clock. **Dev mode** straddles two:
ingesters + gateway on the **host** clock, Redpanda/Prometheus/Grafana in the
**Docker-VM** clock (which drifts and resyncs non-monotonically). The
`out_of_order_time_window: 5m` patches the symptom at the Prometheus layer.
Data-plane latency values are safe (computed from `exchange_ts_ns` /
`local_recv_ts_ns`) **only while ingesters and gateway share one clock** — split
them across host/VM and NBBO `local_ts_ns` (host) mixes with book `local_ts_ns`
(VM), silently corrupting any cross-hop delta.

**Decision.** Adopt **all-in-compose as the canonical, reproducible topology**
(one clock, prod-shaped). Host mode is a dev convenience with a documented caveat:
its Prometheus timestamps are known-skewed. **Keep** the OOO window — it is a
legitimate, harmless setting for any jittery environment, not "the fix." *Audit:*
confirm no Grafana panel uses `timestamp()` / scrape-time arithmetic for latency;
the data-plane `ts_ns` deltas are the only trustworthy source.

## D5 — Schema governance: registry wired but unused

**Finding.** Compose exposes Redpanda's Schema Registry (`:18081`), but nothing
registers or validates schemas. The wire contract lives in two hand-kept copies —
`models.py` (msgspec) and `messages.ts` (TS interfaces) — guarded only by a
"mirror models.py" comment. The Python and Node order books already diverged once
(decimal ordering).

**Decision.** Add a **cross-language oracle test** — same delta stream → Python
`OrderBook` BBO must equal Node `book.ts` BBO at each step — as the lightweight
guard against contract drift. Using the registry for real is the heavier
alternative; revisit if a third language or an external consumer appears.
*Refactor / test session.*

## D6 — Identity rename: identifiers yes, topics no

**Finding.** `crypto-md` survives in `compose name:`, `crypto-md-gateway`,
`pyproject name`, and the repo folder, vs. the README's `crosstick`.

**Decision.** Rename the package / project / folder identifiers. **Leave the
`md.*` topic prefix** — `md` means "market data," not "crypto-md"; renaming it
touches live Redpanda data and buys nothing (no consumer benefit, real migration
cost). Scoping it this way keeps the rename a find-and-replace, not a data
migration. *Refactor session.*

## D7 — Documentation drift

**Finding & decision.**
- `kafka_io.py` cites `docs/data-contracts.md`, which does not exist (only
  `DESIGN_nbbo.md`, `scale-out.md`, this file). Write `data-contracts.md` (natural
  home for the topic/schema contract) or fix the reference.
- Coinbase snapshot size is quoted as **~1.1 MiB** (`kafka_io.py`, `server.ts`)
  and **~5 MiB** (`base_ingester.py`). These measure different things — re-encoded
  Kafka message at current depth vs. raw full-depth WS frame — say so.
- **Verified fine:** `coinbase.py` already sets `ws_max_size = 2**24` (16 MiB), so
  the full-depth WS frame is not rejected at the socket layer. No action.

*Quick pass, any session.*

---

## Already-tracked debt (named, not re-litigated)

In HANDOFF; listed so this doc is complete:

- **gzip-only** is hardcoded in `make_producer` but unenforced — assert
  `compression_type == 'gzip'`, and have the gateway error loudly on a foreign
  codec (today a snappy/zstd producer crashes it with `KafkaJSNotImplemented`).
- **WS per-client drop counter** missing — slow clients grow the socket buffer
  with no metric or eviction signal.
- **Single-writer derived streams** — two gateways = duplicate `md.bbo`/`md.nbbo`;
  needs leader election or symbol-hash sharding. Documented in `scale-out.md`.
- **kafkajs regex subscription** does not pick up topics created post-startup;
  needs a metadata-refresh resubscribe.
- **Integration-test harness** is the biggest test gap — every shipped bug was
  caught by smoke, not the unit suite.

## The analytics seam (spec for the data-model session)

Declared but empty: compose references `materializer.main` and provisions
`lake`/`silver`/`gold` MinIO buckets with `FLUSH_INTERVAL_SEC` / `FLUSH_BYTES`,
but `python/analytics/` is just package stubs and `materializer/` does not exist.
The idempotency spine is already right (`make_consumer` uses
`enable_auto_commit=False` — commit only after the Parquet PUT lands). Open
decisions for that session:

1. **Which topics feed the lake** — book *deltas* (full fidelity, hard to replay
   in SQL) vs. periodic depth *snapshots* (leaning snapshots) vs. trades vs. NBBO.
2. **Grain & partitioning** — per-tick vs. windowed; `exchange/symbol/date`
   Parquet layout.
3. **Star schema** — fact = trades / NBBO ticks; dims = instrument / venue / time;
   `DECIMAL` scale (D3) locked before writing rows.
4. **The questions, which drive the schema** — spread distributions, VWAP, venue
   uptime/latency, NBBO crossing frequency, cross-exchange basis (USDT vs USD).
   This is where D3's exact decimal arithmetic lives (SQL `DECIMAL`), so the
   gateway never needs it.
