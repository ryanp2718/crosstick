# Research thesis (v1) — crypto microstructure signals

The signal-side companion to `DESIGN_analytics.md`. That doc decides *how* we turn
the Redpanda log into a research surface (medallion lake, replay engine, feature
store); this doc argues *what* that surface is for — which crypto market frictions
are real, which strategies they bar versus enable, what signals follow, and the
one data-capture decision the answer forces.

> **Status: strawman for red-pen.** The verdicts below are first-pass positions,
> written to be argued with — not settled findings. The owner steers the quant
> conclusions; this exists to give that steering something concrete to push
> against. Where a call is genuinely open it is flagged in
> **§7 Open questions**, not buried.

---

## 1. The honest premise (what sets the bar for "viable")

What we are determines what "viable" can mean, and pretending otherwise is how
research projects lie to themselves:

- **Solo, no orders of our own.** We observe the market; we do not (yet) send
  flow. So "viable" means *researchable and measurable*, and at most *actionable
  at low frequency* — never "we run a desk." TCA is **market-quality** TCA
  (effective spread, price improvement, trade-through), not our-own-fill TCA
  (`DESIGN_analytics.md` → Out of scope).
- **Retail-latency data.** WebSocket feeds over the public internet, reconstructed
  in Python — tens of milliseconds behind the venue, not microseconds. Anything
  whose edge *is* latency is barred to us as a trade, though often still
  observable as a measurement.
- **Three spot venues, BTC/ETH-first.** Coinbase + Kraken (`BTC-USD`) and Binance
  (`BTC-USDT`), full L2 + trade tape + venue status. Strict per-quote bucketing
  means `USDT ≠ USD` is a first-class basis, not noise (`DESIGN_nbbo.md`).
- **Replication ethos.** The goal is an honest end-to-end replica of a
  production-grade market-data + research platform. Frame outputs as
  capabilities and problems-solved (price discovery, train/serve skew, PIT
  correctness), not as alpha claims. Don't overclaim.

The bar, then: a signal is **in-thesis** if it is (a) computable from data we
record losslessly, (b) PIT-correct under our replay engine, and (c) honestly
describable as *research a real crypto desk would run*, even where *acting* on it
is out of our reach.

---

## 2. Crypto market frictions (the structural facts)

These are the features of crypto markets that make the microstructure distinct
from equities/FX — each is simultaneously a hazard and a source of signal.

- **24/7, no close, no auction.** No opening/closing auction, no overnight gap,
  no circuit-breaker halts. → *Bars* any signal built on session boundaries or
  auction imbalance. → *Enables* continuous-time modeling and clean intraday
  seasonality (funding epochs, Asia/EU/US liquidity waves).
- **Fragmentation with no consolidated tape.** No Reg-NMS, no SIP, no official
  NBBO — every venue is an island and "the price" is a fiction you must
  construct. → *This is the platform's reason to exist*: our cross-venue
  strict-bucketed NBBO **is** the missing tape. → *Enables* price-discovery /
  lead-lag research (who moves first) and cross-venue dislocation statistics.
- **Quote-currency heterogeneity (USD vs USDT vs USDC).** The same base trades
  against fiat and against stablecoins whose peg is itself a credit/FX instrument.
  → *Bars* naïvely merging books across quote assets (a classic crypto data bug).
  → *Enables* the **stablecoin basis** as a native signal — derivable today from
  `BTC-USD` (Coinbase/Kraken) vs `BTC-USDT` (Binance).
- **Settlement & withdrawal latency.** Moving inventory between venues takes
  minutes-to-hours (on-chain confirmations, withdrawal queues, gas) and costs
  fees. → *This is the central friction* that lets cross-venue price gaps persist
  (Makarov & Schoar). → *Bars* us (and most participants) from arbing those gaps
  in real time; → *Enables* studying how wide/persistent gaps get as a function
  of that friction.
- **Perpetual futures + funding rate.** The dominant instrument by volume is the
  perp, kept near spot by a periodic **funding payment** between longs and shorts.
  → *Enables* the marquee crypto-native trade family (carry / funding basis) and a
  rich predictive signal — **but only if we capture it** (we do not today; §6).
- **Inflated / wash volume & venue risk.** Reported volume is unreliable; venues
  fail, freeze withdrawals, or vanish. → *Bars* trusting headline volume; →
  *Enables* (forces) a data-quality floor that scores venues — which we are
  building anyway (`DESIGN_analytics.md` Phase 2) — and makes **venue status** a
  first-class signal, not just ops telemetry.
- **Maker/taker fee & rebate tiers.** Economics are fee-dominated at short
  horizons. → *Bars* any "edge" that evaporates under realistic fees; every
  backtest must be fee-aware from the first run.

---

## 3. Viable vs barred (the strawman matrix)

Verdicts are deliberately blunt so they're easy to overturn.

| Strategy / study | Verdict | Honest why | What it needs |
|---|---|---|---|
| **Latency / cross-venue execution arbitrage** | **Barred to trade** | Edge *is* latency + pre-funded inventory on every venue; withdrawal latency is the whole friction. Our data is ms-late and we hold no inventory. | colocation, direct feeds, capital on N venues — none of which we have |
| **Passive market making for rebates** | **Barred to run** | Requires queue priority + low latency + maker programs. | execution infra we lack |
| **Cross-venue dislocation *measurement*** | **Viable (research)** | We can't capture the trade, but we *can* measure how often a fee-clearing gap exists and how long it survives — a genuine output. | cross-venue NBBO (have), fee model |
| **Price discovery / lead-lag** | **Viable (research + feature)** | Which venue leads (Binance typically); Hasbrouck information share over synchronized tape. | synchronized cross-venue tape (have), clock-domain care (`ARCH` D4) |
| **Stablecoin basis (USDT/USD)** | **Viable (research, maybe low-freq act)** | Directly from captured data; capacity-rich, lower-frequency, genuinely crypto-native. The strict bucketing makes it first-class. | `BTC-USD` vs `BTC-USDT` (have), basis mart (gold) |
| **Short-horizon return prediction** | **Viable (research + feature)** | OFI, queue dynamics, trade-sign autocorrelation → seconds-to-minutes directional features. Acting is low-freq and honestly framed. | full L2 deltas (have), PIT feature store (Phase 5) |
| **Carry / funding basis (perp − spot)** | **Conditional** | The marquee crypto trade and strongest predictive signal — **blocked purely by data**: we don't capture perps/funding. | perps + funding capture (§6 decision) |
| **Market-quality TCA** | **Viable (backbone)** | Not a strategy — the measurement layer (effective spread, trade-through). Already in the silver/gold plan. | enriched trades (Phase 4) |

The pattern: our differentiators are **the constructed consolidated tape** and
**full-fidelity L2 across venues**. The signals that exploit those — price
discovery, dislocation stats, stablecoin basis, order-flow prediction — are all
viable as research today. The single highest-value thing we *can't* do is the
carry trade, and that's a data decision, not a capability gap.

---

## 4. Signals → data → infra

Maps each viable line to the concrete signal, the data it needs (and whether we
have it), and where it lands in the build.

| Signal | Inputs | Have today? | Lands in |
|---|---|---|---|
| Cross-venue NBBO / dislocation | per-venue BBO, canonical bucketing | ✅ live + captured | gold mart (dislocation stats) |
| Price discovery / information share | synchronized trade + BBO tape, `local_recv_ts_ns` | ✅ | silver as-of joins → gold |
| Stablecoin basis (USDT/USD) | same-base different-quote NBBO, as-of joined | ✅ | gold basis mart |
| Order-flow imbalance (OFI) | L2 `book.*.deltas` event stream | ✅ (full fidelity) | silver event-grain → feature store |
| Realized/queue microstructure | event-grain book reconstruction (replay) | ✅ via replay (Phase 3) | feature store (Phase 5) |
| Market-quality TCA | trades as-of NBBO/BBO | ✅ | silver enriched trades |
| **Funding basis / carry** | **perp mark+index, funding rate, OI** | ❌ **not captured** | **new ingest + bronze (§6)** |
| **Liquidation pressure** | **perp liquidation stream** | ❌ | **new ingest (§6)** |

Everything above the rule is buildable on the data we already record. Everything
below it is gated on one decision.

---

## 5. The signal that is *already free* (anchor the v1 work here)

Before the big fork: the **stablecoin basis** deserves to be the first research
output, because it is the rare case where the platform's defining choice (strict
`USD ≠ USDT` bucketing) and the captured data line up to produce a real,
crypto-native signal at **zero additional ingestion cost**. `BTC-USD` (Coinbase,
Kraken) versus `BTC-USDT` (Binance), as-of joined on `local_recv_ts_ns`, *is* the
USDT/USD basis expressed through BTC. It tests the whole spine end-to-end (replay
→ silver as-of join → gold mart → PIT feature) on a signal we can defend as
genuinely interesting. Strawman: **make the basis mart the Phase 4 "hello world."**

---

## 6. The headline decision — spot-only vs. capture perps + funding

This is the fork the thesis exists to resolve, and the one place to spend your
red pen hardest.

**Strawman recommendation: capture perps + funding, sequenced as "Phase 1.5" —
right after the spot materializer proves the bronze pattern, before silver locks
its contract.**

Three reasons, strongest first:

1. **You cannot backfill what you didn't record.** This is already the stated
   rationale for the bronze layer (`DESIGN_analytics.md` → "capture full fidelity
   now, query later"). Applied to funding it is decisive: every day we run
   spot-only is a day of funding-rate, open-interest, and liquidation history
   that is **permanently lost**. Spot L2 we could re-record later; the funding
   time series we skip is gone forever. The asymmetry of regret points one way.
2. **It is the difference between a spot data lake and a credible crypto research
   platform.** Perps are the dominant instrument and funding is *the* crypto-native
   signal; the carry trade is the most capacity-rich, most defensible strategy
   family in the space. A crypto microstructure platform with no funding data is a
   curiosity. With it, the marquee trade (§3) moves from "barred — no data" to
   "researchable."
3. **It slots into the existing pattern cheaply.** Funding/mark/index/OI are just
   more `md.*` streams from venues we already connect to (Binance especially);
   bronze is topic-agnostic, so capture is additive and does not disturb the
   Phase 0/1 contract. The real cost is **ingestion breadth** (new connectors,
   new venues' perp APIs), tracked against `scale-out.md`, not analytics rework.

**The counter-position (your red-pen target).** Stay spot-only through v1 if the
research focus is the cross-venue / stablecoin-basis microstructure for its own
sake, and you'd rather ship the medallion + replay + feature store end-to-end on a
*frozen* spot data domain before widening. This is the cleaner build; it just
accepts the permanent loss of the funding history accumulated in the meantime.

**A middle path worth naming:** capture perps + funding to **bronze now**
(cheap, append-only, stops the clock on lost history) but defer all *silver/gold
funding modeling* until after the spot spine is proven. This buys the
irreversible thing (the data) without paying the modeling cost early. Strawman
leans here if "Phase 1.5 full" feels too eager.

---

## 7. Open questions (the red-pen list)

Decisions left explicitly to the owner — answering these turns this strawman into
a thesis:

1. **The §6 fork:** spot-only / Phase-1.5 full / bronze-now-model-later? *(the big
   one)*
2. **Acting vs. observing:** is the end-state "research surface only," or do we
   eventually wire a low-frequency execution path? It changes how hard PIT and
   fee-modeling discipline must bite.
3. **Strategy focus for v1:** rank stablecoin basis / price-discovery /
   OFI-prediction — which one gets the first full silver→gold→feature treatment?
4. **Venue breadth for perps (if §6 = yes):** Binance only, or add a perp-native
   venue (Bybit/OKX)? Drives the ingestion work.
5. **Horizon honesty:** do we commit to backtests being fee- and latency-aware
   from the first run (recommended), accepting it kills some apparent edges early?

---

## 8. Tiered reading list

Starting points, not gospel — verify editions/links before relying on any of
them (house ethos: source the correct, current reference).

**Tier 1 — microstructure foundations**
- Harris, *Trading and Exchanges: Market Microstructure for Practitioners*
  (2003) — mechanics, order types, who trades and why. The grounding.
- O'Hara, *Market Microstructure Theory* (1995) — the theory canon (adverse
  selection, inventory, information).
- Hasbrouck, *Empirical Market Microstructure* (2007) — estimating from data;
  price discovery and the **information share** (the tool for our lead-lag work).
- Bouchaud, Bonart, Donier & Gould, *Trades, Quotes and Prices* (2018) — modern,
  limit-order-book-centric, market impact. Closest to our L2 data.

**Tier 2 — order-book dynamics, flow & ML**
- Cont, Stoikov & Talreja, "A Stochastic Model for Order Book Dynamics" (2010).
- Cont, Kukanov & Stoikov, "The Price Impact of Order Book Events" (2014) —
  **order-flow imbalance**, computable directly from our deltas.
- Avellaneda & Stoikov, "High-frequency trading in a limit order book" (2008) —
  the MM inventory/quoting model (to understand spread formation, not to run).
- Easley, López de Prado & O'Hara, "Flow Toxicity and Liquidity in a
  High-Frequency World" (2012) — **VPIN**.
- López de Prado, *Advances in Financial Machine Learning* (2018) — labeling,
  sample weighting, and **leakage / PIT discipline** — directly our feature-store
  concern.

**Tier 3 — crypto-specific**
- Makarov & Schoar, "Trading and arbitrage in cryptocurrency markets"
  (J. Financial Economics, 2020) — **the** paper on crypto cross-exchange price
  gaps and the frictions that sustain them. Anchor for §2–§3.
- Perp funding mechanics: read the **exchange methodology docs first** (Binance /
  Bybit / OKX funding-rate + mark-price specs) — they are the authoritative spec;
  any secondary paper is downstream of them.
- Crypto-derivatives / funding-basis academic work (e.g., Alexander and
  co-authors on crypto futures) — identify and verify the specific papers before
  citing.

---

## 9. Cross-references

- `DESIGN_analytics.md` — the build that serves these signals (medallion, replay
  keystone, feature store, the bronze "can't backfill" rationale §6 leans on).
- `DESIGN_nbbo.md` — strict per-quote bucketing; why USDT/USD basis is first-class.
- `ARCHITECTURE.md` — D4 (clock domains, gates cross-venue lead-lag), D5
  (reconstruction oracle), and the analytics seam.
- `scale-out.md` — where the perp-capture ingestion cost (§6) is actually tracked.
