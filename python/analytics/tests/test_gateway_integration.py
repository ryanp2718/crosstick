"""Gateway-in-the-loop integration + replay determinism (Phase 0b / Phase 3).

Replays the golden corpus's gateway *inputs* (md.book.* / md.trades.* /
md.status.*) into an ephemeral Redpanda, runs the real Node gateway against
it, and asserts the derived *outputs* (md.bbo.* per exchange, md.nbbo.* cross-
venue) — closing the integration gap end-to-end across the language seam.

Three tests:

  1. **In-order replay** → exact golden BBO sequences + NBBO end-state. Single
     shot: the aggregator buffers pre-snapshot deltas (D2a), so the pre-D2
     two-phase snapshot-then-delta barrier is gone.
  2. **Adversarial order** — every delta and trade replayed BEFORE any
     snapshot or status — converges to the same end state, with each stream's
     buffered deltas drained and coalesced into a single BBO. End-to-end proof
     of the D2a order-insensitivity through a real broker and gateway.
  3. **Byte-identical determinism** — the same paced replay run twice against
     fresh state must produce byte-identical md.bbo.* and md.nbbo.* streams.
     The Phase 3 acceptance test (DESIGN_analytics.md): any wall-clock leakage
     into NBBO timestamps, leg ages, or eviction (D1) differs across runs and
     fails this.

Harness mechanics worth knowing:

  * Replay is PACED: each record is produced only after the gateway's /metrics
    consumed-counter shows the previous one processed. The broker guarantees
    no cross-topic consumption order, so pacing is what pins the gateway's
    merge order to replay order — making the exact-sequence and byte-identity
    assertions deterministic rather than racy.
  * Replay starts only after the gateway logs its warm-start seek plan (D2b).
    Corpus timestamps are years old, so a record produced before the
    live-edge seek executes would be skipped by it.
  * md.* topic names are fixed by contract, so every gateway session deletes
    and recreates its topics — tests are independent of each other and of
    execution order within the shared Redpanda container.
  * The corpus's synthetic md.bbo records are excluded from replay: the
    gateway derives BBO from books, it does not consume md.bbo. The binance
    "down" status IS replayed (pre-D2a it was excluded as order-racy): binance
    holds the only BTC-USDT leg, so evicting it emits nothing and the
    compacted NBBO end-state stays the planted crossed book.

Requires Docker (testcontainers) AND node on PATH with the gateway's deps
installed (`pnpm -C node/gateway install`); skips otherwise. Run with:

    uv run python -m pytest -m integration
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import urllib.request
import uuid
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

import pytest

from analytics.corpus import CorpusRecord
from analytics.replay import replay_corpus
from analytics.tests.kafka_admin import (
    create_single_partition_topics,
    delete_topics,
    seed_group_offsets,
)
from common.kafka_io import bbo_topic, make_consumer, make_producer
from common.models import BBO, decode

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[3]
GATEWAY_DIR = REPO_ROOT / "node" / "gateway"
DIST_SERVER = GATEWAY_DIR / "dist" / "server.js"
INSTRUMENTS_FILE = REPO_ROOT / "ops" / "instruments.yml"
DASHBOARD_DIR = REPO_ROOT / "dashboard"

# Expected per-exchange md.bbo.* sequences (bid_px, bid_sz, ask_px, ask_sz),
# derived from the golden book streams (see analytics/tests/golden.py).
EXPECTED_BBO: dict[str, list[tuple[str, str, str, str]]] = {
    bbo_topic("coinbase", "BTC-USD"): [
        ("64990.00", "1.5", "65010.00", "1.2"),
        ("64990.00", "1.0", "65020.00", "0.8"),
        ("64995.00", "0.5", "65020.00", "0.8"),
    ],
    bbo_topic("kraken", "BTC/USD"): [
        ("64985.0", "3.0", "65015.0", "2.5"),
        ("64985.0", "3.0", "65015.0", "1.0"),
        ("64986.0", "1.0", "65015.0", "1.0"),
    ],
    bbo_topic("binance", "BTCUSDT"): [
        ("64970.00", "5.0", "65030.00", "4.0"),
        ("64970.00", "5.0", "65030.00", "3.0"),
        ("65040.00", "1.0", "65030.00", "3.0"),  # PLANTED crossed: bid >= ask
    ],
    bbo_topic("binance-futures", "BTCUSDT"): [
        ("64965.00", "8.0", "65035.00", "7.0"),
        ("64965.00", "6.5", "65035.00", "7.0"),
        ("64965.00", "6.5", "65040.00", "2.5"),
    ],
}
# Gateway publishes md.nbbo.<canonical_id> (node/gateway/src/messages.ts).
# The perp buckets separately from spot BTC-USDT despite sharing the native
# symbol BTCUSDT — that distinct-canonical routing is what's under test here.
NBBO_BTC_USD = "md.nbbo.BTC-USD"
NBBO_BTC_USDT = "md.nbbo.BTC-USDT"
NBBO_BTC_USDT_PERP = "md.nbbo.BTC-USDT-PERP"
OUTPUT_TOPICS = [*EXPECTED_BBO.keys(), NBBO_BTC_USD, NBBO_BTC_USDT, NBBO_BTC_USDT_PERP]

CompletePredicate = Callable[[dict[str, list]], bool]


def _is_gateway_input(r: CorpusRecord) -> bool:
    """Records the gateway actually consumes (everything but synthetic md.bbo)."""
    return r.topic.startswith(("md.book.", "md.trades.", "md.status."))


def _is_seed_phase(r: CorpusRecord) -> bool:
    """Snapshots seed the books, status sets venue health; everything else
    (deltas, trades) is the part the adversarial ordering front-loads."""
    return r.topic.endswith(".snapshots") or r.topic.startswith("md.status.")


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


@pytest.fixture(scope="module")
def redpanda():
    from testcontainers.kafka import RedpandaContainer

    with RedpandaContainer() as container:
        yield container


@pytest.fixture(scope="module")
def gateway_built() -> Path:
    """Build the gateway to dist/ once (dist is gitignored, can be stale).

    Invokes the installed tsc directly via node, bypassing `pnpm build` so
    pnpm's pre-run deps check can't try to reinstall node_modules in CI.
    """
    if shutil.which("node") is None:
        pytest.skip("gateway integration needs node on PATH")
    tsc = next(
        GATEWAY_DIR.glob("node_modules/.pnpm/typescript@*/node_modules/typescript/bin/tsc"), None
    )
    if tsc is None:
        pytest.skip("gateway deps not installed; run `pnpm -C node/gateway install`")
    subprocess.run([shutil.which("node"), str(tsc), "-p", "."], cwd=GATEWAY_DIR, check=True)
    assert DIST_SERVER.exists(), f"build did not produce {DIST_SERVER}"
    return DIST_SERVER


@pytest.fixture
def brokers(redpanda, monkeypatch: pytest.MonkeyPatch) -> str:
    bootstrap = redpanda.get_bootstrap_server()
    monkeypatch.setenv("KAFKA_BROKERS", bootstrap)
    return bootstrap


def _start_gateway(bootstrap: str, group_id: str, ws_port: int, log_path: Path):
    node = shutil.which("node")
    assert node is not None
    # No NBBO_LIVENESS_TIMEOUT_MS override: eviction is measured in log time
    # (D1), and the corpus spans ~20ms of stream time — far under the 5s
    # default — so wall-clock pacing delays can't evict anything spuriously.
    env = {
        **os.environ,
        "KAFKA_BROKERS": bootstrap,
        "KAFKA_GROUP_ID": group_id,
        "WS_PORT": str(ws_port),
        "INSTRUMENTS_FILE": str(INSTRUMENTS_FILE),
        "DASHBOARD_DIR": str(DASHBOARD_DIR),
    }
    log_fh = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [node, str(DIST_SERVER)],
        cwd=str(GATEWAY_DIR),
        env=env,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    return proc, log_fh


def _consumed_total(ws_port: int) -> int:
    """Sum gateway_messages_consumed_total across labels from /metrics."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{ws_port}/metrics", timeout=2) as resp:
            text = resp.read().decode()
    except Exception:
        return -1
    return sum(
        int(float(line.rsplit(" ", 1)[1]))
        for line in text.splitlines()
        if line.startswith("gateway_messages_consumed_total{")
    )


async def _await_log(proc, log_path: Path, needle: str, deadline_sec: float = 30.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_sec
    while loop.time() < deadline:
        if needle in log_path.read_text(errors="replace"):
            return
        assert proc.poll() is None, f"gateway exited:\n{log_path.read_text(errors='replace')}"
        await asyncio.sleep(0.2)
    raise AssertionError(f"{needle!r} never logged:\n{log_path.read_text(errors='replace')}")


async def _paced_replay(
    producer, records: list[CorpusRecord], ws_port: int, log_path: Path
) -> None:
    """Replay one record at a time, waiting until the gateway's consumed
    counter reflects it before producing the next (see module docstring)."""
    base = _consumed_total(ws_port)
    assert base >= 0, f"gateway /metrics unreachable:\n{log_path.read_text(errors='replace')}"
    loop = asyncio.get_running_loop()
    for i, r in enumerate(records, start=1):
        await replay_corpus(producer, [r])
        deadline = loop.time() + 10.0
        while _consumed_total(ws_port) < base + i:
            assert loop.time() < deadline, (
                f"gateway never consumed record {i}/{len(records)} ({r.topic}):\n"
                f"{log_path.read_text(errors='replace')}"
            )
            await asyncio.sleep(0.01)


async def _drain_until(consumer, by_topic, predicate, deadline_sec: float) -> bool:
    """Poll into `by_topic` until `predicate` holds and a subsequent poll is
    empty (so trailing fire-and-forget sends are drained), or until deadline."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_sec
    while loop.time() < deadline:
        batches = await consumer.getmany(timeout_ms=1000)
        for tp, msgs in batches.items():
            by_topic[tp.topic].extend(msgs)
        if predicate(by_topic) and not batches:
            return True
    return predicate(by_topic)


async def _replay_session(
    bootstrap: str,
    records: list[CorpusRecord],
    tmp_path: Path,
    name: str,
    complete: CompletePredicate,
) -> dict[str, list]:
    """Reset topics, start a fresh gateway, pace-replay `records`, and return
    the captured derived outputs per topic."""
    input_topics = sorted({r.topic for r in records})
    await delete_topics(input_topics + OUTPUT_TOPICS)
    # Topics must exist before the gateway's regex subscription resolves, and
    # single-partition keeps per-topic order deterministic.
    await create_single_partition_topics(input_topics + OUTPUT_TOPICS)

    group_id = f"gateway-{name}-{uuid.uuid4().hex}"
    # Park the gateway's fromBeginning:false consumer at 0 so it reads every
    # record we replay after it starts (no startup skip race).
    await seed_group_offsets(group_id, input_topics, offset=0)

    ws_port = _free_port()
    log_path = tmp_path / f"gateway-{name}.log"
    proc, log_fh = _start_gateway(bootstrap, group_id, ws_port, log_path)
    producer = await make_producer(client_id=f"replay-{name}")
    out = await make_consumer(
        *OUTPUT_TOPICS, group_id=f"out-{name}-{uuid.uuid4().hex}", auto_offset_reset="earliest"
    )
    by_topic: dict[str, list] = defaultdict(list)
    try:
        await _await_log(proc, log_path, "warm-start: planned")
        await _paced_replay(producer, records, ws_port, log_path)
        assert await _drain_until(out, by_topic, complete, 30.0), (
            f"derived outputs incomplete: { {k: len(v) for k, v in by_topic.items()} }\n"
            f"{log_path.read_text(errors='replace')}"
        )
    finally:
        await out.stop()
        await producer.stop()
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log_fh.close()
    return by_topic


def _bbo_counts_ready(by_topic: dict[str, list], n: int) -> bool:
    return all(len(by_topic.get(t, [])) >= n for t in EXPECTED_BBO)


def _nbbo_present(by_topic: dict[str, list]) -> bool:
    return bool(
        by_topic.get(NBBO_BTC_USD)
        and by_topic.get(NBBO_BTC_USDT)
        and by_topic.get(NBBO_BTC_USDT_PERP)
    )


def _outputs_complete(by_topic: dict[str, list]) -> bool:
    return _bbo_counts_ready(by_topic, 3) and _nbbo_present(by_topic)


def _assert_nbbo_end_state(by_topic: dict[str, list]) -> None:
    """Latest compacted value per canonical — deterministic fields only (the
    adversarial run reaches the same values at different stream-clock points,
    so timestamps/leg ages are asserted via the determinism test instead)."""
    btc_usd = json.loads(by_topic[NBBO_BTC_USD][-1].value)
    assert btc_usd["best_bid"]["exchange"] == "coinbase"
    assert btc_usd["best_bid"]["px"] == "64995.00" and btc_usd["best_bid"]["sz"] == "0.5"
    assert btc_usd["best_ask"]["exchange"] == "kraken"
    assert btc_usd["best_ask"]["px"] == "65015.0" and btc_usd["best_ask"]["sz"] == "1.0"
    assert btc_usd["crossed"] is False
    assert btc_usd["constituents"] == ["coinbase", "kraken"]

    # The planted crossed book → crossed NBBO. The binance "down" that follows
    # evicts the only BTC-USDT leg, which emits nothing — so the crossed record
    # is, correctly, the final compacted value.
    btc_usdt = json.loads(by_topic[NBBO_BTC_USDT][-1].value)
    assert btc_usdt["best_bid"]["exchange"] == "binance"
    assert btc_usdt["best_bid"]["px"] == "65040.00"
    assert btc_usdt["best_ask"]["px"] == "65030.00"
    assert btc_usdt["crossed"] is True
    assert btc_usdt["constituents"] == ["binance"]

    # The perp buckets apart from spot BTC-USDT (same native symbol BTCUSDT,
    # distinct canonical) and survives the spot-binance "down" eviction.
    perp = json.loads(by_topic[NBBO_BTC_USDT_PERP][-1].value)
    assert perp["best_bid"]["exchange"] == "binance-futures"
    assert perp["best_bid"]["px"] == "64965.00" and perp["best_bid"]["sz"] == "6.5"
    assert perp["best_ask"]["px"] == "65040.00" and perp["best_ask"]["sz"] == "2.5"
    assert perp["crossed"] is False
    assert perp["constituents"] == ["binance-futures"]


@pytest.mark.asyncio
async def test_gateway_derives_bbo_and_nbbo_from_replay(
    brokers: str, gateway_built: Path, golden_records: list[CorpusRecord], tmp_path: Path
) -> None:
    records = [r for r in golden_records if _is_gateway_input(r)]
    by_topic = await _replay_session(brokers, records, tmp_path, "inorder", _outputs_complete)

    # ── per-exchange md.bbo: exact ordered sequence (round-trips into BBO) ────
    for topic, expected in EXPECTED_BBO.items():
        decoded = [decode(m.value) for m in by_topic.get(topic, [])]
        assert all(isinstance(b, BBO) for b in decoded), f"{topic} did not round-trip into BBO"
        got = [(b.bid_px, b.bid_sz, b.ask_px, b.ask_sz) for b in decoded]
        assert got == expected, f"{topic} BBO mismatch\n got={got}\n exp={expected}"

    # The planted crossed binance book surfaces as a crossed per-exchange BBO.
    bn_last = decode(by_topic[bbo_topic("binance", "BTCUSDT")][-1].value)
    assert bn_last.bid_px == "65040.00" and bn_last.ask_px == "65030.00"

    _assert_nbbo_end_state(by_topic)


@pytest.mark.asyncio
async def test_gateway_converges_from_adversarial_replay_order(
    brokers: str, gateway_built: Path, golden_records: list[CorpusRecord], tmp_path: Path
) -> None:
    """D2a end-to-end: every delta and trade arrives before any snapshot or
    status. The gateway buffers the orphan deltas, drains them when each
    snapshot lands, and must converge to the same end state as in-order."""
    records = [r for r in golden_records if _is_gateway_input(r)]
    adversarial = [r for r in records if not _is_seed_phase(r)] + [
        r for r in records if _is_seed_phase(r)
    ]

    def complete(bt: dict[str, list]) -> bool:
        return _bbo_counts_ready(bt, 1) and _nbbo_present(bt)

    by_topic = await _replay_session(brokers, adversarial, tmp_path, "adversarial", complete)

    # Each stream's buffered deltas drain in one batch → exactly one coalesced
    # BBO per exchange, equal to the in-order run's FINAL BBO (same book, and
    # same ts fields: they come from the last delta that mutated the book).
    for topic, expected in EXPECTED_BBO.items():
        decoded = [decode(m.value) for m in by_topic.get(topic, [])]
        got = [(b.bid_px, b.bid_sz, b.ask_px, b.ask_sz) for b in decoded]
        assert got == [expected[-1]], f"{topic} did not coalesce to final state\n got={got}"

    _assert_nbbo_end_state(by_topic)


@pytest.mark.asyncio
async def test_replayed_nbbo_is_byte_identical_across_runs(
    brokers: str, gateway_built: Path, golden_records: list[CorpusRecord], tmp_path: Path
) -> None:
    """Phase 3 acceptance (DESIGN_analytics.md): the same replay run twice
    against fresh state produces byte-identical derived streams. NBBO
    timestamps, leg ages, and eviction all derive from the stream clock (D1) —
    any Date.now() leakage would differ between runs and fail here."""
    records = [r for r in golden_records if _is_gateway_input(r)]
    runs: list[dict[str, list[tuple[bytes, bytes]]]] = []
    for name in ("det1", "det2"):
        by_topic = await _replay_session(brokers, records, tmp_path, name, _outputs_complete)
        runs.append({t: [(m.key, m.value) for m in msgs] for t, msgs in by_topic.items()})

    first, second = runs
    assert set(first) == set(second)
    for topic in sorted(first):
        assert first[topic] == second[topic], f"{topic} differs between identical replays"
