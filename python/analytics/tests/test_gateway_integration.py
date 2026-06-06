"""Phase 0b — gateway-in-the-loop integration.

Replays the golden corpus's gateway *inputs* (md.book.* / md.trades.* /
md.status.*) into an ephemeral Redpanda, runs the real Node gateway against it,
and asserts the gateway's derived *outputs* (md.bbo.* per-exchange, md.nbbo.*
cross-venue) — closing the integration gap end-to-end across the language seam.

Why this shape:
  * The gateway derives BBO/NBBO from the book; it does NOT consume md.bbo. So
    the corpus's synthetic md.bbo records are excluded from the replay — we feed
    the gateway only what an ingester emits and check what the gateway computes.
  * Replay is TWO-PHASE against the live gateway: snapshots first, then deltas
    only once each stream's snapshot BBO has been emitted. Snapshots and deltas
    live on separate topics with no cross-topic order guarantee, and the gateway
    drops a delta that arrives before its snapshot — so a single-shot replay is
    non-deterministic. The barrier recreates the live snapshot-then-delta time
    ordering. (Making the gateway replay-safe regardless of order is the D2 fix
    in Phase 3.)
  * Per-exchange md.bbo is asserted as an exact ordered sequence (single topic,
    single partition → deterministic). This carries the PLANTED crossed book:
    the gateway's Book has no crossed-guard (the ingester already validated), so
    the crossed binance delta surfaces as a crossed BBO — proof it propagates.
  * NBBO carries Date.now()-based local_ts_ns/leg_age_ms (D1, fixed in Phase 3),
    so only the deterministic fields are asserted (px/sz/exchange/crossed/
    constituents), and only on the *latest* compacted value per canonical.
  * The binance "down" status is excluded from the replay: its only wire-visible
    effect is evicting a single-venue canonical's NBBO into silence (unobservable
    here, and covered by the gateway's own nbbo unit tests), and including it
    would make BTC-USDT's NBBO depend on cross-topic fetch ordering. Venue-down
    detection is a Phase 2 (data-quality) concern over the md.status log.

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
from pathlib import Path

import pytest

from analytics.corpus import CorpusRecord, read_corpus
from analytics.replay import replay_corpus
from analytics.tests.kafka_admin import create_single_partition_topics, seed_group_offsets
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
}
# Gateway publishes md.nbbo.<canonical_id> (node/gateway/src/messages.ts).
NBBO_BTC_USD = "md.nbbo.BTC-USD"
NBBO_BTC_USDT = "md.nbbo.BTC-USDT"


def _is_gateway_input(r: CorpusRecord) -> bool:
    """Records the gateway actually consumes, excluding the binance 'down'
    status (see module docstring)."""
    if r.topic.startswith(("md.book.", "md.trades.")):
        return True
    if r.topic.startswith("md.status."):
        return decode(r.value).state == "up"
    return False


def _is_snapshot_phase(r: CorpusRecord) -> bool:
    """Phase-1 records: snapshots seed the books, status sets venue health.
    Everything else (deltas, trades) is phase 2."""
    return r.topic.endswith(".snapshots") or r.topic.startswith("md.status.")


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _healthz_ok(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as resp:
            return resp.status == 200
    except Exception:
        return False


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
    env = {
        **os.environ,
        "KAFKA_BROKERS": bootstrap,
        "KAFKA_GROUP_ID": group_id,
        "WS_PORT": str(ws_port),
        "INSTRUMENTS_FILE": str(INSTRUMENTS_FILE),
        "DASHBOARD_DIR": str(DASHBOARD_DIR),
        # Disable the 5s liveness sweep so it can't evict venues mid-test.
        "NBBO_LIVENESS_TIMEOUT_MS": "600000",
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


def _bbo_counts_ready(by_topic: dict[str, list], n: int) -> bool:
    return all(len(by_topic.get(t, [])) >= n for t in EXPECTED_BBO)


def _outputs_complete(by_topic: dict[str, list]) -> bool:
    return bool(
        _bbo_counts_ready(by_topic, 3)
        and by_topic.get(NBBO_BTC_USD)
        and by_topic.get(NBBO_BTC_USDT)
    )


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


async def _await_healthz(proc, ws_port: int, log_path: Path) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 30.0
    while loop.time() < deadline and not _healthz_ok(ws_port):
        if proc.poll() is not None:
            break
        await asyncio.sleep(0.5)
    assert _healthz_ok(ws_port), f"gateway not healthy:\n{log_path.read_text(errors='replace')}"


@pytest.mark.asyncio
async def test_gateway_derives_bbo_and_nbbo_from_replay(
    brokers: str, gateway_built: Path, golden_corpus_path: Path, tmp_path: Path
) -> None:
    records = [r for r in read_corpus(golden_corpus_path) if _is_gateway_input(r)]
    input_topics = sorted({r.topic for r in records})
    output_topics = [*EXPECTED_BBO.keys(), NBBO_BTC_USD, NBBO_BTC_USDT]
    # Snapshots (+ status) replay first; deltas (+ trades) only after the gateway
    # has emitted each stream's snapshot BBO. The gateway drops a delta with no
    # snapshot yet, and snapshot/delta live on separate topics with no
    # cross-topic order guarantee — so we recreate the live time ordering.
    snapshots = [r for r in records if _is_snapshot_phase(r)]
    deltas = [r for r in records if not _is_snapshot_phase(r)]

    # Topics must exist before the gateway's regex subscription resolves, and
    # single-partition keeps per-topic order deterministic.
    await create_single_partition_topics(input_topics + output_topics)

    group_id = f"gateway-{uuid.uuid4().hex}"
    # Park the gateway's fromBeginning:false consumer at 0 so it reads every
    # record we replay after it starts (no startup skip race).
    await seed_group_offsets(group_id, input_topics, offset=0)

    ws_port = _free_port()
    log_path = tmp_path / "gateway.log"
    proc, log_fh = _start_gateway(brokers, group_id, ws_port, log_path)
    producer = await make_producer(client_id="phase0b-replay")
    out = await make_consumer(
        *output_topics, group_id=f"phase0b-out-{uuid.uuid4().hex}", auto_offset_reset="earliest"
    )
    by_topic: dict[str, list] = defaultdict(list)
    try:
        await _await_healthz(proc, ws_port, log_path)

        await replay_corpus(producer, snapshots)
        assert await _drain_until(out, by_topic, lambda bt: _bbo_counts_ready(bt, 1), 30.0), (
            f"snapshot BBOs never arrived:\n{log_path.read_text(errors='replace')}"
        )

        await replay_corpus(producer, deltas)
        assert await _drain_until(out, by_topic, _outputs_complete, 30.0), (
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

    gateway_log = log_path.read_text(errors="replace")

    # ── per-exchange md.bbo: exact ordered sequence (round-trips into BBO) ────
    for topic, expected in EXPECTED_BBO.items():
        decoded = [decode(m.value) for m in by_topic.get(topic, [])]
        assert all(isinstance(b, BBO) for b in decoded), f"{topic} did not round-trip into BBO"
        got = [(b.bid_px, b.bid_sz, b.ask_px, b.ask_sz) for b in decoded]
        assert got == expected, f"{topic} BBO mismatch\n got={got}\n exp={expected}\n{gateway_log}"

    # The planted crossed binance book surfaces as a crossed per-exchange BBO.
    bn_last = decode(by_topic[bbo_topic("binance", "BTCUSDT")][-1].value)
    assert bn_last.bid_px == "65040.00" and bn_last.ask_px == "65030.00"

    # ── md.nbbo: latest value per canonical, deterministic fields only ───────
    btc_usd = json.loads(by_topic[NBBO_BTC_USD][-1].value)
    assert btc_usd["best_bid"]["exchange"] == "coinbase"
    assert btc_usd["best_bid"]["px"] == "64995.00" and btc_usd["best_bid"]["sz"] == "0.5"
    assert btc_usd["best_ask"]["exchange"] == "kraken"
    assert btc_usd["best_ask"]["px"] == "65015.0" and btc_usd["best_ask"]["sz"] == "1.0"
    assert btc_usd["crossed"] is False
    assert btc_usd["constituents"] == ["coinbase", "kraken"]

    btc_usdt = json.loads(by_topic[NBBO_BTC_USDT][-1].value)
    assert btc_usdt["best_bid"]["exchange"] == "binance"
    assert btc_usdt["best_bid"]["px"] == "65040.00"
    assert btc_usdt["best_ask"]["px"] == "65030.00"
    assert btc_usdt["crossed"] is True  # planted crossed book → crossed NBBO
    assert btc_usdt["constituents"] == ["binance"]
