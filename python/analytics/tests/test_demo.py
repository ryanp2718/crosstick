"""Unit tests for the offline-demo entrypoint's pure pieces (analytics/demo.py)."""
from __future__ import annotations

import pytest

from analytics.corpus import CorpusRecord
from analytics.demo import corpus_topics, parse_args, warmstart_planned


def _rec(topic: str) -> CorpusRecord:
    return CorpusRecord(
        topic=topic, partition=0, offset=0, timestamp_ms=0, key=None, value=b"{}", headers=[]
    )


def test_corpus_topics_distinct_and_sorted() -> None:
    records = [
        _rec("md.trades.kraken.BTC-USD"),
        _rec("md.book.binance.BTCUSDT.deltas"),
        _rec("md.book.binance.BTCUSDT.deltas"),
        _rec("md.book.binance.BTCUSDT.snapshots"),
    ]
    assert corpus_topics(records) == [
        "md.book.binance.BTCUSDT.deltas",
        "md.book.binance.BTCUSDT.snapshots",
        "md.trades.kraken.BTC-USD",
    ]


SCRAPE = """\
# HELP gateway_warmstart_planned 1 once the warm-start seek plan has been applied (0 during startup)
# TYPE gateway_warmstart_planned gauge
gateway_warmstart_planned {value}
gateway_messages_consumed_total{{topic="md.status.kraken",result="ok"}} 42
"""


def test_warmstart_planned_reads_the_gauge() -> None:
    assert warmstart_planned(SCRAPE.format(value="1")) is True
    assert warmstart_planned(SCRAPE.format(value="0")) is False


def test_warmstart_planned_requires_exact_metric_name() -> None:
    # A longer-named metric or a missing gauge must not read as planned.
    assert warmstart_planned("gateway_warmstart_planned_total 1\n") is False
    assert warmstart_planned("gateway_messages_consumed_total 7\n") is False
    assert warmstart_planned("") is False


def test_parse_args_subcommands() -> None:
    topics = parse_args(["create-topics", "c.jsonl.gz"])
    assert topics.cmd == "create-topics" and topics.corpus == "c.jsonl.gz"

    replay = parse_args(["replay", "c.jsonl.gz"])
    assert replay.cmd == "replay"
    assert replay.speed == 1.0
    assert replay.gateway_metrics == "http://gateway:8080/metrics"

    fast = parse_args(["replay", "--speed", "2.5", "c.jsonl.gz"])
    assert fast.speed == 2.5

    with pytest.raises(SystemExit):
        parse_args(["replay", "--speed", "0", "c.jsonl.gz"])
    with pytest.raises(SystemExit):
        parse_args([])
