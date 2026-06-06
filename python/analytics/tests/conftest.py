"""Shared fixtures for analytics tests."""
from __future__ import annotations

from pathlib import Path

import pytest

from analytics.corpus import CorpusRecord, write_corpus
from analytics.tests.golden import build_golden_records


@pytest.fixture
def golden_records() -> list[CorpusRecord]:
    """The deterministic golden corpus, in memory."""
    return build_golden_records()


@pytest.fixture
def golden_corpus_path(tmp_path: Path) -> Path:
    """The golden corpus written to a temp `.jsonl.gz` file."""
    path = tmp_path / "golden.jsonl.gz"
    write_corpus(path, build_golden_records())
    return path
