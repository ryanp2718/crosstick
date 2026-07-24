"""Golden-corpus record format: a faithful, replayable capture of Kafka records.

A corpus is a gzipped JSON-lines file (`.jsonl.gz`), one `CorpusRecord` per line.
It preserves everything needed to replay a slice of the log deterministically -
topic, partition, offset, record timestamp, key, value, and headers (including
the latency-tracking `local_recv_ts_ns` / `exchange_ts_ns`). Bytes fields are
base64-encoded by msgspec's JSON codec, matching the wire convention elsewhere
(`common/models.py`: strings/bytes on the wire, decode at the boundary).

This module is deliberately transport-agnostic (no aiokafka import): `capture`
maps `ConsumerRecord`s into `CorpusRecord`s and `replay` maps them back to
producer sends. Keeping it pure makes the format unit-testable without Docker.
"""
from __future__ import annotations

import gzip
from collections.abc import Iterable, Iterator
from pathlib import Path
from types import TracebackType

import msgspec


class CorpusRecord(msgspec.Struct, frozen=True):
    """One captured Kafka record, with enough provenance to replay it.

    `offset` is provenance from the source topic; on replay into a fresh
    single-partition topic the broker assigns new (deterministic 0..N) offsets.
    """

    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]]


_ENCODER = msgspec.json.Encoder()
_DECODER = msgspec.json.Decoder(CorpusRecord)


class CorpusWriter:
    """Streaming writer - open once, write as records arrive, close.

    Used by `capture`, which appends over a live window. One gzip member,
    one JSON object per line.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.count = 0
        self._fh: gzip.GzipFile | None = None

    def __enter__(self) -> CorpusWriter:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = gzip.open(self.path, "wb")
        return self

    def write(self, record: CorpusRecord) -> None:
        if self._fh is None:
            raise RuntimeError("CorpusWriter used outside its context manager")
        self._fh.write(_ENCODER.encode(record))
        self._fh.write(b"\n")
        self.count += 1

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def write_corpus(path: str | Path, records: Iterable[CorpusRecord]) -> int:
    """Write all records to a corpus file; return the count written."""
    with CorpusWriter(path) as w:
        for r in records:
            w.write(r)
    return w.count


def read_corpus(path: str | Path) -> Iterator[CorpusRecord]:
    """Stream records back from a corpus file, in write order."""
    with gzip.open(path, "rb") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped:
                yield _DECODER.decode(stripped)
