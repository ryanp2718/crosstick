"""Per-check data-quality budgets: what an acceptable day is allowed to contain.

`gold.scorecard` measures, this module decides. A budget is a small YAML file
(``ops/dq_budgets.yml``) of limits resolved against **one scorecard row at a time** -
`(exchange, canonical_symbol, date, check)` - so a single bad venue-symbol cannot be
averaged away by sixty clean ones, which is what the flat violation sum it replaces
allowed.

Limits are expressed over columns the scorecard already writes, so budgeting needed
no new measurement:

  ``max_violations``  absolute count       right for `venue_uptime`, `clock_monotonic`
  ``max_rate``        violations/records   right for `sequence_gap`, where a busier
                                           venue makes more gaps at the same health
  ``min_records``     denominator floor    catches a venue-symbol that nearly stopped
  ``max_p99_ms``      latency tail         the only limit `latency.*` can carry, its
                                           `n_violations` being 0 by construction

Each limit is an independent tripwire: a row breaches if it exceeds any of them.

Resolution, least to most specific::

    default            only when the file names neither the check nor its family
    checks[family]     `latency` covers every `latency.<dataset>` check
    checks[check]
    ...exchanges[ex]
    ...exchanges[ex].symbols[sym]

Fields merge, so a per-exchange entry overrides only what it sets, and at equal scope
the exact check name beats the family. `default` is deliberately not a floor under the
named checks: a check the file describes is described completely, so putting a rate
limit on `sequence_gap` cannot silently leave the fail-closed `max_violations: 0`
underneath it.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any

import yaml

from gold.scorecard import CHECKS, LATENCY_PREFIX

# Family key for `latency.<dataset>`. Those check names are open-ended - one per silver
# dataset - so a file that had to name each would rot on the next dataset added.
LATENCY_FAMILY = LATENCY_PREFIX.rstrip(".")


class BudgetError(ValueError):
    """A malformed budget file. Raised at load time, never during evaluation."""


@dataclass(frozen=True)
class Limit:
    """The four bounds a scorecard row can be held to. `None` means unbounded."""

    max_violations: int | None = None
    max_rate: float | None = None
    min_records: int | None = None
    max_p99_ms: float | None = None

    def under(self, other: Limit) -> Limit:
        """This limit with `other`'s set fields laid over it (field-wise merge)."""
        return replace(self, **{f: v for f, v in other._set().items()})

    def _set(self) -> dict[str, Any]:
        return {f: v for f in FIELDS if (v := getattr(self, f)) is not None}


FIELDS: tuple[str, ...] = tuple(f.name for f in fields(Limit))

# What an unlisted check is held to. Fail-closed, so a check added to the scorecard
# arrives loud rather than silently unbudgeted.
FAIL_CLOSED = Limit(max_violations=0)

# Limit name -> the scorecard quantity it bounds, for readable breach messages.
_MEASURED = {
    "max_violations": "n_violations",
    "max_rate": "violation rate",
    "min_records": "n_records",
    "max_p99_ms": "p99_ms",
}


@dataclass(frozen=True)
class Breach:
    """One row against one limit it failed."""

    exchange: str
    canonical_symbol: str | None
    date: str
    check: str
    limit_name: str
    value: float
    bound: float

    def __str__(self) -> str:
        op = "<" if self.limit_name == "min_records" else ">"
        return (
            f"{self.check} {self.exchange}/{self.canonical_symbol or '-'} {self.date}: "
            f"{_MEASURED[self.limit_name]} {self.value:g} {op} "
            f"{self.limit_name} {self.bound:g}"
        )


@dataclass(frozen=True)
class _Scope:
    """One exchange's entry: its own fields plus per-symbol overrides."""

    limit: Limit = Limit()
    symbols: Mapping[str, Limit] = field(default_factory=dict)


@dataclass(frozen=True)
class _Node:
    """One check's (or family's) entry: its own fields plus per-exchange scopes."""

    limit: Limit = Limit()
    exchanges: Mapping[str, _Scope] = field(default_factory=dict)


@dataclass(frozen=True)
class Budget:
    default: Limit = FAIL_CLOSED
    checks: Mapping[str, _Node] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: str | Path) -> Budget:
        doc = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        _reject_unknown(doc, {"default", "checks"}, "top level")
        default = FAIL_CLOSED.under(_limit(doc.get("default") or {}, "default"))
        checks = {}
        for name, node in (doc.get("checks") or {}).items():
            _known_check(name)
            checks[name] = _node(node or {}, f"checks.{name}")
        return cls(default, checks)

    def limit_for(self, check: str, exchange: str, symbol: str | None = None) -> Limit:
        """The merged limit one scorecard row is held to."""
        nodes = [n for n in (self.checks.get(k) for k in _keys(check)) if n is not None]
        out = Limit() if nodes else self.default
        for tier in _ladder(nodes, exchange, symbol):
            out = out.under(tier)
        return out

    def breaches(self, rows: Iterable[Mapping[str, Any]]) -> list[Breach]:
        """Every limit every row failed, in row order."""
        return [breach for row in rows for breach in self.row_breaches(row)]

    def row_breaches(self, row: Mapping[str, Any]) -> list[Breach]:
        limit = self.limit_for(row["check"], row["exchange"], row["canonical_symbol"])
        records = row["n_records"] or 0
        violations = row["n_violations"] or 0
        p99 = row["p99_ms"]
        out: list[Breach] = []

        def flag(name: str, value: float, bound: float) -> None:
            out.append(
                Breach(
                    row["exchange"],
                    row["canonical_symbol"],
                    row["date"],
                    row["check"],
                    name,
                    value,
                    bound,
                )
            )

        if limit.max_violations is not None and violations > limit.max_violations:
            flag("max_violations", violations, limit.max_violations)
        # A rate needs a denominator; an empty partition is min_records' business.
        if limit.max_rate is not None and records > 0 and violations / records > limit.max_rate:
            flag("max_rate", violations / records, limit.max_rate)
        if limit.min_records is not None and records < limit.min_records:
            flag("min_records", records, limit.min_records)
        if limit.max_p99_ms is not None and p99 is not None and p99 > limit.max_p99_ms:
            flag("max_p99_ms", p99, limit.max_p99_ms)
        return out


def _keys(check: str) -> tuple[str, ...]:
    """Config keys that describe this check, least specific first."""
    if check.startswith(LATENCY_PREFIX):
        return (LATENCY_FAMILY, check)
    return (check,)


def _ladder(nodes: list[_Node], exchange: str, symbol: str | None) -> Iterator[Limit]:
    """Every limit that applies, least to most specific. Scope dominates: a family's
    per-exchange entry outranks an exact check's global one."""
    for node in nodes:
        yield node.limit
    scopes = [scope for node in nodes if (scope := node.exchanges.get(exchange)) is not None]
    for scope in scopes:
        yield scope.limit
    if symbol is not None:
        for scope in scopes:
            if symbol in scope.symbols:
                yield scope.symbols[symbol]


def _known_check(name: str) -> None:
    if name in CHECKS or name == LATENCY_FAMILY or name.startswith(LATENCY_PREFIX):
        return
    known = ", ".join([*CHECKS, LATENCY_FAMILY, f"{LATENCY_PREFIX}<dataset>"])
    raise BudgetError(f"unknown check {name!r} under `checks:` (known: {known})")


def _reject_unknown(node: Mapping[str, Any], allowed: set[str], where: str) -> None:
    if not isinstance(node, Mapping):
        raise BudgetError(f"{where} must be a mapping, got {type(node).__name__}")
    unknown = sorted(set(node) - allowed)
    if unknown:
        raise BudgetError(f"unknown key(s) {unknown} at {where} (allowed: {sorted(allowed)})")


def _limit(node: Mapping[str, Any], where: str) -> Limit:
    values: dict[str, Any] = {}
    for name in FIELDS:
        if name not in node:
            continue
        value = node[name]
        if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
            raise BudgetError(f"{where}.{name} must be a non-negative number, got {value!r}")
        values[name] = value
    return Limit(**values)


def _node(node: Mapping[str, Any], where: str) -> _Node:
    _reject_unknown(node, {*FIELDS, "exchanges"}, where)
    scopes = {}
    for exchange, scope in (node.get("exchanges") or {}).items():
        scopes[exchange] = _scope(scope or {}, f"{where}.exchanges.{exchange}")
    return _Node(_limit(node, where), scopes)


def _scope(node: Mapping[str, Any], where: str) -> _Scope:
    _reject_unknown(node, {*FIELDS, "symbols"}, where)
    symbols = {}
    for symbol, limit in (node.get("symbols") or {}).items():
        _reject_unknown(limit or {}, set(FIELDS), f"{where}.symbols.{symbol}")
        symbols[symbol] = _limit(limit or {}, f"{where}.symbols.{symbol}")
    return _Scope(_limit(node, where), symbols)
