"""Declared schema for the feature matrix: what every column is, and why.

`research.features` names its columns with f-strings over venue prefixes discovered at
runtime, crossed with the module-level window constants, so there has never been a static
list to write down. Every consumer recovered the structure by parsing names instead, and
each one parsed differently: `venue_prefixes` found venues by stripping `_ofi`,
`feature_columns` decided what was a model input by excluding a tuple of suffixes,
`clean` split book clocks from tape clocks by a set membership test, and `main` unioned
dates with `how="diagonal"`, which null-fills whatever fails to line up without saying so.

Exclusion is the wrong default for a feature set. A family whose names happen to miss the
drop list silently becomes a model input; one whose names happen to hit it silently
vanishes. Both are invisible from the outside, and the second is the more expensive: it
reports a smaller feature set with no error, and the run still prints a plausible number.
So here every column is declared with a role, and the model asks for the `feature` role
rather than for "not one of these suffixes".

The declaration is a grammar rather than a list, because the list is a product: families
of columns, crossed with the venues present on a date, crossed with the window constants.
`expected_columns` evaluates that product for a given `{venue: streams it published}`,
and `FeatureSchema.from_columns` runs it backwards, recovering the mapping from a built
frame and refusing any column it cannot place.

Running it backwards is what makes this safe without threading structure out of the
builder and through the Parquet cache. Discovery strips a whole declared suffix off a
column, so `binance_futures_ofi` yields `binance_futures` and never `binance` - the
opposite direction to the old `leg_of` prefix match, which is where that trap lived. The
recovered venue set is then expanded back through the same grammar and the expansion is
required to be collision-free, so a venue name that could alias another venue's column
fails loudly instead of quietly stealing its features.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Literal

from research.features import (
    DEPTH_RUNGS,
    FLOW_WINDOWS,
    HORIZONS,
    RETURN_WINDOWS,
)

# What a column is for. `feature` is the only role the model fits on.
#   level       knowable at `t` but non-stationary, so deliberately not an input: a tree
#               would memorise the price of BTC on the training dates.
#   diagnostic  knowable at `t` and used to decide which rows to fit on, never fitted on.
#   target      the only forward-looking columns in the frame, and the only `y_*` ones.
Role = Literal["key", "feature", "target", "level", "diagnostic"]

# The consolidated book is prefixed like a venue but is not one: it has no order flow of
# its own, and cross-venue runs exclude it wholesale because it is built from the very
# venue being predicted.
NBBO = "nbbo"


@dataclass(frozen=True)
class Template:
    """One column, or one column per window when `windows` is set."""

    suffix: str
    role: Role
    windows: tuple[int, ...] = ()

    def suffixes(self) -> tuple[str, ...]:
        return tuple(self.suffix.format(w=w) for w in self.windows) or (self.suffix,)


@dataclass(frozen=True)
class Family:
    """A group of columns a venue gets all of, or none of.

    `stream` is the silver dataset the venue must have published that date. That is the
    only reason a column is ever absent: a spot venue publishes no perp tape, and a venue
    that was down all date publishes nothing. Both are normal, and both are reported
    rather than null-filled in silence.
    """

    name: str
    stream: str
    templates: tuple[Template, ...]
    # Formatted over `{venue}`; a fixed prefix means the family names no venue.
    prefix: str = "{venue}"

    def columns(self, venue: str | None = None) -> tuple[Column, ...]:
        head = self.prefix.format(venue=venue) if venue is not None else self.prefix
        return tuple(
            Column(f"{head}_{suffix}" if head else suffix, t.role, self.name, venue)
            for t in self.templates
            for suffix in t.suffixes()
        )


@dataclass(frozen=True)
class Column:
    name: str
    role: Role
    family: str
    venue: str | None


# ── the grammar ──────────────────────────────────────────────────────────────
# Suffixes here must match `research.features` exactly. The tests build a matrix and
# compare it against this declaration, so a drift in either direction fails.

GRID = Family(
    "grid",
    stream="",
    prefix="",
    templates=(
        Template("ts_ns", "key"),
        # Stamped by `main.cached_features`, not the builder: a builder handles one date
        # and never needs to say which.
        Template("date", "key"),
    ),
)

BOOK = Family(
    "book",
    stream="quotes",
    templates=(
        Template("mid", "level"),
        Template("bid", "level"),
        Template("ask", "level"),
        Template("bid_sz", "feature"),
        Template("ask_sz", "feature"),
        Template("spread_bps", "feature"),
        Template("imbalance", "feature"),
        Template("micro_dev", "feature"),
        Template("depth_imb_{w}", "feature", DEPTH_RUNGS),
        Template("bid_span_bps", "feature"),
        Template("ask_span_bps", "feature"),
        Template(f"depth_{DEPTH_RUNGS[-1]}", "feature"),
        Template("age_ms", "diagnostic"),
    ),
)

OFI = Family(
    "ofi",
    stream="quotes",
    templates=(
        Template("ofi", "feature"),
        Template("ofi_{w}", "feature", FLOW_WINDOWS),
    ),
)

RETURNS = Family(
    "returns",
    stream="quotes",
    templates=(Template("ret_bps_{w}", "feature", RETURN_WINDOWS),),
)

FLOW = Family(
    "flow",
    stream="trades",
    templates=(
        Template("signed_vol", "feature"),
        Template("volume", "feature"),
        Template("n_trades", "feature"),
        Template("vwap", "level"),
        Template("signed_vol_{w}", "feature", FLOW_WINDOWS),
        Template("volume_{w}", "feature", FLOW_WINDOWS),
    ),
)

BASIS = Family(
    "basis",
    stream="mark_price",
    templates=(
        Template("basis_bps", "feature"),
        Template("funding_rate", "feature"),
        Template("funding_in_s", "feature"),
        Template("basis_chg_{w}", "feature", FLOW_WINDOWS),
        Template("mark_age_ms", "diagnostic"),
    ),
)

OPEN_INTEREST = Family(
    "open_interest",
    stream="open_interest",
    templates=(
        Template("oi_chg_bps_{w}", "feature", FLOW_WINDOWS),
        Template("oi_age_ms", "diagnostic"),
    ),
)

LIQUIDATIONS = Family(
    "liquidations",
    stream="liquidations",
    templates=(
        Template("liq_flow", "feature"),
        Template("n_liqs", "feature"),
        Template("liq_flow_{w}", "feature", FLOW_WINDOWS),
        Template("n_liqs_{w}", "feature", FLOW_WINDOWS),
    ),
)

VENUE_TARGETS = Family(
    "target",
    stream="quotes",
    prefix="y_{venue}",
    templates=(
        Template("ret_bps_{w}", "target", HORIZONS),
        Template("dz_{w}", "target", HORIZONS),
    ),
)

CONSOLIDATED = Family(
    "nbbo",
    stream="nbbo",
    prefix=NBBO,
    templates=(
        Template("mid", "level"),
        Template("spread_bps", "feature"),
        Template("n_venues", "feature"),
        Template("age_ms", "diagnostic"),
    ),
)

CONSOLIDATED_TARGETS = Family(
    "target",
    stream="nbbo",
    prefix="y",
    templates=(
        Template("ret_bps_{w}", "target", HORIZONS),
        Template("dz_{w}", "target", HORIZONS),
    ),
)

PER_VENUE = (BOOK, OFI, RETURNS, FLOW, BASIS, OPEN_INTEREST, LIQUIDATIONS, VENUE_TARGETS)
# Present on every date: `build_features` returns nothing without the primary NBBO.
FIXED = (GRID, CONSOLIDATED, CONSOLIDATED_TARGETS)


class UnknownColumn(ValueError):
    """A column the grammar cannot place.

    Loud on purpose. An unrecognised column is either a new feature family nobody
    declared, in which case it is silently absent from every model, or a typo, in which
    case the column it was meant to be is silently absent instead.
    """


def _fixed_index() -> dict[str, Column]:
    return {c.name: c for f in FIXED for c in f.columns()}


def _suffix_index(families: Iterable[Family]) -> tuple[tuple[str, str, Role], ...]:
    """(suffix, family name, role), longest suffix first.

    Longest-first is load-bearing: `binance_futures_oi_age_ms` ends with both `_oi_age_ms`
    and `_age_ms`, and the short match would invent a venue called `binance_futures_oi`
    and hand it the perp's open-interest columns.
    """
    out = [
        (suffix, f.name, t.role) for f in families for t in f.templates for suffix in t.suffixes()
    ]
    return tuple(sorted(out, key=lambda item: len(item[0]), reverse=True))


_FIXED_COLUMNS = _fixed_index()
_VENUE_SUFFIXES = _suffix_index(f for f in PER_VENUE if f is not VENUE_TARGETS)
_TARGET_SUFFIXES = _suffix_index((VENUE_TARGETS,))
_FAMILIES_BY_NAME = {f.name: f for f in PER_VENUE}


def _classify(name: str) -> Column:
    """One column name to its declared identity."""
    fixed = _FIXED_COLUMNS.get(name)
    if fixed is not None:
        return fixed
    # `y_` is reserved for targets (features.build_features: "if a column is not `y_*`,
    # it was knowable at `t`"), so a per-venue target can never be read as a return.
    if name.startswith("y_"):
        return _match(name[2:], _TARGET_SUFFIXES, name)
    return _match(name, _VENUE_SUFFIXES, name)


def _match(body: str, suffixes: tuple[tuple[str, str, Role], ...], name: str) -> Column:
    for suffix, family, role in suffixes:
        tail = f"_{suffix}"
        if body.endswith(tail) and len(body) > len(tail):
            return Column(name, role, family, body[: -len(tail)])
    raise UnknownColumn(f"{name!r} matches no declared feature family")


def expected_columns(streams: Mapping[str, Collection[str]]) -> tuple[Column, ...]:
    """Every column a matrix carries, given what each venue published that date.

    Deterministic in its argument, which is the whole point: the column set is a
    consequence of the capture, not of whatever the builder happened to emit.
    """
    out = [c for f in FIXED for c in f.columns()]
    for venue in sorted(streams):
        for family in PER_VENUE:
            if family.stream in streams[venue]:
                out.extend(family.columns(venue))
    return tuple(out)


@dataclass
class FeatureSchema:
    """What a feature matrix should hold, and what it does hold.

    Both, because they differ for real reasons and the difference is the reportable
    thing: a date on which a venue was down carries fewer columns, and `pl.concat`'s
    diagonal union will null-fill it into the pooled frame either way.

    Two venue names can only collide on a column when one venue's name plus a declared
    suffix spells another venue's column, so the expansion is checked for duplicates.
    Discovery would otherwise have split the frame between them arbitrarily and every
    per-venue number would be wrong with nothing to show for it.
    """

    streams: Mapping[str, frozenset[str]]
    present: frozenset[str]
    columns: tuple[Column, ...] = field(init=False)

    def __post_init__(self) -> None:
        self.columns = expected_columns(self.streams)
        names = [c.name for c in self.columns]
        self._by_name = {c.name: c for c in self.columns}
        if len(self._by_name) != len(names):
            # Reachable only when one venue's name plus a suffix spells another's column.
            dupes = sorted({n for n in names if names.count(n) > 1})
            raise UnknownColumn(f"venue names collide on column(s) {dupes}")

    @classmethod
    def from_columns(cls, names: Iterable[str]) -> FeatureSchema:
        """Recover the schema from a built frame, rejecting anything undeclared."""
        present = frozenset(names)
        streams: dict[str, set[str]] = {}
        for name in sorted(present):
            column = _classify(name)
            if column.venue is not None:
                family = _FAMILIES_BY_NAME[column.family]
                streams.setdefault(column.venue, set()).add(family.stream)
        return cls({v: frozenset(s) for v, s in streams.items()}, present)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def venues(self) -> list[str]:
        """Venue legs, alphabetical.

        Ordering used to be load-bearing (longest-first, so a prefix match resolved
        `binance_futures` before `binance`); a column now declares its own venue, so it
        is not.
        """
        return sorted(self.streams)

    def of(self, name: str) -> Column:
        column = self._by_name.get(name)
        if column is None:
            raise UnknownColumn(f"{name!r} is not in this schema")
        return column

    def venue_of(self, name: str) -> str | None:
        return self.of(name).venue

    def role_of(self, name: str) -> Role:
        return self.of(name).role

    def features(self, columns: Iterable[str], exclude_venue: str | None = None) -> list[str]:
        """Model inputs, in the caller's order, optionally with one venue's view removed.

        Excluding a venue also excludes the NBBO, which is built from that venue's quotes
        and would smuggle the target back in through the consolidated book.

        Order is the frame's rather than the declaration's because it survives into the
        fitted model: the feature matrix is positional, and reordering its columns moves
        which of two equal-gain splits the tree takes.
        """
        return [
            name
            for name in columns
            if (c := self.of(name)).role == "feature"
            and not (exclude_venue and c.venue == exclude_venue)
            and not (exclude_venue and c.family == CONSOLIDATED.name)
        ]

    def age_columns(self) -> tuple[list[str], list[str]]:
        """Staleness columns split into (book clocks, tape clocks).

        The NBBO's own age lands in the tape group, matching what the suffix rule this
        replaced did. It makes no practical difference: the consolidated book is only
        stale when every venue is, and those rows are already dropped by the venue book
        ages, which are held to the tighter tolerance.
        """
        book, tape = [], []
        for c in self.columns:
            if c.role != "diagnostic" or c.name not in self.present:
                continue
            (book if c.family == BOOK.name else tape).append(c.name)
        return book, tape

    def target(self, horizon: int, venue: str | None = None) -> str:
        """The forward-return target, consolidated or per venue."""
        family = VENUE_TARGETS if venue else CONSOLIDATED_TARGETS
        return f"{family.prefix.format(venue=venue)}_ret_bps_{horizon}"

    def spread(self, venue: str | None = None) -> str:
        """The column whose half-width defines a tradeable move on that book."""
        return f"{venue or NBBO}_spread_bps"

    def missing_by_family(self, present: Collection[str] | None = None) -> dict[str, list[str]]:
        """Declared columns a frame does not carry, keyed `venue/family`.

        What `pl.concat(..., how="diagonal")` null-fills without comment. A venue that was
        down for one date of twenty is a legitimate hole and not an error, but it belongs
        in the run's output rather than in the shape of a silently wider frame.
        """
        have = self.present if present is None else frozenset(present)
        gaps: dict[str, list[str]] = {}
        for c in self.columns:
            if c.name in have:
                continue
            gaps.setdefault(f"{c.venue}/{c.family}" if c.venue else c.family, []).append(c.name)
        return gaps
