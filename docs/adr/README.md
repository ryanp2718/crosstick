# Architecture decision records

An ADR captures one significant decision: the context that forced it, the choice
made, and the alternatives rejected. It is a historical record, not living
documentation. Once accepted an ADR is immutable; a later decision that changes
course gets its own ADR and marks the old one superseded. For how the system
works *today*, read the design references (`docs/data-contracts.md`, the
`DESIGN_*.md` docs, and the system design reference); an ADR explains *why* a
given rule in those docs exists.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-byte-identical-replay-guarantee.md) | Byte-identical replay is a first-class guarantee | Accepted |

## Writing one

Copy `0000-template.md` to `NNNN-short-title.md` with the next number, fill it
in, and add the row above. Keep it to the decision and its rationale; move
mechanism detail into the design docs and link to it. Status is `Proposed`,
`Accepted`, `Superseded by ADR-NNNN`, or `Deprecated`.
