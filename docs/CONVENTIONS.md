# Conventions

How commits and pull requests are written here. The mechanical half is enforced
by a hook and by CI; the rest is review-time judgement and is documented rather
than checked.

## Commit subjects

```
type(scope): imperative subject
```

```
fix(silver): enforce the fold's (epoch, sequence) precondition
docs: add a pull request template
```

Conventional Commits over a closed scope list. The scope is optional, and the
breaking-change marker is allowed (`feat(gateway)!: ...`).

- Type from the fixed set below, scope from the closed list below.
- Imperative mood: `cap`, `enforce`, `render`, not `capped` or `capping`.
- Lowercase opening, unless the first word is an acronym or an identifier
  (`NBBO`, `MinIO`, `PartitionWriter`) or is not a word at all (`30d`,
  `--check`).
- No trailing period.
- At most 72 characters. The median subject in this repo is 57, so the limit
  binds only on subjects that are doing too much.
- One change per commit. A subject that needs "and" or a semicolon to stay
  honest is usually two commits.

### Types

| Type | Means |
|---|---|
| `feat` | behaviour a caller, an operator or a downstream layer can see |
| `fix` | behaviour that was wrong and now is not |
| `perf` | same behaviour, measurably cheaper |
| `refactor` | same behaviour, different shape |
| `test` | tests and fixtures only |
| `build` | packaging, hand-pinned dependencies, build configuration |
| `ci` | workflows, pinned actions, the hooks that gate a commit |
| `docs` | `docs/`, `README.md`, ADRs |
| `chore` | no product effect, including the Dependabot bumps |
| `revert` | undoing a commit |

### Scopes

| Scope | Covers |
|---|---|
| `gateway` | `node/gateway`: consume, book maintenance, NBBO, WS fan-out |
| `ingest` | the per-exchange ingesters and the wire contracts they speak |
| `silver` | bronze to silver: the fold, the reorder, the NBBO build |
| `gold` | gold rollups, data-quality budgets, the build gate |
| `lake` | object layout, `PartitionWriter`, schema registry, data contracts |
| `materializer` | the materializer service |
| `exporter` | the lake-exporter |
| `dashboard` | the browser dashboard |
| `research` | feature matrix, models, walk-forward evaluation, run records |
| `common` | shared library code with no single layer as its home |
| `ops` | compose, alerts, Prometheus and Grafana, retention, host limits |
| `demo` | the offline demo corpus and its replay |
| `deps`, `deps-dev` | what Dependabot emits, and not written by hand |

Omit the scope when a change genuinely spans the repo. Reach for it otherwise:
`fix(silver):` is the form worth having, and a bare `fix:` on a change that
lives in one layer is a subject that gave up.

Prefixes used earlier in the history, and where they go now:

| Was | Now |
|---|---|
| a bare area prefix (`ops:`, `gateway:`, `silver:`) | the type that fits, with the area as the scope |
| `analytics` | `silver`, `gold` or `research`, whichever layer moved |
| `observability`, `compose` | `ops` |
| `contracts` | `lake` |
| `nbbo` | `silver` |
| `integration` | the scope under test |
| `style` | `refactor` or `chore` |

The log had been running two conventions in parallel: 80 commits with a bare
area prefix against 54 in this form, interleaved month by month rather than one
having replaced the other. Conventional Commits wins the tie because it is the
format a reader recognises without being told, this repository is public, and
its scope field carries the area anyway, so nothing is given up by keeping the
type alongside it. Nothing in the tree parses the type today (there is no
commitlint, semantic-release or changelog generation), so it is carried for
readers rather than for machines, and the door stays open if that changes.

Dependabot is configured to match, emitting `chore(deps):` and `ci(deps):`
rather than a bare `deps:` (`.github/dependabot.yml`).

### Bodies

Subject, blank line, then a body when the change earns one: why it is being
made, what was measured, what was rejected and why. No trailers and no
attribution footers. Reference pull requests by number when a decision was
argued there.

History predating this document is not rewritten. Of the 210 non-merge commits
before it, 123 would fail the check. That is the recorded cost of the decision,
and it is roughly what the alternative would have cost too: the bare area form
would have failed 107 of the same commits.

## Pull request bodies

`.github/pull_request_template.md` carries the shape and the reasoning behind
it. In short: prose rather than checkboxes, written for someone outside the
repo, numbers rather than adjectives, `##` headings that only appear once a body
carries more than one concern, and the canonical names `The fix`,
`Verification` and `Scope` so the corpus stays greppable. A stacked pull request
ends with its merge order under a `---` rule.

## Enforcement

`scripts/check_commit_subject.py` runs as a `commit-msg` hook and checks the
five objective rules: known type, known scope, lowercase opening, no trailing
period, length. Merges, reverts and rebase instruction commits (`fixup!`,
`squash!`, `amend!`) are exempt, since their wording belongs to git.

```
uv tool install pre-commit
pre-commit install --hook-type pre-commit --hook-type commit-msg
```

The second flag matters: a plain `pre-commit install` wires only the pre-commit
stage, and the subject check never runs.

CI runs the same script over a pull request's own commits, measured from the
merge base so a main that moved while the branch was open cannot drag historical
subjects into scope. That is what catches a commit made with `--no-verify` or
from a clone where the hooks were never installed; the `conventions` job in
`ci.yml` carries it alongside the em-dash guard.

The same script checks history in bulk, which is how the 72-character limit and
the scope list were calibrated:

```
python scripts/check_commit_subject.py --range main~20..main
```

Mood, one-change-per-commit, and everything about pull request bodies are not
mechanically checked. They need a reader.
