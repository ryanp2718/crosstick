#!/usr/bin/env python3
"""Fail if a commit subject does not match house style.

House style is Conventional Commits over a closed scope list:
`type(scope): imperative subject`, lowercase, no trailing period, at most 72
characters. Types and scopes are documented in docs/CONVENTIONS.md.

Runs as a commit-msg hook over the message file (pre-commit passes its path),
or over a revision range with --range for checking history in bulk.
"""

import re
import subprocess
import sys

TYPES = (
    "feat",
    "fix",
    "perf",
    "refactor",
    "test",
    "build",
    "ci",
    "docs",
    "chore",
    "revert",
)

# The scope is the area of the system that moved. `deps` and `deps-dev` are
# what Dependabot emits; everything else maps onto a layer or a service.
SCOPES = (
    "gateway",
    "ingest",
    "silver",
    "gold",
    "lake",
    "materializer",
    "exporter",
    "dashboard",
    "research",
    "common",
    "ops",
    "demo",
    "deps",
    "deps-dev",
)

MAX_LENGTH = 72

# Merges, reverts and rebase instruction commits are generated; git and the
# rebase machinery own their wording, so they are not ours to reshape.
EXEMPT = ("Merge ", "Revert ", "fixup!", "squash!", "amend!")

SCISSORS = "# ------------------------ >8 ------------------------"

SUBJECT = re.compile(r"^([a-z]+)(?:\(([a-z0-9-]+)\))?(!)?: (.+)$")


def subject_from_message(path):
    """The first non-blank, non-comment line of a commit message file."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    body = text.split(SCISSORS)[0]
    for line in body.splitlines():
        if line.startswith("#"):
            continue
        if line.strip():
            return line.rstrip()
    return ""


def subjects_in_range(rev_range):
    out = subprocess.run(
        ["git", "log", "--no-merges", "--pretty=%h%x09%s", rev_range],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    ).stdout
    return [line.split("\t", 1) for line in out.splitlines() if "\t" in line]


def opens_uncapitalized(rest):
    """True unless the subject opens with an ordinary capitalized word.

    `cap MinIO memory` is the norm, and a subject may legitimately open on a
    digit (`30d ...`), a flag (`--check ...`), an acronym (`NBBO ...`) or an
    identifier (`PartitionWriter ...`). Only `Cap the memory` is caught.
    """
    first = rest.split()[0]
    if not first[:1].isupper():
        return True
    return first.isupper() or any(c.isupper() for c in first[1:])


def violations(subject):
    if not subject:
        return ["empty subject"]
    if subject.startswith(EXEMPT):
        return []

    match = SUBJECT.match(subject)
    if not match:
        return [f"expected `type(scope): subject`, types: {', '.join(TYPES)}"]

    type_, scope, _breaking, rest = match.groups()
    found = []
    if type_ not in TYPES:
        found.append(f"unknown type `{type_}`; use one of: {', '.join(TYPES)}")
    if scope is not None and scope not in SCOPES:
        found.append(f"unknown scope `{scope}`; use one of: {', '.join(SCOPES)}")
    if not opens_uncapitalized(rest):
        found.append("subject should open in lowercase")
    if subject.endswith("."):
        found.append("no trailing period")
    if len(subject) > MAX_LENGTH:
        found.append(f"{len(subject)} characters, limit is {MAX_LENGTH}")
    return found


def main(argv):
    # Subjects can carry non-ASCII; a cp1252 console would otherwise crash here.
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    args = argv[1:]
    if args[:1] == ["--range"]:
        checked = subjects_in_range(args[1])
    elif len(args) == 1:
        checked = [("", subject_from_message(args[0]))]
    else:
        print("usage: check_commit_subject.py <message-file> | --range <rev-range>")
        return 2

    failures = [(ref, s, violations(s)) for ref, s in checked]
    failures = [f for f in failures if f[2]]
    for ref, subject, reasons in failures:
        where = f"{ref} " if ref else ""
        print(f"{where}{subject}")
        for reason in reasons:
            print(f"    {reason}")
    if failures:
        print(f"\n{len(failures)} subject(s) off house style; see docs/CONVENTIONS.md")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
