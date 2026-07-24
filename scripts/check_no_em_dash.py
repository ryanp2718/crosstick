#!/usr/bin/env python3
"""Fail if any tracked file contains an em-dash (U+2014).

House style is zero em-dashes; use a hyphen, comma, or parentheses instead.
Runs in CI over all tracked files, and as a pre-commit hook over the staged
files (pre-commit passes them as arguments).
"""
import subprocess
import sys

EM_DASH = chr(0x2014)  # literal avoided so this file passes its own check


def tracked_files():
    out = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, encoding="utf-8"
    ).stdout
    return out.splitlines()


def main(argv):
    files = argv[1:] or tracked_files()
    hits = []
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                for lineno, line in enumerate(f, 1):
                    if EM_DASH in line:
                        hits.append(f"{path}:{lineno}: {line.rstrip()}")
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError, PermissionError):
            continue
    if hits:
        print("em-dash (U+2014) found; use a hyphen, comma, or parentheses:\n")
        print("\n".join(hits))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
