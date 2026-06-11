#!/usr/bin/env python3
"""Enforce LHP Coding Constitution file-size rules (§3.3, §9.3).

Rules:
  §3.3  No source file under src/lhp/ exceeds 500 lines without a
        `# JUSTIFIED:` block in the first 30 lines.
  §9.3  No source file under src/lhp/ exceeds 800 lines.  Hard rule —
        `# JUSTIFIED:` does NOT grant an exception.

Scope: src/lhp/**/*.py.  Tests and tooling are excluded.

Usage:
  python scripts/check_file_sizes.py <path>...   # check one or more files
  python scripts/check_file_sizes.py --all       # check every in-scope file

Multiple-path invocation is what pre-commit and other batch runners use;
the Claude Code hook passes a single path.  Either form works.

Exit codes:
  0  no violations (or every path was out-of-scope)
  2  at least one violation reported

A file may be both >800 (hard) and lack a `# JUSTIFIED:` block; both
findings are reported.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src" / "lhp"
# Packaged sample-project template tree (`lhp init --sample`) — package
# DATA shipped verbatim, never imported by LHP code; out of gate scope.
INIT_SAMPLE_DIR = SRC_ROOT / "templates" / "init_sample"

SOFT_CAP = 500
HARD_CAP = 800
JUSTIFICATION_WINDOW = 30
JUSTIFICATION_MARKER = "# JUSTIFIED:"


def in_scope(path: Path) -> bool:
    resolved = path.resolve()
    try:
        resolved.relative_to(SRC_ROOT.resolve())
    except ValueError:
        return False
    if resolved.is_relative_to(INIT_SAMPLE_DIR.resolve()):
        return False
    return path.suffix == ".py"


def has_justification(path: Path) -> bool:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh):
                if i >= JUSTIFICATION_WINDOW:
                    break
                if JUSTIFICATION_MARKER in line:
                    return True
    except OSError:
        return False
    return False


def line_count(path: Path) -> int:
    with path.open("rb") as fh:
        return sum(1 for _ in fh)


def check_file(path: Path) -> list[str]:
    findings: list[str] = []
    if not in_scope(path):
        return findings

    n = line_count(path)
    rel = path.resolve().relative_to(REPO_ROOT)

    if n > HARD_CAP:
        findings.append(
            f"{rel}:{n}: §9.3 file exceeds hard cap of {HARD_CAP} lines "
            f"(actual {n}); `# JUSTIFIED:` does NOT grant an exception — "
            f"decompose"
        )

    if n > SOFT_CAP and not has_justification(path):
        findings.append(
            f"{rel}:{n}: §3.3 file exceeds {SOFT_CAP} lines without "
            f"`{JUSTIFICATION_MARKER}` block in first {JUSTIFICATION_WINDOW} "
            f"lines (actual {n})"
        )

    return findings


def iter_all_files() -> list[Path]:
    if not SRC_ROOT.exists():
        return []
    return sorted(p for p in SRC_ROOT.rglob("*.py") if "__pycache__" not in p.parts)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="One or more files to check.  Omit when using --all.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Check every Python file under src/lhp/.",
    )
    args = parser.parse_args(argv)

    if args.all and args.paths:
        parser.error("use --all OR one-or-more paths, not both")
    if not args.all and not args.paths:
        parser.error("provide at least one path or --all")

    targets = iter_all_files() if args.all else args.paths

    all_findings: list[str] = []
    for target in targets:
        if target is None or not target.exists():
            continue
        all_findings.extend(check_file(target))

    if all_findings:
        for line in all_findings:
            print(line)
        print(
            f"\n[check_file_sizes] {len(all_findings)} violation(s) — "
            f"see LHP Coding Constitution §3.3 / §9.3"
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
