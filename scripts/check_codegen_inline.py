#!/usr/bin/env python3
"""Enforce LHP Coding Constitution §9.14 (no inline Python-code-as-f-string
in per-action generators) and Target Architecture §"Summary" item #10
(per-action generators use Jinja2 templates for code construction).

Rule checked, with its finding code:

  LHP-9.14  §1.10 / §9.14  An `ast.JoinedStr` (f-string) node in
                           src/lhp/generators/ that spans more than 20
                           source lines.  Such heredocs are
                           Python-code-as-string and must be a Jinja2
                           template in src/lhp/templates/ instead.

Scope: src/lhp/generators/**/*.py.  Anything outside that subtree is
out-of-scope and silently produces 0 findings (so pre-commit can pass
arbitrary files in batch mode).

Usage:
  python scripts/check_codegen_inline.py <path>...  # check one or more files
  python scripts/check_codegen_inline.py --all      # check every in-scope file

Exit codes:
  0  no violations (or every path was out-of-scope / unparseable)
  2  at least one violation reported

A file that cannot be read or that fails to parse contributes zero
findings — let the parser / linter that owns those errors report them.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src" / "lhp"
GENERATORS_ROOT = SRC_ROOT / "generators"
MAX_JOINED_STR_LINES = 20  # §9.14


def is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def rel(path: Path) -> Path:
    return path.resolve().relative_to(REPO_ROOT)


def check_file(path: Path) -> list[str]:
    """Return list of finding strings for one file."""
    findings: list[str] = []
    if path.suffix != ".py":
        return findings
    # Scope is strictly src/lhp/generators/.  Out-of-scope files are
    # silently passed (pre-commit may pass arbitrary paths in batch mode).
    if not is_under(path, GENERATORS_ROOT):
        return findings
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return findings
    try:
        tree = ast.parse(text)
    except SyntaxError:
        # Broken file — let the parser / other linters report it.
        return findings
    for node in ast.walk(tree):
        if not isinstance(node, ast.JoinedStr):
            continue
        if node.end_lineno is None:
            continue
        span = node.end_lineno - node.lineno
        if span <= MAX_JOINED_STR_LINES:
            continue
        findings.append(
            f"{rel(path)}:{node.lineno}: LHP-9.14 inline f-string codegen "
            f"spans {span} lines (>{MAX_JOINED_STR_LINES}) in generators/ — "
            f"extract to a Jinja2 template"
        )
    return findings


def iter_all_files() -> list[Path]:
    if not GENERATORS_ROOT.exists():
        return []
    return sorted(
        p for p in GENERATORS_ROOT.rglob("*.py") if "__pycache__" not in p.parts
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="One or more files to check.  Omit when using --all.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Check every Python file under src/lhp/generators/.",
    )
    args = parser.parse_args(argv)

    if args.all and args.paths:
        parser.error("use --all OR one-or-more paths, not both")
    if not args.all and not args.paths:
        parser.error("provide at least one path or --all")

    targets: list[Path] = iter_all_files() if args.all else list(args.paths)

    all_findings: list[str] = []
    for target in targets:
        if target is None or not target.exists():
            continue
        all_findings.extend(check_file(target))

    if all_findings:
        for line in all_findings:
            print(line)
        print(
            f"\n[check_codegen_inline] {len(all_findings)} violation(s) — "
            f"see LHP Coding Constitution §9.14"
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
