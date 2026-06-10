#!/usr/bin/env python3
"""Enforce LHP Coding Constitution stability-annotation rules (§1.13, §4.11).

Every public symbol defined in src/lhp/api/**/*.py must declare its
stability lifecycle in its docstring:

    :stability: stable        # semver-protected; rename = major bump
    :stability: provisional   # may change in any minor; default for new
    :stability: experimental  # may change without notice
    :stability: deprecated    # scheduled for removal (§6.4)

Public symbol, for the purpose of this check (§1.13 broader reading):
  * Module-level class definitions whose name does not start with `_`.
  * Module-level function definitions whose name does not start with `_`.

Methods are NOT checked individually — they inherit their class's
stability annotation.  An individual method MAY carry its own annotation
to deviate (e.g., an experimental method on a stable class), but that is
optional and out of scope here.

`__init__.py` re-exports (no `class`/`def` statements) are skipped; they
alias symbols defined in sibling modules where the annotation lives.

Scope: src/lhp/api/**/*.py.  Files outside lhp/api/ are out of scope and
produce no findings.

Usage:
  python scripts/check_stability_drift.py <path>...  # check one or more files
  python scripts/check_stability_drift.py --all      # check every api file

Exit codes:
  0  no violations (also when lhp/api/ has no .py files yet)
  2  at least one violation reported
"""
from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src" / "lhp"
API_ROOT = SRC_ROOT / "api"

VALID_LEVELS = ("stable", "provisional", "experimental", "deprecated")
RE_STABILITY = re.compile(
    r":stability:\s+(stable|provisional|experimental|deprecated)\b"
)


def in_api(path: Path) -> bool:
    try:
        path.resolve().relative_to(API_ROOT.resolve())
    except ValueError:
        return False
    return path.suffix == ".py"


def rel(path: Path) -> Path:
    return path.resolve().relative_to(REPO_ROOT)


def is_public(name: str) -> bool:
    return not name.startswith("_")


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def docstring_stability(node: ast.AST) -> str | None:
    """Return the stability level declared in the node's docstring, or
    None if the docstring is absent or carries no `:stability:` marker.
    """
    if not isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return None
    doc = ast.get_docstring(node, clean=False)
    if doc is None:
        return None
    match = RE_STABILITY.search(doc)
    if match is None:
        return None
    return match.group(1)


def check_file(path: Path) -> list[str]:
    findings: list[str] = []
    if not in_api(path):
        return findings

    source = read_text(path)
    if not source.strip():
        return findings

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        findings.append(
            f"{rel(path)}:{exc.lineno or 1}: LHP-1.13 syntax error — cannot "
            f"audit stability ({exc.msg})"
        )
        return findings

    for node in tree.body:
        if not isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not is_public(node.name):
            continue
        kind = "class" if isinstance(node, ast.ClassDef) else "function"
        level = docstring_stability(node)
        if level is None:
            findings.append(
                f"{rel(path)}:{node.lineno}: LHP-1.13 public {kind} "
                f"`{node.name}` missing `:stability:` annotation in "
                f"docstring (§1.13, §4.11) — declare one of "
                f"{'/'.join(VALID_LEVELS)}"
            )
            continue
        if level not in VALID_LEVELS:
            findings.append(
                f"{rel(path)}:{node.lineno}: LHP-1.13 public {kind} "
                f"`{node.name}` has unknown stability level `{level}` "
                f"(§1.13) — use one of {'/'.join(VALID_LEVELS)}"
            )
    return findings


def iter_api_files() -> list[Path]:
    if not API_ROOT.exists():
        return []
    return sorted(
        p for p in API_ROOT.rglob("*.py") if "__pycache__" not in p.parts
    )


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
        help="Check every Python file under src/lhp/api/.",
    )
    args = parser.parse_args(argv)

    if args.all and args.paths:
        parser.error("use --all OR one-or-more paths, not both")
    if not args.all and not args.paths:
        parser.error("provide at least one path or --all")

    targets = iter_api_files() if args.all else args.paths

    all_findings: list[str] = []
    for target in targets:
        if target is None or not target.exists():
            continue
        all_findings.extend(check_file(target))

    if all_findings:
        for line in all_findings:
            print(line)
        print(
            f"\n[check_stability_drift] {len(all_findings)} violation(s) — "
            f"see LHP Coding Constitution §1.13 / §4.11"
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
