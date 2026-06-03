#!/usr/bin/env python3
"""Enforce LHP Documentation Constitution mechanical rules.

Checks the high-confidence, low-false-positive subset of
`.claude/DOCS_CONSTITUTION.md`.  The judgement-heavy rules (mode purity,
voice, cross-links, length) stay with the `docs-reviewer` agent.

Rules checked:
  §5.2/§12.5  Forbidden admonitions.  The Databricks palette is
              note/important/tip only; `.. warning::`/`.. caution::` (and the
              rest of the off-palette docutils set) are not allowed.  Route
              data-loss caveats to `.. important::`.
  §6.2/§6.3   Legacy product names outside migration content.  "Delta Live
              §12.7       Tables" and "Databricks Asset Bundle(s)" are
              superseded by "Lakeflow Spark Declarative Pipelines (SDP)" and
              "Declarative Automation Bundles".
  §3.7        Every page opens with a `.. meta:: :description:` block near the
              top.

Scope: docs/**/*.rst.

This gate is wired only where it sees CHANGED files — pre-commit and the
Claude Code PostToolUse hook — NOT as a `--all` CI step, because the
pre-rewrite docs corpus still carries known violations.  It judges a page at
the moment you write or rewrite it, not the whole legacy tree.  Promote to a
blocking `--all` CI step once the rewrite lands (see DOCS_CONSTITUTION §13).

Usage:
  python scripts/check_docs.py <path>...   # check one or more files
  python scripts/check_docs.py --all       # check every in-scope file

Exit codes:
  0  no violations (or every path was out-of-scope)
  2  at least one violation reported
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_ROOT = REPO_ROOT / "docs"

META_WINDOW = 12

# §5 — only note/important/tip + titled admonitions are in the Databricks
# palette.  These docutils admonitions are off-palette.
FORBIDDEN_ADMONITION = re.compile(
    r"^\s*\.\.\s+(warning|caution|danger|error|attention|hint)::"
)

# §6 — legacy product names.  Full phrases only; bare "DLT"/"DAB" recur in
# URLs and code identifiers, so the docs-reviewer agent judges those in context.
LEGACY_TERMS = (
    (re.compile(r"Delta Live Tables"), "Delta Live Tables"),
    (re.compile(r"Databricks Asset Bundles?"), "Databricks Asset Bundle(s)"),
)

META_DIRECTIVE = re.compile(r"^\s*\.\.\s+meta::")


def is_migration_page(path: Path) -> bool:
    """Migration content may use legacy names (§6.2/§6.3)."""
    return "migrate" in path.name.lower()


def in_scope(path: Path) -> bool:
    try:
        path.resolve().relative_to(DOCS_ROOT.resolve())
    except ValueError:
        return False
    return path.suffix == ".rst"


def read_lines(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        return fh.readlines()


def check_file(path: Path) -> list[str]:
    findings: list[str] = []
    if not in_scope(path):
        return findings

    rel = path.resolve().relative_to(REPO_ROOT)
    lines = read_lines(path)

    for i, line in enumerate(lines, start=1):
        m = FORBIDDEN_ADMONITION.match(line)
        if m:
            findings.append(
                f"{rel}:{i}: §5.2 forbidden admonition `.. {m.group(1)}::` — "
                f"the Databricks palette is note/important/tip only; route "
                f"data-loss caveats to `.. important::`"
            )

    if not is_migration_page(path):
        for i, line in enumerate(lines, start=1):
            for pattern, label in LEGACY_TERMS:
                if pattern.search(line):
                    findings.append(
                        f"{rel}:{i}: §6 legacy product name '{label}' outside "
                        f"migration content — use 'Lakeflow Spark Declarative "
                        f"Pipelines (SDP)' / 'Declarative Automation Bundles'"
                    )

    if not any(META_DIRECTIVE.match(line) for line in lines[:META_WINDOW]):
        findings.append(
            f"{rel}:1: §3.7 missing `.. meta:: :description:` block in the "
            f"first {META_WINDOW} lines"
        )

    return findings


def iter_all_files() -> list[Path]:
    if not DOCS_ROOT.exists():
        return []
    return sorted(p for p in DOCS_ROOT.rglob("*.rst") if "_build" not in p.parts)


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
        help="Check every .rst file under docs/.",
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
            f"\n[check_docs] {len(all_findings)} violation(s) — "
            f"see LHP Documentation Constitution (.claude/DOCS_CONSTITUTION.md)"
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
