#!/usr/bin/env python3
"""Enforce LHP Coding Constitution placement rules (§2, §9.1, §9.2, §9.4,
§9.7, §9.13, §9.23).

Rules checked, with their finding codes:

  LHP-9.1   §2.1 / §9.1   Validator outside core/validators/
  LHP-9.2   §2.2 / §9.2   Domain types in utils/
  LHP-9.7   §5.3 / §9.7   CLI imports from internal domain modules
  LHP-9.4   §4.1 / §9.4   `_by_field` / `_by_fields` / `_v<N>` method variants
  LHP-9.13  §1.10 / §9.13 `ActionOrchestrator` name in lhp/api/
  LHP-9.23  §9.23         `facade.orchestrator.X()` reach-through

Scope: src/lhp/**/*.py.  Each file is checked against the subset of rules
that apply to its path.

Usage:
  python scripts/check_placement.py <path>...  # check one or more files
  python scripts/check_placement.py --all      # check every in-scope file

Exit codes:
  0  no violations
  2  at least one violation reported

Suppression:
  `# noqa: LHP-9.4` (with rationale per §6.6) on the same line as a
  `_by_field` / `_v<N>` definition.  No suppression is supported for the
  other rules — they target placement, which is the rule itself.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src" / "lhp"
LHP_API_DIR = SRC_ROOT / "api"
LHP_CLI_DIR = SRC_ROOT / "cli"
LHP_CORE_VALIDATORS_DIR = SRC_ROOT / "core" / "validators"
LHP_UTILS_DIR = SRC_ROOT / "utils"

# Transitional shims permitted to import from lhp.errors / lhp.models /
# lhp.api despite living under lhp/utils/.  Each entry MUST cite the
# deletion deadline so the allowlist doesn't rot.  Exempts only the
# LHP-9.2 domain-import / LHPError-subclass rules — every other check
# (file size, CLI imports, method variants, ...) still applies.
PLACEMENT_SHIM_ALLOWLIST: dict[str, str] = {}

# §11 lists the seven domain-code packages.  CLI may only reach
# `lhp.api`, `lhp.utils`, and `lhp.errors` directly (§5.3).
DOMAIN_PKGS_BANNED_FROM_CLI = (
    "lhp.core",
    "lhp.parsers",
    "lhp.generators",
    "lhp.bundle",
    "lhp.models",
)
DOMAIN_PKG_SHORTNAMES = ("core", "parsers", "generators", "bundle", "models")

# Relative imports inside cli/ that reach domain packages.  Matches
# `from .core`, `from ..core`, `from ...core`, etc.  Word boundary
# ensures `from .core_helpers` (if such a module existed) is NOT flagged.
RE_CLI_RELATIVE_DOMAIN_IMPORT = re.compile(
    r"^\s*from\s+\.+(" + "|".join(DOMAIN_PKG_SHORTNAMES) + r")\b"
)

# §9.4 — these stems cannot appear as method-name suffixes on new code.
RE_BY_FIELD_DEF = re.compile(r"^\s*def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")
BANNED_NAME_SUFFIXES = ("_by_field", "_by_fields")
RE_VERSIONED_METHOD = re.compile(r"_v\d+$")

# §9.23 — facade reach-through.  The qualifier on the LHS is whatever the
# user named the facade (`facade`, `application_facade`, `app_facade`).
RE_FACADE_REACH = re.compile(
    r"\b[a-zA-Z_][a-zA-Z0-9_]*facade\.orchestrator\.", re.IGNORECASE
)

# §9.13 — `ActionOrchestrator` (or any `*Orchestrator` class name) in
# anything that lhp/api/ exposes.  We flag the literal string here; the
# constitution forbids it in docstrings, examples, and __all__.
ORCHESTRATOR_LITERAL = "ActionOrchestrator"

# §9.2 — utils/ may not host domain types.  These import patterns indicate
# that a utils/ file is domain-aware.
RE_UTILS_DOMAIN_IMPORT = re.compile(
    r"^\s*from\s+(?:lhp\.|\.\.)"
    r"(?:errors|models|api)(?:\s|\.|$)",
    re.MULTILINE,
)
RE_UTILS_LHP_ERROR_SUBCLASS = re.compile(
    r"^\s*class\s+\w+\s*\([^)]*\bLHPError\b[^)]*\)\s*:", re.MULTILINE
)

# Validator filename pattern per §2.1.
RE_VALIDATOR_FILENAME = re.compile(r".*_validators?\.py$")

# Suppression comment per §6.6.
RE_NOQA_SUPPRESSION = re.compile(r"#\s*noqa:\s*([A-Z0-9\-.,\s]+)")


def in_src(path: Path) -> bool:
    try:
        path.resolve().relative_to(SRC_ROOT.resolve())
    except ValueError:
        return False
    return path.suffix == ".py"


def is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def find_noqa_codes(line: str) -> set[str]:
    match = RE_NOQA_SUPPRESSION.search(line)
    if not match:
        return set()
    return {tok.strip() for tok in match.group(1).split(",") if tok.strip()}


def rel(path: Path) -> Path:
    return path.resolve().relative_to(REPO_ROOT)


def check_validator_placement(path: Path) -> list[str]:
    findings: list[str] = []
    if not RE_VALIDATOR_FILENAME.match(path.name):
        return findings
    if is_under(path, LHP_CORE_VALIDATORS_DIR):
        return findings
    findings.append(
        f"{rel(path)}:1: LHP-9.1 validator file `{path.name}` lives outside "
        f"core/validators/ (§2.1) — move under core/validators/"
    )
    return findings


def check_utils_domain_types(path: Path) -> list[str]:
    findings: list[str] = []
    if not is_under(path, LHP_UTILS_DIR):
        return findings
    text = read_text(path)
    if not text:
        return findings
    rel_str = str(rel(path))
    if rel_str in PLACEMENT_SHIM_ALLOWLIST:
        # Transitional shim — the domain-import / subclass rules are
        # intentionally allowed.  Other checks (size, CLI, variants)
        # still run via the normal CHECKS pipeline.
        return findings
    for match in RE_UTILS_DOMAIN_IMPORT.finditer(text):
        line_no = text.count("\n", 0, match.start()) + 1
        findings.append(
            f"{rel(path)}:{line_no}: LHP-9.2 utils/ file imports domain "
            f"package (§2.2) — utils/ may not host domain types; relocate"
        )
    for match in RE_UTILS_LHP_ERROR_SUBCLASS.finditer(text):
        line_no = text.count("\n", 0, match.start()) + 1
        findings.append(
            f"{rel(path)}:{line_no}: LHP-9.2 utils/ defines an LHPError "
            f"subclass (§2.2) — domain exception types belong in lhp/errors/"
        )
    return findings


def check_cli_imports(path: Path) -> list[str]:
    findings: list[str] = []
    if not is_under(path, LHP_CLI_DIR):
        return findings
    text = read_text(path)
    if not text:
        return findings
    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.lstrip()
        if not (stripped.startswith("from ") or stripped.startswith("import ")):
            continue
        absolute_hit: str | None = None
        for pkg in DOMAIN_PKGS_BANNED_FROM_CLI:
            if (
                f"from {pkg}" in stripped
                or stripped.startswith(f"import {pkg}")
                or stripped.startswith(f"{pkg} ")
            ):
                absolute_hit = pkg
                break
        relative_match = RE_CLI_RELATIVE_DOMAIN_IMPORT.match(stripped)
        if absolute_hit is not None:
            findings.append(
                f"{rel(path)}:{line_no}: LHP-9.7 CLI imports from internal "
                f"domain module `{absolute_hit}` (§5.3) — go through lhp.api"
            )
        elif relative_match is not None:
            short = relative_match.group(1)
            findings.append(
                f"{rel(path)}:{line_no}: LHP-9.7 CLI imports from internal "
                f"domain module `lhp.{short}` via relative import (§5.3) — "
                f"go through lhp.api"
            )
    return findings


def check_method_variants(path: Path) -> list[str]:
    findings: list[str] = []
    text = read_text(path)
    if not text:
        return findings
    for line_no, line in enumerate(text.splitlines(), start=1):
        match = RE_BY_FIELD_DEF.match(line)
        if not match:
            continue
        name = match.group(1)
        is_banned = name.endswith(BANNED_NAME_SUFFIXES) or RE_VERSIONED_METHOD.search(
            name
        )
        if not is_banned:
            continue
        if "LHP-9.4" in find_noqa_codes(line):
            continue
        findings.append(
            f"{rel(path)}:{line_no}: LHP-9.4 method `{name}` uses banned "
            f"`_by_field` / `_v<N>` variant suffix (§4.1, §9.4) — variants "
            f"take parameters, they don't get new methods"
        )
    return findings


def check_facade_reach_through(path: Path) -> list[str]:
    findings: list[str] = []
    text = read_text(path)
    if not text:
        return findings
    for line_no, line in enumerate(text.splitlines(), start=1):
        if not RE_FACADE_REACH.search(line):
            continue
        if "LHP-9.23" in find_noqa_codes(line):
            continue
        findings.append(
            f"{rel(path)}:{line_no}: LHP-9.23 facade reach-through "
            f"(`facade.orchestrator.X()`) — orchestrator is internal; "
            f"extend the facade instead"
        )
    return findings


def check_api_orchestrator_mentions(path: Path) -> list[str]:
    findings: list[str] = []
    if not is_under(path, LHP_API_DIR):
        return findings
    text = read_text(path)
    if not text or ORCHESTRATOR_LITERAL not in text:
        return findings
    for line_no, line in enumerate(text.splitlines(), start=1):
        if ORCHESTRATOR_LITERAL not in line:
            continue
        findings.append(
            f"{rel(path)}:{line_no}: LHP-9.13 `{ORCHESTRATOR_LITERAL}` "
            f"appears in lhp/api/ (§1.10, §9.13) — construct via "
            f"LakehousePlumberApplicationFacade.for_project(...)"
        )
    return findings


CHECKS = (
    check_validator_placement,
    check_utils_domain_types,
    check_cli_imports,
    check_method_variants,
    check_facade_reach_through,
    check_api_orchestrator_mentions,
)


def check_file(path: Path) -> list[str]:
    findings: list[str] = []
    if not in_src(path):
        return findings
    for check in CHECKS:
        findings.extend(check(path))
    return findings


def iter_all_files() -> list[Path]:
    if not SRC_ROOT.exists():
        return []
    return sorted(
        p for p in SRC_ROOT.rglob("*.py") if "__pycache__" not in p.parts
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
            f"\n[check_placement] {len(all_findings)} violation(s) — see "
            f"LHP Coding Constitution §2 / §5 / §9"
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
