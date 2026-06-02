"""Registry-completeness / drift guard for ``lhp.errors.codes``.

This is the safety net that makes ``ALL_CODES`` a *verified mirror* of the
raise sites rather than a parallel list that silently drifts. Two guarantees:

1. Per-constant code synthesis (``test_per_constant_code_synthesis``) — every
   :class:`ErrorCode` reproduces the exact ``LHP-<CAT>-<NNN>`` string that
   ``LHPError.__init__`` synthesizes (``errors/types.py``), and no two records
   collapse to the same rendered ``.code``.

2. SSOT subset guarantee (``test_registry_is_superset_of_literal_raise_sites``)
   — every *literal* error-code construction found by statically scanning
   ``src/lhp/`` is present in the registry. The direction is deliberately
   **subset** (raise-sites ⊆ registry), NOT equality, so it stays green as
   raise sites migrate to the factory: fewer literals exist, but the remaining
   ones are still a subset. The exact-match end-state (registry ==
   raise-sites) is enforced by ``test_no_stray_literal_codes_end_state``.

Scan technique — **regex / line-based text reading, never AST**. This is
intentional: the scan must be robust to a source file being mid-edit by a
concurrent process (a half-written file must not crash collection).

What the scan matches
---------------------
The real unit of error identity is the ``(ErrorCategory, code_number)`` pair
(the same ``code_number`` recurs across categories). A code is synthesized,
never stored, so we capture the two halves of each construction:

  * a *literal* keyword argument ``code_number="NNN"`` (or single-quoted),
    written with **no space** around ``=`` (the keyword-argument convention
    that ruff/black enforce). Requiring no space deliberately excludes plain
    *assignments* like ``code_number = "000"`` (a local default) and class
    attributes like ``_code_number = "020"`` — those are not construction
    call-sites.
  * the ``ErrorCategory.<MEMBER>`` it is paired with. In every literal raise
    site today the ``category=`` (or ``_category =``) precedes ``code_number=``
    within the same construction, so the category is resolved as the *nearest
    preceding* ``ErrorCategory.<MEMBER>`` in the file text. (Verified: 0
    unpaired literal sites across ``src/lhp/``.)

Excluded from the scan
-----------------------
  * ``src/lhp/errors/codes.py`` — the registry itself (would be circular).
  * ``src/lhp/errors/factory.py`` — the factory; it sources pairs *from*
    ``codes.py``, so scanning it would be circular too. (Guarded for absence:
    a non-existent path simply never appears in the walk.)

End-state allowlist — see ``_LITERAL_CODE_NUMBER_ALLOWLIST``
-----------------------------------------------------------
After the ErrorFactory migration the ONLY legitimate literal
``code_number="NNN"`` occurrences are:

  (a) the registry (``errors/codes.py``) and the factory
      (``errors/factory.py``) — EXCLUDED from the scan; the registry uses
      positional ``ErrorCode(ErrorCategory.X, "NNN")`` and the factory stamps
      ``code_number=code.number`` (a variable), so neither produces a literal
      ``code_number="…"`` match anyway;
  (b) helper call-args — a literal forwarded into a helper whose construction
      uses ``code_number=<that variable>`` (``_build_family_error`` in
      ``core/coordination/_cross_flowgroup_issues.py`` and
      ``_resolve_and_parse_file`` in ``core/dependencies/builder.py``); and
  (c) custom-subclass / type-definition constructions in ``errors/types.py``
      whose bespoke ``__init__``/dispatch call
      ``super().__init__(..., code_number="NNN", ...)`` directly.

(b) and (c) are enumerated exhaustively in the module-level
``_LITERAL_CODE_NUMBER_ALLOWLIST`` and enforced by
``test_no_stray_literal_codes_end_state``. Anything else is a raise site that
bypassed the factory.

Two related forms are intentionally NOT in the allowlist because the strict
no-space ``_CODE_NUMBER_RE`` does not match them: spaced *assignments* like the
``code_number = "000"`` default in ``cli/commands/init_command.py`` and the
``_code_number = "020"`` class attrs on the bundle subclasses in
``errors/types.py``. The empty round-trip note ``code_number=""`` in
``api/_converters_common.py`` is likewise excluded (the regex requires ≥1 char).

This file lives in ``tests/errors/`` with **no** ``__init__.py`` (collection is
``testpaths``-based; sibling test dirs like ``tests/api/`` carry none — adding
one here causes import-mode surprises).
"""

import re
from pathlib import Path

import pytest

from lhp.errors.categories import ErrorCategory
from lhp.errors.codes import (
    ALL_CODES,
    DEPR_001,
    DEPR_002,
    DEPR_003,
    DEPR_004,
)
from lhp.errors.factory import ErrorFactory
from lhp.errors.types import LHPError

# Repo root resolved from this file: <repo_root>/tests/errors/test_codes.py
REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_LHP = REPO_ROOT / "src" / "lhp"

# Files excluded from the raise-site scan (see module docstring). ``factory.py``
# may not exist yet — that is fine; a non-existent path simply never appears in
# the ``rglob`` walk, and listing it here makes the intent explicit and keeps
# the scan correct once the file lands.
_EXCLUDED_FILES = {
    SRC_LHP / "errors" / "codes.py",
    SRC_LHP / "errors" / "factory.py",
}

# Literal keyword-argument form only: ``code_number="NNN"`` / ``code_number='NNN'``
# with NO space around ``=``. The no-space requirement is load-bearing: it
# excludes assignments (``code_number = "000"``) and class attrs
# (``_code_number = "020"``), which are not construction call-sites.
_CODE_NUMBER_RE = re.compile(r"""code_number=["'](?P<num>[^"']+)["']""")

# Any ``ErrorCategory.<MEMBER>`` reference (keyword ``category=ErrorCategory.X``
# or attribute ``_category = ErrorCategory.X`` both contain this substring).
_CATEGORY_RE = re.compile(r"ErrorCategory\.(?P<member>[A-Z_]+)")


# End-state allowlist.
# After the ErrorFactory migration, the registry in ``errors/codes.py`` is the
# single source of code numbers and the factory (``errors/factory.py``) stamps
# them via ``code_number=code.number`` (a *variable*, not a literal). Both of
# those files are excluded from the scan (see ``_EXCLUDED_FILES``).
#
# The ONLY legitimate literal ``code_number="NNN"`` occurrences remaining
# anywhere else under ``src/lhp/`` are the entries below. They split into two
# kinds, both of which are construction-by-design rather than a raise site that
# bypassed the factory:
#
#   (b) HELPER CALL-ARGS — a literal forwarded into a helper whose own
#       construction passes ``code_number=<that forwarded variable>``. The
#       literal lives at the *call site*; the helper itself is dynamic. Folding
#       these through the factory would not remove the literal, only relocate
#       it, so they stay at the call site and are allowlisted here.
#         * ``_build_family_error(code_number=...)`` in
#           ``core/coordination/_cross_flowgroup_issues.py`` (VAL-009 / VAL-010)
#         * ``_resolve_and_parse_file(code_number=...)`` in
#           ``core/dependencies/builder.py`` (IO-002 / IO-003)
#
#   (c) CUSTOM-SUBCLASS / TYPE-DEFINITION CONSTRUCTIONS — ``errors/types.py``
#       hosts the closed set of LHP exception classes. A handful of them have
#       bespoke ``__init__`` signatures (or a worker-reconstruction dispatcher)
#       that call ``super().__init__(..., code_number="NNN", ...)`` directly.
#       These ARE the type definitions; the factory sources its codes *from*
#       the registry but does not own these custom constructors. ``types.py``
#       is deliberately NOT excluded from the scan, so they are enumerated here.
#       (The four bundle subclasses also declare ``_code_number = "020"`` style
#       class attrs, but those are spaced *assignments* that the no-space
#       ``_CODE_NUMBER_RE`` does not match — they need no allowlist entry.)
#
# The scan uses the same strict no-space ``_CODE_NUMBER_RE`` as the subset test,
# so this allowlist is keyed by the literal form that regex actually matches.
# Entries are keyed on ``(relative_posix_path, code_number)`` — robust to line
# drift but precise to the exact file and code. Any literal ``code_number="…"``
# that appears OUTSIDE this allowlist (and outside the excluded registry /
# factory) means a raise site bypassed the factory, and the end-state test
# below FAILS with the offending ``file:line``.
_LITERAL_CODE_NUMBER_ALLOWLIST: set[tuple[str, str]] = {
    # (b) helper call-args forwarded into a dynamic-``code_number`` helper
    ("src/lhp/core/coordination/_cross_flowgroup_issues.py", "009"),
    ("src/lhp/core/coordination/_cross_flowgroup_issues.py", "010"),
    ("src/lhp/core/dependencies/builder.py", "002"),
    ("src/lhp/core/dependencies/builder.py", "003"),
    # (c) custom-subclass / type-definition constructions in errors/types.py
    ("src/lhp/errors/types.py", "902"),  # LHPError.from_unexpected_exception
    ("src/lhp/errors/types.py", "019"),  # PythonFunctionConflictError.__init__
    ("src/lhp/errors/types.py", "901"),  # lhp_error_from_worker_failure dispatch
    ("src/lhp/errors/types.py", "003"),  # MultiDocumentError.__init__
}


def _scan_text_for_pairs(text: str) -> list[tuple[str, str, int]]:
    """Return ``[(category_value, code_number, line_no), ...]`` for one source.

    Pure / text-only / AST-free so it is robust to a mid-edit file and is
    directly unit-testable on a synthetic string (see the non-vacuity test).

    For each literal ``code_number="NNN"`` match the paired category is the
    *nearest preceding* ``ErrorCategory.<MEMBER>`` in ``text``; the member is
    mapped to its rendered ``.value`` via :class:`ErrorCategory`. A literal
    whose preceding category member is unknown (cannot happen with the real
    enum, but guards against typos) is skipped rather than crashing the scan.
    """
    cat_positions = [
        (m.start(), m.group("member")) for m in _CATEGORY_RE.finditer(text)
    ]
    results: list[tuple[str, str, int]] = []
    for m in _CODE_NUMBER_RE.finditer(text):
        num = m.group("num")
        pos = m.start()
        preceding = [member for (p, member) in cat_positions if p < pos]
        if not preceding:
            # No category precedes this literal — record with a sentinel so the
            # subset test surfaces it as a hard miss (it will not be in the
            # registry) rather than silently dropping a real raise site.
            line_no = text.count("\n", 0, pos) + 1
            results.append(("<UNPAIRED>", num, line_no))
            continue
        member = preceding[-1]
        try:
            value = ErrorCategory[member].value
        except KeyError:
            continue
        line_no = text.count("\n", 0, pos) + 1
        results.append((value, num, line_no))
    return results


def _scan_src_for_literal_pairs() -> dict[tuple[str, str], str]:
    """Walk ``src/lhp/`` and collect every literal ``(category, number)`` pair.

    Returns a mapping ``{(category_value, number): "<relpath>:<line>"}`` — one
    representative file:line per distinct pair, used to build a clear failure
    message. Excluded files (the registry and factory) are skipped. Files that
    fail to decode (e.g. transiently mid-write) are skipped defensively rather
    than crashing the run.
    """
    found: dict[tuple[str, str], str] = {}
    for path in sorted(SRC_LHP.rglob("*.py")):
        if path in _EXCLUDED_FILES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        rel = path.relative_to(REPO_ROOT)
        for value, num, line_no in _scan_text_for_pairs(text):
            found.setdefault((value, num), f"{rel}:{line_no}")
    return found


def _scan_src_for_literal_code_numbers() -> list[tuple[str, str, int]]:
    """Walk ``src/lhp/`` and return EVERY literal ``code_number="NNN"`` site.

    Unlike :func:`_scan_src_for_literal_pairs` (which dedups on the
    ``(category, number)`` identity), this returns one tuple
    ``(relative_posix_path, code_number, line_no)`` per *occurrence* so the
    end-state test can match each literal against the allowlist and report the
    precise offending ``file:line`` for any stray. Uses the same strict
    no-space ``_CODE_NUMBER_RE`` and the same excluded-files set as the subset
    test; same AST-free / mid-edit-robust read strategy.
    """
    occurrences: list[tuple[str, str, int]] = []
    for path in sorted(SRC_LHP.rglob("*.py")):
        if path in _EXCLUDED_FILES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        for m in _CODE_NUMBER_RE.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            occurrences.append((rel, m.group("num"), line_no))
    return occurrences


@pytest.mark.unit
def test_per_constant_code_synthesis():
    """Each ``ErrorCode.code`` matches the synthesized string; no duplicates.

    Mirrors ``LHPError.__init__``'s ``f"LHP-{category.value}-{code_number}"``
    so the registry is byte-identical to constructed errors, and asserts the
    rendered ``.code`` strings are unique across ``ALL_CODES``.
    """
    assert ALL_CODES, "ALL_CODES is empty — registry did not load"

    seen: dict[str, ErrorCategory] = {}
    duplicates: list[str] = []
    for ec in ALL_CODES:
        expected = f"LHP-{ec.category.value}-{ec.number}"
        assert ec.code == expected, (
            f"ErrorCode({ec.category!r}, {ec.number!r}).code == {ec.code!r}, "
            f"expected {expected!r}"
        )
        if ec.code in seen:
            duplicates.append(ec.code)
        seen[ec.code] = ec.category

    assert not duplicates, (
        f"Duplicate rendered .code strings in ALL_CODES: {sorted(set(duplicates))}"
    )


@pytest.mark.unit
def test_registry_is_superset_of_literal_raise_sites():
    """Every literal ``(category, number)`` raise site is in the registry.

    SSOT subset guarantee (raise-sites ⊆ registry). This keeps the registry
    honest mid-migration: a raise site whose pair is missing from ``codes.py``
    fails here with its ``file:line``. The reverse direction (registry-only
    pairs, e.g. dynamic-only / allowlisted codes) is permitted — that is what
    makes this stay green as sites migrate to the factory. Exact-match
    (registry == raise-sites) is ``test_no_stray_literal_codes_end_state``'s job.
    """
    registry = {(ec.category.value, ec.number) for ec in ALL_CODES}
    found = _scan_src_for_literal_pairs()

    # Non-vacuity guard: the scan must actually find literal sites on the
    # current tree, otherwise the subset check is trivially satisfied.
    assert found, (
        "Scan found NO literal code_number= construction sites under "
        f"{SRC_LHP} — the regex or walk is broken (subset check would be "
        "vacuously true)."
    )

    missing = sorted(pair for pair in found if pair not in registry)
    if missing:
        lines = "\n".join(
            f"  ({cat!r}, {num!r})  at {found[(cat, num)]}" for cat, num in missing
        )
        pytest.fail(
            "Literal error-code raise sites are missing from "
            "src/lhp/errors/codes.py (registry must be a superset of all "
            f"literal raise sites):\n{lines}\n"
            "Add the corresponding ErrorCode(s) to ALL_CODES, or — if the "
            "site uses a dynamic code_number — confirm it belongs on the "
            "documented allowlist in this test's module docstring."
        )


@pytest.mark.unit
def test_no_stray_literal_codes_end_state():
    """End-state invariant: no stray literal ``code_number="…"``.

    After the ErrorFactory migration, the ONLY legitimate literal
    ``code_number="NNN"`` occurrences under ``src/lhp/`` are:

      (a) the registry (``errors/codes.py``) and the factory
          (``errors/factory.py``) — both EXCLUDED from the scan, because the
          registry uses positional ``ErrorCode(ErrorCategory.X, "NNN")`` and the
          factory stamps ``code_number=code.number`` (a variable, not a
          literal);
      (b) helper call-args — a literal forwarded into a helper whose own
          construction uses ``code_number=<that variable>``; and
      (c) custom-subclass / type-definition constructions in ``errors/types.py``
          whose bespoke ``__init__``/dispatch calls
          ``super().__init__(..., code_number="NNN", ...)`` directly.

    (b) and (c) are enumerated exhaustively in
    ``_LITERAL_CODE_NUMBER_ALLOWLIST``. Any literal outside that set means a
    raise site bypassed the factory; this test fails with its ``file:line``.

    This is the exact-match end-state guard that the (deliberately subset)
    ``test_registry_is_superset_of_literal_raise_sites`` defers to.
    """
    occurrences = _scan_src_for_literal_code_numbers()

    # Non-vacuity guard: the allowlisted sites still exist on the tree, so the
    # scan must find them. An empty result means the regex / walk is broken and
    # the subset assertion below would be trivially satisfied.
    assert occurrences, (
        "Scan found NO literal code_number= occurrences under "
        f"{SRC_LHP} — the regex or walk is broken (the end-state subset check "
        "would be vacuously true)."
    )

    stray = sorted(
        (rel, num, line_no)
        for (rel, num, line_no) in occurrences
        if (rel, num) not in _LITERAL_CODE_NUMBER_ALLOWLIST
    )
    if stray:
        lines = "\n".join(
            f"  code_number={num!r}  at {rel}:{line_no}"
            for (rel, num, line_no) in stray
        )
        pytest.fail(
            "Stray literal code_number= site(s) found that bypass the "
            "ErrorFactory and are not on the documented allowlist:\n"
            f"{lines}\n"
            "Route the raise site through ErrorFactory / errors/codes.py, or — "
            "if it is a legitimate helper call-arg or a custom-subclass "
            "constructor — add it to _LITERAL_CODE_NUMBER_ALLOWLIST with a "
            "one-line rationale."
        )


@pytest.mark.unit
def test_deprecation_category_and_codes():
    """Freeze the DEPR taxonomy: category value, four rendered codes, registry.

    The DEPRECATION category and its four contiguous codes (``LHP-DEPR-001``
    .. ``LHP-DEPR-004``) are the §7 deprecation contract that the C-slice
    producer tasks emit via :meth:`ErrorFactory.deprecation_error`.
    """
    assert ErrorCategory.DEPRECATION.value == "DEPR"

    expected = {
        DEPR_001: "LHP-DEPR-001",
        DEPR_002: "LHP-DEPR-002",
        DEPR_003: "LHP-DEPR-003",
        DEPR_004: "LHP-DEPR-004",
    }
    for code, rendered in expected.items():
        assert code.category is ErrorCategory.DEPRECATION
        assert code.code == rendered
        assert code in ALL_CODES


@pytest.mark.unit
def test_deprecation_factory_builds_valid_lhp_error():
    """``ErrorFactory.deprecation_error`` returns an LHPError carrying the code."""
    err = ErrorFactory.deprecation_error(
        code=DEPR_002,
        title="Deprecated field 'database'",
        details="The 'database' field is deprecated.",
        suggestions=["Use the replacement field instead"],
        context={"Field": "database"},
    )
    assert type(err) is LHPError
    assert err.code == "LHP-DEPR-002"
    assert err.title == "Deprecated field 'database'"
    assert err.details == "The 'database' field is deprecated."


@pytest.mark.unit
def test_category_docstring_count_matches_members():
    """``categories.py`` module docstring word-count tracks the enum size.

    Guards the FREEZE-2 docstring fix: the count word ('Eight' today) must
    equal the actual number of :class:`ErrorCategory` members so the docstring
    never silently drifts as categories are added.
    """
    import lhp.errors.categories as categories_module

    member_count = len(list(ErrorCategory))
    assert member_count == 8

    number_words = {
        1: "One",
        2: "Two",
        3: "Three",
        4: "Four",
        5: "Five",
        6: "Six",
        7: "Seven",
        8: "Eight",
        9: "Nine",
        10: "Ten",
    }
    expected_word = number_words[member_count]
    docstring = categories_module.__doc__ or ""
    assert docstring.startswith(f"{expected_word} categories matching") or (
        f"{expected_word} categories" in docstring
    ), (
        f"Module docstring should state '{expected_word} categories' to match "
        f"the {member_count} ErrorCategory members; got: {docstring!r}"
    )
