"""Unit tests for the LHP-9.23 facade-internal reach-through gate.

Covers the additions to ``scripts/check_placement.py``:

  * ``_facade_internal_reach_findings`` — the pure, IO-free line matcher.
  * ``check_facade_internal_reach_through`` — the public check, which
    gates on ``is_under(path, LHP_API_DIR)`` and applies per-line
    ``# noqa: LHP-9.23`` suppression.

Import is white-box (Decision A): ``scripts/`` is intentionally a
NON-package (no ``__init__.py``) per the constitution, so the gate
modules are imported by bare name (``import check_placement``) rather
than as ``scripts.check_placement``.  This test file puts the repo
``scripts/`` dir on ``sys.path`` (below, before the import) so that the
bare-name import resolves the non-package module directly.
"""

import sys
from pathlib import Path

import pytest

# Ensure the repo ``scripts/`` dir is importable before importing the gate
# module by bare name.  Layout: <repo_root>/tests/scripts/<this file>, so
# parents[2] == <repo_root>.
_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import check_placement  # noqa: E402  (must follow the sys.path insertion above)

# Repo root, resolved robustly relative to this test file rather than the
# process CWD: <repo_root>/tests/scripts/test_check_placement.py
REPO_ROOT = Path(__file__).resolve().parents[2]
API_DIR = REPO_ROOT / "src" / "lhp" / "api"


@pytest.mark.unit
def test_pure_matcher_flags_private_reach_through():
    """A private-orchestrator reach-through line yields exactly one match."""
    matches = check_placement._facade_internal_reach_findings(
        "self._orchestrator._foo()"
    )
    assert len(matches) == 1
    line_no, line = matches[0]
    assert line_no == 1
    assert line == "self._orchestrator._foo()"


@pytest.mark.unit
def test_noqa_suppresses_finding_in_public_check(tmp_path, monkeypatch):
    """`# noqa: LHP-9.23` on a reach-through line suppresses the finding.

    The public check gates on ``is_under(path, LHP_API_DIR)`` and formats
    findings via ``rel(path)`` (relative to ``REPO_ROOT``).  Point both
    module globals at ``tmp_path`` so a temp file (a) is treated as living
    under lhp/api/ and (b) can be rendered relative to the root without a
    ``ValueError`` on the control case that does produce a finding.
    """
    monkeypatch.setattr(check_placement, "LHP_API_DIR", tmp_path)
    monkeypatch.setattr(check_placement, "REPO_ROOT", tmp_path)

    suppressed = tmp_path / "suppressed.py"
    suppressed.write_text(
        "self._orchestrator._foo()  # noqa: LHP-9.23 documented exception\n",
        encoding="utf-8",
    )
    assert check_placement.check_facade_internal_reach_through(suppressed) == []

    # Control: the identical reach-through WITHOUT the noqa is reported once.
    control = tmp_path / "control.py"
    control.write_text("self._orchestrator._foo()\n", encoding="utf-8")
    control_findings = check_placement.check_facade_internal_reach_through(control)
    assert len(control_findings) == 1
    assert "LHP-9.23" in control_findings[0]


@pytest.mark.unit
def test_pure_matcher_ignores_dunder_access():
    """Dunder access (`.__class__`) is not a private reach-through."""
    matches = check_placement._facade_internal_reach_findings(
        "self._orchestrator.__class__"
    )
    assert matches == []


@pytest.mark.unit
def test_pure_matcher_ignores_public_service_access():
    """Public-service access (`orchestrator.discovery.foo()`) is allowed."""
    matches = check_placement._facade_internal_reach_findings(
        "self._orchestrator.discovery.foo()"
    )
    assert matches == []


@pytest.mark.unit
def test_real_api_tree_has_no_reach_through():
    """The gate must report zero findings over the real src/lhp/api/ tree."""
    py_files = sorted(p for p in API_DIR.rglob("*.py") if "__pycache__" not in p.parts)
    assert py_files, f"expected Python files under {API_DIR}"

    offenders: list[str] = []
    for f in py_files:
        offenders.extend(check_placement.check_facade_internal_reach_through(f))
    assert offenders == [], "unexpected LHP-9.23 findings:\n" + "\n".join(offenders)
