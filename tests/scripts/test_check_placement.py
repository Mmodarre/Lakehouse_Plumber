"""Unit tests for the check_placement gate.

``scripts/`` is intentionally a NON-package (no ``__init__.py``) per the
constitution, so modules are imported by bare name (``import check_placement``).
This file pre-seeds ``sys.path`` with the repo ``scripts/`` dir so the
bare-name import resolves correctly.
"""

import sys
from pathlib import Path

import pytest

# Pre-seed sys.path before the bare-name import; parents[2] == <repo_root>.
_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import check_placement  # noqa: E402  (must follow the sys.path insertion above)

# Resolved relative to this file (not process CWD) for worktree safety.
REPO_ROOT = Path(__file__).resolve().parents[2]
API_DIR = REPO_ROOT / "src" / "lhp" / "api"
DOCS_DIR = REPO_ROOT / "docs"
VALIDATORS_DIR = REPO_ROOT / "src" / "lhp" / "core" / "validators"


@pytest.mark.unit
def test_pure_matcher_flags_private_reach_through():
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

    Both ``LHP_API_DIR`` and ``REPO_ROOT`` are patched to ``tmp_path`` so the
    temp file is treated as inside lhp/api/ and renders without a ``ValueError``
    on the control case.
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
    py_files = sorted(p for p in API_DIR.rglob("*.py") if "__pycache__" not in p.parts)
    assert py_files, f"expected Python files under {API_DIR}"

    offenders: list[str] = []
    for f in py_files:
        offenders.extend(check_placement.check_facade_internal_reach_through(f))
    assert offenders == [], "unexpected LHP-9.23 findings:\n" + "\n".join(offenders)


@pytest.mark.unit
def test_real_docs_tree_has_no_orchestrator_leak():
    """Scans every ``.rst`` exactly as the --all traversal does — §9.13 gate must report nothing."""
    rst_files = sorted(
        p for p in DOCS_DIR.rglob("*.rst") if "__pycache__" not in p.parts
    )
    assert rst_files, f"expected .rst files under {DOCS_DIR}"

    offenders: list[str] = []
    for f in rst_files:
        offenders.extend(check_placement.check_docs_orchestrator_leak(f))
    assert offenders == [], "unexpected LHP-9.13-docs findings:\n" + "\n".join(
        offenders
    )


@pytest.mark.unit
def test_docs_leak_fires_on_seeded_rst(tmp_path, monkeypatch):
    """A seeded `.rst` naming ActionOrchestrator / the dead path FIRES.

    ``DOCS_ROOT`` and ``REPO_ROOT`` are patched to ``tmp_path`` so the temp
    file is treated as inside ``docs/`` and renders without a ``ValueError``.
    """
    monkeypatch.setattr(check_placement, "DOCS_ROOT", tmp_path)
    monkeypatch.setattr(check_placement, "REPO_ROOT", tmp_path)

    leaked = tmp_path / "api.rst"
    leaked.write_text(
        "Public API\n"
        "==========\n"
        "\n"
        ".. autoclass:: lhp.core.orchestrator.ActionOrchestrator\n"
        "\n"
        "``ActionOrchestrator`` is not thread-safe.\n",
        encoding="utf-8",
    )
    findings = check_placement.check_docs_orchestrator_leak(leaked)
    # Two offending lines: the autoclass/dead-path line and the
    # thread-safety name-leak line.
    assert len(findings) == 2, findings
    assert all("LHP-9.13-docs" in f for f in findings)
    assert any("lhp.core.orchestrator" in f for f in findings)
    assert any("ActionOrchestrator" in f for f in findings)


@pytest.mark.unit
def test_docs_leak_ignores_non_rst_and_non_docs(tmp_path, monkeypatch):
    """Non-`.rst` and `.rst` files outside docs/ are not flagged — gate is safe on arbitrary paths."""
    monkeypatch.setattr(check_placement, "DOCS_ROOT", tmp_path / "docs")
    monkeypatch.setattr(check_placement, "REPO_ROOT", tmp_path)

    # `.py` with the literal — wrong suffix, ignored.
    py_file = tmp_path / "docs" / "leak.py"
    py_file.parent.mkdir(parents=True)
    py_file.write_text("x = 'ActionOrchestrator'\n", encoding="utf-8")
    assert check_placement.check_docs_orchestrator_leak(py_file) == []

    # `.rst` OUTSIDE docs/ — right suffix, wrong location, ignored.
    outside = tmp_path / "elsewhere.rst"
    outside.write_text("lhp.core.orchestrator\n", encoding="utf-8")
    assert check_placement.check_docs_orchestrator_leak(outside) == []


@pytest.mark.unit
def test_docs_leak_noqa_suppresses(tmp_path, monkeypatch):
    monkeypatch.setattr(check_placement, "DOCS_ROOT", tmp_path)
    monkeypatch.setattr(check_placement, "REPO_ROOT", tmp_path)

    suppressed = tmp_path / "history.rst"
    suppressed.write_text(
        "The old ``ActionOrchestrator`` name.  # noqa: LHP-9.13-docs historical\n",
        encoding="utf-8",
    )
    assert check_placement.check_docs_orchestrator_leak(suppressed) == []

    # Control: the identical line WITHOUT the noqa is reported once.
    control = tmp_path / "control.rst"
    control.write_text("The old ``ActionOrchestrator`` name.\n", encoding="utf-8")
    control_findings = check_placement.check_docs_orchestrator_leak(control)
    assert len(control_findings) == 1
    assert "LHP-9.13-docs" in control_findings[0]


@pytest.mark.unit
def test_main_all_scans_docs_and_reports_leak(tmp_path, monkeypatch, capsys):
    """Proves the separate docs traversal is wired into main() and drives exit code 2."""
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    (docs_root / "api.rst").write_text(
        ".. autoclass:: lhp.core.orchestrator.ActionOrchestrator\n",
        encoding="utf-8",
    )
    empty_src = tmp_path / "src" / "lhp"
    empty_src.mkdir(parents=True)

    monkeypatch.setattr(check_placement, "DOCS_ROOT", docs_root)
    monkeypatch.setattr(check_placement, "SRC_ROOT", empty_src)
    monkeypatch.setattr(check_placement, "REPO_ROOT", tmp_path)

    exit_code = check_placement.main(["--all"])
    out = capsys.readouterr().out
    assert exit_code == 2, out
    assert "LHP-9.13-docs" in out


@pytest.mark.unit
def test_real_validators_tree_has_no_loose_top_level_files():
    """Depth-0 files (``__init__.py``, ``_base.py``, ``config_validator.py``) and subdir files must all pass — zero LHP-2.1 findings."""
    py_files = sorted(
        p for p in VALIDATORS_DIR.rglob("*.py") if "__pycache__" not in p.parts
    )
    assert py_files, f"expected Python files under {VALIDATORS_DIR}"

    offenders: list[str] = []
    for f in py_files:
        offenders.extend(check_placement.check_validator_directory_membership(f))
    assert offenders == [], "unexpected LHP-2.1 findings:\n" + "\n".join(offenders)


@pytest.mark.unit
def test_loose_top_level_validator_fires(tmp_path, monkeypatch):
    """A loose `.py` directly under core/validators/ FIRES with LHP-2.1.

    ``VALIDATORS_PKG_DIR`` and ``REPO_ROOT`` are patched to ``tmp_path`` so
    findings render relative to the root.  Non-vacuity: ``foo_validator.py``
    must appear in findings so a no-op check would fail the assertion below.
    """
    monkeypatch.setattr(check_placement, "VALIDATORS_PKG_DIR", tmp_path)
    monkeypatch.setattr(check_placement, "REPO_ROOT", tmp_path)

    loose = tmp_path / "foo_validator.py"
    loose.write_text("# a loose validator\n", encoding="utf-8")
    findings = check_placement.check_validator_directory_membership(loose)
    assert len(findings) == 1, findings
    assert "LHP-2.1" in findings[0]
    assert "foo_validator.py" in findings[0]

    # An allow-listed top-level file is NOT flagged.
    allowed = tmp_path / "config_validator.py"
    allowed.write_text("# the aggregate config validator\n", encoding="utf-8")
    assert check_placement.check_validator_directory_membership(allowed) == []

    # A file in a depth-1 subdir is NOT flagged (membership, not suffix).
    subdir = tmp_path / "compatibility"
    subdir.mkdir()
    nested = subdir / "dlt_cdc.py"
    nested.write_text("# a real validator, no suffix\n", encoding="utf-8")
    assert check_placement.check_validator_directory_membership(nested) == []


@pytest.mark.unit
def test_loose_top_level_validator_noqa_suppresses(tmp_path, monkeypatch):
    monkeypatch.setattr(check_placement, "VALIDATORS_PKG_DIR", tmp_path)
    monkeypatch.setattr(check_placement, "REPO_ROOT", tmp_path)

    suppressed = tmp_path / "legacy_validator.py"
    suppressed.write_text(
        "# noqa: LHP-2.1 transitional shim, removed by v0.9.0\n",
        encoding="utf-8",
    )
    assert check_placement.check_validator_directory_membership(suppressed) == []

    # Control: the identical loose file WITHOUT the noqa is reported once.
    control = tmp_path / "control_validator.py"
    control.write_text("# a loose validator\n", encoding="utf-8")
    control_findings = check_placement.check_validator_directory_membership(control)
    assert len(control_findings) == 1
    assert "LHP-2.1" in control_findings[0]
