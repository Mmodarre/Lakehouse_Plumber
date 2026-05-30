"""Regression guard: an orphan blueprint instance raises LHP-VAL-041.

Pins CODING_CONSTITUTION §9.24 — blueprint *instances* are ALWAYS discovered,
symmetrically with the validate path, so instance/blueprint validation is never
silently skipped just because no blueprint *files* are present. The production
contract lives in
:meth:`lhp.core.coordination.bootstrap_service.FlowgroupBootstrapService._expand_blueprints`,
which calls ``BlueprintDiscoverer.discover_instances`` unconditionally; the
041 raise itself is in
:meth:`lhp.parsers.blueprint_parser.BlueprintParser.parse_instance_file` when
the referenced blueprint name is absent from the registry.

This is the orphan-instance guard: a project that contains exactly one
instance file referencing a blueprint that does not exist must raise
``LHPValidationError`` (code ``LHP-VAL-041``) when discovery runs — proving the
always-discover-instances path is wired through the orchestrator boundary.

Written before the Phase-2 parser refactor as a baseline that must stay green
through that refactor.

:stability: provisional
"""

from pathlib import Path

import pytest
import yaml

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.errors import LHPValidationError


def _build_orphan_instance_project(tmpdir: Path) -> Path:
    """Build a minimal LHP project whose only pipeline file is an orphan instance.

    Mirrors the construction idiom in
    ``tests/core/coordination/test_bootstrap_discovery_memo.py`` (presets/,
    templates/, substitutions/dev.yaml), but the single file under
    ``pipelines/`` is a blueprint *instance* (``use_blueprint:`` shape) that
    references a blueprint name which is never defined. No ``lhp.yaml`` is
    written, so the default ``instance_include = ['pipelines/**/*.yaml']`` glob
    discovers the instance file.
    """
    project_root = Path(tmpdir)
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "presets").mkdir()
    (project_root / "templates").mkdir()
    (project_root / "substitutions").mkdir()
    (project_root / "pipelines").mkdir()

    substitutions = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(substitutions, f)

    # Orphan instance: references a blueprint that is never provided. The
    # new-syntax `use_blueprint:` + nested `parameters:` shape is what
    # `BlueprintParser.looks_like_instance` keys on, so discovery routes this
    # file to `parse_instance_file`, which raises 041 on the missing blueprint.
    orphan_instance = {
        "use_blueprint": "nonexistent_blueprint",
        "parameters": {"site_name": "apac_sg"},
    }
    with open(project_root / "pipelines" / "orphan.yaml", "w") as f:
        yaml.dump(orphan_instance, f)

    return project_root


@pytest.mark.integration
def test_orphan_instance_raises_val_041(tmp_path):
    """Discovery on a project with an orphan instance raises LHP-VAL-041.

    Exercises the always-run instance-discovery path end-to-end through the
    orchestrator boundary (§9.24): ``discover_all_flowgroups`` ->
    ``FlowgroupBootstrapService._expand_blueprints`` ->
    ``BlueprintDiscoverer.discover_instances`` -> ``parse_instance_file``.
    """
    project_root = _build_orphan_instance_project(tmp_path / "orphan_project")
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)

    with pytest.raises(LHPValidationError) as exc_info:
        orchestrator.discover_all_flowgroups()

    # `code_number` is the bare error number set by LHPError.__init__; `code`
    # is the fully-qualified "LHP-VAL-041" (category "VAL" + code_number).
    assert exc_info.value.code_number == "041"
    assert exc_info.value.code == "LHP-VAL-041"
