"""Pipeline-config load errors keep their own code, category and title in preflight.

The bundle catalog/schema check loads ``pipeline_config.yaml``, so every error
the loader raises reaches :func:`lhp.api._preflight._check_bundle_assets`. That
converter used to re-stamp all of them with the "Bundle catalog/schema
validation failed" title, the catalog/schema suggestions and the CONFIG
category, so ``LHP-VAL-009`` surfaced as ``LHP-CFG-009`` and the #351
``LHP-CFG-072`` errors never said which key was wrong. Only the catalog/schema
failure itself (``LHP-CFG-026``) keeps that title.

Drives the real composition over an isolated copy of the e2e fixture, as
:mod:`tests.api.test_preflight_cfg023` does.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterator, List

import pytest

from lhp.api import LakehousePlumberApplicationFacade
from lhp.api._preflight import _run_project_preflight
from lhp.api.views import ValidationIssueView

_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"
_PIPELINE_CONFIG_REL = "config/pipeline_config.yaml"
_GENERIC_TITLE = "Bundle catalog/schema validation failed"


@pytest.fixture
def project_root(tmp_path: Path) -> Iterator[Path]:
    root = tmp_path / "test_project"
    shutil.copytree(_FIXTURE_PATH, root)
    original_cwd = os.getcwd()
    os.chdir(root)
    try:
        yield root
    finally:
        os.chdir(original_cwd)


def _add_pipeline_document(project_root: Path, body: str) -> None:
    with (project_root / _PIPELINE_CONFIG_REL).open("a", encoding="utf-8") as fh:
        fh.write(f"\n---\npipeline: acmi_edw_silver\n{body}")


def _issues_with_code(project_root: Path, code: str) -> List[ValidationIssueView]:
    facade = LakehousePlumberApplicationFacade.for_project(
        project_root,
        pipeline_config_path=_PIPELINE_CONFIG_REL,
        enforce_version=False,
    )
    issues = _run_project_preflight(
        facade._orchestrator,
        env="dev",
        bundle_enabled=True,
        include_tests=False,
    )
    return [issue for issue in issues if issue.code == code]


@pytest.mark.unit
class TestPreflightConfigLoadErrors:
    def test_generated_key_error_keeps_its_title_and_details(self, project_root):
        _add_pipeline_document(project_root, "root_path: /Workspace/elsewhere\n")

        [issue] = _issues_with_code(project_root, "LHP-CFG-072")

        assert issue.category == "CFG"
        assert issue.title == "'root_path' cannot be set in pipeline_config"
        assert "sys.path" in (issue.details or "")
        assert not any("catalog" in s for s in issue.suggestions)

    def test_validation_error_keeps_its_category(self, project_root):
        _add_pipeline_document(project_root, "edition: BOGUS\n")

        [issue] = _issues_with_code(project_root, "LHP-VAL-009")

        assert issue.category == "VAL"
        assert issue.title == "Invalid pipeline edition"
        assert _issues_with_code(project_root, "LHP-CFG-009") == []

    def test_catalog_schema_failure_keeps_generic_title(self, project_root):
        _add_pipeline_document(project_root, 'schema: ""\n')

        [issue] = _issues_with_code(project_root, "LHP-CFG-026")

        assert issue.title == _GENERIC_TITLE
        assert any("catalog" in s for s in issue.suggestions)
