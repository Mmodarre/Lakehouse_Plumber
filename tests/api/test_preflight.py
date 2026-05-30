"""Unit tests for the shared project-preflight helper (§9.24).

Drives the REAL composition in :func:`lhp.api._preflight._run_project_preflight`
end-to-end over an isolated copy of the e2e fixture project. Per §8.8, the
only things faked/injected are TRUE EXTERNAL boundaries:

* the filesystem (deleting the test-reporting provider file to trigger the
  file-existence check),
* the project's ``config/pipeline_config.yaml`` and added flowgroup YAML
  (to drive the bundle catalog/schema check and the duplicate-flowgroup
  check, respectively).

No LHP service is mocked — real flowgroup discovery, the real
``ConfigValidator``, the real ``TestReportingHookGenerator``, and the real
bundle facade run. The contract asserted: a duplicate yields ``LHP-VAL-009``;
a missing test-reporting provider yields the test-reporting code even with
``include_tests=False``; a missing catalog/schema with ``bundle_enabled``
yields ``LHP-CFG-026``; and ``_run_project_preflight`` NEVER raises.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterator, Tuple

import pytest

from lhp.api import LakehousePlumberApplicationFacade
from lhp.api._preflight import _run_project_preflight
from lhp.api.views import ValidationIssueView

_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"

_PIPELINE_CONFIG_REL = "config/pipeline_config.yaml"
_PROVIDER_REL = "py_functions/test_reporting_publisher.py"
_DUP_SOURCE_REL = (
    "pipelines/01_raw_ingestion/json_ingestions/part_ingestion.yaml"
)
_DUP_COPY_REL = (
    "pipelines/01_raw_ingestion/json_ingestions/part_ingestion_dup.yaml"
)


@pytest.fixture
def project_root(tmp_path: Path) -> Iterator[Path]:
    """Isolated deep copy of the e2e fixture, with cwd swapped to its root.

    LHP resolves several relative paths off ``Path.cwd()``, so the copy is
    made the working directory for the duration of the test.
    """
    root = tmp_path / "test_project"
    shutil.copytree(_FIXTURE_PATH, root)
    original_cwd = os.getcwd()
    os.chdir(root)
    try:
        yield root
    finally:
        os.chdir(original_cwd)


def _build_orchestrator(project_root: Path) -> object:
    """Construct the fully-wired orchestrator both facades hold."""
    facade = LakehousePlumberApplicationFacade.for_project(
        project_root,
        pipeline_config_path=_PIPELINE_CONFIG_REL,
        enforce_version=False,
    )
    return facade._orchestrator


def _codes(issues: Tuple[ValidationIssueView, ...]) -> set[str]:
    return {issue.code for issue in issues}


@pytest.mark.unit
class TestRunProjectPreflight:
    """Behavioural contract for the shared project-preflight helper."""

    def test_duplicate_flowgroup_yields_val_009(self, project_root: Path) -> None:
        """A duplicate (pipeline, flowgroup) yields exactly one LHP-VAL-009."""
        # External boundary: add a second YAML with the SAME pipeline+flowgroup.
        shutil.copyfile(
            project_root / _DUP_SOURCE_REL,
            project_root / _DUP_COPY_REL,
        )
        orchestrator = _build_orchestrator(project_root)

        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=False,
            include_tests=False,
        )

        val_009 = [i for i in issues if i.code == "LHP-VAL-009"]
        assert len(val_009) == 1
        assert "Duplicate" in val_009[0].title

    def test_missing_test_reporting_provider_fires_without_include_tests(
        self, project_root: Path
    ) -> None:
        """A missing provider file is a preflight failure even when
        include_tests is False (file-existence runs regardless of the gate)."""
        # External boundary: remove the provider module the fixture references.
        (project_root / _PROVIDER_REL).unlink()
        orchestrator = _build_orchestrator(project_root)

        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=False,
            include_tests=False,
        )

        test_reporting = [i for i in issues if i.code == "LHP-CFG-032"]
        assert len(test_reporting) == 1
        assert "test_reporting" in test_reporting[0].details

    def test_missing_provider_clean_when_present(self, project_root: Path) -> None:
        """Sanity guard: with the provider present and no bundle/duplicate
        perturbation, no test-reporting issue is raised."""
        orchestrator = _build_orchestrator(project_root)

        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=False,
            include_tests=False,
        )

        assert "LHP-CFG-032" not in _codes(issues)

    def test_missing_catalog_schema_yields_cfg_026_when_bundle_enabled(
        self, project_root: Path
    ) -> None:
        """With bundle_enabled, a pipeline missing catalog/schema yields
        an LHP-CFG-026 issue."""
        # External boundary: strip catalog/schema from the pipeline config so
        # every discovered pipeline fails the bundle catalog/schema preflight.
        (project_root / _PIPELINE_CONFIG_REL).write_text(
            "project_defaults:\n  serverless: true\n",
            encoding="utf-8",
        )
        orchestrator = _build_orchestrator(project_root)

        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=True,
            include_tests=False,
        )

        assert "LHP-CFG-026" in _codes(issues)

    def test_bundle_check_skipped_when_disabled(self, project_root: Path) -> None:
        """When bundle_enabled is False, the catalog/schema check does not
        run — no LHP-CFG-026 even with a broken pipeline config."""
        (project_root / _PIPELINE_CONFIG_REL).write_text(
            "project_defaults:\n  serverless: true\n",
            encoding="utf-8",
        )
        orchestrator = _build_orchestrator(project_root)

        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=False,
            include_tests=False,
        )

        assert "LHP-CFG-026" not in _codes(issues)

    def test_never_raises_across_all_failure_modes(
        self, project_root: Path
    ) -> None:
        """All three backend failures triggered at once: the helper composes
        them into issues and NEVER raises."""
        shutil.copyfile(
            project_root / _DUP_SOURCE_REL,
            project_root / _DUP_COPY_REL,
        )
        (project_root / _PROVIDER_REL).unlink()
        (project_root / _PIPELINE_CONFIG_REL).write_text(
            "project_defaults:\n  serverless: true\n",
            encoding="utf-8",
        )
        orchestrator = _build_orchestrator(project_root)

        # No pytest.raises wrapper: the call itself must not raise.
        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=True,
            include_tests=False,
        )

        codes = _codes(issues)
        assert "LHP-VAL-009" in codes
        assert "LHP-CFG-032" in codes
        assert "LHP-CFG-026" in codes

    def test_returns_tuple_of_issue_views(self, project_root: Path) -> None:
        """The return value is a tuple of ValidationIssueView (empty on a
        clean project with no bundle check)."""
        orchestrator = _build_orchestrator(project_root)

        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=False,
            include_tests=False,
        )

        assert isinstance(issues, tuple)
        assert all(isinstance(i, ValidationIssueView) for i in issues)
