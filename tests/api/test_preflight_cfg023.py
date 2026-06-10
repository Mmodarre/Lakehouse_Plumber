"""LHP-CFG-023 (missing ``pipeline_config`` for bundle validation) preflight.

Pins the §9.24 contract for the bundle-enabled-but-no-``pipeline_config``
case: :func:`lhp.api._preflight._run_project_preflight` surfaces EXACTLY ONE
``LHP-CFG-023`` issue carrying the proper "pipeline_config is required for
bundle validation" title — not the generic "Bundle catalog/schema validation
failed" title that the catalog/schema converter (:func:`_check_bundle_assets`)
would otherwise re-stamp onto the ``LHP-CFG-023``-coded
``validate_bundle_assets`` result.

Drives the REAL composition end-to-end over an isolated copy of the e2e
fixture project (mirroring :mod:`tests.api.test_preflight`); the only thing
varied is whether the facade is constructed WITH or WITHOUT a
``pipeline_config_path``.

Surfacing of the single CFG-023 issue differs by path (§9.24): the generate
stream raises it after one ``ErrorEmitted``; the validate stream folds it onto
a non-success ``BatchValidationResponse`` (NO ``ErrorEmitted``/raise — see the
in-test note). Both carry the ``LHP-CFG-023`` code.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterator, Tuple

import pytest

from lhp.api import LakehousePlumberApplicationFacade
from lhp.api._preflight import _run_project_preflight
from lhp.api.events import ErrorEmitted, ValidationCompleted
from lhp.api.views import ValidationIssueView
from lhp.errors import LHPError

_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"

_PIPELINE_CONFIG_REL = "config/pipeline_config.yaml"
_CFG_023 = "LHP-CFG-023"
_PIPELINE_CONFIG_TITLE = "pipeline_config is required for bundle validation"


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


def _facade_without_pipeline_config(
    project_root: Path,
) -> LakehousePlumberApplicationFacade:
    """Facade whose orchestrator has ``pipeline_config_path is None``."""
    return LakehousePlumberApplicationFacade.for_project(
        project_root,
        enforce_version=False,
    )


def _facade_with_pipeline_config(
    project_root: Path,
) -> LakehousePlumberApplicationFacade:
    """Facade whose orchestrator HAS a configured ``pipeline_config_path``."""
    return LakehousePlumberApplicationFacade.for_project(
        project_root,
        pipeline_config_path=_PIPELINE_CONFIG_REL,
        enforce_version=False,
    )


def _codes(issues: Tuple[ValidationIssueView, ...]) -> set[str]:
    return {issue.code for issue in issues}


@pytest.mark.unit
class TestPreflightCfg023Direct:
    """Direct :func:`_run_project_preflight` contract for CFG-023."""

    def test_no_pipeline_config_yields_exactly_one_cfg_023(
        self, project_root: Path
    ) -> None:
        """bundle_enabled + no pipeline_config_path → exactly one CFG-023 issue
        carrying the pipeline_config title (NOT the generic bundle title)."""
        facade = _facade_without_pipeline_config(project_root)
        orchestrator = facade._orchestrator
        # Precondition the whole behaviour hinges on.
        assert orchestrator.pipeline_config_path is None

        issues = _run_project_preflight(
            orchestrator,
            env="dev",
            bundle_enabled=True,
            include_tests=False,
        )

        cfg_023 = [i for i in issues if i.code == _CFG_023]
        assert len(cfg_023) == 1
        # Clean fixture: the CFG-023 is the ONLY issue (no duplicate / test
        # reporting perturbation), so it is issues[0] — the entry wrappers
        # surface.
        assert len(issues) == 1
        assert issues[0].code == _CFG_023
        # The proper title, not "Bundle catalog/schema validation failed".
        assert cfg_023[0].title == _PIPELINE_CONFIG_TITLE
        assert "catalog/schema validation failed" not in cfg_023[0].title.lower()

    def test_never_raises(self, project_root: Path) -> None:
        """Preflight stays never-raises on the no-pipeline_config bundle path."""
        facade = _facade_without_pipeline_config(project_root)

        # No pytest.raises wrapper: the call itself must not raise.
        issues = _run_project_preflight(
            facade._orchestrator,
            env="dev",
            bundle_enabled=True,
            include_tests=False,
        )

        assert isinstance(issues, tuple)
        assert _CFG_023 in _codes(issues)

    def test_with_pipeline_config_yields_no_cfg_023(self, project_root: Path) -> None:
        """Negative: when a pipeline_config IS configured, the catalog/schema
        branch runs instead — no CFG-023 is surfaced."""
        facade = _facade_with_pipeline_config(project_root)
        assert facade._orchestrator.pipeline_config_path is not None

        issues = _run_project_preflight(
            facade._orchestrator,
            env="dev",
            bundle_enabled=True,
            include_tests=False,
        )

        assert _CFG_023 not in _codes(issues)


@pytest.mark.integration
class TestPreflightCfg023Stream:
    """Stream-level surfacing of the CFG-023 preflight issue (§9.24)."""

    def test_generate_emits_error_then_raises_cfg_023(
        self, project_root: Path, tmp_path: Path
    ) -> None:
        """The generate stream raises the CFG-023 after one ``ErrorEmitted``."""
        facade = _facade_without_pipeline_config(project_root)
        output_dir = tmp_path / "generated"

        collected: list = []
        gen = facade.generate_pipelines(
            env="dev",
            output_dir=output_dir,
            bundle_enabled=True,
            include_tests=False,
        )
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)

        assert exc_info.value.code == _CFG_023
        assert exc_info.value.title == _PIPELINE_CONFIG_TITLE

        error_events = [e for e in collected if isinstance(e, ErrorEmitted)]
        assert len(error_events) == 1
        assert error_events[0].lhp_error.code == _CFG_023
        # ErrorEmitted is the terminal event before the raise (§1.4 rendezvous).
        assert isinstance(collected[-1], ErrorEmitted)

    def test_validate_folds_cfg_023_onto_failed_batch(self, project_root: Path) -> None:
        """The validate stream surfaces the SAME CFG-023, but per §9.24 it folds
        it onto a non-success ``BatchValidationResponse`` carried by a terminal
        ``ValidationCompleted`` — it does NOT emit ``ErrorEmitted`` or raise.

        NOTE: this differs from the task brief's stated expectation (which said
        validate also emits ``ErrorEmitted`` and raises). The DELIVERED §9.24
        contract — pinned by ``test_preflight``/``_validation_facade`` — folds
        preflight issues onto a batch DTO for the validate path; only generate
        raises. This test asserts the real contract and the same CFG-023 code.
        """
        facade = _facade_without_pipeline_config(project_root)

        collected: list = []
        for event in facade.validate_pipelines(
            env="dev",
            bundle_enabled=True,
            include_tests=False,
        ):
            collected.append(event)

        # No ErrorEmitted / raise on the validate preflight-folding path.
        assert not any(isinstance(e, ErrorEmitted) for e in collected)

        completed = [e for e in collected if isinstance(e, ValidationCompleted)]
        assert len(completed) == 1
        response = completed[0].response
        assert response.success is False
        assert response.error_code == _CFG_023
