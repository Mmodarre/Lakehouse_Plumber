"""``--sandbox`` wiring of ``ValidationFacade.validate_pipelines``.

Pins the validate stream's sandbox pre-pass on REAL temp projects (no mocks —
the facade, orchestrator, and engine are the production drivers):

1. **Preflight stance (§1.4 / §9.19)** — a failing sandbox pre-pass
   (missing ``.lhp/profile.yaml`` → ``LHP-IO-025``, environment not
   sandbox-enabled → ``LHP-CFG-065``, zero-match profile scope →
   ``LHP-VAL-064``) is a MODE error, not a pipeline finding: the stream
   yields :class:`ErrorEmitted` carrying the live :class:`LHPError` and THEN
   raises it — never one without the other, and never a terminal
   :class:`ValidationCompleted` / :class:`BatchValidationResponse`. The
   pre-pass sits AFTER the discover phase pair and BEFORE the preflight
   phase (mirroring the generate stream).
2. **Profile-driven scope (D5/D6)** — ``sandbox=True`` validates ONLY the
   pipelines the personal profile resolves to; the structured rewrite plan
   is threaded to the workers so validation sees the sandbox names (the
   run completes and the terminal batch covers only the scoped pipeline).
3. **Warning slot** — the rewrite plan's carried warnings (mixed-producer
   sink → ``LHP-VAL-065``) surface as
   ``WarningEmitted(category="sandbox")`` in the stream's post-validate
   warning slot.
4. **Inertness** — ``sandbox=False`` (the default) ignores an existing
   profile entirely: whole-project worklist, no sandbox-category warnings.

Tests import from :mod:`lhp.api` plus the public :mod:`lhp.errors` package —
no internal modules.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pytest
import yaml

from lhp.api import (
    BatchValidationResponse,
    ErrorEmitted,
    LakehousePlumberApplicationFacade,
    LHPEvent,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineStarted,
    ValidationCompleted,
    WarningEmitted,
)
from lhp.errors import LHPError


def _write_pipeline(project_root: Path, pipeline: str, table: str) -> None:
    """Write one clean single-flowgroup pipeline producing ``table``."""
    pdir = project_root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    actions = [
        {
            "name": f"load_{pipeline}",
            "type": "load",
            "target": f"v_{pipeline}_raw",
            "source": {
                "type": "cloudfiles",
                "path": "${landing_path}/" + pipeline,
                "format": "json",
            },
        },
        {
            "name": f"write_{pipeline}",
            "type": "write",
            "source": f"v_{pipeline}_raw",
            "write_target": {
                "type": "streaming_table",
                "catalog": "${catalog}",
                "schema": "${bronze_schema}",
                "table": table,
                "create_table": True,
            },
        },
    ]
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(
            {"pipeline": pipeline, "flowgroup": f"{pipeline}_fg", "actions": actions}, f
        )


def _project(
    tmp_path: Path,
    pipelines: dict[str, str],
    *,
    sandbox_block: Optional[dict] = None,
    profile: Optional[dict] = None,
) -> Path:
    """Build a real temp project. ``pipelines`` maps pipeline name → produced
    table. ``sandbox_block`` is the team policy for ``lhp.yaml``; ``profile``
    the personal ``.lhp/profile.yaml`` payload (``None`` = no profile)."""
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    lhp_yaml: dict = {"name": "validate_sandbox", "version": "1.0"}
    if sandbox_block is not None:
        lhp_yaml["sandbox"] = sandbox_block
    with open(project_root / "lhp.yaml", "w") as f:
        yaml.dump(lhp_yaml, f)
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )
    if profile is not None:
        lhp_dir = project_root / ".lhp"
        lhp_dir.mkdir(parents=True, exist_ok=True)
        with open(lhp_dir / "profile.yaml", "w") as f:
            yaml.dump({"sandbox": profile}, f)
    for name, table in pipelines.items():
        _write_pipeline(project_root, name, table)
    return project_root


@pytest.fixture
def facade_at(tmp_path: Path):
    """Build the real facade for a project root, restoring cwd afterwards."""
    created: list[str] = []

    def _build(project_root: Path) -> LakehousePlumberApplicationFacade:
        created.append(os.getcwd())
        os.chdir(project_root)
        return LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )

    yield _build
    if created:
        os.chdir(created[0])


def _collect_until_raise(
    stream,
) -> tuple[list[LHPEvent], Optional[LHPError]]:
    """Drain the stream, capturing events yielded BEFORE any LHPError raise."""
    events: list[LHPEvent] = []
    try:
        for event in stream:
            events.append(event)
    except LHPError as exc:
        return events, exc
    return events, None


def _assert_mode_error_stance(
    events: list[LHPEvent], error: Optional[LHPError], code: str
) -> None:
    """§1.4 / §9.19: ErrorEmitted yielded (with the SAME live error) before the
    raise; pre-pass placement after discover, before preflight; no terminal
    batch response (mode error, not a pipeline finding)."""
    assert error is not None, f"expected {code} to be raised"
    assert error.code == code
    # The event PRECEDES the raise: it is the last yielded event and carries
    # the identical exception instance.
    assert events, "no events yielded before the raise"
    assert isinstance(events[-1], ErrorEmitted)
    assert events[-1].lhp_error is error
    # Placement: discover phase pair completed, preflight never started.
    assert isinstance(events[0], OperationStarted)
    phases_started = [e.phase for e in events if isinstance(e, PhaseStarted)]
    phases_completed = [e.phase for e in events if isinstance(e, PhaseCompleted)]
    assert phases_started == ["discover"]
    assert phases_completed == ["discover"]
    # A mode error never folds into a BatchValidationResponse.
    assert not any(isinstance(e, ValidationCompleted) for e in events)


@pytest.mark.integration
class TestValidateSandboxPreflightStance:
    """Sandbox pre-pass failures: ErrorEmitted + raise, from the validate
    entry point (the preflight coverage mirrored from the generate side)."""

    def test_missing_profile_emits_error_then_raises_io_025(self, tmp_path, facade_at):
        project_root = _project(tmp_path, {"p_one": "p_one_table"}, profile=None)
        facade = facade_at(project_root)

        events, error = _collect_until_raise(
            facade.validation.validate_pipelines(env="dev", sandbox=True)
        )

        _assert_mode_error_stance(events, error, "LHP-IO-025")

    def test_disallowed_env_emits_error_then_raises_cfg_065(self, tmp_path, facade_at):
        project_root = _project(
            tmp_path,
            {"p_one": "p_one_table"},
            sandbox_block={"allowed_envs": ["sandbox_dev"]},
            profile={"namespace": "alice", "pipelines": ["p_one"]},
        )
        facade = facade_at(project_root)

        events, error = _collect_until_raise(
            facade.validation.validate_pipelines(env="dev", sandbox=True)
        )

        _assert_mode_error_stance(events, error, "LHP-CFG-065")

    def test_zero_match_scope_emits_error_then_raises_val_064(
        self, tmp_path, facade_at
    ):
        project_root = _project(
            tmp_path,
            {"p_one": "p_one_table"},
            profile={"namespace": "alice", "pipelines": ["no_such_pipeline_*"]},
        )
        facade = facade_at(project_root)

        events, error = _collect_until_raise(
            facade.validation.validate_pipelines(env="dev", sandbox=True)
        )

        _assert_mode_error_stance(events, error, "LHP-VAL-064")


@pytest.mark.integration
class TestValidateSandboxScopedRun:
    """Happy path: profile scope drives the worklist; the structured rewrite
    plan threads to the workers; the run completes."""

    def test_scoped_run_validates_only_in_scope_pipeline(self, tmp_path, facade_at):
        project_root = _project(
            tmp_path,
            {"p_one": "p_one_table", "p_two": "p_two_table"},
            profile={"namespace": "alice", "pipelines": ["p_one"]},
        )
        facade = facade_at(project_root)

        events, error = _collect_until_raise(
            facade.validation.validate_pipelines(env="dev", sandbox=True)
        )

        assert error is None
        assert not any(isinstance(e, ErrorEmitted) for e in events)
        # Only the in-scope pipeline runs (D6: profile pipelines only).
        started = [e.pipeline for e in events if isinstance(e, PipelineStarted)]
        assert started == ["p_one"]
        # All three phases ran (pre-pass succeeded between discover and
        # preflight).
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "validate",
        ]
        # Terminal batch covers ONLY the scoped pipeline and validated clean
        # with the structured sandbox renames applied in the workers.
        assert isinstance(events[-1], ValidationCompleted)
        response = events[-1].response
        assert isinstance(response, BatchValidationResponse)
        assert response.success is True
        assert response.validated_pipelines == ("p_one",)
        assert set(response.pipeline_responses) == {"p_one"}

    def test_mixed_producer_sink_emits_sandbox_warning_val_065(
        self, tmp_path, facade_at
    ):
        # Both pipelines produce the SAME resolved table; scoping only p_one
        # makes it a mixed-producer sink → ONE folded LHP-VAL-065 carried on
        # the rewrite plan and re-emitted in the post-validate warning slot.
        project_root = _project(
            tmp_path,
            {"p_one": "shared_table", "p_two": "shared_table"},
            profile={"namespace": "alice", "pipelines": ["p_one"]},
        )
        facade = facade_at(project_root)

        events, error = _collect_until_raise(
            facade.validation.validate_pipelines(env="dev", sandbox=True)
        )

        assert error is None
        sandbox_warnings = [
            e
            for e in events
            if isinstance(e, WarningEmitted) and e.category == "sandbox"
        ]
        assert len(sandbox_warnings) == 1
        warning = sandbox_warnings[0]
        assert warning.code == "LHP-VAL-065"
        assert "p_two" in warning.message
        # The warning is non-fatal: the scoped run still completes clean.
        assert isinstance(events[-1], ValidationCompleted)
        assert events[-1].response.success is True
        assert events[-1].response.validated_pipelines == ("p_one",)


@pytest.mark.integration
class TestValidateSandboxOff:
    """``sandbox=False`` (default) is byte-identical pre-sandbox behavior."""

    def test_default_run_ignores_profile_and_validates_whole_project(
        self, tmp_path, facade_at
    ):
        # The profile exists and scopes p_one only — a non-sandbox run must
        # ignore it: whole-project worklist, no sandbox-category warnings.
        project_root = _project(
            tmp_path,
            {"p_one": "p_one_table", "p_two": "p_two_table"},
            profile={"namespace": "alice", "pipelines": ["p_one"]},
        )
        facade = facade_at(project_root)

        events, error = _collect_until_raise(
            facade.validation.validate_pipelines(env="dev")
        )

        assert error is None
        assert not any(isinstance(e, ErrorEmitted) for e in events)
        started = sorted(e.pipeline for e in events if isinstance(e, PipelineStarted))
        assert started == ["p_one", "p_two"]
        assert not any(
            isinstance(e, WarningEmitted) and e.category == "sandbox" for e in events
        )
        assert isinstance(events[-1], ValidationCompleted)
        response = events[-1].response
        assert response.success is True
        assert sorted(response.validated_pipelines) == ["p_one", "p_two"]
