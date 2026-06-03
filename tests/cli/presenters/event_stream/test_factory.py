"""Tests for :mod:`...event_stream.renderer_factory`.

Covers the renderer-selection matrix (decided once on ``console.is_terminal``,
overridable by ``no_progress`` / ``CI`` / ``LHP_NO_PROGRESS``) and the
``render`` entry point on both the clean-success and failure-rendezvous
streams.

This is a test module (not under ``cli/presenters/**``), so it MAY import
``lhp.errors`` to narrow the expected propagating exception — the factory
under test never does (sole-bridge invariant, §9.5).
"""

from __future__ import annotations

import io
from collections import Counter

import pytest
from rich.console import Console

import lhp.cli.presenters.event_stream.renderer_factory as factory
from lhp.api import (
    BatchValidationResponse,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    ValidationCompleted,
    ValidationResponse,
)
from lhp.api.views import ValidationIssueView
from lhp.cli.presenters.event_stream._model import RenderOptions, RunHeader, RunOutcome
from lhp.cli.presenters.event_stream.live_renderer import LiveRenderer
from lhp.cli.presenters.event_stream.log_renderer import LogRenderer
from lhp.cli.presenters.event_stream.renderer_factory import render, select_renderer
from lhp.errors import LHPError
from tests.cli.presenters.event_stream._fixtures import (
    clean_generate_stream,
    error_raise_stream,
)


def _console(*, terminal: bool) -> Console:
    """A Console whose ``is_terminal`` is pinned ``terminal``."""
    return Console(
        file=io.StringIO(),
        force_terminal=True if terminal else None,
        force_interactive=False,
        width=120,
        height=24,
        color_system=None,
    )


def _header() -> RunHeader:
    return RunHeader(command="generate", env="dev", pipeline_count=2)


@pytest.fixture(autouse=True)
def _clear_progress_env(monkeypatch):
    """Neutralise CI / LHP_NO_PROGRESS so the matrix controls them explicitly.

    The host may run inside CI (``CI=1``), which would otherwise force the
    LogRenderer for every case. Each matrix case re-sets only what it needs.
    """
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("LHP_NO_PROGRESS", raising=False)


# ---------------------------------------------------------------------------
# Selection matrix
# ---------------------------------------------------------------------------
def test_non_terminal_selects_log_renderer():
    console = _console(terminal=False)
    err_console = _console(terminal=False)
    assert console.is_terminal is False

    renderer = select_renderer(_header(), console=console, err_console=err_console)
    assert isinstance(renderer, LogRenderer)


def test_terminal_selects_live_renderer():
    console = _console(terminal=True)
    err_console = _console(terminal=False)
    assert console.is_terminal is True

    renderer = select_renderer(_header(), console=console, err_console=err_console)
    assert isinstance(renderer, LiveRenderer)


def test_terminal_with_no_progress_selects_log_renderer():
    console = _console(terminal=True)
    err_console = _console(terminal=False)

    renderer = select_renderer(
        _header(), no_progress=True, console=console, err_console=err_console
    )
    assert isinstance(renderer, LogRenderer)


def test_terminal_with_ci_env_selects_log_renderer(monkeypatch):
    monkeypatch.setenv("CI", "1")
    console = _console(terminal=True)
    err_console = _console(terminal=False)

    renderer = select_renderer(_header(), console=console, err_console=err_console)
    assert isinstance(renderer, LogRenderer)


def test_terminal_with_lhp_no_progress_env_selects_log_renderer(monkeypatch):
    monkeypatch.setenv("LHP_NO_PROGRESS", "1")
    console = _console(terminal=True)
    err_console = _console(terminal=False)

    renderer = select_renderer(_header(), console=console, err_console=err_console)
    assert isinstance(renderer, LogRenderer)


# ---------------------------------------------------------------------------
# render(): success
# ---------------------------------------------------------------------------
def test_render_clean_generate_returns_outcome_with_terminal_response():
    console = _console(terminal=True)
    err_console = _console(terminal=False)
    events = clean_generate_stream()
    terminal_response = events[-1].response

    outcome = render(
        iter(events),
        _header(),
        options=RenderOptions(),
        console=console,
        err_console=err_console,
    )

    assert isinstance(outcome, RunOutcome)
    assert outcome.response is terminal_response
    assert outcome.errored is False


def test_render_clean_generate_non_terminal_path_returns_outcome():
    # The LogRenderer branch must also fold the terminal response into the
    # returned RunOutcome.
    console = _console(terminal=False)
    err_console = _console(terminal=False)
    events = clean_generate_stream()
    terminal_response = events[-1].response

    outcome = render(
        iter(events),
        _header(),
        options=RenderOptions(),
        console=console,
        err_console=err_console,
    )

    assert outcome.response is terminal_response
    assert outcome.errored is False


# ---------------------------------------------------------------------------
# render(): failure-rendezvous re-raises after teardown
# ---------------------------------------------------------------------------
def test_render_error_stream_reraises_after_teardown(monkeypatch):
    console = _console(terminal=True)
    err_console = _console(terminal=False)

    # Capture the renderer the factory selects so teardown can be asserted:
    # render() does not return it on the raising path.
    captured: dict[str, LiveRenderer] = {}
    real_select = select_renderer

    def _spy(header, **kwargs):
        renderer = real_select(header, **kwargs)
        captured["renderer"] = renderer  # type: ignore[assignment]
        return renderer

    monkeypatch.setattr(factory, "select_renderer", _spy)

    with pytest.raises(LHPError):
        render(
            error_raise_stream(),
            _header(),
            options=RenderOptions(),
            console=console,
            err_console=err_console,
        )

    renderer = captured["renderer"]
    assert isinstance(renderer, LiveRenderer)
    # Factory tore the Live down before re-raising (idempotent with the
    # renderer's own on_error teardown) so error_boundary renders on a
    # clean stderr.
    assert renderer._started is False
    assert renderer._live.is_started is False


# ---------------------------------------------------------------------------
# render(): validate folds terminal-response issues into RunOutcome (D0)
# ---------------------------------------------------------------------------
def _validate_header() -> RunHeader:
    return RunHeader(command="validate", env="prod", pipeline_count=3)


def _issue(*, severity, code, title, pipeline, flowgroup=None, file=None):
    from pathlib import Path

    category = code.split("-")[1] if code.count("-") >= 2 else "VAL"
    return ValidationIssueView(
        code=code,
        category=category,
        severity=severity,
        title=title,
        pipeline_name=pipeline,
        flowgroup_name=flowgroup,
        file_path=Path(file) if file else None,
    )


def _validate_stream_with_issues():
    """A validate stream modeling the REAL facade shape (regression for Fix-V).

    Validate never raises, but it DOES emit a ``PipelineFailed`` per failing
    pipeline (for live progress) AND folds every issue, fully attributed, into
    the terminal ``BatchValidationResponse`` — so each failing pipeline appears
    on BOTH surfaces. This builder reproduces that overlap exactly: a
    ``PipelineStarted`` + ``PipelineFailed`` pair for ``bronze`` and ``silver``
    (the failing pipelines), a ``PipelineStarted`` + ``PipelineCompleted`` pair
    for the clean ``gold``, then a terminal carrying one error-issue per
    failing pipeline (plus a warning). ``render`` must reconcile the two so
    each issue yields EXACTLY ONE ``FailureLine`` (not two — the double-count
    bug Fix-V closes).
    """
    err_a = _issue(
        severity="error",
        code="LHP-VAL-021",
        title="Missing target",
        pipeline="bronze",
        flowgroup="ingest",
        file="ingest.yaml",
    )
    err_b = _issue(
        severity="error",
        code="LHP-VAL-005",
        title="Duplicate action name",
        pipeline="silver",
        flowgroup="enrich",
        file="enrich.yaml",
    )
    warn = _issue(
        severity="warning",
        code="LHP-DEPR-002",
        title="deprecated field 'bar'",
        pipeline="silver",
        flowgroup="enrich",
        file="enrich.yaml",
    )
    response = BatchValidationResponse(
        success=False,
        pipeline_responses={
            "bronze": ValidationResponse(
                success=False, issues=(err_a,), validated_pipelines=("bronze",)
            ),
            "silver": ValidationResponse(
                success=False, issues=(err_b, warn), validated_pipelines=("silver",)
            ),
            "gold": ValidationResponse(
                success=True, issues=(), validated_pipelines=("gold",)
            ),
        },
        total_errors=2,
        total_warnings=1,
        validated_pipelines=("bronze", "silver", "gold"),
    )
    return [
        OperationStarted(operation_name="validate", env="prod"),
        PhaseStarted(phase="validate"),
        # Per-pipeline progress: each failing pipeline emits PipelineFailed
        # (event-derived, unattributed) AND rides the terminal response below.
        PipelineStarted(pipeline="bronze"),
        PipelineFailed(pipeline="bronze", code="LHP-VAL-021", message="Missing target"),
        PipelineStarted(pipeline="silver"),
        PipelineFailed(
            pipeline="silver", code="LHP-VAL-005", message="Duplicate action name"
        ),
        PipelineStarted(pipeline="gold"),
        PipelineCompleted(pipeline="gold", duration_s=0.02, files_written=0),
        PhaseCompleted(phase="validate", duration_s=0.05, success=False),
        ValidationCompleted(response=response),
    ]


def _batch_level_validate_stream():
    """A validate stream whose terminal failure lives only at batch level.

    Models a preflight (e.g. CFG-023) folded for the non-raising validate
    path: success=False with an error_code/error_message but no per-pipeline
    issues. render() must synthesize exactly one FailureLine.
    """
    response = BatchValidationResponse(
        success=False,
        pipeline_responses={},
        total_errors=1,
        total_warnings=0,
        validated_pipelines=(),
        error_message="Error [LHP-CFG-023]: Project preflight failed",
        error_code="LHP-CFG-023",
    )
    return [
        OperationStarted(operation_name="validate", env="prod"),
        PhaseStarted(phase="validate"),
        PhaseCompleted(phase="validate", duration_s=0.01, success=False),
        ValidationCompleted(response=response),
    ]


@pytest.mark.parametrize("terminal", [True, False])
def test_render_validate_folds_issue_failures_and_warnings(terminal):
    console = _console(terminal=terminal)
    err_console = _console(terminal=False)

    outcome = render(
        iter(_validate_stream_with_issues()),
        _validate_header(),
        options=RenderOptions(),
        console=console,
        err_console=err_console,
    )

    assert isinstance(outcome, RunOutcome)
    assert outcome.errored is False
    # Regression for Fix-V: bronze + silver each emitted a PipelineFailed
    # (event-derived) AND ride the terminal response (terminal-derived). The
    # terminal set REPLACES the event set, so there are EXACTLY TWO
    # FailureLines — one per failing pipeline — not four (the double-count
    # bug would have appended, yielding 2 unattributed + 2 attributed = 4).
    assert len(outcome.failures) == 2
    # Each failing pipeline appears exactly once.
    failures_per_pipeline = Counter(f.pipeline for f in outcome.failures)
    assert failures_per_pipeline == {"bronze": 1, "silver": 1}
    # The surviving FailureLines carry the TERMINAL attribution (flowgroup +
    # file), not the bare event-derived shape (which has flowgroup/file None).
    by_code = {f.code: f for f in outcome.failures}
    assert set(by_code) == {"LHP-VAL-021", "LHP-VAL-005"}
    assert by_code["LHP-VAL-021"].pipeline == "bronze"
    assert by_code["LHP-VAL-021"].flowgroup == "ingest"
    assert by_code["LHP-VAL-021"].file == "ingest.yaml"
    assert by_code["LHP-VAL-021"].message == "Missing target"
    assert by_code["LHP-VAL-005"].pipeline == "silver"
    assert by_code["LHP-VAL-005"].flowgroup == "enrich"
    assert by_code["LHP-VAL-005"].file == "enrich.yaml"
    # The warning-issue is folded into warnings (and surfaces exactly once).
    warn_codes = Counter(w.code for w in outcome.warnings)
    assert warn_codes["LHP-DEPR-002"] == 1


def test_render_validate_batch_level_error_yields_one_failure():
    console = _console(terminal=False)
    err_console = _console(terminal=False)

    outcome = render(
        iter(_batch_level_validate_stream()),
        _validate_header(),
        options=RenderOptions(),
        console=console,
        err_console=err_console,
    )

    assert len(outcome.failures) == 1
    only = outcome.failures[0]
    assert only.code == "LHP-CFG-023"
    assert only.pipeline == ""
    assert "LHP-CFG-023" in only.message


def test_render_clean_generate_yields_no_failures():
    # The generate terminal (BatchGenerationResponse) is NOT folded: a clean
    # generate stream stays at zero failures (no synthesis from the response).
    console = _console(terminal=False)
    err_console = _console(terminal=False)

    outcome = render(
        iter(clean_generate_stream()),
        _header(),
        options=RenderOptions(),
        console=console,
        err_console=err_console,
    )

    assert outcome.failures == ()
    assert outcome.warnings == ()
