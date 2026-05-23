"""Tests for the discovery spinner emitted by ``render_live_frame``.

Phase B of the CLI UX hardening replaces the previously empty first
Live frame (a ~5s silent terminal on the 100-pipeline fixture project)
with a ``Discovering`` spinner via the ``PhaseTracker`` active-phase
slot. These tests pin the contract:

* When ``Discovering`` is the active phase, the rendered panel body
  contains a Rich ``Spinner`` so the user sees motion within the first
  frame.
* Once ``Discovering`` completes, no Spinner remains in the body (the
  ``OverallProgress`` instance is still present, but it is a
  ``rich.progress.Progress``, not a bare Spinner).
* Per-pipeline progress is now driven by an ``OverallProgress``
  (``rich.progress.Progress``) instance and the panel title carries the
  total pipeline count, rather than a counter spinner inside the body.
"""

from rich.console import Group
from rich.panel import Panel
from rich.progress import Progress
from rich.spinner import Spinner

from lhp.cli.live_panel import (
    ActivityTail,
    HeaderContext,
    OverallProgress,
    PhaseTracker,
    PipelineRecord,
    render_live_frame,
)


def _has_spinner(node) -> bool:
    if isinstance(node, Spinner):
        return True
    if isinstance(node, Group):
        return any(_has_spinner(child) for child in node.renderables)
    return False


def _make_header(total: int = 0) -> HeaderContext:
    return HeaderContext(command_name="generate", env="dev", total_pipelines=total)


def test_render_live_frame_active_discovery_contains_spinner() -> None:
    """Active ``Discovering`` phase: first Live frame must show motion via a Spinner."""
    phase_tracker = PhaseTracker()
    phase_tracker.start("Discovering")
    overall = OverallProgress("Generating", total=0)
    activity = ActivityTail()

    # ``console_width=80`` keeps the body as a single-column ``Group`` so
    # the recursive ``_has_spinner`` walker can traverse it directly.
    panel = render_live_frame(
        phase_tracker,
        overall,
        activity,
        [],
        header_context=_make_header(0),
        elapsed_text="00:00",
        show_progress=False,
        console_width=80,
    )

    assert isinstance(panel, Panel)
    assert _has_spinner(panel.renderable) is True


def test_render_live_frame_after_discovery_completes_has_no_spinner() -> None:
    """Completed ``Discovering`` phase: discovery placeholder is replaced by the standard render path."""
    phase_tracker = PhaseTracker()
    phase_tracker.start("Discovering")
    # ``complete()`` renders by default, so the phase lands in
    # ``phase_tracker.completed`` regardless of duration.
    phase_tracker.complete("Discovering", success=True)
    overall = OverallProgress("Generating", total=0)
    activity = ActivityTail()

    panel = render_live_frame(
        phase_tracker,
        overall,
        activity,
        [],
        header_context=_make_header(0),
        elapsed_text="00:01",
        show_progress=False,
        console_width=80,
    )

    assert _has_spinner(panel.renderable) is False
    assert any(name == "Discovering" for name, _, _ in phase_tracker.completed)


def test_render_live_frame_with_records_shows_overall_progress() -> None:
    """Per-pipeline progress is rendered via the Progress widget rather than a bare counter spinner."""
    phase_tracker = PhaseTracker()
    phase_tracker.start("Discovering")
    phase_tracker.complete("Discovering", success=True)
    overall = OverallProgress("Generating", total=1)
    activity = ActivityTail()
    _ = PipelineRecord("p1")

    panel = render_live_frame(
        phase_tracker,
        overall,
        activity,
        [],
        header_context=_make_header(1),
        elapsed_text="00:02",
        show_progress=True,
        console_width=80,
    )

    body = panel.renderable
    assert isinstance(body, Group)
    progresses = [c for c in body.renderables if isinstance(c, Progress)]
    assert len(progresses) == 1
    # Panel title carries the pipeline count rather than an in-body
    # "N of M" spinner row. Title is a ``rich.text.Text`` (contract
    # change in V0.8.8 wave D); read ``.plain`` for substring assertions.
    assert "pipelines=1" in panel.title.plain
