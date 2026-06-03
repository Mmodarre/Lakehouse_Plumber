"""Tests for :class:`LogRenderer` — the CI / non-TTY event-stream sink.

Verifies the three load-bearing properties:

- One stderr line per *distinct* rendered event (warnings deduped by
  ``(code, file)``); record-only / begin-marker callbacks emit nothing.
- ``rich.live.Live`` is never constructed (the log renderer must not open
  a live region).
- stdout stays empty — all output is on the injected stderr ``Console``.

A non-terminal ``Console`` writing to an in-memory ``StringIO`` is injected
into the renderer, mirroring ``tests.conftest.capture_lhp_console``.
"""

from __future__ import annotations

import io
from typing import Iterator, List

from rich.console import Console

from lhp.api.events import (
    LHPEvent,
    PhaseCompleted,
    PipelineCompleted,
    PipelineFailed,
    WarningEmitted,
)
from lhp.cli.presenters.event_stream._event_dispatch import drive
from lhp.cli.presenters.event_stream.log_renderer import LogRenderer
from tests.cli.presenters.event_stream._fixtures import (
    clean_generate_stream,
    deduped_warnings_stream,
    one_failure_stream,
)


def _make_renderer(*, show_details: bool = False) -> tuple[LogRenderer, io.StringIO]:
    """A ``LogRenderer`` wired to a deterministic non-terminal stderr Console."""
    buf = io.StringIO()
    console = Console(
        file=buf, stderr=True, force_terminal=False, no_color=True, width=100
    )
    return LogRenderer(console, show_details=show_details), buf


def _expected_rendered_line_count(events: List[LHPEvent]) -> int:
    """Independently compute the renderer's printed-line count from the stream.

    Closed rule (mirrors ``LogRenderer``): one line per ``PhaseCompleted``,
    ``PipelineCompleted``, and ``PipelineFailed``, plus one line per distinct
    ``(code, file)`` warning. Begin-markers, ``OperationStarted``,
    ``ErrorEmitted`` and the terminal emit no line.
    """
    count = 0
    seen_warning_keys = set()
    for event in events:
        if isinstance(event, (PhaseCompleted, PipelineCompleted, PipelineFailed)):
            count += 1
        elif isinstance(event, WarningEmitted):
            file_str = str(event.file) if event.file is not None else None
            key = (event.code, file_str)
            if key not in seen_warning_keys:
                seen_warning_keys.add(key)
                count += 1
    return count


def _nonempty_line_count(buf: io.StringIO) -> int:
    """Count non-blank rendered lines in the captured stderr buffer."""
    return len([line for line in buf.getvalue().splitlines() if line.strip()])


def test_begin_is_a_noop_no_output_no_live():
    # The log renderer paints nothing before the stream opens: begin() must
    # emit no line and (like the rest of the log renderer) open no Live region.
    renderer, buf = _make_renderer()

    renderer.begin()

    assert _nonempty_line_count(buf) == 0
    assert not hasattr(renderer, "_live")


def test_clean_stream_prints_one_line_per_distinct_event():
    events = clean_generate_stream()
    renderer, buf = _make_renderer()

    drive(iter(events), renderer)

    assert _nonempty_line_count(buf) == _expected_rendered_line_count(events)


def test_failure_stream_prints_one_line_per_distinct_event():
    events = one_failure_stream()
    renderer, buf = _make_renderer()

    drive(iter(events), renderer)

    # The PipelineFailed line is among the rendered lines and is recorded.
    assert _nonempty_line_count(buf) == _expected_rendered_line_count(events)
    assert any(f.code == "LHP-VAL-021" for f in renderer.outcome.failures)


def test_repeated_warnings_collapse_to_one_line_each():
    # Per-file warning lines are emitted only with show_details; this test
    # exercises that path so the dedup-on-(code, file) collapse is observable.
    events = deduped_warnings_stream()
    renderer, buf = _make_renderer(show_details=True)

    drive(iter(events), renderer)

    # Four WarningEmitted events, two distinct (code, file) pairs -> 2 lines.
    assert _expected_rendered_line_count(events) < len(events)
    assert _nonempty_line_count(buf) == _expected_rendered_line_count(events)
    assert len(renderer.outcome.warnings) == 2
    codes = {w.code for w in renderer.outcome.warnings}
    assert codes == {"LHP-DEPR-001", "LHP-DEPR-002"}


def test_warnings_suppressed_by_default_but_still_accumulated():
    """Default (no show_details): no per-file warning line, count preserved.

    The renderer must keep accumulating every distinct ``(code, file)`` into
    ``outcome.warnings`` (the counts banner and the summary's per-code rollup
    read it) while printing none of them, so a project with thousands of
    warnings is not buried under one line per file.
    """
    events = deduped_warnings_stream()
    renderer, buf = _make_renderer()  # show_details=False

    drive(iter(events), renderer)

    rendered = buf.getvalue()
    # No warning surfaced inline: neither code nor the warning glyph is printed.
    assert "LHP-DEPR-001" not in rendered
    assert "LHP-DEPR-002" not in rendered
    assert "!" not in rendered
    # Only the non-warning rendered events (PipelineCompleted + PhaseCompleted).
    assert _nonempty_line_count(buf) == 2
    # Accumulation is untouched: both distinct warnings are still recorded.
    assert len(renderer.outcome.warnings) == 2
    assert {w.code for w in renderer.outcome.warnings} == {
        "LHP-DEPR-001",
        "LHP-DEPR-002",
    }


def test_live_is_never_constructed(monkeypatch):
    """Driving a stream through the log renderer must not open a rich Live."""
    import rich.live

    def _boom(self, *args, **kwargs):
        raise AssertionError("LogRenderer must not construct rich.live.Live")

    monkeypatch.setattr(rich.live.Live, "__init__", _boom)

    renderer, _ = _make_renderer()
    # Exercise every printing path; if any constructed a Live, _boom fires.
    drive(iter(clean_generate_stream()), renderer)
    drive(iter(one_failure_stream()), renderer)
    drive(iter(deduped_warnings_stream()), renderer)


def test_stdout_stays_empty():
    """All output is on the injected stderr Console; real stdout is untouched."""
    import contextlib

    events = clean_generate_stream()
    renderer, _ = _make_renderer()

    stdout_buf = io.StringIO()
    with contextlib.redirect_stdout(stdout_buf):
        drive(iter(events), renderer)

    assert stdout_buf.getvalue() == ""


def test_on_error_records_without_printing_a_panel():
    """``on_error`` flips ``errored`` and records a code, but prints no panel.

    Drives a manual stream that ends in ``on_error`` (no raise here — the
    raise path is covered by the dispatch tests); asserts the single
    rendered line is the phase line, not an error panel, and that the
    outcome reflects the error.
    """
    from lhp.api.events import OperationStarted, PhaseStarted

    class _StubError:
        code = "LHP-IO-001"

    def _stream() -> Iterator[LHPEvent]:
        from lhp.api.events import ErrorEmitted

        yield OperationStarted(operation_name="generate", env="dev")
        yield PhaseStarted(phase="generate")
        yield PhaseCompleted(phase="generate", duration_s=0.1, success=False)
        yield ErrorEmitted(lhp_error=_StubError())

    renderer, buf = _make_renderer()
    drive(_stream(), renderer)

    # Only the PhaseCompleted produced a line; on_error printed nothing.
    assert _nonempty_line_count(buf) == 1
    outcome = renderer.outcome
    assert outcome.errored is True
    assert any(f.code == "LHP-IO-001" for f in outcome.failures)
