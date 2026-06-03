"""Post-run summary block for the single-stream generate / validate commands.

Renders the terminal summary the CLI prints to **stderr** after an event
stream has been drained into a :class:`RunOutcome` (see
``cli/presenters/event_stream/_model.py``): a one-line counts banner, a
failures-only attribution block, and a copy-pasteable next-step hint.
With ``--show-details`` each failure expands to a full error-style panel.

Sole-bridge invariant (constitution §5.2 / §9.5): this module renders
rich but MUST NOT import ``lhp.errors``. Per-failure panels are built by
``cli.error_panel.render_issue_panel`` from the frozen
:class:`lhp.api.views.ValidationIssueView` records the terminal response
carries — error formatting stays single-sourced in ``error_panel.py``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Tuple

from rich.text import Text

from ..error_panel import render_issue_panel
from ._style import warning_suffix_parts

if TYPE_CHECKING:
    from rich.console import Console

    from .event_stream._model import FailureLine, RenderOptions, RunHeader, RunOutcome

logger = logging.getLogger(__name__)


def _format_elapsed(elapsed_s: float) -> str:
    """One-decimal seconds, e.g. ``4.4s``."""
    return f"{elapsed_s:.1f}s"


def _verb(command: str) -> str:
    """The past-tense verb for the counts banner — ``validated`` for validate, else ``generated``."""
    return "validated" if command == "validate" else "generated"


def _success_count(outcome: "RunOutcome", header: "RunHeader") -> int:
    """Count of pipelines that succeeded (for the counts banner).

    Validate counts validated pipelines; generate counts pipelines that
    produced output. Both fall back to ``header.pipeline_count`` minus the
    failures when the response does not expose a richer figure.
    """
    response = outcome.response
    pipeline_responses = getattr(response, "pipeline_responses", None)
    if pipeline_responses is not None:
        return sum(
            1 for r in pipeline_responses.values() if getattr(r, "success", False)
        )
    return max(header.pipeline_count - len(outcome.failures), 0)


def _counts_line(outcome: "RunOutcome", header: "RunHeader", elapsed_s: float) -> Text:
    """Assemble the ``N generated · M warning · K failed · 4.4s`` banner."""
    succeeded = _success_count(outcome, header)
    failed = len(outcome.failures)
    parts = [
        (f"{succeeded} {_verb(header.command)}", "bold"),
    ]
    parts.extend(warning_suffix_parts(len(outcome.warnings)))
    if failed:
        parts.append((" · ", "default"))
        parts.append((f"{failed} failed", "bold red"))
    parts.append((" · ", "default"))
    parts.append((_format_elapsed(elapsed_s), "dim"))
    return Text.assemble(*parts)


def _failure_line(failure: "FailureLine") -> Text:
    """Render one failure as ``pipeline / flowgroup  file  CODE  msg``.

    The ``flowgroup`` and ``file`` segments are omitted when ``None`` —
    generate failures are pipeline-level and carry neither.
    """
    text = Text()
    text.append(failure.pipeline, style="bold")
    if failure.flowgroup:
        text.append(" / ", style="dim")
        text.append(failure.flowgroup)
    if failure.file:
        text.append("  ")
        text.append(failure.file, style="dim")
    if failure.code:
        text.append("  ")
        text.append(failure.code, style="bold red")
    if failure.message:
        text.append("  ")
        text.append(failure.message)
    return text


def _next_step_hint(header: "RunHeader", options: "RenderOptions") -> Text:
    """A copy-pasteable command suggesting how to dig into the failures.

    Already-``--show-details`` runs have nothing more to expand, so the
    hint points at re-running the same command; otherwise it tells the
    user to add ``--show-details`` for the full panels.
    """
    base = f"lhp {header.command} --env {header.env}"
    if options.show_details:
        text = Text("Fix the issues above, then re-run: ", style="dim")
        text.append(base, style="bold")
    else:
        text = Text("Re-run with details: ", style="dim")
        text.append(f"{base} --show-details", style="bold")
    return text


def _iter_response_issues(response: Any) -> Tuple[Any, ...]:
    """Collect every ``ValidationIssueView`` the terminal response carries.

    Handles both terminal response shapes by duck typing:

    - validate's ``BatchValidationResponse`` — each
      ``pipeline_responses[*].issues`` is a tuple of views.
    - generate's ``BatchGenerationResponse`` — each
      ``pipeline_responses[*].error`` is a single view (or ``None``).
    """
    pipeline_responses = getattr(response, "pipeline_responses", None)
    if pipeline_responses is None:
        return ()
    collected: list = []
    for per_pipeline in pipeline_responses.values():
        issues = getattr(per_pipeline, "issues", None)
        if issues:
            collected.extend(issues)
        single = getattr(per_pipeline, "error", None)
        if single is not None:
            collected.append(single)
    return tuple(collected)


def _match_issue(failure: "FailureLine", issues: Tuple[Any, ...]) -> Optional[Any]:
    """Find the ``ValidationIssueView`` backing a ``FailureLine``.

    Matches on the tuple ``(code, pipeline, flowgroup)`` against the view's
    ``code`` / ``pipeline_name`` / ``flowgroup_name``, then relaxes to a
    code-only match so a failure still expands when the response omitted
    location fields. Returns ``None`` when nothing structured matches —
    the caller then prints the plain failure line instead of a panel.
    """
    if not failure.code:
        return None
    code_matches = [i for i in issues if getattr(i, "code", "") == failure.code]
    if not code_matches:
        return None
    for issue in code_matches:
        if (
            getattr(issue, "pipeline_name", None) == failure.pipeline
            and getattr(issue, "flowgroup_name", None) == failure.flowgroup
        ):
            return issue
    return code_matches[0]


# The rollup glyph mirrors the live renderer's warning marker (GLYPH_WARN);
# defined locally so the summary does not reach into the event_stream package
# for a single-character constant.
_WARN_GLYPH = "⚠"


def _warning_rollup(outcome: "RunOutcome", width: int) -> list[Text]:
    """Per-code warning rollup for the collapsed (no ``--show-details``) view.

    Returns a header line — total warnings plus the number of distinct codes —
    then one indented ``CODE  message  ×count`` row per code. Warnings are
    already deduped on ``(code, file)`` upstream, so ``count`` is the
    distinct-file count for that code. The message is flattened and ellipsised
    to ``width`` so each code stays on a single line regardless of how long its
    message is; the full per-file lines remain available via ``--show-details``.
    """
    by_code: dict[str, list] = {}
    for warning in outcome.warnings:
        entry = by_code.get(warning.code)
        if entry is None:
            by_code[warning.code] = [1, warning.message]
        else:
            entry[0] += 1

    total = len(outcome.warnings)
    n_types = len(by_code)
    warn_noun = "warning" if total == 1 else "warnings"
    type_noun = "type" if n_types == 1 else "types"

    header = Text()
    header.append(f"{_WARN_GLYPH} ", style="bold yellow")
    header.append(f"{total} {warn_noun} ({n_types} {type_noun})", style="bold yellow")
    header.append(" — run with ", style="dim")
    header.append("--show-details", style="bold")
    header.append(" to list", style="dim")

    lines = [header]
    for code, (count, message) in by_code.items():
        suffix = f"  ×{count}"
        # Keep CODE and the ×count tail fully visible; ellipsise only the
        # message so the row never wraps onto a second line.
        budget = max(20, width - len(code) - len(suffix) - 4)
        flat = " ".join(message.split())
        if len(flat) > budget:
            flat = flat[: budget - 1].rstrip() + "…"
        row = Text("  ")
        row.append(code, style="bold yellow")
        row.append("  ")
        row.append(flat)
        row.append(suffix, style="dim")
        lines.append(row)
    return lines


def print_run_summary(
    outcome: "RunOutcome",
    header: "RunHeader",
    *,
    elapsed_s: float,
    options: "RenderOptions",
    err_console: "Console",
) -> None:
    """Print the post-run summary for a generate / validate run to stderr.

    Order: the counts banner; then — when there are warnings and details are
    collapsed — a per-code warning rollup (the per-file lines were suppressed
    during the run, so the rollup is where the warnings surface by default,
    pointing at ``--show-details`` for the full list); then (when there are
    failures) either a per-failure error panel block (``options.show_details``)
    or a compact attribution block, then a copy-pasteable next-step hint. A
    clean run with no warnings and no failures prints only the banner.
    """
    err_console.print(_counts_line(outcome, header, elapsed_s))

    if outcome.warnings and not options.show_details:
        for line in _warning_rollup(outcome, err_console.width):
            err_console.print(line)

    if not outcome.failures:
        return

    if options.show_details:
        issues = _iter_response_issues(outcome.response)
        for failure in outcome.failures:
            issue = _match_issue(failure, issues)
            if issue is not None:
                err_console.print(render_issue_panel(issue))
            else:
                err_console.print(_failure_line(failure))
    else:
        for failure in outcome.failures:
            err_console.print(_failure_line(failure))

    err_console.print(_next_step_hint(header, options))
