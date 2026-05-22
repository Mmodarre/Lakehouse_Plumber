"""Shared Rich Live-panel primitives for the generate and validate CLIs.

Holds the pieces that both commands' Live status frames depend on: the
per-pipeline tracking record, the logging-handler swap that keeps the
panel border clean during failure rendering, the phase-marker line
appender (with sub-second suppression), and the group composer that
combines phase markers, inline failure lines, and the in-flight spinner.

The ``warning_suffix_parts`` helper also lives here because both
``generate_summary`` and ``validate_summary`` emit identical
``"; N warning(s)"`` tails on their success and partial-failure footers.

The public surface intentionally takes plain arguments rather than
closing over command-scope locals so each helper can be unit-tested
without spinning up the full ``execute()`` orchestration.
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

from rich.console import Console as _RichConsole
from rich.console import ConsoleRenderable, Group
from rich.logging import RichHandler
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

# Phase markers shorter than this duration are suppressed to avoid
# sub-second flicker in the Live panel. 250ms matches the visual
# refresh budget of the terminal at ``refresh_per_second=10``.
_PHASE_MARKER_MIN_DURATION_S = 0.25


@dataclass
class PipelineRecord:
    """In-memory tracking for one pipeline during a generate/validate run.

    Populated by the ``on_pipeline_complete`` callback (main thread,
    completion order) and consumed by ``print_summary_table`` or
    ``print_validate_summary_table`` after the Rich ``Live`` context
    exits. Lives at module scope so it is testable without entering
    the full ``execute()`` orchestration.

    Generate populates ``files`` / ``duration_s``; validate populates
    ``errors_count`` / ``warnings_count``. The two surfaces share the
    same record so ``rich_handler_attached`` / ``render_status_group``
    don't have to be duplicated per command.
    """

    name: str
    success: Optional[bool] = None
    files: int = 0
    duration_s: float = 0.0
    error_code: Optional[str] = None
    errors_count: int = 0
    warnings_count: int = 0


@contextmanager
def rich_handler_attached(err_console: _RichConsole) -> Iterator[None]:
    """Attach a Rich logging handler for the duration of the Live frame
    and restore the original stream handlers on exit.

    Critical for clean failure rendering: the cli_error_boundary panel
    must print to a stderr that has no Rich handler still attached, or
    Rich's redirect_stderr machinery from a closed Live frame can cause
    line-gluing on the final panel border. By using a context manager
    we guarantee teardown order: Live exits -> handler detaches -> raise
    propagates -> cli_error_boundary prints through clean stderr.
    """
    root_logger = logging.getLogger()
    rich_handler = RichHandler(
        console=err_console,
        show_time=False,
        show_level=True,
        show_path=False,
        markup=False,
    )
    rich_handler.setLevel(logging.WARNING)
    removed_handlers: List[logging.Handler] = []
    for h in list(root_logger.handlers):
        if isinstance(h, logging.StreamHandler) and not isinstance(
            h, logging.FileHandler
        ):
            root_logger.removeHandler(h)
            removed_handlers.append(h)
    root_logger.addHandler(rich_handler)
    try:
        yield
    finally:
        root_logger.removeHandler(rich_handler)
        for h in removed_handlers:
            root_logger.addHandler(h)


def append_phase_marker_line(
    phase_lines: List[Text],
    label: str,
    duration_s: float,
    *,
    force: bool = False,
    success: bool = True,
) -> None:
    """Append a phase marker line to ``phase_lines`` (in place).

    Module-level helper that captures the phase-line append semantics so
    the closure inside :meth:`GenerateCommand.execute` stays a thin
    wrapper that pre-binds the closure-local ``phase_lines`` list. This
    keeps the rendering contract -- threshold suppression, force
    override, marker glyph, marker style -- unit-testable without
    spinning up the full ``execute()`` orchestration.

    ``force=True`` bypasses the duration threshold so a failing phase
    always emits a diagnostic line. ``success=False`` swaps the default
    ``✓`` (bold green) marker for ``✗`` (bold red) so failing phases
    render with the failure glyph and style.
    """
    if not force and duration_s < _PHASE_MARKER_MIN_DURATION_S:
        return
    marker = "✓" if success else "✗"
    marker_style = "bold green" if success else "bold red"
    phase_lines.append(
        Text.assemble(
            ("  ", "default"),
            (label.ljust(36), "dim"),
            (marker, marker_style),
            (f"   {duration_s:.1f}s", "dim"),
        )
    )


def render_status_group(
    records: Dict[str, PipelineRecord],
    phase_lines: List[Text],
    failure_lines: List[Text],
    *,
    elapsed_text: str,
    in_flight_verb: str = "Generating",
) -> Group:
    """Build the Rich ``Group`` rendered inside the Live status panel.

    Composes -- in order -- the accumulated phase marker lines, the
    inline per-pipeline failure lines, and (while pipelines are still
    in flight) a single spinner row that summarises completion progress.
    The spinner is omitted when every record has completed (the panel
    handed off to the post-run summary table).

    Before any records or phase lines have been appended (i.e. while
    discovery is still walking the YAML tree on the main thread), the
    function renders a single ``Discovering flowgroups`` spinner so the
    user sees motion within the first Live frame instead of a silent
    terminal. The spinner is atomically replaced once
    ``append_phase_marker_line`` records the first ``Discovering`` phase
    or pipeline records are seeded.

    ``in_flight_verb`` controls the in-flight spinner label
    (``"Generating"`` for the generate command; ``"Validating"`` for
    validate). The discovery spinner is identical across commands.

    ``elapsed_text`` is the caller's pre-formatted ``MM:SS`` clock; it
    is passed in rather than computed here so the function stays pure
    and the caller controls the time source.
    """
    if not records and not phase_lines and not failure_lines:
        return Group(
            Spinner(
                "dots",
                text=Text.assemble(
                    ("Discovering flowgroups", "bold"),
                    ("…   ", "default"),
                    (elapsed_text, "dim"),
                ),
            )
        )

    elements: List[ConsoleRenderable] = []
    elements.extend(phase_lines)
    elements.extend(failure_lines)

    completed = sum(1 for r in records.values() if r.success is not None)
    total = len(records)
    if total > 0 and completed < total:
        failed_so_far = sum(1 for r in records.values() if r.success is False)
        failed_suffix = f" ({failed_so_far} failed)" if failed_so_far else ""
        elements.append(
            Spinner(
                "dots",
                text=Text.assemble(
                    (f"{in_flight_verb}  ", "bold"),
                    (
                        f"{completed} of {total} pipelines done",
                        "default",
                    ),
                    (failed_suffix, "red"),
                    (f"   {elapsed_text}", "dim"),
                ),
            )
        )
    return Group(*elements)


def make_summary_table(title: str) -> Table:
    """Build the shared skeleton for generate/validate post-run summary tables.

    Both commands print a Rich ``Table`` with the same title styling,
    border style, and a leading two-column pair (status icon + Pipeline
    name). Centralising the skeleton here keeps the two surfaces visually
    aligned and avoids two near-identical ``Table(...)`` constructors in
    the summary modules; callers append only their own data columns.
    """
    table = Table(
        title=title,
        title_style="bold",
        show_header=True,
        header_style="bold dim",
        border_style="dim",
    )
    table.add_column("", width=2)
    table.add_column("Pipeline")
    return table


def warning_suffix_parts(warning_count: int) -> List[Tuple[str, str]]:
    """Return Rich ``Text.assemble`` part tuples for the warning suffix.

    Empty list when ``warning_count`` is zero so callers can splat it
    into ``Text.assemble`` unconditionally and the suffix vanishes when
    there are no warnings. Pluralization is intentional: ``1 warning``
    vs ``N warnings``. Warning counts are never negative in practice
    (the collector is monotonic), so the zero-check is an exact match.
    """
    if warning_count == 0:
        return []
    noun = "warning" if warning_count == 1 else "warnings"
    return [
        ("; ", "default"),
        (f"{warning_count} {noun}", "bold yellow"),
    ]
