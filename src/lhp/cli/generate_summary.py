"""Post-run summary renderers for the ``lhp generate`` command.

Renders the per-pipeline summary table and totals footer printed once
the Rich ``Live`` panel exits. Imports its shared primitives from
:mod:`lhp.cli.live_panel` so the rendering contract stays in one place
across generate and validate.

The public surface intentionally takes plain arguments rather than
closing over command-scope locals so it can be unit-tested without
spinning up the full ``execute()`` orchestration.
"""

from typing import Dict

from rich.text import Text

from .live_panel import PipelineRecord, make_summary_table, warning_suffix_parts


def _print_generate_footer_only(
    *,
    total: int,
    total_files: int,
    total_duration: float,
    dry_run: bool,
    warning_count: int,
) -> None:
    """Print the generate command's single-line footer with no table.

    Used by ``print_summary_table`` when ``show_all`` is False and every
    pipeline succeeded. The wording matches the regular success footer
    plus a terminal phrase that names the count of passing pipelines so
    the user has the count without scanning a table.

    When ``total > 1``, appends a dim ``(use -a to list)`` hint so users
    discover the flag that surfaces the per-pipeline table. (This helper
    is only called on the ``not show_all`` path, so the hint is always
    relevant here.)
    """
    from . import console as _console_module

    verb = "Would generate" if dry_run else "Generated"
    pipelines_word = "pipeline" if total == 1 else "pipelines"
    _console_module.console.print(
        Text.assemble(
            ("\n  ", "default"),
            (f"{verb} ", "default"),
            (f"{total_files:,}", "bold green"),
            (" files in ", "default"),
            (f"{total_duration:.1f}s", "bold"),
            (" — ", "default"),
            (f"all {total} {pipelines_word} passed", "default"),
            *warning_suffix_parts(warning_count),
            *((Text(" (use -a to list)", style="dim"),) if total > 1 else ()),
        )
    )


def print_summary_table(
    records: Dict[str, PipelineRecord],
    dry_run: bool,
    *,
    failed: bool = False,
    show_all: bool = False,
    warning_count: int = 0,
) -> None:
    """Render the post-run per-pipeline summary table and total line.

    Dereferences the console singleton via the module attribute so the
    autouse test fixture's monkeypatch in ``tests/conftest.py`` is
    honored. The verb on the total line switches between ``Generated``
    and ``Would generate`` so dry-run output is distinguishable.

    When ``failed`` is True, the footer line is adapted:

    - Partial failure (some records succeeded): the footer adds
      ``" (F of T pipelines failed)"`` after the standard success line.
    - Total failure (no records succeeded): the footer is replaced with
      ``"Failed to generate -- 0 of T pipelines succeeded in DURATION"``.

    When ``show_all`` is False (the default), the table is filtered to
    failed pipelines only. On a full-success run with no failures, the
    table is suppressed entirely and only a single-line footer is
    printed. ``--show-all`` opts into the unfiltered table.

    ``warning_count`` (when > 0) is appended to the success/partial
    footer wording (``"; N warnings"``) so the user does not have to
    scroll to the warning panel to learn there were any.

    An empty ``records`` dict short-circuits (no table, no footer) so the
    caller can invoke this unconditionally even on early failures that
    never seeded any pipeline records.
    """
    if not records:
        return

    # Pre-compute per-record success counts up front so the footer-only
    # path and the table path agree on the same totals.
    success_count = sum(1 for r in records.values() if r.success is True)
    failure_count = sum(1 for r in records.values() if r.success is not True)
    total = success_count + failure_count
    total_files = sum(r.files for r in records.values() if r.success is True)
    total_duration = sum(r.duration_s for r in records.values())

    # Failures-only filtering. On full success with no failed rows, skip
    # the table entirely and emit only the single-line footer so the user
    # doesn't scroll past N green checkmarks.
    if not show_all:
        filtered = {k: v for k, v in records.items() if v.success is not True}
        if not filtered and not failed:
            _print_generate_footer_only(
                total=total,
                total_files=total_files,
                total_duration=total_duration,
                dry_run=dry_run,
                warning_count=warning_count,
            )
            return
        records_to_render = filtered if filtered else records
    else:
        records_to_render = records

    from . import console as _console_module

    table = make_summary_table("Generation Summary")
    table.add_column("Files", justify="right")
    table.add_column("Duration", justify="right")

    for rec in records_to_render.values():
        if rec.success is True:
            table.add_row(
                Text("✓", style="bold green"),
                rec.name,
                f"{rec.files:,}",
                f"{rec.duration_s:.1f}s",
            )
        else:
            table.add_row(
                Text("✗", style="bold red"),
                rec.name,
                "—",
                f"{rec.duration_s:.1f}s",
            )

    _console_module.console.print(table)

    # ``failed`` is a hint from the orchestrator. The actual footer is
    # selected by cross-referencing it with the per-record success counts
    # so the table cannot disagree with the footer.
    is_total_failure = failed and success_count == 0
    is_partial_failure = failed and success_count > 0 and failure_count > 0

    if is_total_failure:
        _console_module.console.print(
            Text.assemble(
                ("\n  ", "default"),
                ("Failed to generate -- ", "bold red"),
                ("0", "bold red"),
                (f" of {total} pipelines succeeded in ", "default"),
                (f"{total_duration:.1f}s", "bold"),
                *warning_suffix_parts(warning_count),
            )
        )
        return

    verb = "Would generate" if dry_run else "Generated"
    if is_partial_failure:
        _console_module.console.print(
            Text.assemble(
                ("\n  ", "default"),
                (f"{verb} ", "default"),
                (f"{total_files:,}", "bold green"),
                (" files in ", "default"),
                (f"{total_duration:.1f}s", "bold"),
                (f" ({failure_count} of {total} pipelines failed)", "red"),
                *warning_suffix_parts(warning_count),
            )
        )
        return

    _console_module.console.print(
        Text.assemble(
            ("\n  ", "default"),
            (f"{verb} ", "default"),
            (f"{total_files:,}", "bold green"),
            (" files in ", "default"),
            (f"{total_duration:.1f}s", "bold"),
            *warning_suffix_parts(warning_count),
        )
    )
