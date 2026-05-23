"""Post-run summary renderers for the ``lhp generate`` command."""

from typing import Dict

from rich.text import Text

from .live_panel import PipelineRecord, make_summary_table, warning_suffix_parts


def _print_generate_footer_only(
    *,
    total: int,
    total_files: int,
    dry_run: bool,
    warning_count: int,
    elapsed_s: float,
) -> None:
    from . import console as _console_module

    verb = "Would generate" if dry_run else "Generated"
    pipelines_word = "pipeline" if total == 1 else "pipelines"
    _console_module.console.print(
        Text.assemble(
            ("\n  ", "default"),
            (f"{verb} ", "default"),
            (f"{total_files:,}", "bold green"),
            (" files in ", "default"),
            (f"{elapsed_s:.1f}s", "bold"),
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
    elapsed_s: float,
) -> None:
    """Render the post-run per-pipeline summary table and total line.

    ``elapsed_s`` must be the caller's wall-clock duration; summing per-pipeline
    ``duration_s`` would over-count under parallel worker scheduling.
    """
    if not records:
        return

    success_count = sum(1 for r in records.values() if r.success is True)
    failure_count = sum(1 for r in records.values() if r.success is not True)
    total = success_count + failure_count
    total_files = sum(r.files for r in records.values() if r.success is True)
    total_duration = elapsed_s

    if not show_all:
        filtered = {k: v for k, v in records.items() if v.success is not True}
        if not filtered and not failed:
            _print_generate_footer_only(
                total=total,
                total_files=total_files,
                dry_run=dry_run,
                warning_count=warning_count,
                elapsed_s=elapsed_s,
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

    is_total_failure = failed and success_count == 0
    is_partial_failure = failed and success_count > 0 and failure_count > 0
    # Orchestrator-level failure that fired AFTER every pipeline succeeded
    # (e.g. monitoring-artifact finalization raised post-run). Without this
    # branch the footer would claim "Generated N files" directly above the
    # failure Panel the caller prints.
    is_orchestrator_failure = failed and success_count > 0 and failure_count == 0

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

    if is_orchestrator_failure:
        pipelines_word = "pipeline" if total == 1 else "pipelines"
        _console_module.console.print(
            Text.assemble(
                ("\n  ", "default"),
                (
                    f"All {total} {pipelines_word} completed but the run failed in post-processing",
                    "bold red",
                ),
                (" in ", "default"),
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
