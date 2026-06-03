"""Post-run summary renderers for the ``lhp validate`` command.

The public surface intentionally takes plain arguments rather than closing over
command-scope locals so it can be unit-tested without spinning up the full
``execute()`` orchestration.
"""

from typing import Dict

from rich.text import Text

from .live_panel import PipelineRecord, make_summary_table, warning_suffix_parts


def _print_validate_footer_only(
    *,
    total: int,
    warning_count: int,
) -> None:
    """Print the single-line footer when all pipelines passed.

    Only called on the ``not show_all`` path, so the ``(use -a to list)`` hint
    is always relevant here.
    """
    from . import console as _console_module

    pipelines_word = "pipeline" if total == 1 else "pipelines"
    _console_module.console.print(
        Text.assemble(
            ("\n  ", "default"),
            ("Validated ", "default"),
            (f"{total} {pipelines_word}", "bold green"),
            (" — all passed", "default"),
            *warning_suffix_parts(warning_count),
            *((Text(" (use -a to list)", style="dim"),) if total > 1 else ()),
        )
    )


def print_validate_summary_table(
    records: Dict[str, PipelineRecord],
    *,
    failed: bool = False,
    show_all: bool = False,
    warning_count: int = 0,
) -> None:
    """Render the post-run per-pipeline validate summary table.

    Dereferences the console singleton via the module attribute so the autouse
    test fixture's monkeypatch in ``tests/conftest.py`` is honored.
    """
    if not records:
        return

    success_count = sum(1 for r in records.values() if r.success is True)
    failure_count = sum(1 for r in records.values() if r.success is not True)
    total = success_count + failure_count
    total_warnings = sum(r.warnings_count for r in records.values())

    if not show_all:
        filtered = {k: v for k, v in records.items() if v.success is not True}
        if not filtered and not failed:
            _print_validate_footer_only(
                total=total,
                warning_count=warning_count,
            )
            return
        records_to_render = filtered if filtered else records
    else:
        records_to_render = records

    from . import console as _console_module

    table = make_summary_table("Validation Summary")
    table.add_column("Errors", justify="right")
    table.add_column("Warnings", justify="right")

    for rec in records_to_render.values():
        glyph = (
            Text("✓", style="bold green")
            if rec.success
            else Text("✗", style="bold red")
        )
        table.add_row(
            glyph,
            rec.name,
            f"{rec.errors_count:,}",
            f"{rec.warnings_count:,}",
        )

    _console_module.console.print(table)

    is_total_failure = failed and success_count == 0
    is_partial_failure = failed and success_count > 0 and failure_count > 0

    if is_total_failure:
        _console_module.console.print(
            Text.assemble(
                ("\n  ", "default"),
                ("Validation failed -- ", "bold red"),
                ("0", "bold red"),
                (f" of {total} pipelines passed", "default"),
                *warning_suffix_parts(warning_count),
            )
        )
        return

    if is_partial_failure:
        _console_module.console.print(
            Text.assemble(
                ("\n  ", "default"),
                ("Pipelines validated  ", "default"),
                (f"{success_count}", "bold green"),
                (" passed   ", "default"),
                (f"{failure_count}", "bold red"),
                (" failed   ", "default"),
                (f"{total_warnings}", "bold yellow" if total_warnings else "default"),
                (" warnings", "default"),
                *warning_suffix_parts(warning_count),
            )
        )
        return

    _console_module.console.print(
        Text.assemble(
            ("\n  ", "default"),
            ("Pipelines validated  ", "default"),
            (f"{success_count}", "bold green"),
            (" passed   ", "default"),
            ("0", "default"),
            (" failed   ", "default"),
            (f"{total_warnings}", "bold yellow" if total_warnings else "default"),
            (" warnings", "default"),
            *warning_suffix_parts(warning_count),
        )
    )
