"""Post-run summary renderers for the ``lhp validate`` command.

Renders the per-pipeline summary table and totals footer printed once
the Rich ``Live`` panel exits. Mirrors :mod:`lhp.cli.generate_summary`
in shape but uses validate's columns (Errors / Warnings, no Duration)
and validate's footer wording (``Pipelines validated`` /
``Validation failed``).

The public surface intentionally takes plain arguments rather than
closing over command-scope locals so it can be unit-tested without
spinning up the full ``execute()`` orchestration.
"""

from typing import Dict

from rich.text import Text

from .live_panel import PipelineRecord, make_summary_table, warning_suffix_parts


def _print_validate_footer_only(
    *,
    total: int,
    warning_count: int,
) -> None:
    """Print the validate command's single-line footer with no table.

    Used by ``print_validate_summary_table`` when ``show_all`` is False
    and every pipeline passed validation. Mirrors the generate footer
    shape but with validate's wording.

    When ``total > 1``, appends a dim ``(use -a to list)`` hint so users
    discover the flag that surfaces the per-pipeline table. (This helper
    is only called on the ``not show_all`` path, so the hint is always
    relevant here.)
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

    Mirrors :func:`lhp.cli.generate_summary.print_summary_table` but
    with validate's columns (status icon, Pipeline, Errors, Warnings —
    no Duration) and validate's footer wording (``Pipelines validated``
    / ``Validation failed`` rather than ``Generated`` /
    ``Failed to generate``).

    Dereferences the console singleton via the module attribute so the
    autouse test fixture's monkeypatch in ``tests/conftest.py`` is
    honored.

    When ``failed`` is True, the footer line is adapted:

    - Partial failure (some records succeeded): the footer adds
      ``" (F of T pipelines failed)"`` after the totals line.
    - Total failure (no records succeeded): the footer is replaced
      with ``"Validation failed -- 0 of T pipelines passed"``.

    When ``show_all`` is False (the default), the table is filtered to
    failed pipelines only. On a full-success run with no failed rows,
    the table is suppressed entirely and only a single-line footer is
    printed. ``--show-all`` opts into the unfiltered table.

    ``warning_count`` (when > 0) is appended to the success/partial
    footer wording (``"; N warnings"``) so the user does not have to
    scroll to the warning panel to learn there were any. Validate's
    table already has a Warnings column, so this remains the single
    source of truth on the footer-only path.
    """
    if not records:
        return

    success_count = sum(1 for r in records.values() if r.success is True)
    failure_count = sum(1 for r in records.values() if r.success is not True)
    total = success_count + failure_count
    total_warnings = sum(r.warnings_count for r in records.values())

    # Failures-only filtering. On full success with no failed rows, skip
    # the table entirely and emit only the single-line footer.
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
