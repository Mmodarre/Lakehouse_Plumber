"""Unit tests for ``src/lhp/cli/render.py``.

Each helper is exercised in both terminal mode (``Console(record=True)``,
inspected via ``export_text()``) and non-TTY mode (``Console`` pointed at an
``io.StringIO`` so raw ``file.write`` calls — including embedded tabs — are
captured verbatim).
"""

from __future__ import annotations

import io

import pytest
from rich.console import Console

from lhp.cli.live_panel import _LHP_WORDMARK, _WORDMARK_MIN_PANEL_WIDTH
from lhp.cli.render import (
    ColumnSpec,
    render_command_header,
    render_empty_state,
    render_listing_table,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tty(width: int = 120) -> Console:
    return Console(
        force_terminal=True,
        width=width,
        record=True,
        color_system="truecolor",
    )


def _plain() -> tuple[Console, io.StringIO]:
    # ``record=True`` would not capture raw ``file.write`` calls, so use a
    # real in-memory buffer that the test reads via ``getvalue()``.
    buffer = io.StringIO()
    return Console(file=buffer, force_terminal=False, width=120), buffer


# ---------------------------------------------------------------------------
# render_listing_table -- terminal mode
# ---------------------------------------------------------------------------


class TestRenderListingTableTerminal:
    def test_empty_rows(self):
        sink = _tty()
        render_listing_table(
            "Empty Listing",
            [ColumnSpec("Name"), ColumnSpec("Type")],
            [],
            sink=sink,
        )
        out = sink.export_text()
        assert "Empty Listing" in out
        assert "Name" in out
        assert "Type" in out

    def test_single_row(self):
        sink = _tty()
        render_listing_table(
            "One Row",
            [ColumnSpec("Name"), ColumnSpec("Type")],
            [["alpha", "load"]],
            sink=sink,
        )
        out = sink.export_text()
        assert "One Row" in out
        assert "alpha" in out
        assert "load" in out

    def test_many_rows(self):
        sink = _tty()
        rows = [[f"name_{i}", f"type_{i}"] for i in range(25)]
        render_listing_table(
            "Many Rows",
            [ColumnSpec("Name"), ColumnSpec("Type")],
            rows,
            sink=sink,
        )
        out = sink.export_text()
        assert "name_0" in out
        assert "name_12" in out
        assert "name_24" in out

    def test_long_strings_do_not_crash(self):
        """Long cell values must not raise; truncation/wrap is acceptable."""
        sink = _tty(width=80)
        long_cell = "x" * 500
        render_listing_table(
            "Long Strings",
            [ColumnSpec("Name"), ColumnSpec("Detail")],
            [["short", long_cell]],
            sink=sink,
        )
        out = sink.export_text()
        assert "Long Strings" in out
        assert "short" in out

    def test_total_label_present(self):
        sink = _tty()
        render_listing_table(
            "Has Total",
            [ColumnSpec("Name")],
            [["a"], ["b"], ["c"]],
            total_label="items",
            sink=sink,
        )
        out = sink.export_text()
        assert "Total items: 3" in out

    def test_total_label_absent(self):
        sink = _tty()
        render_listing_table(
            "Just Rows",
            [ColumnSpec("Name")],
            [["a"], ["b"]],
            sink=sink,
        )
        out = sink.export_text()
        assert "Total " not in out

    def test_column_styles_are_applied(self):
        """Smoke test: passing ``style`` and ``justify`` does not crash."""
        sink = _tty()
        render_listing_table(
            "Styled",
            [
                ColumnSpec("Name", style="bold"),
                ColumnSpec("Count", style="dim", justify="right"),
            ],
            [["alpha", "42"]],
            sink=sink,
        )
        out = sink.export_text()
        assert "alpha" in out
        assert "42" in out


# ---------------------------------------------------------------------------
# render_listing_table -- non-TTY mode
# ---------------------------------------------------------------------------


class TestRenderListingTablePlain:
    def test_empty_rows_no_box_drawing(self):
        sink, buf = _plain()
        render_listing_table(
            "Empty Plain",
            [ColumnSpec("Name"), ColumnSpec("Type")],
            [],
            sink=sink,
        )
        out = buf.getvalue()
        assert "Empty Plain" in out
        assert "Name\tType" in out
        assert "─" not in out
        assert "│" not in out
        assert "┌" not in out

    def test_single_row_tab_separated(self):
        sink, buf = _plain()
        render_listing_table(
            "One Plain",
            [ColumnSpec("Name"), ColumnSpec("Type")],
            [["alpha", "load"]],
            sink=sink,
        )
        out = buf.getvalue()
        lines = [line for line in out.splitlines() if line.strip()]
        assert lines[0] == "One Plain"
        assert lines[1] == "Name\tType"
        assert lines[2] == "alpha\tload"

    def test_many_rows_tab_separated(self):
        sink, buf = _plain()
        rows = [[f"name_{i}", f"type_{i}"] for i in range(10)]
        render_listing_table(
            "Many Plain",
            [ColumnSpec("Name"), ColumnSpec("Type")],
            rows,
            sink=sink,
        )
        out = buf.getvalue()
        for i in range(10):
            assert f"name_{i}\ttype_{i}" in out

    def test_long_strings_no_crash(self):
        sink, buf = _plain()
        long_cell = "x" * 500
        render_listing_table(
            "Long Plain",
            [ColumnSpec("Name"), ColumnSpec("Detail")],
            [["short", long_cell]],
            sink=sink,
        )
        out = buf.getvalue()
        # Plain mode does not wrap; the full long value is preserved.
        assert long_cell in out

    def test_total_label_present(self):
        sink, buf = _plain()
        render_listing_table(
            "Has Total Plain",
            [ColumnSpec("Name")],
            [["a"], ["b"], ["c"]],
            total_label="things",
            sink=sink,
        )
        out = buf.getvalue()
        assert "Total things: 3" in out
        assert any(
            line == "Total things: 3" for line in out.splitlines() if line.strip()
        )

    def test_total_label_absent(self):
        sink, buf = _plain()
        render_listing_table(
            "Plain Rows",
            [ColumnSpec("Name")],
            [["a"]],
            sink=sink,
        )
        out = buf.getvalue()
        assert "Total " not in out


# ---------------------------------------------------------------------------
# render_empty_state
# ---------------------------------------------------------------------------


class TestRenderEmptyState:
    def test_terminal_renders_panel_with_both_lines(self):
        sink = _tty()
        render_empty_state(
            "No flowgroups found.",
            "Try running 'lhp init' to bootstrap a project.",
            sink=sink,
        )
        out = sink.export_text()
        assert "No flowgroups found." in out
        assert "Try running 'lhp init'" in out

    def test_terminal_uses_box_drawing(self):
        sink = _tty()
        render_empty_state("Nothing here.", "Hint.", sink=sink)
        out = sink.export_text()
        assert any(ch in out for ch in "╭╮╯╰─│")

    def test_plain_is_two_plain_lines(self):
        sink, buf = _plain()
        render_empty_state(
            "No flowgroups found.",
            "Try running 'lhp init' to bootstrap a project.",
            sink=sink,
        )
        out = buf.getvalue()
        lines = [line for line in out.splitlines() if line.strip()]
        assert lines == [
            "No flowgroups found.",
            "Try running 'lhp init' to bootstrap a project.",
        ]
        assert "─" not in out
        assert "│" not in out

    def test_long_strings_no_crash(self):
        sink = _tty(width=80)
        long_hint = "y" * 400
        render_empty_state("Title", long_hint, sink=sink)
        out = sink.export_text()
        assert "Title" in out
        assert "y" in out

    def test_single_line_title_and_hint(self):
        sink = _tty()
        render_empty_state("Empty", "Add data.", sink=sink)
        out = sink.export_text()
        assert "Empty" in out
        assert "Add data." in out


# ---------------------------------------------------------------------------
# render_command_header
# ---------------------------------------------------------------------------


_WORDMARK_FRAGMENT = "██████╗"


class TestRenderCommandHeader:
    def test_tty_wide_enough_renders_wordmark(self):
        sink = _tty(width=120)
        render_command_header("lhp info", sink=sink)
        out = sink.export_text()
        assert _WORDMARK_FRAGMENT in out
        assert "lhp info" in out

    def test_tty_renders_subtitle_when_provided(self):
        sink = _tty(width=120)
        render_command_header("lhp show", subtitle="bronze.customers", sink=sink)
        out = sink.export_text()
        assert "lhp show" in out
        assert "bronze.customers" in out
        assert _WORDMARK_FRAGMENT in out

    def test_tty_narrow_is_silent_noop(self):
        sink = _tty(width=_WORDMARK_MIN_PANEL_WIDTH - 1)
        render_command_header("lhp info", sink=sink)
        out = sink.export_text()
        assert out == ""

    def test_tty_at_threshold_renders(self):
        sink = _tty(width=_WORDMARK_MIN_PANEL_WIDTH)
        render_command_header("lhp info", sink=sink)
        out = sink.export_text()
        assert _WORDMARK_FRAGMENT in out

    def test_non_tty_is_silent_noop(self):
        sink, buf = _plain()
        render_command_header("lhp info", sink=sink)
        assert buf.getvalue() == ""

    def test_non_tty_silent_even_when_wide(self):
        buf = io.StringIO()
        sink = Console(file=buf, force_terminal=False, width=200)
        render_command_header("lhp info", sink=sink)
        assert buf.getvalue() == ""

    def test_wordmark_constant_is_reused_not_reinvented(self):
        sink = _tty(width=120)
        render_command_header("lhp info", sink=sink)
        out = sink.export_text()
        unique_row = "███████╗██║  ██║██║"
        assert unique_row in _LHP_WORDMARK
        assert unique_row in out


# ---------------------------------------------------------------------------
# Lazy-resolution of the default sink
# ---------------------------------------------------------------------------


class TestDefaultSinkResolution:
    """Default ``sink=None`` must resolve at call time, not at definition —
    ``tests/conftest.py`` swaps the module-level console per test, which only
    works if helpers re-read the singleton on every call.
    """

    def test_default_resolves_at_call_time(self, monkeypatch):
        import lhp.cli.console as console_module
        import lhp.cli.render as render_module

        replacement, buf = _plain()
        monkeypatch.setattr(console_module, "console", replacement)
        # The render module imports the singleton by name; patch both
        # locations to mirror what conftest does for other helpers.
        monkeypatch.setattr(render_module, "_default_console", replacement)

        render_listing_table(
            "Default Sink",
            [ColumnSpec("Name")],
            [["alpha"]],
        )
        out = buf.getvalue()
        assert "Default Sink" in out
        assert "alpha" in out


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
