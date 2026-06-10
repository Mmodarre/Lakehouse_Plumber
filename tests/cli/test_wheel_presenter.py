"""Unit tests for the wheel inspection / extraction presenter.

The presenter is a pure DTO -> Rich rendering bridge: it formats a hand-built
:class:`lhp.api.WheelContentsView` or :class:`lhp.api.WheelExtractionResult`
into a table (or an empty-state notice). These tests build the DTOs directly —
no real wheels are constructed — render into a plain-text in-memory console,
and assert the expected arcnames / sizes / written paths appear as substrings.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from rich.console import Console

from lhp.api import WheelContentsView, WheelExtractionResult, WheelModuleView
from lhp.cli.presenters.wheel_presenter import render_extraction, render_inspection


def _console() -> tuple[Console, io.StringIO]:
    """A plain-text, non-TTY console whose buffer captures bytes verbatim."""
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, no_color=True, width=200)
    return console, buf


@pytest.mark.unit
def test_render_inspection_lists_modules_with_sizes():
    """Several modules -> each arcname + its size appears in the output."""
    view = WheelContentsView(
        wheel_path=Path("/dist/my_pipeline-0.1.0-py3-none-any.whl"),
        pipeline="my_pipeline",
        env="dev",
        modules=(
            WheelModuleView(arcname="my_pipeline/bronze_customers.py", size_bytes=1234),
            WheelModuleView(arcname="my_pipeline/silver_orders.py", size_bytes=56789),
        ),
        module_count=2,
    )

    console, buf = _console()
    render_inspection(view, console=console)
    text = buf.getvalue()

    # Wheel identity + context in the title.
    assert "my_pipeline-0.1.0-py3-none-any.whl" in text
    assert "pipeline=my_pipeline" in text
    assert "env=dev" in text
    # Each module arcname and its size.
    assert "my_pipeline/bronze_customers.py" in text
    assert "1234" in text
    assert "my_pipeline/silver_orders.py" in text
    assert "56789" in text
    # Total footer.
    assert "Total modules: 2" in text


@pytest.mark.unit
def test_render_inspection_empty_state_no_crash():
    """Zero modules -> a friendly notice, not a blank table, and no crash."""
    view = WheelContentsView(
        wheel_path=Path("/dist/empty-0.1.0-py3-none-any.whl"),
        pipeline=None,
        env=None,
        modules=(),
        module_count=0,
    )

    console, buf = _console()
    render_inspection(view, console=console)
    text = buf.getvalue()

    assert "No .py modules found in empty-0.1.0-py3-none-any.whl." in text
    # No table footer leaked for the empty case.
    assert "Total modules:" not in text


@pytest.mark.unit
def test_render_inspection_omits_missing_context():
    """No pipeline / env -> the title carries no parenthetical context."""
    view = WheelContentsView(
        wheel_path=Path("/dist/plain-0.1.0-py3-none-any.whl"),
        pipeline=None,
        env=None,
        modules=(WheelModuleView(arcname="plain/mod.py", size_bytes=10),),
        module_count=1,
    )

    console, buf = _console()
    render_inspection(view, console=console)
    text = buf.getvalue()

    assert "Modules in plain-0.1.0-py3-none-any.whl" in text
    assert "pipeline=" not in text
    assert "env=" not in text
    assert "plain/mod.py" in text


@pytest.mark.unit
def test_render_extraction_lists_written_paths():
    """Output dir + each written path + the count appear in the output."""
    result = WheelExtractionResult(
        wheel_path=Path("/dist/my_pipeline-0.1.0-py3-none-any.whl"),
        output_dir=Path("/tmp/extracted"),
        written_paths=(
            Path("/tmp/extracted/my_pipeline/bronze_customers.py"),
            Path("/tmp/extracted/my_pipeline/silver_orders.py"),
        ),
        written_count=2,
    )

    console, buf = _console()
    render_extraction(result, console=console)
    text = buf.getvalue()

    # Source wheel + output dir named in the title.
    assert "my_pipeline-0.1.0-py3-none-any.whl" in text
    assert "/tmp/extracted" in text
    # Each written path.
    assert "/tmp/extracted/my_pipeline/bronze_customers.py" in text
    assert "/tmp/extracted/my_pipeline/silver_orders.py" in text
    # Count footer.
    assert "Total files: 2" in text


@pytest.mark.unit
def test_render_extraction_empty_state_no_crash():
    """Zero written paths -> a friendly notice, not a blank table, and no crash."""
    result = WheelExtractionResult(
        wheel_path=Path("/dist/empty-0.1.0-py3-none-any.whl"),
        output_dir=Path("/tmp/extracted"),
        written_paths=(),
        written_count=0,
    )

    console, buf = _console()
    render_extraction(result, console=console)
    text = buf.getvalue()

    assert "No files extracted from empty-0.1.0-py3-none-any.whl." in text
    assert "Total files:" not in text
