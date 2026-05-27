"""Render tests for :func:`lhp.cli.warning_panel.render_warning_panel`.

These tests pin the Rich-rendering contract for the WarningCollector
data class moved to :mod:`lhp.api.callbacks` in Week 7. The data-side
contract (export, dedup, pickle round-trip, no-rich-in-api) lives in
``tests/api/test_warning_collector_contract.py``.
"""

import io

from rich.console import Console

from lhp.api import WarningCollector
from lhp.cli.warning_panel import render_warning_panel


def _render(collector: WarningCollector) -> str:
    """Render ``collector`` into a plain-text buffer for assertion."""
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, no_color=True, width=80)
    panel = render_warning_panel(collector)
    if panel is not None:
        console.print(panel)
    return buf.getvalue()


def test_render_skips_empty() -> None:
    """render_warning_panel returns None when nothing has been collected."""
    assert render_warning_panel(WarningCollector()) is None


def test_render_single_warning_uses_deprecation_title() -> None:
    """A solo deprecation warning gets the ``Deprecation Warning`` title."""
    collector = WarningCollector()
    collector.add(
        "deprecation",
        "The bare {token} substitution syntax is deprecated.",
    )

    out = _render(collector)
    assert "Deprecation Warning" in out
    assert "[deprecation]" in out
    assert "bare {token}" in out


def test_render_multiple_warnings_uses_generic_title() -> None:
    """Two-or-more warnings switch to the plural ``Warnings`` title."""
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("other", "msg-b")

    out = _render(collector)
    assert "Warnings" in out
    assert "Deprecation Warning" not in out
    assert "[deprecation]" in out
    assert "[other]" in out


def test_render_single_other_uses_generic_title() -> None:
    """A solo non-deprecation warning still uses the plural ``Warnings`` title."""
    collector = WarningCollector()
    collector.add("performance", "Slow pipeline detected")

    out = _render(collector)
    assert "Warnings" in out
    assert "Deprecation Warning" not in out


def test_render_dedup_preserves_first_seen_order() -> None:
    """Insertion order is preserved across re-adds; dedup is silent.

    Assert via the rendered output — ``msg-a`` must appear before
    ``msg-b`` before ``msg-c``, even though ``msg-a`` was re-added
    after ``msg-b``.
    """
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("other", "msg-b")
    collector.add("deprecation", "msg-a")
    collector.add("third", "msg-c")

    out = _render(collector)
    pos_a = out.find("msg-a")
    pos_b = out.find("msg-b")
    pos_c = out.find("msg-c")
    assert pos_a != -1 and pos_b != -1 and pos_c != -1
    assert pos_a < pos_b < pos_c
