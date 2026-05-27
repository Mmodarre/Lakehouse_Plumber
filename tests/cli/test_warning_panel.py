"""Render tests for :func:`lhp.cli.warning_panel.render_warning_panel`.

Data-side contract (export, dedup, pickle, no-rich-in-api) lives in
``tests/api/test_warning_collector_contract.py``.
"""

import io

from rich.console import Console

from lhp.api import WarningCollector
from lhp.cli.warning_panel import render_warning_panel


def _render(collector: WarningCollector) -> str:
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, no_color=True, width=80)
    panel = render_warning_panel(collector)
    if panel is not None:
        console.print(panel)
    return buf.getvalue()


def test_render_skips_empty() -> None:
    assert render_warning_panel(WarningCollector()) is None


def test_render_single_warning_uses_deprecation_title() -> None:
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
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("other", "msg-b")

    out = _render(collector)
    assert "Warnings" in out
    assert "Deprecation Warning" not in out
    assert "[deprecation]" in out
    assert "[other]" in out


def test_render_single_other_uses_generic_title() -> None:
    collector = WarningCollector()
    collector.add("performance", "Slow pipeline detected")

    out = _render(collector)
    assert "Warnings" in out
    assert "Deprecation Warning" not in out


def test_render_dedup_preserves_first_seen_order() -> None:
    """Order must hold across re-adds: ``msg-a`` is re-added after
    ``msg-b`` but must still precede it in output. Asserted via the
    rendered text.
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
