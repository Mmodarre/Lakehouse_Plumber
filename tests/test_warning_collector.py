"""Unit tests for :class:`lhp.cli.warning_collector.WarningCollector`."""

import io

from rich.console import Console

from lhp.cli.warning_collector import WarningCollector


def _render(collector: WarningCollector) -> str:
    """Render ``collector`` into a plain-text buffer for assertion."""
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, no_color=True, width=80)
    collector.render(console)
    return buf.getvalue()


def test_collector_add_and_count() -> None:
    """Three adds with one duplicate yields two unique entries."""
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("deprecation", "msg-a")  # duplicate, should be silently dropped
    collector.add("other", "msg-b")
    assert collector.count == 2


def test_collector_render_skips_empty() -> None:
    """Render is a no-op when nothing has been collected."""
    collector = WarningCollector()
    assert _render(collector) == ""


def test_collector_render_single_warning_uses_deprecation_title() -> None:
    """A solo deprecation warning gets the ``Deprecation Warning`` title."""
    collector = WarningCollector()
    collector.add(
        "deprecation",
        "The bare {token} substitution syntax is deprecated.",
    )

    out = _render(collector)
    assert "Deprecation Warning" in out
    # Body text and category prefix must appear in the rendered panel.
    assert "[deprecation]" in out
    assert "bare {token}" in out


def test_collector_render_multiple_warnings_uses_generic_title() -> None:
    """Two-or-more warnings switch to the plural ``Warnings`` title."""
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("other", "msg-b")

    out = _render(collector)
    # The plural-form title is rendered; the deprecation-specific one is not.
    assert "Warnings" in out
    assert "Deprecation Warning" not in out
    # Both category prefixes must be visible.
    assert "[deprecation]" in out
    assert "[other]" in out


def test_collector_render_single_other_uses_generic_title() -> None:
    """A solo non-deprecation warning still uses the plural ``Warnings`` title."""
    collector = WarningCollector()
    collector.add("performance", "Slow pipeline detected")

    out = _render(collector)
    assert "Warnings" in out
    assert "Deprecation Warning" not in out


def test_collector_dedup_preserves_first_seen_order() -> None:
    """Insertion order is preserved across re-adds; dedup is silent.

    Assert via the public ``render()`` path — the rendered output must
    list ``msg-a`` before ``msg-b`` before ``msg-c``, even though
    ``msg-a`` was re-added after ``msg-b``.
    """
    collector = WarningCollector()
    collector.add("deprecation", "msg-a")
    collector.add("other", "msg-b")
    collector.add("deprecation", "msg-a")  # duplicate, must not move msg-a
    collector.add("third", "msg-c")

    out = _render(collector)
    pos_a = out.find("msg-a")
    pos_b = out.find("msg-b")
    pos_c = out.find("msg-c")
    assert pos_a != -1 and pos_b != -1 and pos_c != -1
    assert pos_a < pos_b < pos_c
