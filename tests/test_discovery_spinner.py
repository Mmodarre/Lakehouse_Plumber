"""Tests for the discovery spinner emitted by ``render_status_group``.

Phase B of the CLI UX hardening replaces the previously empty first
Live frame (a ~5s silent terminal on the 100-pipeline fixture project)
with a ``Discovering flowgroups`` spinner. These tests pin the contract:

* Empty inputs -> exactly one spinner whose text starts with the
  discovery label so the user sees motion within the first frame.
* Once ``phase_lines`` or ``records`` populate, the discovery spinner is
  replaced by the existing render path; there is no double-spinner state.
"""

from rich.spinner import Spinner
from rich.text import Text

from lhp.cli.live_panel import PipelineRecord, render_status_group


def test_render_status_group_empty_returns_discovery_spinner() -> None:
    """Empty records + empty phase_lines + empty failure_lines -> one
    discovery spinner whose label reads ``Discovering flowgroups``.

    The spinner is a placeholder that occupies the otherwise silent
    first Live frame; it is atomically replaced once the first
    ``Discovering`` phase marker line is appended.
    """
    group = render_status_group({}, [], [], elapsed_text="00:00")

    children = tuple(group.renderables)
    assert len(children) == 1
    spinner = children[0]
    assert isinstance(spinner, Spinner)

    # ``Spinner.text`` is the Rich ``Text`` instance constructed above.
    text = spinner.text
    assert isinstance(text, Text)
    assert "Discovering flowgroups" in text.plain


def test_render_status_group_with_phase_lines_skips_discovery_spinner() -> None:
    """A non-empty ``phase_lines`` list -> no discovery spinner.

    Once the first ``Discovering`` phase marker has been appended, the
    placeholder is replaced by the standard rendering path: only the
    accumulated phase lines (and any failure lines / progress spinner
    if applicable) appear.
    """
    phase_lines = [Text("  Discovering   ✓   0.5s")]
    group = render_status_group({}, phase_lines, [], elapsed_text="00:01")

    children = tuple(group.renderables)
    # No discovery placeholder spinner. Without records, the progress
    # spinner is also omitted -- only the phase line remains.
    assert all(not isinstance(c, Spinner) for c in children)
    assert phase_lines[0] in children


def test_render_status_group_with_records_skips_discovery_spinner() -> None:
    """Non-empty ``records`` -> no discovery spinner.

    Once pipelines have been seeded, the generation progress spinner
    takes over; the discovery placeholder must not coexist with it.
    """
    records = {"p1": PipelineRecord("p1")}
    group = render_status_group(records, [], [], elapsed_text="00:02")

    spinners = [r for r in group.renderables if isinstance(r, Spinner)]
    # Exactly one spinner -- the generation-progress spinner, not the
    # discovery placeholder.
    assert len(spinners) == 1
    assert "Discovering flowgroups" not in spinners[0].text.plain
    assert "Generating" in spinners[0].text.plain
