"""The update hint is one fixed line, rendered by the presenter alone."""

from __future__ import annotations

import pytest

from lhp.cli.presenters.telemetry_presenter import render_update_hint

pytestmark = pytest.mark.unit


def test_render_update_hint_is_the_documented_line() -> None:
    assert render_update_hint("0.9.3", "0.9.2") == (
        "lhp 0.9.3 is available (installed 0.9.2): "
        "pip install -U lakehouse-plumber  [LHP_UPDATE_CHECK=off to silence]"
    )
