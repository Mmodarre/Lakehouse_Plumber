"""Tests for :mod:`lhp.telemetry._update_check`.

``pending_update_hint`` is the whole decision: whether the hint is DUE. What
the line looks like belongs to the CLI presenter, so these tests assert the
version string that comes back, never a rendering.
"""

from typing import Any, Dict

import pytest

from lhp.telemetry._update_check import is_newer, pending_update_hint

CURRENT = "0.9.2"
NEWER = "0.9.3"
NOW = "2026-09-21T10:15:30.123000+00:00"


def _state(**overrides: Any) -> Dict[str, Any]:
    state: Dict[str, Any] = {"latest_known_version": NEWER}
    state.update(overrides)
    return state


def _hint(**overrides: Any) -> Any:
    kwargs: Dict[str, Any] = {
        "current_version": CURRENT,
        "now_iso": NOW,
        "environ": {},
        "interactive": True,
        "in_ci": False,
    }
    state = overrides.pop("state", _state())
    kwargs.update(overrides)
    return pending_update_hint(state, **kwargs)


# is_newer


@pytest.mark.unit
@pytest.mark.parametrize(
    ("candidate", "current", "expected"),
    [
        ("0.9.3", "0.9.2", True),
        ("0.10.0", "0.9.9", True),
        ("1.0.0", "1.0.0rc1", True),
        ("0.9.2", "0.9.2", False),
        ("0.9.1", "0.9.2", False),
        ("0.9.10", "0.9.9", True),
    ],
)
def test_is_newer_uses_pep440_ordering(
    candidate: str, current: str, expected: bool
) -> None:
    assert is_newer(candidate, current) is expected


@pytest.mark.unit
def test_is_newer_compares_numerically_not_lexically() -> None:
    # A string comparison would call "0.9.9" newer than "0.10.0".
    assert is_newer("0.9.9", "0.10.0") is False


@pytest.mark.unit
@pytest.mark.parametrize(
    ("candidate", "current"),
    [("not-a-version", "0.9.2"), ("0.9.3", "not-a-version"), ("", "0.9.2")],
)
def test_is_newer_is_false_for_an_unparseable_version(
    candidate: str, current: str
) -> None:
    assert is_newer(candidate, current) is False


@pytest.mark.unit
def test_is_newer_is_false_for_a_non_string() -> None:
    assert is_newer(None, "0.9.2") is False  # type: ignore[arg-type]


# pending_update_hint


@pytest.mark.unit
def test_hint_returns_the_latest_version_when_due() -> None:
    assert _hint() == NEWER


@pytest.mark.unit
def test_hint_is_none_when_the_stored_version_is_not_newer() -> None:
    assert _hint(state=_state(latest_known_version=CURRENT)) is None


@pytest.mark.unit
def test_hint_is_none_when_no_version_has_been_stored() -> None:
    assert _hint(state={}) is None
    assert _hint(state={"latest_known_version": None}) is None


@pytest.mark.unit
@pytest.mark.parametrize("value", ["off", "OFF", "0", "false", "False"])
def test_hint_is_silenced_by_lhp_update_check(value: str) -> None:
    assert _hint(environ={"LHP_UPDATE_CHECK": value}) is None


@pytest.mark.unit
def test_hint_survives_an_unrelated_lhp_update_check_value() -> None:
    assert _hint(environ={"LHP_UPDATE_CHECK": "on"}) == NEWER


@pytest.mark.unit
def test_hint_is_none_when_not_interactive() -> None:
    assert _hint(interactive=False) is None


@pytest.mark.unit
def test_hint_is_none_in_ci() -> None:
    assert _hint(in_ci=True) is None


@pytest.mark.unit
def test_hint_is_suppressed_within_twenty_four_hours_of_the_last_one() -> None:
    shown = "2026-09-21T00:15:30.123000+00:00"
    assert _hint(state=_state(update_hint_shown_at=shown)) is None


@pytest.mark.unit
def test_hint_returns_once_the_twenty_four_hours_have_passed() -> None:
    shown = "2026-09-20T10:15:30.123000+00:00"
    assert _hint(state=_state(update_hint_shown_at=shown)) == NEWER


@pytest.mark.unit
def test_hint_treats_an_unparseable_shown_at_as_never_shown() -> None:
    assert _hint(state=_state(update_hint_shown_at="yesterday")) == NEWER


@pytest.mark.unit
def test_hint_tolerates_a_non_string_shown_at() -> None:
    assert _hint(state=_state(update_hint_shown_at=12345)) == NEWER


@pytest.mark.unit
def test_hint_accepts_the_z_suffixed_timestamps_the_client_writes() -> None:
    state = _state(update_hint_shown_at="2026-09-20T10:15:30.123Z")
    assert (
        pending_update_hint(
            state,
            current_version=CURRENT,
            now_iso="2026-09-21T10:15:30.123Z",
            environ={},
            interactive=True,
            in_ci=False,
        )
        == NEWER
    )
