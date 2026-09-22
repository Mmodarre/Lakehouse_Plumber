"""Tests for :mod:`lhp.utils.env_flags`.

Pins the two readings of an environment switch the telemetry client relies
on: ``env_truthy`` answers "is this variable SET", where CI vendors spell
"set" as ``1``/``true``/an opaque build id, and ``env_value_in`` answers "is
this variable one of these explicit words", which is how the off switches are
matched.
"""

import pytest

from lhp.utils.env_flags import env_truthy, env_value_in


@pytest.mark.unit
@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", "build-42", " "])
def test_env_truthy_accepts_any_non_empty_non_falsy_value(value: str) -> None:
    assert env_truthy({"CI": value}, "CI") is True


@pytest.mark.unit
@pytest.mark.parametrize("value", ["", "0", "false", "FALSE", "no", "No"])
def test_env_truthy_rejects_empty_and_falsy_words(value: str) -> None:
    assert env_truthy({"CI": value}, "CI") is False


@pytest.mark.unit
def test_env_truthy_missing_variable_is_false() -> None:
    assert env_truthy({}, "CI") is False


@pytest.mark.unit
def test_env_value_in_matches_case_insensitively() -> None:
    assert env_value_in({"LHP_TELEMETRY": "OFF"}, "LHP_TELEMETRY", ("off",)) is True


@pytest.mark.unit
def test_env_value_in_rejects_a_value_outside_the_set() -> None:
    values = ("off", "0", "false")
    assert env_value_in({"LHP_TELEMETRY": "log"}, "LHP_TELEMETRY", values) is False


@pytest.mark.unit
def test_env_value_in_missing_variable_is_false() -> None:
    assert env_value_in({}, "LHP_TELEMETRY", ("off",)) is False


@pytest.mark.unit
def test_env_value_in_empty_value_is_not_a_match() -> None:
    assert env_value_in({"LHP_TELEMETRY": ""}, "LHP_TELEMETRY", ("off",)) is False
