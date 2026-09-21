"""Tests for :mod:`lhp.telemetry._consent`.

Consent is a pure decision over an injected environment plus a lazily read
state file, so every case here is table-driven: the environment mapping and
the state reader go in, one ``TelemetryState`` comes out. The reader is a
callable precisely so a test can prove it is NOT called when an environment
layer has already decided.
"""

from dataclasses import FrozenInstanceError
from typing import Dict, Optional

import pytest

from lhp.telemetry._consent import TelemetryState, resolve_consent
from lhp.telemetry._store import StateFile

NOW = "2026-09-21T10:15:30.123Z"
FUTURE = "2026-09-22T10:15:30.123Z"
PAST = "2026-09-20T10:15:30.123Z"


def _resolve(
    environ: Dict[str, str], state: Optional[StateFile] = None
) -> TelemetryState:
    return resolve_consent(environ, state=lambda: state, now_iso=NOW)


# the precedence table


@pytest.mark.unit
@pytest.mark.parametrize(
    ("environ", "state", "expected"),
    [
        ({}, None, TelemetryState(True, "send", "default")),
        ({}, StateFile(enabled=True), TelemetryState(True, "send", "default")),
        (
            {"LHP_TELEMETRY": "off"},
            None,
            TelemetryState(False, "off", "env:LHP_TELEMETRY"),
        ),
        (
            {"LHP_TELEMETRY": "OFF"},
            None,
            TelemetryState(False, "off", "env:LHP_TELEMETRY"),
        ),
        (
            {"LHP_TELEMETRY": "0"},
            None,
            TelemetryState(False, "off", "env:LHP_TELEMETRY"),
        ),
        (
            {"LHP_TELEMETRY": "false"},
            None,
            TelemetryState(False, "off", "env:LHP_TELEMETRY"),
        ),
        ({"LHP_TELEMETRY": "on"}, None, TelemetryState(True, "send", "default")),
        ({"LHP_TELEMETRY": ""}, None, TelemetryState(True, "send", "default")),
        ({"DO_NOT_TRACK": "1"}, None, TelemetryState(False, "off", "env:DO_NOT_TRACK")),
        (
            {"DO_NOT_TRACK": "true"},
            None,
            TelemetryState(False, "off", "env:DO_NOT_TRACK"),
        ),
        ({"DO_NOT_TRACK": "0"}, None, TelemetryState(True, "send", "default")),
        ({"DO_NOT_TRACK": ""}, None, TelemetryState(True, "send", "default")),
        (
            {"LHP_DISABLE_ANALYTICS": "1"},
            None,
            TelemetryState(False, "off", "env:LHP_DISABLE_ANALYTICS"),
        ),
        (
            {"LHP_DISABLE_ANALYTICS": "yes"},
            None,
            TelemetryState(False, "off", "env:LHP_DISABLE_ANALYTICS"),
        ),
        (
            {"LHP_DISABLE_ANALYTICS": "no"},
            None,
            TelemetryState(True, "send", "default"),
        ),
        (
            {"PYTEST_CURRENT_TEST": "tests/x.py::test (call)"},
            None,
            TelemetryState(False, "off", "pytest"),
        ),
        ({}, StateFile(enabled=False), TelemetryState(False, "off", "user_state")),
        (
            {"LHP_TELEMETRY": "log"},
            None,
            TelemetryState(True, "log", "env:LHP_TELEMETRY"),
        ),
        (
            {"LHP_TELEMETRY": "LOG"},
            None,
            TelemetryState(True, "log", "env:LHP_TELEMETRY"),
        ),
        (
            {"LHP_TELEMETRY": "log"},
            StateFile(enabled=False),
            TelemetryState(False, "off", "user_state"),
        ),
        (
            {"LHP_TELEMETRY": "log", "DO_NOT_TRACK": "1"},
            None,
            TelemetryState(False, "off", "env:DO_NOT_TRACK"),
        ),
        (
            {"LHP_TELEMETRY": "log", "PYTEST_CURRENT_TEST": "x"},
            None,
            TelemetryState(False, "off", "pytest"),
        ),
        (
            {"LHP_TELEMETRY": "off", "DO_NOT_TRACK": "1"},
            None,
            TelemetryState(False, "off", "env:LHP_TELEMETRY"),
        ),
        (
            {"DO_NOT_TRACK": "1", "LHP_DISABLE_ANALYTICS": "1"},
            None,
            TelemetryState(False, "off", "env:DO_NOT_TRACK"),
        ),
        (
            {"LHP_DISABLE_ANALYTICS": "1", "PYTEST_CURRENT_TEST": "x"},
            None,
            TelemetryState(False, "off", "env:LHP_DISABLE_ANALYTICS"),
        ),
        (
            {"PYTEST_CURRENT_TEST": "x"},
            StateFile(enabled=False),
            TelemetryState(False, "off", "pytest"),
        ),
        (
            {},
            StateFile(enabled=True, server_disabled_until=FUTURE),
            TelemetryState(False, "off", "server"),
        ),
        (
            {"LHP_TELEMETRY": "log"},
            StateFile(enabled=True, server_disabled_until=FUTURE),
            TelemetryState(False, "off", "server"),
        ),
        (
            {},
            StateFile(enabled=False, server_disabled_until=FUTURE),
            TelemetryState(False, "off", "user_state"),
        ),
        (
            {},
            StateFile(enabled=True, server_disabled_until=PAST),
            TelemetryState(True, "send", "default"),
        ),
        (
            {},
            StateFile(enabled=True, server_disabled_until="not-a-timestamp"),
            TelemetryState(True, "send", "default"),
        ),
    ],
)
def test_consent_precedence(
    environ: Dict[str, str], state: Optional[StateFile], expected: TelemetryState
) -> None:
    assert _resolve(environ, state) == expected


# the state reader is lazy


@pytest.mark.unit
@pytest.mark.parametrize(
    "environ",
    [
        {"LHP_TELEMETRY": "off"},
        {"DO_NOT_TRACK": "1"},
        {"LHP_DISABLE_ANALYTICS": "1"},
        {"PYTEST_CURRENT_TEST": "x"},
    ],
)
def test_an_environment_off_switch_never_reads_the_state(
    environ: Dict[str, str],
) -> None:
    def reader() -> Optional[StateFile]:
        raise AssertionError("state must not be read when the environment says off")

    assert resolve_consent(environ, state=reader, now_iso=NOW).enabled is False


@pytest.mark.unit
@pytest.mark.parametrize("environ", [{}, {"LHP_TELEMETRY": "log"}])
def test_the_state_is_read_exactly_once_when_the_environment_allows(
    environ: Dict[str, str],
) -> None:
    calls: list = []

    def reader() -> Optional[StateFile]:
        calls.append(None)
        return StateFile(enabled=True)

    resolve_consent(environ, state=reader, now_iso=NOW)
    assert len(calls) == 1


# the value object


@pytest.mark.unit
def test_telemetry_state_is_frozen() -> None:
    state = TelemetryState(True, "send", "default")
    with pytest.raises(FrozenInstanceError):
        state.enabled = False  # type: ignore[misc]


@pytest.mark.unit
def test_log_mode_reports_enabled() -> None:
    state = _resolve({"LHP_TELEMETRY": "log"})
    assert state.enabled is True
    assert state.mode == "log"
