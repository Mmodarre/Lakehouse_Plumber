"""Tests for :mod:`lhp.telemetry._preferences` through the package surface.

Every case hands an explicit ``environ`` mapping to the entry point so the
suite-wide ``LHP_TELEMETRY=off`` guard never leaks in and no test reaches the
developer's real config directory.
"""

import io
import re
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

import pytest

from lhp import telemetry
from lhp.telemetry._environment import lhp_version
from lhp.telemetry._paths import spool_path, state_path
from lhp.telemetry._spool import append_spool, read_lines
from lhp.telemetry._store import StateFile, read_state, utc_now_iso, write_state

TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
PROPS = {"command": "generate", "exit_code": 0}


@pytest.fixture
def cfg(tmp_path: Path) -> Path:
    return tmp_path / "cfg"


@pytest.fixture
def send_env(cfg: Path) -> Dict[str, str]:
    return {"LHP_CONFIG_DIR": str(cfg), "LHP_TELEMETRY_ENDPOINT": "http://127.0.0.1:1/"}


def _record(env: Dict[str, str], name: str = "cli.command", **props: Any) -> None:
    telemetry.record(name, project_root=None, props=props or PROPS, environ=env)


# set_user_enabled


@pytest.mark.unit
def test_set_user_enabled_off_writes_the_flag_only(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    append_spool(cfg, '{"kept":true}')
    path = telemetry.set_user_enabled(False, environ=send_env)
    assert path == state_path(cfg)
    state = read_state(cfg)
    assert state is not None
    assert state.enabled is False
    assert state.install_id is None
    assert state.created_at is not None
    _record(send_env)
    assert read_lines(spool_path(cfg)) == ['{"kept":true}']


@pytest.mark.unit
def test_set_user_enabled_on_keeps_the_existing_install_id(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    write_state(cfg, StateFile(install_id="existing-id", enabled=False))
    telemetry.set_user_enabled(True, environ=send_env)
    state = read_state(cfg)
    assert state is not None
    assert state.enabled is True
    assert state.install_id == "existing-id"


@pytest.mark.unit
def test_set_user_enabled_raises_the_real_oserror(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("file")
    with pytest.raises(OSError) as excinfo:
        telemetry.set_user_enabled(
            False, environ={"LHP_CONFIG_DIR": str(blocker / "cfg")}
        )
    assert excinfo.value.errno is not None


@pytest.mark.unit
def test_set_user_enabled_works_even_when_an_env_switch_is_on(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    telemetry.set_user_enabled(False, environ={**send_env, "DO_NOT_TRACK": "1"})
    state = read_state(cfg)
    assert state is not None and state.enabled is False


# update hint plumbing


@pytest.mark.unit
def test_mark_update_hint_shown_stamps_an_existing_state(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    write_state(cfg, StateFile(install_id="x"))
    telemetry.mark_update_hint_shown(environ=send_env)
    state = read_state(cfg)
    assert state is not None and state.update_hint_shown_at is not None
    assert TS.match(state.update_hint_shown_at)


@pytest.mark.unit
def test_mark_update_hint_shown_never_creates_state(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    telemetry.mark_update_hint_shown(environ=send_env)
    assert not cfg.exists()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("latest", "extra_env", "expected"),
    [
        ("99.0.0", {}, "99.0.0"),
        ("0.0.1", {}, None),
        (lhp_version(), {}, None),
        ("99.0.0", {"LHP_UPDATE_CHECK": "off"}, None),
        ("99.0.0", {"LHP_UPDATE_CHECK": "0"}, None),
        (None, {}, None),
    ],
)
def test_newer_version_available(
    cfg: Path,
    send_env: Dict[str, str],
    latest: Any,
    extra_env: Dict[str, str],
    expected: Any,
) -> None:
    write_state(cfg, StateFile(latest_known_version=latest))
    assert (
        telemetry.newer_version_available(environ={**send_env, **extra_env}) == expected
    )


@pytest.mark.unit
def test_newer_version_available_without_state_is_none(
    send_env: Dict[str, str],
) -> None:
    assert telemetry.newer_version_available(environ=send_env) is None


@pytest.mark.unit
def test_newer_version_available_ignores_consent(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    write_state(cfg, StateFile(latest_known_version="99.0.0", enabled=False))
    assert telemetry.newer_version_available(environ=send_env) == "99.0.0"


# spool inspection


@pytest.mark.unit
def test_spool_count_and_spooled_events(cfg: Path, send_env: Dict[str, str]) -> None:
    for exit_code in range(3):
        _record(send_env, command="generate", exit_code=exit_code)
    assert telemetry.spool_count(environ=send_env) == 4
    newest = telemetry.spooled_events(2, environ=send_env)
    assert [e["props"]["exit_code"] for e in newest] == [1, 2]
    assert len(telemetry.spooled_events(50, environ=send_env)) == 4


@pytest.mark.unit
def test_spooled_events_skips_corrupt_lines(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    append_spool(cfg, "{not json")
    append_spool(cfg, '{"event":"cli.command"}')
    assert telemetry.spooled_events(10, environ=send_env) == [{"event": "cli.command"}]


# due_update_hint


class _Tty(io.StringIO):
    """A stderr stand-in that claims to be a terminal."""

    def isatty(self) -> bool:
        return True


def _pretend_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make stderr claim to be a terminal for the rest of the test body.

    Called from the body rather than from a fixture: pytest's capture re-binds
    ``sys.stderr`` when the call phase resumes, which would undo a
    replacement made during setup.
    """
    monkeypatch.setattr(sys, "stderr", _Tty())


def _state_with_latest(cfg: Path, latest: str = "99.0.0", shown_at: Any = None) -> None:
    write_state(
        cfg,
        StateFile(
            install_id="00000000-0000-4000-8000-000000000000",
            created_at="2026-01-01T00:00:00.000Z",
            last_version_seen=lhp_version(),
            latest_known_version=latest,
            update_hint_shown_at=shown_at,
        ),
    )


@pytest.mark.unit
def test_due_update_hint_names_the_newer_release(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _pretend_terminal(monkeypatch)
    _state_with_latest(cfg)
    assert telemetry.due_update_hint(environ=send_env) == "99.0.0"


@pytest.mark.unit
@pytest.mark.parametrize(
    "switch",
    [
        {"LHP_TELEMETRY": "log"},
        {"LHP_TELEMETRY": "off"},
        {"DO_NOT_TRACK": "1"},
        {"CI": "1"},
        {"LHP_UPDATE_CHECK": "off"},
    ],
)
def test_due_update_hint_is_quiet_outside_an_interactive_send_mode_run(
    cfg: Path,
    send_env: Dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    switch: Dict[str, str],
) -> None:
    _pretend_terminal(monkeypatch)
    _state_with_latest(cfg)
    assert telemetry.due_update_hint(environ={**send_env, **switch}) is None


@pytest.mark.unit
def test_due_update_hint_is_quiet_without_a_terminal(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _state_with_latest(cfg)
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    assert telemetry.due_update_hint(environ=send_env) is None


@pytest.mark.unit
def test_due_update_hint_waits_a_day_after_the_last_one(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _pretend_terminal(monkeypatch)
    _state_with_latest(cfg, shown_at=utc_now_iso())
    assert telemetry.due_update_hint(environ=send_env) is None
    _state_with_latest(cfg, shown_at=utc_now_iso(-timedelta(hours=25)))
    assert telemetry.due_update_hint(environ=send_env) == "99.0.0"


@pytest.mark.unit
def test_due_update_hint_without_state_or_a_newer_release_is_none(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _pretend_terminal(monkeypatch)
    assert telemetry.due_update_hint(environ=send_env) is None
    _state_with_latest(cfg, latest="0.0.1")
    assert telemetry.due_update_hint(environ=send_env) is None
