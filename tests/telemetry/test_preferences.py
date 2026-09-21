"""Tests for :mod:`lhp.telemetry._preferences` through the package surface.

Every case hands an explicit ``environ`` mapping to the entry point so the
suite-wide ``LHP_TELEMETRY=off`` guard never leaks in and no test reaches the
developer's real config directory.
"""

import re
from pathlib import Path
from typing import Any, Dict

import pytest

from lhp import telemetry
from lhp.telemetry._environment import lhp_version
from lhp.telemetry._paths import spool_path, state_path
from lhp.telemetry._spool import append_spool, read_lines
from lhp.telemetry._store import StateFile, read_state, write_state

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
