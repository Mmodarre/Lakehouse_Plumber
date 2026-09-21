"""Tests for :mod:`lhp.telemetry._store`.

Every case runs against a throwaway config directory under ``tmp_path``. The
store is inert: a failure leaves a DEBUG record and a falsy return, never an
exception — except the strict write behind ``lhp telemetry on|off``, which
must surface the real ``OSError``.
"""

import json
import logging
import os
import re
import stat
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from lhp.telemetry._environment import lhp_version
from lhp.telemetry._paths import state_path
from lhp.telemetry._store import (
    StateFile,
    ensure_install,
    read_state,
    utc_now_iso,
    write_state,
)

POSIX_ONLY = pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")


@pytest.fixture
def cfg(tmp_path: Path) -> Path:
    return tmp_path / "cfg"


@pytest.fixture
def unwritable_cfg(tmp_path: Path) -> Path:
    """A config-dir path that is a regular file, so it can never be created."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    return blocker / "cfg"


def _state(**overrides) -> StateFile:
    return replace(
        StateFile(
            install_id="c0ffee11-2222-4333-8444-555566667777",
            enabled=True,
            created_at="2026-09-21T10:15:30.123Z",
            last_version_seen="0.9.2",
        ),
        **overrides,
    )


# state file


@pytest.mark.unit
def test_read_state_is_none_when_the_file_is_missing(cfg: Path) -> None:
    assert read_state(cfg) is None
    assert not cfg.exists()


@pytest.mark.unit
def test_state_round_trips(cfg: Path) -> None:
    state = _state(
        latest_known_version="0.9.3", server_disabled_until="2026-09-22T00:00:00.000Z"
    )
    assert write_state(cfg, state) is True
    assert read_state(cfg) == state


@pytest.mark.unit
def test_state_file_is_the_documented_json_document(cfg: Path) -> None:
    write_state(cfg, _state())
    document = json.loads(state_path(cfg).read_text("utf-8"))
    assert document == {
        "schema_version": 1,
        "install_id": "c0ffee11-2222-4333-8444-555566667777",
        "enabled": True,
        "created_at": "2026-09-21T10:15:30.123Z",
        "last_version_seen": "0.9.2",
        "latest_known_version": None,
        "latest_checked_at": None,
        "update_hint_shown_at": None,
        "server_disabled_until": None,
    }


@pytest.mark.unit
@pytest.mark.parametrize("payload", ["{not json", "[1, 2]", '"text"', "", "null"])
def test_read_state_is_none_for_an_unreadable_document(
    cfg: Path, payload: str, caplog: pytest.LogCaptureFixture
) -> None:
    cfg.mkdir()
    state_path(cfg).write_text(payload, "utf-8")
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        assert read_state(cfg) is None
    assert all(record.levelno == logging.DEBUG for record in caplog.records)


@pytest.mark.unit
def test_read_state_ignores_unknown_keys_and_wrong_types(cfg: Path) -> None:
    cfg.mkdir()
    state_path(cfg).write_text(
        json.dumps(
            {
                "install_id": 42,
                "enabled": "no",
                "latest_known_version": "0.9.3",
                "future_field": {"x": 1},
            }
        ),
        "utf-8",
    )
    state = read_state(cfg)
    assert state == StateFile(latest_known_version="0.9.3")
    assert state is not None and state.enabled is True


@pytest.mark.unit
def test_write_state_replaces_atomically_and_leaves_no_temp_file(cfg: Path) -> None:
    write_state(cfg, _state())
    write_state(cfg, _state(enabled=False))
    assert [p.name for p in cfg.iterdir()] == ["telemetry.json"]
    assert read_state(cfg) == _state(enabled=False)


@POSIX_ONLY
@pytest.mark.unit
def test_state_file_and_config_dir_are_private(cfg: Path) -> None:
    write_state(cfg, _state())
    assert stat.S_IMODE(state_path(cfg).stat().st_mode) == 0o600
    assert stat.S_IMODE(cfg.stat().st_mode) == 0o700


@pytest.mark.unit
def test_write_state_is_inert_when_the_dir_cannot_be_created(
    unwritable_cfg: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        assert write_state(unwritable_cfg, _state()) is False
    assert caplog.records and all(r.levelno == logging.DEBUG for r in caplog.records)


@pytest.mark.unit
def test_strict_write_state_propagates_the_real_oserror(unwritable_cfg: Path) -> None:
    with pytest.raises(OSError) as excinfo:
        write_state(unwritable_cfg, _state(), strict=True)
    assert excinfo.value.errno is not None
    assert excinfo.value.filename is not None


@pytest.mark.unit
def test_strict_write_state_returns_true_on_success(cfg: Path) -> None:
    assert write_state(cfg, _state(), strict=True) is True


@pytest.mark.unit
def test_state_file_is_frozen() -> None:
    with pytest.raises(FrozenInstanceError):
        _state().enabled = False  # type: ignore[misc]


@pytest.mark.unit
def test_utc_now_iso_is_millisecond_utc_with_z() -> None:
    stamp = utc_now_iso()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", stamp)


# ensure_install


@pytest.mark.unit
def test_ensure_install_mints_an_id_and_reports_first_seen(cfg: Path) -> None:
    state, events = ensure_install(cfg, None, "0.9.2", "2026-09-21T10:15:30.123Z")
    assert state is not None
    assert state.install_id is not None
    assert state.created_at == "2026-09-21T10:15:30.123Z"
    assert state.last_version_seen == "0.9.2"
    assert events == [("install.first_seen", {})]
    assert read_state(cfg) == state


@pytest.mark.unit
def test_ensure_install_reports_an_upgrade_once(cfg: Path) -> None:
    first, _ = ensure_install(cfg, None, "0.0.1", "2026-09-21T10:15:30.123Z")
    second, events = ensure_install(cfg, first, "0.9.2", "2026-09-21T10:16:30.123Z")
    assert second is not None and second.install_id == first.install_id
    assert events == [("install.upgraded", {"previous_version": "0.0.1"})]
    third, events = ensure_install(cfg, second, "0.9.2", "2026-09-21T10:17:30.123Z")
    assert third == second
    assert events == []


@pytest.mark.unit
def test_ensure_install_is_none_when_the_write_fails(unwritable_cfg: Path) -> None:
    state, events = ensure_install(
        unwritable_cfg, None, "0.9.2", "2026-09-21T10:15:30.123Z"
    )
    assert state is None
    assert events == [("install.first_seen", {})]
