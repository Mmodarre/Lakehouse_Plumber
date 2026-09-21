"""Tests for :mod:`lhp.telemetry._paths`.

Every case injects ``environ``, ``os_name`` and ``home`` so the resolution
order is exercised without reading the developer's real environment or home
directory — in particular the Windows branch, which must be testable from a
POSIX host.
"""

import logging
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from lhp.telemetry._paths import (
    DEFAULT_ENDPOINT,
    config_dir,
    resolve_endpoint,
    spool_path,
    state_path,
)

FAKE_HOME = Path("/fake/home")


def _home() -> Path:
    return FAKE_HOME


@pytest.mark.unit
def test_lhp_config_dir_wins_over_every_other_source() -> None:
    environ = {
        "LHP_CONFIG_DIR": "/explicit/dir",
        "APPDATA": "/appdata",
        "XDG_CONFIG_HOME": "/xdg",
    }
    result = config_dir(environ, os_name="nt", home=_home)
    assert result == Path("/explicit/dir")


@pytest.mark.unit
def test_empty_lhp_config_dir_is_ignored() -> None:
    result = config_dir({"LHP_CONFIG_DIR": ""}, os_name="posix", home=_home)
    assert result == FAKE_HOME / ".config" / "lhp"


@pytest.mark.unit
def test_windows_uses_appdata() -> None:
    environ = {"APPDATA": r"C:\Users\dev\AppData\Roaming", "XDG_CONFIG_HOME": "/xdg"}
    result = config_dir(environ, os_name="nt", home=_home)
    assert PureWindowsPath(result).as_posix().endswith("AppData/Roaming/lhp")


@pytest.mark.unit
def test_windows_without_appdata_falls_back_under_home() -> None:
    result = config_dir({}, os_name="nt", home=_home)
    assert result == FAKE_HOME / "AppData" / "Roaming" / "lhp"


@pytest.mark.unit
def test_xdg_config_home_is_used_when_absolute() -> None:
    result = config_dir({"XDG_CONFIG_HOME": "/xdg"}, os_name="posix", home=_home)
    assert result == Path("/xdg") / "lhp"


@pytest.mark.unit
def test_relative_xdg_config_home_is_ignored() -> None:
    # The XDG spec says a relative value is invalid and must be ignored.
    result = config_dir(
        {"XDG_CONFIG_HOME": "relative/dir"}, os_name="posix", home=_home
    )
    assert result == FAKE_HOME / ".config" / "lhp"


@pytest.mark.unit
def test_posix_default_is_dot_config() -> None:
    result = config_dir({}, os_name="posix", home=_home)
    assert result == FAKE_HOME / ".config" / "lhp"


@pytest.mark.unit
def test_state_path_is_telemetry_json_in_the_config_dir() -> None:
    assert state_path(Path("/cfg")) == Path("/cfg/telemetry.json")


@pytest.mark.unit
def test_spool_path_is_nested_under_a_telemetry_subdirectory() -> None:
    assert PurePosixPath(spool_path(Path("/cfg"))) == PurePosixPath(
        "/cfg/telemetry/spool.jsonl"
    )


@pytest.mark.unit
def test_resolution_touches_no_filesystem(tmp_path: Path) -> None:
    # Path resolution is pure: nothing is created, not even the config dir.
    result = config_dir({"LHP_CONFIG_DIR": str(tmp_path / "cfg")}, home=_home)
    assert not result.exists()
    assert not state_path(result).exists()


# endpoint resolution


@pytest.mark.unit
def test_default_endpoint_is_the_placeholder_hostname() -> None:
    assert DEFAULT_ENDPOINT == "https://telemetry.lakehouse-plumber.invalid/v1/events"


@pytest.mark.unit
@pytest.mark.parametrize(
    "override",
    [
        "https://collector.example.org/v1/events",
        "http://127.0.0.1:8787/v1/events",
        "http://localhost/v1/events",
        "http://localhost:9000/v1/events",
    ],
)
def test_resolve_endpoint_accepts_https_and_loopback_http(override: str) -> None:
    assert resolve_endpoint({"LHP_TELEMETRY_ENDPOINT": override}) == override


@pytest.mark.unit
@pytest.mark.parametrize(
    "override",
    [
        "http://evil.example/v1/events",
        "http://localhost.evil.example/v1/events",
        "http://127.0.0.1.evil.example/",
        "ftp://127.0.0.1/",
        "file:///etc/passwd",
        "not a url",
        "",
    ],
)
def test_resolve_endpoint_rejects_everything_else(
    override: str, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        assert (
            resolve_endpoint({"LHP_TELEMETRY_ENDPOINT": override}) == DEFAULT_ENDPOINT
        )
    assert all(record.levelno == logging.DEBUG for record in caplog.records)


@pytest.mark.unit
def test_resolve_endpoint_defaults_when_unset() -> None:
    assert resolve_endpoint({}) == DEFAULT_ENDPOINT
