"""Guards that keep the whole test suite away from telemetry and the network.

These tests assert the behaviour of the fixtures in ``tests/conftest.py``
rather than any production code: if they fail, every other test in the suite
is potentially reading the real user config directory or opening a real
connection.
"""

import os
import socket
from pathlib import Path

import pytest
from pytest_socket import SocketConnectBlockedError

# Environment variables that consent resolution treats as "telemetry off".
_OFF_SWITCHES = (
    "DO_NOT_TRACK",
    "LHP_DISABLE_ANALYTICS",
    "CI",
    "GITHUB_ACTIONS",
    "TF_BUILD",
)


def test_ordinary_test_runs_with_telemetry_off(tmp_path):
    """The autouse guard forces ``off`` and a throwaway config directory.

    ``tmp_path`` is function-scoped, so the directory asserted here is the
    same one the autouse fixture pointed ``LHP_CONFIG_DIR`` at.
    """
    assert os.environ["LHP_TELEMETRY"] == "off"
    assert Path(os.environ["LHP_CONFIG_DIR"]) == tmp_path / "lhp-config"
    assert "LHP_TELEMETRY_ENDPOINT" not in os.environ


def test_telemetry_log_mode_clears_every_off_switch(telemetry_log_mode, tmp_path):
    """The opt-in fixture leaves ``log`` mode reachable by consent resolution."""
    assert os.environ["LHP_TELEMETRY"] == "log"
    assert "PYTEST_CURRENT_TEST" not in os.environ
    for name in _OFF_SWITCHES:
        assert name not in os.environ
    assert telemetry_log_mode == tmp_path / "lhp-config"
    assert Path(os.environ["LHP_CONFIG_DIR"]) == telemetry_log_mode


def test_connecting_to_a_non_loopback_host_is_blocked():
    """No test may reach anything but loopback.

    The address is TEST-NET-1 (RFC 5737), which is reserved for documentation
    and never routed, so the assertion needs no DNS lookup and no packet ever
    leaves the machine — the guard rejects the connection before the socket
    is used.
    """
    with pytest.raises(SocketConnectBlockedError):
        socket.create_connection(("192.0.2.1", 443), timeout=0.5)
