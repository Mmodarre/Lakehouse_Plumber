"""Acceptance tests for the ``lhp telemetry`` command group.

Every case drives the group object through ``CliRunner`` (Click 8.4+: stdout
and stderr are captured separately) and asserts the exact lines a user sees —
the output is a support surface, quoted verbatim in bug reports, so it is
pinned character for character.

The environment is normalised per test: the host may export CI markers or a
do-not-track switch, and either would change what consent resolves to. The
``neutral_env`` fixture hides all of them; a test that wants one back sets it
explicitly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest
from click.testing import CliRunner

from lhp.cli.commands.telemetry_command import telemetry
from lhp.telemetry import DEFAULT_ENDPOINT
from lhp.telemetry._client import _reset_for_tests
from lhp.telemetry._environment import _CI_VENDORS
from lhp.telemetry._spool import (
    append_spool,
    restore_inflight,
    spool_count,
    take_inflight,
)
from lhp.telemetry._store import StateFile, read_state, write_state
from tests.helpers.telemetry import ENVELOPE_PREFIX, assert_allowlisted_cli_command

pytestmark = pytest.mark.unit

_DOCS_URL = (
    "https://lakehouse-plumber.readthedocs.io/en/latest/reference/telemetry.html"
)
_INSTALL_ID = "00000000-0000-4000-8000-000000000000"
_OFF_SWITCHES = ("DO_NOT_TRACK", "LHP_DISABLE_ANALYTICS")
# A refused port keeps a send-mode run offline and fast: the connection fails
# immediately instead of waiting out the sender's timeout.
_REFUSED_ENDPOINT = "http://127.0.0.1:1/v1/events"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def _fresh_sender_state() -> Any:
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.fixture
def neutral_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hide every CI marker and do-not-track switch the host might export."""
    for variable, _ in _CI_VENDORS:
        monkeypatch.delenv(variable, raising=False)
    for variable in _OFF_SWITCHES:
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture
def config_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """The throwaway config directory the command reads and writes."""
    cfg = tmp_path / "telemetry-config"
    monkeypatch.setenv("LHP_CONFIG_DIR", str(cfg))
    return cfg


@pytest.fixture
def consenting(
    telemetry_log_mode: Path, neutral_env: None, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Telemetry resolved to ``log`` for one test; yields its config directory.

    ``telemetry_log_mode`` is the fixture that keeps ``PYTEST_CURRENT_TEST``
    out of the way for the duration of the call phase, which consent treats as
    an off switch.
    """
    monkeypatch.delenv("LHP_TELEMETRY_ENDPOINT", raising=False)
    return telemetry_log_mode


@pytest.fixture
def sending(consenting: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Telemetry resolved to ``send`` against a refused loopback endpoint."""
    monkeypatch.delenv("LHP_TELEMETRY", raising=False)
    monkeypatch.setenv("LHP_TELEMETRY_ENDPOINT", _REFUSED_ENDPOINT)
    return consenting


def _event(marker: str) -> Dict[str, Any]:
    """A spooled envelope stand-in, recognisable by its ``event_id``."""
    return {"schema_version": 1, "event_id": marker, "event": "cli.command"}


def _compact(event: Dict[str, Any]) -> str:
    return json.dumps(event, separators=(",", ":"))


def _seed(cfg: Path, *lines: str) -> None:
    for line in lines:
        assert append_spool(cfg, line)


def _claim_unanswered(cfg: Path, *lines: str) -> None:
    """Leave ``lines`` claimed by a send, as the resend of an unanswered batch."""
    _seed(cfg, *lines)
    unanswered = take_inflight(cfg)
    assert unanswered is not None
    restore_inflight(cfg, unanswered, unconfirmed=True)
    assert take_inflight(cfg) is not None


def _stored_state(cfg: Path, **overrides: Any) -> None:
    write_state(
        cfg,
        StateFile(
            install_id=_INSTALL_ID,
            created_at="2026-01-01T00:00:00.000Z",
            **overrides,
        ),
    )


def _lines(text: str) -> List[str]:
    return text.splitlines()


# status


def test_status_names_the_environment_switch_that_decided(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    """The default suite environment is off by ``LHP_TELEMETRY``, reported as such."""
    result = runner.invoke(telemetry, ["status"])

    assert result.exit_code == 0, result.output
    assert _lines(result.stdout) == [
        "Telemetry: off",
        "Decided by: env LHP_TELEMETRY",
        "Mode: off",
        f"Config dir: {config_dir}",
        "Install id: none",
        f"Endpoint: {DEFAULT_ENDPOINT}",
        "Spooled events: 0",
        f"Docs: {_DOCS_URL}",
    ]


def test_status_names_the_state_file_and_counts_the_spool(
    runner: CliRunner, consenting: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stored ``enabled: false`` decides, and the install id and spool are shown."""
    monkeypatch.delenv("LHP_TELEMETRY", raising=False)
    _stored_state(consenting, enabled=False)
    _seed(consenting, _compact(_event("a")), _compact(_event("b")))

    result = runner.invoke(telemetry, ["status"])

    assert result.exit_code == 0, result.output
    assert _lines(result.stdout) == [
        "Telemetry: off",
        "Decided by: user state file",
        "Mode: off",
        f"Config dir: {consenting}",
        f"Install id: {_INSTALL_ID}",
        f"Endpoint: {DEFAULT_ENDPOINT}",
        "Spooled events: 2",
        f"Docs: {_DOCS_URL}",
    ]


def test_status_counts_a_batch_claimed_by_a_send(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    """An event is pending until a send settles it, so a claimed batch still counts."""
    _claim_unanswered(config_dir, _compact(_event("a")), _compact(_event("b")))
    _seed(config_dir, _compact(_event("c")))

    result = runner.invoke(telemetry, ["status"])

    assert result.exit_code == 0, result.output
    assert "Spooled events: 3" in _lines(result.stdout)


def test_status_reports_no_install_id_under_ci(
    runner: CliRunner, consenting: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No install id is reported in CI, even when the state file holds one."""
    _stored_state(consenting)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    result = runner.invoke(telemetry, ["status"])

    assert result.exit_code == 0, result.output
    assert "Install id: none (CI)" in result.stdout
    assert _INSTALL_ID not in result.stdout


def test_status_reports_the_send_mode_of_a_consenting_run(
    runner: CliRunner, sending: Path
) -> None:
    result = runner.invoke(telemetry, ["status"])

    assert result.exit_code == 0, result.output
    assert "Telemetry: on" in result.stdout
    assert "Decided by: default" in result.stdout
    assert "Mode: send" in result.stdout
    assert f"Endpoint: {_REFUSED_ENDPOINT}" in result.stdout


# show


def test_show_prints_the_preview_envelope_then_the_spool(
    runner: CliRunner, consenting: Path
) -> None:
    """The first stdout line is the event this run would send; the rest is the spool."""
    _seed(consenting, _compact(_event("a")))

    result = runner.invoke(telemetry, ["show"])

    assert result.exit_code == 0, result.output
    stdout = _lines(result.stdout)
    preview = json.loads(stdout[0])
    assert_allowlisted_cli_command(preview)
    assert preview["props"]["command"] == "telemetry.show"
    assert stdout[1:] == [_compact(_event("a"))]
    assert "1 spooled event(s) shown." in result.stderr


def test_show_without_consent_still_lists_the_spool(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    """Off means no preview to show, but what is already spooled is still listed.

    The off-notice is commentary, so stdout stays pure JSON lines for ``jq``.
    """
    _seed(config_dir, _compact(_event("a")), _compact(_event("b")))

    result = runner.invoke(telemetry, ["show"])

    assert result.exit_code == 0, result.output
    assert _lines(result.stdout) == [_compact(_event("b")), _compact(_event("a"))]
    assert "Telemetry is off; nothing would be sent." in result.stderr
    assert "2 spooled event(s) shown." in result.stderr
    for line in _lines(result.stdout):
        json.loads(line)


def test_show_lists_the_newest_events_first_up_to_last(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    _seed(
        config_dir,
        _compact(_event("oldest")),
        _compact(_event("middle")),
        _compact(_event("newest")),
    )

    result = runner.invoke(telemetry, ["show", "--last", "2"])

    assert result.exit_code == 0, result.output
    assert _lines(result.stdout) == [
        _compact(_event("newest")),
        _compact(_event("middle")),
    ]
    assert "2 spooled event(s) shown." in result.stderr


def test_show_skips_a_corrupt_spool_line(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    """Neither unparseable text nor a JSON scalar is an envelope."""
    _seed(config_dir, "not json at all", '"a bare string"', _compact(_event("a")))

    result = runner.invoke(telemetry, ["show"])

    assert result.exit_code == 0, result.output
    assert _lines(result.stdout) == [_compact(_event("a"))]
    assert "1 spooled event(s) shown." in result.stderr


def test_show_lists_a_claimed_batch_as_it_was_recorded(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    """The unconfirmed mark is bookkeeping, never part of the listed envelope."""
    _claim_unanswered(config_dir, _compact(_event("a")))

    result = runner.invoke(telemetry, ["show"])

    assert result.exit_code == 0, result.output
    assert _lines(result.stdout) == [_compact(_event("a"))]
    assert "1 spooled event(s) shown." in result.stderr
    assert "No spooled events." not in result.stderr


def test_show_reports_an_empty_spool(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    result = runner.invoke(telemetry, ["show"])

    assert result.exit_code == 0, result.output
    assert result.stdout == ""
    assert "No spooled events." in result.stderr


# on / off


def test_on_writes_the_preference_and_confirms(
    runner: CliRunner, neutral_env: None, config_dir: Path
) -> None:
    result = runner.invoke(telemetry, ["on"])

    assert result.exit_code == 0, result.output
    assert result.stdout == f"✓ Telemetry on. State: {config_dir / 'telemetry.json'}\n"
    state = read_state(config_dir)
    assert state is not None and state.enabled is True


def test_on_warns_when_an_environment_switch_still_forces_off(
    runner: CliRunner,
    neutral_env: None,
    config_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The preference is stored, but the user is told it does not take effect.

    ``LHP_TELEMETRY`` is cleared so the warning names the switch under test:
    consent reports the FIRST layer that opts out, and that variable is
    resolved ahead of ``DO_NOT_TRACK``.
    """
    monkeypatch.delenv("LHP_TELEMETRY", raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")

    result = runner.invoke(telemetry, ["on"])

    assert result.exit_code == 0, result.output
    assert result.stdout.startswith("✓ Telemetry on. State: ")
    assert "⚠ Telemetry remains off: env DO_NOT_TRACK" in result.stderr
    state = read_state(config_dir)
    assert state is not None and state.enabled is True


def test_off_writes_only_the_enabled_flag(
    runner: CliRunner, consenting: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The install id and the spool survive: only the flag changes."""
    monkeypatch.delenv("LHP_TELEMETRY", raising=False)
    _stored_state(consenting, last_version_seen="0.9.2")
    _seed(consenting, _compact(_event("a")))

    result = runner.invoke(telemetry, ["off"])

    assert result.exit_code == 0, result.output
    assert result.stdout == (
        "✓ Telemetry off for this user on this machine. "
        f"State: {consenting / 'telemetry.json'}\n"
    )
    state = read_state(consenting)
    assert state is not None
    assert state.enabled is False
    assert state.install_id == _INSTALL_ID
    assert state.last_version_seen == "0.9.2"
    assert spool_count(consenting) == 1


def test_off_records_no_event_for_itself_in_log_mode(
    runner: CliRunner, consenting: Path
) -> None:
    """Consent is re-resolved after the body, so the run that turns it off is silent."""
    result = runner.invoke(telemetry, ["off"])

    assert result.exit_code == 0, result.output
    assert ENVELOPE_PREFIX not in result.stderr


def test_status_does_record_its_own_event_in_log_mode(
    runner: CliRunner, consenting: Path
) -> None:
    """The counterpart of the silent ``off``: every other subcommand is recorded."""
    result = runner.invoke(telemetry, ["status"])

    assert result.exit_code == 0, result.output
    recorded = [
        json.loads(line)
        for line in _lines(result.stderr)
        if line.startswith(ENVELOPE_PREFIX)
    ]
    assert [event["props"]["command"] for event in recorded] == ["telemetry.status"]


def test_off_adds_nothing_to_the_spool_in_send_mode(
    runner: CliRunner, sending: Path
) -> None:
    _stored_state(sending)
    _seed(sending, _compact(_event("a")))

    result = runner.invoke(telemetry, ["off"])

    assert result.exit_code == 0, result.output
    assert spool_count(sending) == 1


def test_an_unwritable_config_dir_reports_io_028(
    runner: CliRunner,
    neutral_env: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed state-file write is the one telemetry failure a user hears about."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    monkeypatch.setenv("LHP_CONFIG_DIR", str(blocker / "lhp"))

    result = runner.invoke(telemetry, ["off"])

    assert result.exit_code == 1, result.output
    assert "LHP-IO-028" in result.stderr
    assert "LHP_CONFIG_DIR" in result.stderr
