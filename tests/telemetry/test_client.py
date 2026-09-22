"""Tests for :mod:`lhp.telemetry._client` through the package surface.

Every case either opts into ``log`` mode with the ``telemetry_log_mode``
fixture or hands an explicit ``environ`` mapping to the entry point, so the
suite-wide ``LHP_TELEMETRY=off`` guard never leaks in and no test reaches the
developer's real config directory. Send-mode tests point the endpoint at a
loopback port nothing listens on, or replace ``urllib.request.urlopen`` with
a fake, so no request ever leaves the host.
"""

import json
import re
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pytest

from lhp import telemetry
from lhp.telemetry import _client
from lhp.telemetry._environment import lhp_version
from lhp.telemetry._paths import spool_path, state_path
from lhp.telemetry._spool import append_spool, read_lines
from lhp.telemetry._store import StateFile, read_state, write_state

ENVELOPE_KEYS = [
    "schema_version",
    "event_id",
    "event",
    "ts",
    "install_id",
    "project_id",
    "project_id_source",
    "lhp_version",
    "python",
    "os",
    "arch",
    "install_kind",
    "ci_vendor",
    "agent",
    "databricks_runtime",
    "interactive",
    "props",
]
UUID4 = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
LOOPBACK_ENDPOINT = "http://127.0.0.1:1/v1/events"
PROPS = {"command": "generate", "exit_code": 0}


@pytest.fixture(autouse=True)
def _fresh_sender_state() -> Iterator[None]:
    _client._reset_for_tests()
    yield
    _client._reset_for_tests()


@pytest.fixture
def cfg(tmp_path: Path) -> Path:
    return tmp_path / "cfg"


@pytest.fixture
def send_env(cfg: Path) -> Dict[str, str]:
    return {"LHP_CONFIG_DIR": str(cfg), "LHP_TELEMETRY_ENDPOINT": LOOPBACK_ENDPOINT}


def _record(env: Dict[str, str], name: str = "cli.command", **props: Any) -> None:
    telemetry.record(name, project_root=None, props=props or PROPS, environ=env)


def _spooled(cfg: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in read_lines(spool_path(cfg))]


class _Opener:
    """A fake ``urlopen``: answers 200, optionally after blocking until released,
    or fails with a transport error once released so the batch takes the
    retry path."""

    def __init__(self, block: bool = False, fail: bool = False) -> None:
        self.release = threading.Event()
        self.block = block
        self.fail = fail
        self.calls = 0

    def __call__(self, request: Any, **kwargs: Any) -> "_Opener":
        self.calls += 1
        if self.block:
            self.release.wait(10.0)
        if self.fail:
            raise urllib.error.URLError("unreachable")
        return self

    @property
    def status(self) -> int:
        return 200

    def read(self, amt: Optional[int] = None) -> bytes:
        return b'{"accepted":1,"rejected":0,"latest":"0.9.3"}'

    def __enter__(self) -> "_Opener":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


# off: nothing on disk


@pytest.mark.unit
@pytest.mark.parametrize(
    "switch",
    [{"LHP_TELEMETRY": "off"}, {"DO_NOT_TRACK": "1"}, {"PYTEST_CURRENT_TEST": "x"}],
)
def test_off_returns_before_any_disk_access(cfg: Path, switch: Dict[str, str]) -> None:
    env = {"LHP_CONFIG_DIR": str(cfg), **switch}
    _record(env)
    assert telemetry.flush(environ=env) is None
    assert (
        telemetry.new_event("cli.command", project_root=None, props=PROPS, environ=env)
        is None
    )
    assert telemetry.spool_count(environ=env) == 0
    assert not cfg.exists()


# log mode


@pytest.mark.unit
def test_log_mode_prints_one_exact_json_line_to_stderr(
    telemetry_log_mode: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    telemetry.record("cli.command", project_root=None, props=PROPS)
    err = capsys.readouterr().err
    lines = err.splitlines()
    assert len(lines) == 1
    assert err.endswith("\n")
    payload = json.loads(lines[0])
    assert list(payload) == ENVELOPE_KEYS
    assert payload["event"] == "cli.command"
    assert payload["props"] == PROPS
    assert payload["install_id"] is None
    assert payload["project_id"] is None
    assert payload["project_id_source"] == "none"
    assert payload["lhp_version"] == lhp_version()
    assert UUID4.match(payload["event_id"])
    assert TS.match(payload["ts"])
    assert json.dumps(payload, separators=(",", ":")) == lines[0]


@pytest.mark.unit
def test_log_mode_writes_nothing_and_starts_nothing(
    telemetry_log_mode: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    telemetry.record("cli.command", project_root=None, props=PROPS)
    assert telemetry.flush(join_s=1.0) is None
    assert not telemetry_log_mode.exists()
    assert telemetry.effective_state().mode == "log"


@pytest.mark.unit
def test_log_mode_reports_an_existing_install_id_without_writing(
    telemetry_log_mode: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_state(
        telemetry_log_mode,
        StateFile(install_id="existing-id", last_version_seen="0.0.1"),
    )
    before = state_path(telemetry_log_mode).read_bytes()
    telemetry.record("cli.command", project_root=None, props=PROPS)
    payload = json.loads(capsys.readouterr().err.splitlines()[0])
    assert payload["install_id"] == "existing-id"
    assert state_path(telemetry_log_mode).read_bytes() == before


# send mode


@pytest.mark.unit
def test_first_record_mints_the_install_and_spools_first_seen_before_the_event(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    _record(send_env)
    state = read_state(cfg)
    assert state is not None
    assert state.install_id is not None and UUID4.match(state.install_id)
    assert state.enabled is True
    assert state.created_at is not None and TS.match(state.created_at)
    assert state.last_version_seen == lhp_version()
    events = _spooled(cfg)
    assert [e["event"] for e in events] == ["install.first_seen", "cli.command"]
    assert events[0]["props"] == {}
    assert events[1]["props"] == PROPS
    assert {e["install_id"] for e in events} == {state.install_id}
    assert len({e["event_id"] for e in events}) == 2
    assert all(e["ci_vendor"] == "none" for e in events)


@pytest.mark.unit
def test_later_records_only_spool_the_event(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    _record(send_env)
    _record(send_env, command="validate", exit_code=1)
    events = _spooled(cfg)
    assert [e["event"] for e in events] == [
        "install.first_seen",
        "cli.command",
        "cli.command",
    ]
    assert events[-1]["props"] == {"command": "validate", "exit_code": 1}


@pytest.mark.unit
def test_a_version_change_spools_upgraded_before_the_event(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    write_state(cfg, StateFile(install_id="keep-me", last_version_seen="0.0.1"))
    _record(send_env)
    events = _spooled(cfg)
    assert [e["event"] for e in events] == ["install.upgraded", "cli.command"]
    assert events[0]["props"] == {"previous_version": "0.0.1"}
    state = read_state(cfg)
    assert state is not None
    assert state.install_id == "keep-me"
    assert state.last_version_seen == lhp_version()


@pytest.mark.unit
def test_a_state_file_without_an_install_id_gets_one_and_first_seen(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    write_state(cfg, StateFile(enabled=True))
    _record(send_env)
    state = read_state(cfg)
    assert state is not None and state.install_id is not None
    assert [e["event"] for e in _spooled(cfg)] == ["install.first_seen", "cli.command"]


@pytest.mark.unit
def test_ci_runs_spool_without_an_install_id_and_never_write_state(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    env = {**send_env, "GITHUB_ACTIONS": "true"}
    _record(env)
    assert read_state(cfg) is None
    events = _spooled(cfg)
    assert [e["event"] for e in events] == ["cli.command"]
    assert events[0]["install_id"] is None
    assert events[0]["ci_vendor"] == "github_actions"


@pytest.mark.unit
def test_ci_runs_ignore_an_existing_install_id(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    write_state(cfg, StateFile(install_id="existing-id", last_version_seen="0.0.1"))
    before = state_path(cfg).read_bytes()
    _record({**send_env, "TF_BUILD": "True"})
    assert _spooled(cfg)[0]["install_id"] is None
    assert state_path(cfg).read_bytes() == before


@pytest.mark.unit
def test_an_unknown_event_name_is_refused(cfg: Path, send_env: Dict[str, str]) -> None:
    _record(send_env, name="web.ui")
    assert telemetry.spool_count(environ=send_env) == 0


@pytest.mark.unit
def test_record_never_raises_on_unserialisable_props(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    telemetry.record(
        "cli.command", project_root=None, props={"bad": object()}, environ=send_env
    )
    assert "cli.command" not in [e["event"] for e in _spooled(cfg)]


@pytest.mark.unit
def test_record_is_inert_when_the_config_dir_cannot_be_created(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("file")
    env = {"LHP_CONFIG_DIR": str(blocker / "cfg")}
    _record(env)
    assert telemetry.spool_count(environ=env) == 0


@pytest.mark.unit
def test_record_is_safe_from_many_threads(cfg: Path, send_env: Dict[str, str]) -> None:
    def worker() -> None:
        for _ in range(5):
            _record(send_env)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10.0)
    events = _spooled(cfg)
    assert [e["event"] for e in events].count("install.first_seen") == 1
    assert len(events) == 41


# new_event


@pytest.mark.unit
def test_new_event_fills_the_envelope_from_the_project_without_writing(
    cfg: Path, send_env: Dict[str, str], tmp_path: Path
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "lhp.yaml").write_text("name: demo\nproject_id: abc\n", "utf-8")
    envelope = telemetry.new_event(
        "web.run", project_root=root, props={"kind": "validate"}, environ=send_env
    )
    assert envelope is not None
    assert envelope.event == "web.run"
    assert envelope.project_id == telemetry.read_project_identity(root).project_id
    assert envelope.project_id_source == "lhp_yaml"
    assert envelope.install_id is None
    assert envelope.props == {"kind": "validate"}
    assert not cfg.exists()


@pytest.mark.unit
def test_new_event_carries_the_stored_install_id(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    write_state(cfg, StateFile(install_id="existing-id"))
    envelope = telemetry.new_event(
        "cli.command", project_root=None, props=PROPS, environ=send_env
    )
    assert envelope is not None and envelope.install_id == "existing-id"
    assert telemetry.spool_count(environ=send_env) == 0


# effective_state


@pytest.mark.unit
def test_effective_state_is_send_by_default(send_env: Dict[str, str]) -> None:
    assert telemetry.effective_state(environ=send_env) == telemetry.TelemetryState(
        True, "send", "default"
    )


@pytest.mark.unit
def test_effective_state_reads_the_user_choice(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    telemetry.set_user_enabled(False, environ=send_env)
    assert telemetry.effective_state(environ=send_env) == telemetry.TelemetryState(
        False, "off", "user_state"
    )


# flush


@pytest.mark.unit
def test_flush_sends_the_spool_on_a_daemon_thread(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    opener = _Opener()
    monkeypatch.setattr(urllib.request, "urlopen", opener)
    _record(send_env)
    thread = telemetry.flush(join_s=2.0, environ=send_env)
    assert thread is not None
    assert thread.daemon is True
    assert thread.name == "lhp-telemetry-sender"
    assert not thread.is_alive()
    assert opener.calls == 1
    assert telemetry.spool_count(environ=send_env) == 0
    state = read_state(cfg)
    assert state is not None and state.latest_known_version == "0.9.3"


@pytest.mark.unit
def test_flush_returns_none_when_nothing_is_spooled(send_env: Dict[str, str]) -> None:
    assert telemetry.flush(environ=send_env) is None


@pytest.mark.unit
def test_flush_is_idempotent_while_a_send_is_in_flight(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    opener = _Opener(block=True)
    monkeypatch.setattr(urllib.request, "urlopen", opener)
    _record(send_env)
    first = telemetry.flush(environ=send_env)
    assert first is not None and first.is_alive()
    _record(send_env)
    assert telemetry.flush(environ=send_env) is None
    assert telemetry.flush(join_s=0.1, environ=send_env) is None
    assert opener.calls == 1
    opener.release.set()
    first.join(5.0)
    assert not first.is_alive()
    second = telemetry.flush(join_s=2.0, environ=send_env)
    assert second is not None and second is not first
    assert opener.calls == 2


@pytest.mark.unit
def test_flush_against_a_black_holed_endpoint_returns_within_the_join_budget(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    opener = _Opener(block=True)
    monkeypatch.setattr(urllib.request, "urlopen", opener)
    _record(send_env)
    started = time.perf_counter()
    thread = telemetry.flush(join_s=1.0, environ=send_env)
    elapsed = time.perf_counter() - started
    assert thread is not None and thread.is_alive()
    assert 0.9 <= elapsed < 2.0, elapsed
    opener.release.set()
    thread.join(5.0)


@pytest.mark.unit
def test_flush_leaves_the_batch_spooled_when_the_endpoint_refuses(
    cfg: Path, send_env: Dict[str, str]
) -> None:
    _record(send_env)
    thread = telemetry.flush(join_s=5.0, environ=send_env)
    assert thread is not None and not thread.is_alive()
    assert telemetry.spool_count(environ=send_env) == 2
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == []


@pytest.mark.unit
def test_flush_does_not_send_a_retained_spool_once_the_user_opts_out(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    opener = _Opener()
    monkeypatch.setattr(urllib.request, "urlopen", opener)
    _record(send_env)
    telemetry.set_user_enabled(False, environ=send_env)
    assert telemetry.flush(join_s=1.0, environ=send_env) is None
    assert opener.calls == 0
    assert telemetry.spool_count(environ=send_env) == 2


@pytest.mark.unit
def test_a_settling_sender_never_loses_a_concurrent_record(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    opener = _Opener(block=True, fail=True)
    monkeypatch.setattr(urllib.request, "urlopen", opener)
    for _ in range(20):
        _record(send_env)
    batch = telemetry.spool_count(environ=send_env)
    thread = telemetry.flush(environ=send_env)
    assert thread is not None and thread.is_alive()

    def worker() -> None:
        for _ in range(10):
            _record(send_env)

    workers = [threading.Thread(target=worker) for _ in range(6)]
    for w in workers:
        w.start()
    opener.release.set()
    for w in workers:
        w.join(10.0)
    thread.join(10.0)
    assert not thread.is_alive()
    events = _spooled(cfg)
    assert len(events) == batch + 60
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == []
    assert len({e["install_id"] for e in events}) == 1


@pytest.mark.unit
def test_a_settling_sender_never_overwrites_the_user_choice(
    cfg: Path, send_env: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    opener = _Opener(block=True)
    monkeypatch.setattr(urllib.request, "urlopen", opener)
    _record(send_env)
    thread = telemetry.flush(environ=send_env)
    assert thread is not None
    opener.release.set()
    telemetry.set_user_enabled(False, environ=send_env)
    thread.join(10.0)
    state = read_state(cfg)
    assert state is not None
    assert state.enabled is False
    assert state.latest_known_version == "0.9.3"


# package surface


@pytest.mark.unit
def test_package_exports_the_consumer_surface() -> None:
    expected = {
        "record",
        "new_event",
        "flush",
        "effective_state",
        "TelemetryState",
        "TelemetryEnvelope",
        "CliCommandProps",
        "WebSessionProps",
        "WebRunProps",
        "InstallProps",
        "ProjectShape",
        "read_project_identity",
        "ProjectIdentity",
        "env_class",
        "newer_version_available",
        "pending_update_hint",
        "mark_update_hint_shown",
        "set_user_enabled",
        "spool_count",
        "spooled_events",
        "fold_project_shape",
        "PROJECT_SHAPE_KEYS",
        "DEFAULT_ENDPOINT",
        "to_json_dict",
        # What ``lhp telemetry status`` reports: where the state lives, which
        # machine it identifies, where events would go, and whether CI means
        # no identity at all. Exported so the command never has to reach into
        # a private module for them.
        "config_dir",
        "state_path",
        "install_id",
        "resolve_endpoint",
        "ci_vendor",
        # The CLI's one-call update-hint decision.
        "due_update_hint",
        # The shared LHP code filter every emitter applies before a code
        # becomes a wire value or a counter key.
        "is_lhp_code",
        "LHP_CODE_PATTERN",
    }
    assert set(telemetry.__all__) == expected
    for name in expected:
        assert getattr(telemetry, name) is not None
