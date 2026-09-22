"""Behaviour of the per-invocation CLI telemetry hook.

Every case runs inside a real Click context — the only place the hook keeps
state — and reads the ``cli.command`` envelope that ``log`` mode prints to
stderr, so the assertions cover exactly what would be sent. Send-mode cases
point the endpoint at loopback ports that either refuse or never answer.
"""

from __future__ import annotations

import io
import json
import shutil
import socket
import sys
import threading
import time
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List

import click
import pytest
from click.testing import CliRunner

from lhp import telemetry
from lhp.cli import _telemetry_hook
from lhp.cli._app_context import build_facade
from lhp.cli.commands.dag_command import deps
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.exit_codes import ExitCode
from lhp.cli.presenters.event_stream._model import (
    FailureLine,
    RunOutcome,
    WarningLine,
)
from lhp.errors import ErrorCategory, LHPError
from lhp.telemetry import _client
from lhp.telemetry._environment import lhp_version
from lhp.telemetry._paths import spool_path
from lhp.telemetry._spool import read_lines
from lhp.telemetry._store import StateFile, read_state, write_state

pytestmark = pytest.mark.unit

_FIXTURE = Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
_ENVELOPE_PREFIX = '{"schema_version"'
_PROPS_KEYS = {f.name for f in fields(telemetry.CliCommandProps)}
_SENDER_THREAD = "lhp-telemetry-sender"

_MINIMAL_FLOWGROUP = """\
pipeline: hook_pipeline
flowgroup: hook_flowgroup
actions:
  - name: load_seed
    type: load
    source:
      type: sql
      sql: "SELECT 1 AS id"
    target: v_seed
  - name: write_seed
    type: write
    source: v_seed
    write_target:
      type: streaming_table
      database: hook_db
      table: output_table
"""

_BUNDLE_WITH_TARGETS = """\
bundle:
  name: hook_bundle
targets:
  dev:
    mode: development
  prod:
    mode: production
"""


class _Tty(io.StringIO):
    """A stderr stand-in that claims to be a terminal."""

    def isatty(self) -> bool:
        return True


@pytest.fixture(autouse=True)
def _fresh_sender_state() -> Iterator[None]:
    _client._reset_for_tests()
    yield
    _client._reset_for_tests()


@pytest.fixture
def minimal_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A one-flowgroup project as the cwd, without a bundle file."""
    root = tmp_path / "project"
    (root / "pipelines").mkdir(parents=True)
    (root / "lhp.yaml").write_text("name: hook_project\nversion: '1.0'\n")
    (root / "pipelines" / "hook.yaml").write_text(_MINIMAL_FLOWGROUP)
    monkeypatch.chdir(root)
    return root


@pytest.fixture
def fixture_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A deep copy of the e2e fixture project as the cwd."""
    dest = tmp_path / "testing_project"
    shutil.copytree(_FIXTURE, dest)
    monkeypatch.chdir(dest)
    return dest


def _envelopes(stderr: str) -> List[Dict[str, Any]]:
    lines = [line for line in stderr.splitlines() if line.startswith(_ENVELOPE_PREFIX)]
    return [json.loads(line) for line in lines]


def _only_envelope(stderr: str) -> Dict[str, Any]:
    found = _envelopes(stderr)
    assert len(found) == 1, stderr
    return found[0]


def _probe(operation: str, *, with_env: bool = True) -> click.Command:
    """A boundary-wrapped command with a value option, a flag and a defaulted int."""
    params: List[click.Parameter] = [
        click.Option(["--strict"], is_flag=True),
        click.Option(["--limit"], type=int, default=3),
    ]
    if with_env:
        params.insert(0, click.Option(["--env"], default=None))

    @cli_error_boundary(operation)
    def callback(**_: Any) -> None:
        return None

    return click.Command("probe", params=params, callback=callback)


def _failing_probe(message: str) -> click.Command:
    @cli_error_boundary("probe")
    def callback() -> None:
        raise ValueError(message)

    return click.Command("probe", params=[], callback=callback)


def _lhp_error() -> LHPError:
    return LHPError(
        category=ErrorCategory.CONFIG,
        code_number="001",
        title="Config error",
        details="Bad config",
    )


def _outcome(files_written: int = 12) -> RunOutcome:
    return RunOutcome(
        response=SimpleNamespace(total_files_written=files_written),
        warnings=(
            WarningLine("LHP-DEP-002", "m", None),
            WarningLine("LHP-DEP-002", "m", None),
            WarningLine("LHP-DEPR-001", "m", "f"),
        ),
        failures=(FailureLine("p", "LHP-VAL-007", "m"),),
        errored=False,
    )


def _finish_ok() -> None:
    _telemetry_hook.finish(exit_code=0, error_code=None, exception_class=None)


# finish: the cli.command props


def test_finish_records_command_flag_names_and_env_class(
    telemetry_log_mode: Path, minimal_project: Path
) -> None:
    (minimal_project / "databricks.yml").write_text(_BUNDLE_WITH_TARGETS)
    result = CliRunner().invoke(
        _probe("probe run"), ["--env", "dev", "--strict"], catch_exceptions=False
    )
    assert result.exit_code == 0, result.stderr

    envelope = _only_envelope(result.stderr)
    assert envelope["event"] == "cli.command"
    props = envelope["props"]
    assert set(props) == _PROPS_KEYS
    assert props["command"] == "probe.run"
    assert props["flags"] == ["env", "strict"]
    assert props["env_class"] == "development"
    assert props["exit_code"] == 0
    assert props["error_code"] is None
    assert props["exception_class"] is None
    assert isinstance(props["duration_ms"], int) and props["duration_ms"] >= 0
    assert props["warning_codes"] == {} and props["failure_codes"] == {}
    assert props["files_written"] is None
    assert props["bundle_enabled"] is None
    assert props["cache_used"] is None
    assert props["project"] is None


def test_flags_carry_parameter_names_never_values(
    telemetry_log_mode: Path, minimal_project: Path
) -> None:
    result = CliRunner().invoke(
        _probe("probe"),
        ["--env", "supersecret-target", "--limit", "7"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.stderr

    (line,) = [
        ln for ln in result.stderr.splitlines() if ln.startswith(_ENVELOPE_PREFIX)
    ]
    assert "supersecret-target" not in line
    assert json.loads(line)["props"]["flags"] == ["env", "limit"]


@pytest.mark.parametrize(
    ("bundle", "env", "expected"),
    [
        (False, "dev", "none"),
        (True, "dev", "development"),
        (True, "prod", "production"),
        (True, "staging", "unspecified"),
    ],
)
def test_env_class_follows_the_bundle_target_mode(
    telemetry_log_mode: Path,
    minimal_project: Path,
    bundle: bool,
    env: str,
    expected: str,
) -> None:
    if bundle:
        (minimal_project / "databricks.yml").write_text(_BUNDLE_WITH_TARGETS)
    result = CliRunner().invoke(_probe("probe"), ["--env", env], catch_exceptions=False)
    assert result.exit_code == 0, result.stderr
    assert _only_envelope(result.stderr)["props"]["env_class"] == expected


def test_env_class_is_null_for_a_command_without_an_env_parameter(
    telemetry_log_mode: Path, minimal_project: Path
) -> None:
    (minimal_project / "databricks.yml").write_text(_BUNDLE_WITH_TARGETS)
    result = CliRunner().invoke(
        _probe("probe", with_env=False), ["--strict"], catch_exceptions=False
    )
    assert result.exit_code == 0, result.stderr
    assert _only_envelope(result.stderr)["props"]["env_class"] is None


def test_finish_records_the_error_code_and_class_of_a_failure(
    telemetry_log_mode: Path, minimal_project: Path
) -> None:
    result = CliRunner().invoke(_failing_probe("sensitive-message"), [])
    assert result.exit_code == ExitCode.INTERNAL_ERROR

    (line,) = [
        ln for ln in result.stderr.splitlines() if ln.startswith(_ENVELOPE_PREFIX)
    ]
    props = json.loads(line)["props"]
    assert props["exit_code"] == 3
    assert props["error_code"] == "LHP-GEN-902"
    assert props["exception_class"] == "ValueError"
    assert "sensitive-message" not in line


def test_deps_alias_records_command_deps_with_the_parent_flags(
    telemetry_log_mode: Path, minimal_project: Path
) -> None:
    result = CliRunner().invoke(deps, ["--format", "json"], catch_exceptions=False)
    assert result.exit_code == 0, result.stderr

    props = _only_envelope(result.stderr)["props"]
    assert props["command"] == "deps"
    assert "output_format" in props["flags"]
    assert props["env_class"] is None
    assert props["project"]["flowgroups"] == 1


@pytest.mark.parametrize(
    ("error_code", "expected"),
    [
        ("LHP-CFG-001", "LHP-CFG-001"),
        ("LHP-VAL-DUPFG", "LHP-VAL-DUPFG"),
        ("LHP-EVT-SOFT-CAP", "LHP-EVT-SOFT-CAP"),
        ("", None),
        ("Bad config in /home/someone/lhp.yaml", None),
    ],
)
def test_finish_sends_only_a_recognised_lhp_error_code(
    telemetry_log_mode: Path,
    minimal_project: Path,
    capsys: pytest.CaptureFixture[str],
    error_code: str,
    expected: Any,
) -> None:
    with click.Context(click.Command("probe")):
        _telemetry_hook.begin("probe")
        _telemetry_hook.finish(
            exit_code=1, error_code=error_code, exception_class="LHPError"
        )
    props = _only_envelope(capsys.readouterr().err)["props"]
    assert props["error_code"] == expected


def test_opted_out_finish_reads_no_identity_and_sends_nothing(
    minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The suite-wide ``LHP_TELEMETRY=off`` guard is the consent under test."""
    touched: List[str] = []

    def spy(name: str) -> Any:
        def call(*args: Any, **kwargs: Any) -> None:
            touched.append(name)
            raise RuntimeError(name)

        return call

    for name in ("env_class", "record", "flush", "due_update_hint"):
        monkeypatch.setattr(telemetry, name, spy(name))
    (minimal_project / "databricks.yml").write_text(_BUNDLE_WITH_TARGETS)

    result = CliRunner().invoke(_probe("probe"), ["--env", "dev"])

    assert result.exit_code == 0, result.stderr
    assert touched == []


# begin


def test_begin_never_raises_into_the_command(
    minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def explode(ctx: Any) -> None:
        raise RuntimeError("run state")

    monkeypatch.setattr(_telemetry_hook, "_run_state", explode)
    result = CliRunner().invoke(_probe("probe"), [], catch_exceptions=False)
    assert result.exit_code == 0, result.stderr


def test_note_alias_names_the_run_on_the_shared_context_object() -> None:
    with click.Context(click.Command("deps")) as ctx:
        _telemetry_hook.note_alias("deps")
        _telemetry_hook.begin("dag")
        assert ctx.obj["telemetry"]["alias"] == "deps"
        assert ctx.obj["telemetry"]["operation"] == "dag"


def test_note_alias_outside_a_click_context_does_nothing() -> None:
    _telemetry_hook.note_alias("deps")


def test_note_alias_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def explode(ctx: Any) -> None:
        raise RuntimeError("run state")

    monkeypatch.setattr(_telemetry_hook, "_run_state", explode)
    with click.Context(click.Command("deps")):
        _telemetry_hook.note_alias("deps")


def test_begin_mutates_the_existing_run_dict_in_place() -> None:
    marker = {"alias": "deps"}
    with click.Context(click.Command("dag"), obj={"telemetry": marker}) as ctx:
        _telemetry_hook.begin("dag")
        assert ctx.obj["telemetry"] is marker
    assert marker["alias"] == "deps"
    assert marker["operation"] == "dag"


def test_begin_creates_the_context_object_when_a_command_runs_alone() -> None:
    with click.Context(click.Command("probe")) as ctx:
        assert ctx.obj is None
        _telemetry_hook.begin("probe")
        assert ctx.obj["telemetry"]["operation"] == "probe"


# note_run


def test_note_run_stores_the_project_shape_and_outcome_counters(
    telemetry_log_mode: Path, fixture_project: Path
) -> None:
    facade = build_facade(fixture_project)
    with click.Context(click.Command("generate")) as ctx:
        _telemetry_hook.begin("generate")
        _telemetry_hook.note_run(
            facade, _outcome(), bundle_enabled=False, no_cache=False
        )
        run = ctx.obj["telemetry"]

    assert set(run["project"]) == set(telemetry.PROJECT_SHAPE_KEYS)
    assert run["project"]["pipelines"] > 0
    assert run["project"]["flowgroups"] > 0
    assert run["warning_codes"] == {"LHP-DEP-002": 2, "LHP-DEPR-001": 1}
    assert run["failure_codes"] == {"LHP-VAL-007": 1}
    assert run["files_written"] == 12
    assert run["bundle_enabled"] is False
    assert run["cache_used"] is True


def test_note_run_folds_every_unrecognised_code_into_other(
    telemetry_log_mode: Path, minimal_project: Path
) -> None:
    outcome = RunOutcome(
        response=SimpleNamespace(total_files_written=0),
        warnings=(
            WarningLine("", "m", None),
            WarningLine(None, "m", None),  # type: ignore[arg-type]
            WarningLine("event buffer near limit", "m", None),
            WarningLine("LHP-VAL-DUPFG", "m", None),
            WarningLine("LHP-EVT-SOFT-CAP", "m", None),
            WarningLine("LHP-DEP-002", "m", None),
        ),
        failures=(
            FailureLine("p", "LHP-VAL-DUPFG", "m"),
            FailureLine("p", "", "m"),
        ),
        errored=False,
    )
    facade = build_facade(minimal_project)
    with click.Context(click.Command("validate")) as ctx:
        _telemetry_hook.begin("validate")
        _telemetry_hook.note_run(facade, outcome, bundle_enabled=None, no_cache=False)
        run = ctx.obj["telemetry"]

    assert run["warning_codes"] == {
        "other": 3,
        "LHP-VAL-DUPFG": 1,
        "LHP-EVT-SOFT-CAP": 1,
        "LHP-DEP-002": 1,
    }
    assert run["failure_codes"] == {"LHP-VAL-DUPFG": 1, "other": 1}


def test_note_run_without_an_outcome_records_empty_counters(
    telemetry_log_mode: Path, minimal_project: Path
) -> None:
    facade = build_facade(minimal_project)
    with click.Context(click.Command("dag")) as ctx:
        _telemetry_hook.begin("dag")
        _telemetry_hook.note_run(facade, None, bundle_enabled=None, no_cache=True)
        run = ctx.obj["telemetry"]

    assert run["project"]["flowgroups"] == 1
    assert run["warning_codes"] == {} and run["failure_codes"] == {}
    assert run["files_written"] is None
    assert run["bundle_enabled"] is None
    assert run["cache_used"] is False


def test_note_run_reports_the_cache_unused_under_lhp_no_cache(
    telemetry_log_mode: Path, minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LHP_NO_CACHE", "1")
    facade = build_facade(minimal_project)
    with click.Context(click.Command("dag")) as ctx:
        _telemetry_hook.begin("dag")
        _telemetry_hook.note_run(facade, None, bundle_enabled=None, no_cache=False)
        assert ctx.obj["telemetry"]["cache_used"] is False


def test_note_run_reads_nothing_when_consent_is_off(minimal_project: Path) -> None:
    """The suite-wide ``LHP_TELEMETRY=off`` guard is the consent under test."""
    accessed: List[str] = []

    class _Facade:
        def __getattr__(self, name: str) -> Any:
            accessed.append(name)
            raise AttributeError(name)

    with click.Context(click.Command("dag")) as ctx:
        _telemetry_hook.begin("dag")
        _telemetry_hook.note_run(_Facade(), None, bundle_enabled=None, no_cache=False)
        assert "project" not in ctx.obj["telemetry"]
    assert accessed == []


def test_note_run_never_raises(
    telemetry_log_mode: Path, minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import lhp.api._telemetry_shape as shape_module

    def explode(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("shape")

    monkeypatch.setattr(shape_module, "build_project_shape", explode)
    with click.Context(click.Command("dag")) as ctx:
        _telemetry_hook.begin("dag")
        _telemetry_hook.note_run(object(), None, bundle_enabled=None, no_cache=False)
        assert ctx.obj["telemetry"].get("project") is None


# finish: robustness


def test_finish_never_raises_when_record_raises(
    telemetry_log_mode: Path, minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def explode(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("record")

    monkeypatch.setattr(telemetry, "record", explode)
    with click.Context(click.Command("probe")):
        _telemetry_hook.begin("probe")
        _finish_ok()


def test_finish_records_once_per_run(
    telemetry_log_mode: Path, minimal_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with click.Context(click.Command("probe")):
        _telemetry_hook.begin("probe")
        _finish_ok()
        _telemetry_hook.finish(exit_code=1, error_code=None, exception_class=None)
    assert len(_envelopes(capsys.readouterr().err)) == 1


def test_finish_outside_a_click_context_records_nothing(
    telemetry_log_mode: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _telemetry_hook.begin("probe")
    _finish_ok()
    assert _envelopes(capsys.readouterr().err) == []


def test_finish_without_begin_records_nothing(
    telemetry_log_mode: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with click.Context(click.Command("probe")):
        _finish_ok()
    assert _envelopes(capsys.readouterr().err) == []


# finish: send mode


def _sender_threads() -> List[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == _SENDER_THREAD]


def test_finish_returns_within_the_join_budget_against_a_black_holed_endpoint(
    telemetry_log_mode: Path, minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A listener that never answers must cost the command at most the join."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    monkeypatch.delenv("LHP_TELEMETRY")
    monkeypatch.setenv("LHP_TELEMETRY_ENDPOINT", f"http://127.0.0.1:{port}/v1/events")
    assert telemetry.effective_state().mode == "send"

    try:
        with click.Context(click.Command("probe")):
            _telemetry_hook.begin("probe")
            started = time.perf_counter()
            _finish_ok()
            elapsed = time.perf_counter() - started
        assert elapsed < 2.0, elapsed
        assert any(t.is_alive() for t in _sender_threads()), "the send did not block"
    finally:
        listener.close()
    for thread in _sender_threads():
        thread.join(5.0)

    events = telemetry.spooled_events(10)
    assert [event["event"] for event in events] == ["install.first_seen", "cli.command"]
    assert events[-1]["props"]["command"] == "probe"
    returned = read_lines(spool_path(telemetry_log_mode))
    assert len(returned) == 2
    assert all(line.endswith(',"_unconfirmed":true}') for line in returned)


def _state_with_latest(cfg: Path, latest: str) -> None:
    write_state(
        cfg,
        StateFile(
            install_id="00000000-0000-4000-8000-000000000000",
            created_at="2026-01-01T00:00:00.000Z",
            last_version_seen=lhp_version(),
            latest_known_version=latest,
        ),
    )


def test_update_hint_is_printed_once_and_stamped(
    telemetry_log_mode: Path, minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("LHP_TELEMETRY")
    monkeypatch.setenv("LHP_TELEMETRY_ENDPOINT", "http://127.0.0.1:1/v1/events")
    _state_with_latest(telemetry_log_mode, "99.0.0")

    first = _Tty()
    monkeypatch.setattr(sys, "stderr", first)
    with click.Context(click.Command("probe")):
        _telemetry_hook.begin("probe")
        _finish_ok()
    assert f"lhp 99.0.0 is available (installed {lhp_version()})" in first.getvalue()
    assert "[LHP_UPDATE_CHECK=off to silence]" in first.getvalue()
    state = read_state(telemetry_log_mode)
    assert state is not None and state.update_hint_shown_at is not None

    second = _Tty()
    monkeypatch.setattr(sys, "stderr", second)
    with click.Context(click.Command("probe")):
        _telemetry_hook.begin("probe")
        _finish_ok()
    assert "is available" not in second.getvalue()


def test_update_hint_is_not_printed_after_a_failed_command(
    telemetry_log_mode: Path, minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("LHP_TELEMETRY")
    monkeypatch.setenv("LHP_TELEMETRY_ENDPOINT", "http://127.0.0.1:1/v1/events")
    _state_with_latest(telemetry_log_mode, "99.0.0")

    stderr = _Tty()
    monkeypatch.setattr(sys, "stderr", stderr)
    with click.Context(click.Command("probe")):
        _telemetry_hook.begin("probe")
        _telemetry_hook.finish(exit_code=1, error_code=None, exception_class=None)
    assert "is available" not in stderr.getvalue()
    state = read_state(telemetry_log_mode)
    assert state is not None and state.update_hint_shown_at is None


# classify_exit


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (None, (0, None, None)),
        (SystemExit(0), (0, None, None)),
        (SystemExit(None), (0, None, None)),
        (SystemExit(2), (2, None, None)),
        (SystemExit(ExitCode.ERROR), (1, None, None)),
        (SystemExit("message"), (1, None, None)),
        (click.UsageError("bad"), (2, None, "UsageError")),
        (click.BadParameter("bad"), (2, None, "BadParameter")),
        (click.ClickException("bad"), (1, None, "ClickException")),
        (click.exceptions.Exit(4), (4, None, "Exit")),
        (_lhp_error(), (1, "LHP-CFG-001", "LHPError")),
        (KeyboardInterrupt(), (130, None, "KeyboardInterrupt")),
        (ValueError("x"), (3, "LHP-GEN-902", "ValueError")),
    ],
)
def test_classify_exit(exc: Any, expected: Any) -> None:
    result = _telemetry_hook.classify_exit(exc)
    assert result == expected
    assert type(result[0]) is int
