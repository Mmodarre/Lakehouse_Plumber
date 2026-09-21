"""The Worker's key contract (DESIGN §D6), checked against emitted props.

The Worker rejects a whole batch when one event's props break the request
schema: a prop name outside ``^[a-z][a-z0-9_]{0,40}$``, or a key of a nested
object outside ``^[A-Za-z][A-Za-z0-9_.:-]{0,95}$``. Counter keys are the only
props whose names come from data rather than from a dataclass, so each event
is built here through the code path that emits it, fed every key that path
can produce plus the most hostile codes it accepts, JSON round-tripped, and
checked key by key.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import click
import pytest

from lhp import telemetry
from lhp.cli import _telemetry_hook
from lhp.cli._app_context import build_facade
from lhp.cli.presenters.event_stream._model import FailureLine, RunOutcome, WarningLine
from lhp.errors import codes
from lhp.webapp.middleware.telemetry_session import (
    OTHER_FAMILY,
    PREFIX_FAMILIES,
    ROUTE_FAMILIES,
)
from lhp.webapp.routers.telemetry import _label
from lhp.webapp.schemas.assistant import _PROVIDER_MODES
from lhp.webapp.schemas.telemetry import UI_ACTIONS, UI_SURFACES, UI_VIA, UiEvent
from lhp.webapp.services import _telemetry_events
from lhp.webapp.services.file_kinds import FileKind
from lhp.webapp.services.run_recorder import _TerminalOutcome
from lhp.webapp.services.telemetry_sessions import WebSessionRegistry

pytestmark = pytest.mark.unit

_PROP_NAME = re.compile(r"^[a-z][a-z0-9_]{0,40}$")
_OBJECT_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,95}$")
_ENVELOPE_PREFIX = '{"schema_version"'
_SID = "0f1e2d3c-4b5a-4978-8a9b-0c1d2e3f4a5b"

_REAL_CODES = ("LHP-DEP-002", "LHP-VAL-DUPFG", "LHP-EVT-SOFT-CAP")
_HOSTILE_CODES: tuple[Optional[str], ...] = (
    "",
    None,
    "event buffer near limit",
    "LHP-DEP-002\n",
    "x" * 200,
)

_FLOWGROUP = """\
pipeline: wire_pipeline
flowgroup: wire_flowgroup
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
      database: wire_db
      table: output_table
"""


def _assert_d6_props(props: Dict[str, Any]) -> None:
    """Assert every name and nested key of ``props`` survives a JSON round trip
    in the §D6 spelling, and every value keeps a §D6 type."""
    for name, value in props.items():
        if isinstance(value, dict):
            assert all(isinstance(key, str) for key in value), (name, list(value))
    wire = json.loads(json.dumps(props))
    assert len(wire) <= 64
    for name, value in wire.items():
        assert _PROP_NAME.fullmatch(name), name
        if isinstance(value, dict):
            for key, inner in value.items():
                assert _OBJECT_KEY.fullmatch(key), (name, key)
                assert inner is None or isinstance(inner, (bool, int, str)), key
        elif isinstance(value, list):
            assert len(value) <= 64, name
            assert all(isinstance(item, str) and len(item) <= 64 for item in value)
        elif isinstance(value, str):
            assert len(value) <= 128, name
        else:
            assert value is None or isinstance(value, (bool, int)), name


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "project"
    (root / "pipelines").mkdir(parents=True)
    (root / "lhp.yaml").write_text("name: wire_project\nversion: '1.0'\n")
    (root / "pipelines" / "wire.yaml").write_text(_FLOWGROUP)
    monkeypatch.chdir(root)
    return root


def test_every_registered_error_code_passes_the_shared_filter() -> None:
    rejected = [
        code.code for code in codes.ALL_CODES if not telemetry.is_lhp_code(code.code)
    ]
    assert rejected == []


def test_cli_command_props_meet_the_key_contract(
    telemetry_log_mode: Path, project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    all_codes = (*_REAL_CODES, *_HOSTILE_CODES)
    outcome = RunOutcome(
        response=SimpleNamespace(total_files_written=1),
        warnings=tuple(WarningLine(code, "m", None) for code in all_codes),  # type: ignore[arg-type]
        failures=tuple(FailureLine("p", code, "m") for code in all_codes),  # type: ignore[arg-type]
        errored=False,
    )
    with click.Context(click.Command("generate")):
        _telemetry_hook.begin("generate")
        _telemetry_hook.note_run(
            build_facade(project), outcome, bundle_enabled=True, no_cache=False
        )
        _telemetry_hook.finish(
            exit_code=1, error_code="Invalid config", exception_class="LHPError"
        )

    lines = [
        line
        for line in capsys.readouterr().err.splitlines()
        if line.startswith(_ENVELOPE_PREFIX)
    ]
    props = json.loads(lines[-1])["props"]
    _assert_d6_props(props)
    assert props["error_code"] is None
    assert props["warning_codes"] == {
        **dict.fromkeys(_REAL_CODES, 1),
        "other": len(_HOSTILE_CODES),
    }
    assert props["failure_codes"] == props["warning_codes"]
    assert props["project"]["flowgroups"] == 1


def _every_family() -> List[str]:
    families = {*ROUTE_FAMILIES.values(), OTHER_FAMILY}
    families.update(family for _, _, family in PREFIX_FAMILIES)
    return sorted(families)


def _every_ui_label() -> List[str]:
    labels = []
    for surface in UI_SURFACES:
        for action in UI_ACTIONS:
            for via in (None, *UI_VIA):
                label = _label(UiEvent(surface=surface, action=action, via=via))
                if label is not None:
                    labels.append(label)
    return labels


def test_web_session_props_meet_the_key_contract() -> None:
    delivered: List[Dict[str, Any]] = []

    def sink(name: str, *, project_root: Optional[Path], props: Dict[str, Any]) -> None:
        delivered.append(props)

    registry = WebSessionRegistry(sink=sink, flush=lambda: None)
    registry.touch(_SID)
    for family in _every_family():
        registry.count_request(_SID, family)
    for kind in FileKind:
        for op in ("created", "updated", "deleted"):
            registry.count_file(_SID, kind.value, op)
    for kind, trigger in (
        ("validate", "auto"),
        ("validate", "manual"),
        ("generate", "manual"),
    ):
        registry.count_run(_SID, kind, trigger, sandbox=True)
    labels = _every_ui_label()
    for label in labels:
        registry.count_ui(_SID, label)
    provider, modes = next(iter(_PROVIDER_MODES.items()))
    registry.mark_assistant(_SID, provider, next(iter(modes)))

    assert registry.emit_all("shutdown") == 1
    (props,) = delivered
    _assert_d6_props(props)
    assert set(props["ui"]) == set(labels)


@pytest.mark.parametrize("code", [*_REAL_CODES, *_HOSTILE_CODES])
def test_web_run_props_meet_the_key_contract(code: Optional[str]) -> None:
    ctx = _telemetry_events.RunTelemetryContext(
        session_id=_SID,
        trigger="manual",
        sandbox=False,
        pipeline_filter=True,
        bundle_enabled=True,
        registry=WebSessionRegistry(sink=lambda *a, **k: None, flush=lambda: None),
    )
    outcome = _TerminalOutcome(
        status="failed", summary={"success": False, "error_code": code}, issues=()
    )

    props = _telemetry_events.build_web_run_props(ctx, "generate", "none", outcome, 5)

    _assert_d6_props(props)
    assert props["error_code"] == (code if code in _REAL_CODES else None)
