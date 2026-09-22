"""Event envelope and per-event prop dataclasses.

These are NOT :class:`lhp.api.events.LHPEvent` subclasses and must never
become any: ``LHPEvent`` is the in-process streaming protocol the public API
emits to its callers, while these types are the wire format of an anonymous
usage record. The two have different audiences, different lifetimes and
different compatibility rules, and joining them would put a public contract
behind a telemetry schema bump.

The envelope's key ORDER is part of the contract with the Worker's allowlist
and the Delta table, so :func:`to_json_dict` derives it from the field
declaration order below rather than from a second list.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from typing import Any, Dict, Mapping, Optional, Tuple, TypeGuard

from lhp.telemetry._project_shape import (
    PROJECT_SHAPE_KEYS,
    ProjectShape,
    fold_project_shape,
)

__all__ = [
    "EVENT_NAMES",
    "LHP_CODE_PATTERN",
    "PROJECT_SHAPE_KEYS",
    "SCHEMA_VERSION",
    "CliCommandProps",
    "InstallProps",
    "ProjectShape",
    "TelemetryEnvelope",
    "WebRunProps",
    "WebSessionProps",
    "fold_project_shape",
    "is_lhp_code",
    "to_json_dict",
]

SCHEMA_VERSION = 1

# Every registered code, numeric (``LHP-DEP-002``) or named (``LHP-VAL-DUPFG``,
# ``LHP-EVT-SOFT-CAP``), in at most 24 characters: the wire's error-code cap.
LHP_CODE_PATTERN = re.compile(r"^LHP-[A-Z]{2,5}-[A-Z0-9][A-Z0-9-]{1,13}$")


def is_lhp_code(value: object) -> TypeGuard[str]:
    """Whether ``value`` may travel as an error code or a counter key."""
    return isinstance(value, str) and LHP_CODE_PATTERN.fullmatch(value) is not None


# ``web.ui`` is reserved in the wire schema but not emitted in v1: UI counts
# ride along in ``web.session.ui``.
EVENT_NAMES: Tuple[str, ...] = (
    "cli.command",
    "web.session",
    "web.run",
    "install.first_seen",
    "install.upgraded",
)


@dataclass(frozen=True)
class TelemetryEnvelope:
    """One anonymous usage record, ready to serialise.

    ``props`` is copied at construction so an envelope handed to the sender
    thread is not aliased to the mapping its caller built. The copy is a plain
    dict rather than a read-only view because the props of a built envelope
    still have to survive ``dataclasses.asdict``, ``copy.deepcopy`` and
    ``pickle``, none of which accept a mapping proxy.
    """

    schema_version: int
    event_id: str
    event: str
    ts: str
    install_id: Optional[str]
    project_id: Optional[str]
    project_id_source: str
    lhp_version: str
    python: str
    os: str
    arch: str
    install_kind: str
    ci_vendor: str
    agent: str
    databricks_runtime: bool
    interactive: bool
    props: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "props", dict(self.props))


@dataclass(frozen=True)
class CliCommandProps:
    """Props of a ``cli.command`` event.

    ``flags`` carries parameter NAMES only, never their values, and
    ``project`` is present only for the commands that resolve a project.
    """

    command: str
    flags: Tuple[str, ...]
    env_class: Optional[str]
    duration_ms: int
    exit_code: int
    error_code: Optional[str] = None
    exception_class: Optional[str] = None
    warning_codes: Mapping[str, int] = field(default_factory=dict)
    failure_codes: Mapping[str, int] = field(default_factory=dict)
    files_written: Optional[int] = None
    bundle_enabled: Optional[bool] = None
    cache_used: Optional[bool] = None
    project: Optional[ProjectShape] = None


@dataclass(frozen=True)
class WebSessionProps:
    """Props of a ``web.session`` event: one browser tab's whole session.

    The per-kind mappings are keyed by bounded enums — route families, file
    kinds, run kinds and ``surface.action`` labels — never by URLs, paths or
    file names.
    """

    session_id: str
    duration_s: int
    end_reason: str
    sse_seen: bool
    requests_by_family: Mapping[str, int]
    files_created: Mapping[str, int]
    files_updated: Mapping[str, int]
    files_deleted: Mapping[str, int]
    runs: Mapping[str, int]
    dag_views: int
    lineage_views: int
    sandbox_toggles: int
    assistant_used: bool
    assistant_provider: Optional[str]
    assistant_mode: Optional[str]
    ui: Mapping[str, int]


@dataclass(frozen=True)
class WebRunProps:
    """Props of a ``web.run`` event: one validate, generate or sandbox run.

    ``pipeline_filter`` records only THAT a filter was applied; the pipeline
    names it selected are never collected.
    """

    session_id: str
    kind: str
    trigger: str
    env_class: str
    sandbox: bool
    pipeline_filter: bool
    bundle_enabled: Optional[bool]
    duration_ms: int
    success: bool
    aborted: bool
    error_code: Optional[str]
    error_count: int
    warning_count: int
    files_written: Optional[int]


@dataclass(frozen=True)
class InstallProps:
    """Props of an ``install.upgraded`` event.

    ``install.first_seen`` carries an empty props mapping instead — there is
    no previous version to report.
    """

    previous_version: str


def to_json_dict(envelope: TelemetryEnvelope) -> Dict[str, Any]:
    """Render ``envelope`` as a JSON-ready dict in the wire key order.

    ``props`` is copied, so mutating the returned payload never reaches the
    envelope the sender thread still holds.
    """
    payload: Dict[str, Any] = {
        f.name: getattr(envelope, f.name) for f in fields(envelope)
    }
    payload["props"] = dict(envelope.props)
    return payload
