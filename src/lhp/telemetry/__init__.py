"""Anonymous usage-telemetry client for LHP (see TARGET_ARCHITECTURE §12).

Layering position: between ``lhp.models`` and ``lhp.errors`` in the
import-linter layers contract. The package may depend only on stdlib,
``pyyaml``, ``packaging`` and ``lhp.utils``; it must never import
``lhp.errors``, ``lhp.parsers``, ``lhp.core``, ``lhp.bundle``, ``lhp.models``,
``lhp.api``, ``lhp.cli`` or ``lhp.webapp``. YAML is read with
``yaml.safe_load``, not the ``lhp.parsers`` loaders, and ``lhp.errors`` is
excluded because nothing here constructs an ``LHPError`` (see the invariants
below).

Consumers: ``lhp.cli``, ``lhp.webapp`` and ``lhp.api`` only. Every other
package in the layers contract is barred by the ``Only CLI, webapp and api
may import telemetry`` contract, so subprocess workers can never emit
(constitution §13.5). Consumers import this package, never its modules.

The wire type is ``TelemetryEnvelope``. It is not an
``lhp.api.events.LHPEvent`` and never subclasses it: ``LHPEvent`` is the
public in-process operation-stream protocol, while an envelope is an opaque
record shipped off-box under its own schema.

Two invariants hold for every public entry point: importing this package has
no side effects (no disk, no network, no threads, no state mutation), and no
call raises into its caller — with one sanctioned exit. ``set_user_enabled``,
the write behind ``lhp telemetry on|off``, lets a plain ``OSError``
propagate, because a user who asked to change the setting has to hear that it
did not take. It is never an ``LHPError``: wrapping it as ``LHP-IO-028`` via
``ErrorFactory.io_error(...) from e`` is the job of
``cli/commands/telemetry_command.py``, which is why this package still needs
no ``lhp.errors`` import. Every other call logs its failures at DEBUG and
goes inert.
"""

from lhp.telemetry._client import effective_state, flush, new_event, record
from lhp.telemetry._consent import TelemetryState
from lhp.telemetry._events import (
    PROJECT_SHAPE_KEYS,
    CliCommandProps,
    InstallProps,
    ProjectShape,
    TelemetryEnvelope,
    WebRunProps,
    WebSessionProps,
    fold_project_shape,
    to_json_dict,
)
from lhp.telemetry._identity import ProjectIdentity, env_class, read_project_identity
from lhp.telemetry._paths import DEFAULT_ENDPOINT
from lhp.telemetry._preferences import (
    mark_update_hint_shown,
    newer_version_available,
    set_user_enabled,
    spool_count,
    spooled_events,
)
from lhp.telemetry._update_check import pending_update_hint

__all__ = [
    "DEFAULT_ENDPOINT",
    "PROJECT_SHAPE_KEYS",
    "CliCommandProps",
    "InstallProps",
    "ProjectIdentity",
    "ProjectShape",
    "TelemetryEnvelope",
    "TelemetryState",
    "WebRunProps",
    "WebSessionProps",
    "effective_state",
    "env_class",
    "flush",
    "fold_project_shape",
    "mark_update_hint_shown",
    "new_event",
    "newer_version_available",
    "pending_update_hint",
    "read_project_identity",
    "record",
    "set_user_enabled",
    "spool_count",
    "spooled_events",
    "to_json_dict",
]
