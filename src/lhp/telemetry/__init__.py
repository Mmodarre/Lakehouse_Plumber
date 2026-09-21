"""Anonymous usage-telemetry client for LHP (see TARGET_ARCHITECTURE §12).

Layering position: between ``lhp.models`` and ``lhp.errors`` in the
import-linter layers contract. The package may depend only on stdlib,
``pyyaml``, ``packaging`` and ``lhp.utils``; it must never import
``lhp.errors``, ``lhp.parsers``, ``lhp.core``, ``lhp.bundle``, ``lhp.models``,
``lhp.api``, ``lhp.cli`` or ``lhp.webapp``. YAML is read with
``yaml.safe_load``, not the ``lhp.parsers`` loaders, and ``lhp.errors`` is
excluded because nothing here raises: an ``LHPError`` would have no caller to
catch it.

Consumers: ``lhp.cli``, ``lhp.webapp`` and ``lhp.api`` only. Every other LHP
package is barred by the ``Only CLI, webapp and api may import telemetry``
contract, so subprocess workers can never emit (constitution §13.5).

Two invariants hold for every public entry point, with no exceptions:
importing this package has no side effects (no disk, no network, no threads,
no state mutation), and no call raises into its caller. Failures are logged
at DEBUG and reported by return value — a write that could not happen comes
back as a falsy result, and it is the caller's job to decide whether that
matters. Only ``cli/commands/telemetry_command.py`` turns such a result into
an ``LHPError`` (``LHP-IO-028``), because only there does a user who asked to
change the setting need to hear that it did not take.
"""
