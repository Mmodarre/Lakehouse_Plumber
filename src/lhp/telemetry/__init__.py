"""Anonymous usage-telemetry client for LHP (see TARGET_ARCHITECTURE §12).

Layering position: between ``lhp.models`` and ``lhp.errors`` in the
import-linter layers contract. The package may depend only on stdlib,
``pyyaml``, ``packaging`` and ``lhp.utils``; it must never import
``lhp.errors``, ``lhp.parsers``, ``lhp.core``, ``lhp.bundle``, ``lhp.models``,
``lhp.api``, ``lhp.cli`` or ``lhp.webapp``. YAML is read with
``yaml.safe_load``, not the ``lhp.parsers`` loaders, and ``lhp.errors`` is
excluded because nothing here constructs an ``LHPError`` (see the invariants
below).

Consumers: ``lhp.cli``, ``lhp.webapp`` and ``lhp.api`` only. Every other LHP
package is barred by the ``Only CLI, webapp and api may import telemetry``
contract, so subprocess workers can never emit (constitution §13.5).

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
