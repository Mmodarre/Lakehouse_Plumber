"""Anonymous usage-telemetry client for LHP (see TARGET_ARCHITECTURE §12).

Layering position: between ``lhp.models`` and ``lhp.errors`` in the
import-linter layers contract. The package may depend only on stdlib,
``pyyaml``, ``packaging`` and ``lhp.utils``; it must never import
``lhp.parsers``, ``lhp.core``, ``lhp.bundle``, ``lhp.models``, ``lhp.api``,
``lhp.cli`` or ``lhp.webapp`` (YAML is read with ``yaml.safe_load``, not the
``lhp.parsers`` loaders).

Consumers: ``lhp.cli``, ``lhp.webapp`` and ``lhp.api`` only. Every other LHP
package is barred by the ``Only CLI, webapp and api may import telemetry``
contract, so subprocess workers can never emit (constitution §13.5).

Two invariants hold for every public entry point: importing this package has
no side effects (no disk, no network, no threads, no state mutation), and no
call raises into its caller — failures are swallowed and logged at DEBUG.
"""
