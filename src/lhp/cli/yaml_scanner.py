"""DEAD MODULE — pending deletion in the CLI rebuild (C5).

This module used to host ``emit_deprecation_warning_if_needed``: the
pre-pool, main-thread scan that read each discovered flowgroup YAML looking
for the deprecated bare-``{token}`` substitution syntax and recorded a
single ``WarningCollector`` entry on the FIRST match.

As of C4 that detection moved to the core flowgroup-read path —
:meth:`lhp.core.discovery.flowgroup_discoverer.FlowgroupDiscoveryService.scan_deprecation_warnings`
(delegating to :func:`lhp.core.discovery.deprecation_scanner.scan_bare_token_deprecations`).
The core scan is strictly better: it scans EVERY file (no first-match
early return) and emits one structured
:class:`lhp.models.processing.DeprecationWarningRecord` (``LHP-DEPR-001``)
PER offending file, exposed via the discovery-service ABC method so the
facade can pick it up.

The detection logic and both old call sites (``cli/commands/generate_command.py``
and ``cli/commands/validate_command.py``) have been removed. This file is
left as a tombstone rather than deleted because the CLI rebuild is a later
phase: C5 wires ``scan_deprecation_warnings`` through the facade as
``WarningEmitted`` events, retires the ``WarningCollector`` side channel,
and deletes this module. It is intentionally empty and imported by nothing.
"""

__all__: list[str] = []
