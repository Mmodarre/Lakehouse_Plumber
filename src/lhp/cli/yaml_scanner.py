"""Pre-pool YAML scan helpers shared by ``lhp generate`` and ``lhp validate``.

Workers spawned by the generate/validate pools attach a ``NullHandler``
only (see :func:`_init_worker_logger`), so any ``logger.warning``
emitted from within :class:`EnhancedSubstitutionManager` during
substitution cannot reach the user. The pre-pool scan in this module
compensates by reading each discovered flowgroup YAML on the main
thread, looking for the same deprecated bare-``{token}`` syntax, and
recording the warning on a per-run :class:`WarningCollector`.

The helper is intentionally separate from :mod:`lhp.cli.live_panel`
(rendering) and :mod:`lhp.cli.warning_collector` (accumulation): one
file per concern, importable from both command modules without a
cycle.
"""

import re
from pathlib import Path
from typing import Iterable, Optional

from .warning_collector import WarningCollector

# Scans raw YAML BEFORE any substitution pass, so it must reject every
# non-bare syntax that LHP supports:
#   * ``${X}``  — dollar-prefix tokens (lookbehind rejects ``$``)
#   * ``%{X}``  — local-variable tokens (lookbehind rejects ``%``)
#   * ``{{X}}`` — Jinja2 template parameters (lookbehind rejects ``{``,
#     lookahead rejects ``}``; both ends required to catch the
#     no-internal-space ``{{X}}`` case in addition to ``{{ X }}``)
_BARE_TOKEN_PATTERN = re.compile(r"(?<![${%])\{(\w+)\}(?!\})")

_DEPRECATION_MESSAGE = (
    "The bare {token} substitution syntax is deprecated and will be removed "
    "in v1.0. Use ${token} instead."
)


def emit_deprecation_warning_if_needed(
    warning_collector: WarningCollector,
    flowgroup_yaml_paths: Iterable[Optional[Path]],
) -> None:
    """Pre-pool scan: record the bare-``{token}`` deprecation warning.

    Reads each YAML path on the main thread. The first match flips the
    flag on ``warning_collector`` (further duplicates are silently
    suppressed by the collector's dedup) and the function returns
    immediately — no need to keep scanning after the warning has been
    surfaced.

    Args:
        warning_collector: Per-run collector that will receive the
            ``("deprecation", ...)`` entry on first match.
        flowgroup_yaml_paths: Iterable of optional paths. ``None`` or
            non-``Path`` values are skipped defensively (the orchestrator
            returns ``Optional[Path]``; tests sometimes pass ``Mock``).
    """
    for path in flowgroup_yaml_paths:
        # Defensive: skip anything that isn't an actual Path. Production
        # callers pass ``Optional[Path]`` per the type hint, but unit
        # tests that mock the orchestrator may surface ``Mock`` objects
        # whose ``read_text`` returns non-string. Treat anything that
        # isn't a real Path as "no YAML to scan".
        if not isinstance(path, Path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if _BARE_TOKEN_PATTERN.search(text):
            warning_collector.add("deprecation", _DEPRECATION_MESSAGE)
            return
