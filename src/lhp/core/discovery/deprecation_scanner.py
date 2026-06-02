"""Main-thread bare-``{token}`` deprecation scan (LHP-DEPR-001).

The single source of truth for detecting the deprecated bare-``{token}``
substitution syntax (use ``${token}`` instead — ``%{local_var}`` stays
valid). The scan runs on the MAIN THREAD over the discovered flowgroup
YAML files, before any worker pool is spawned: workers attach a
``NullHandler`` only, so a ``logger.warning`` from inside a worker never
reaches the user.

This replaces the two earlier detection points:

  * ``lhp.cli.yaml_scanner.emit_deprecation_warning_if_needed`` — the
    pre-pool CLI scan that stopped at the FIRST offending file; and
  * the per-pipeline ``has_deprecated_bare_tokens`` fan-out in
    ``flowgroup_worklist_builder.build_flowgroup_worklist``.

The scan emits structured :class:`DeprecationWarningRecord` data (the same
transport the worker-side warnings ride on); the facade wrapper merges these
into the event stream as ``WarningEmitted`` events. See
:func:`scan_bare_token_deprecations`.
"""

import re
from pathlib import Path
from typing import Dict, Iterable

from ...errors import ErrorFactory, codes
from ...models.processing import DeprecationWarningRecord

# Scans raw YAML BEFORE any substitution pass, so it must reject every
# non-bare syntax LHP supports:
#   * ``${X}``  — dollar-prefix tokens (lookbehind rejects ``$``)
#   * ``%{X}``  — local-variable tokens (lookbehind rejects ``%``)
#   * ``{{X}}`` — Jinja2 template parameters (lookbehind rejects ``{``,
#     lookahead rejects ``}``; both ends required to catch the
#     no-internal-space ``{{X}}`` case in addition to ``{{ X }}``)
_BARE_TOKEN_PATTERN = re.compile(r"(?<![${%])\{(\w+)\}(?!\})")


def _render_deprecation_message() -> str:
    """Render the LHP-DEPR-001 warning text once, via the error factory.

    Routing through :meth:`ErrorFactory.deprecation_error` keeps the
    ``DEPR_001`` code and the factory path live (not dead code). Composes
    only the human-facing ``title`` + ``details`` — NOT the full
    error-framed ``str(LHPError)`` panel, which is inappropriate for a
    non-fatal warning surfaced as :class:`~lhp.api.WarningEmitted`. This
    matches the worker-side composition in
    :func:`lhp.models.deprecations._deprecation_message`, so the two warning
    sources read identically once merged into the event stream.

    The text is file-agnostic — the per-file identity lives in
    :attr:`DeprecationWarningRecord.file`, so one render is reused across
    every offending file.
    """
    error = ErrorFactory.deprecation_error(
        code=codes.DEPR_001,
        title="Deprecated bare {token} substitution syntax",
        details=(
            "The bare {token} substitution syntax is deprecated and will be "
            "removed in v1.0. Use ${token} instead (the only valid non-$ "
            "braces form is %{local_var} for local variables)."
        ),
    )
    return f"{error.title} {error.details}"


def scan_bare_token_deprecations(
    yaml_files: Iterable[Path],
) -> tuple[DeprecationWarningRecord, ...]:
    """Detect bare-``{token}`` syntax across ``yaml_files``; one record per file.

    Reads each path on the main thread and tests it against
    :data:`_BARE_TOKEN_PATTERN`. Scans EVERY file — it does NOT stop at the
    first match — and emits EXACTLY ONE :class:`DeprecationWarningRecord`
    per offending file (multiple bare tokens in a single file collapse to a
    single per-file record; ``file`` is that file's path, ``flowgroup`` is
    ``None`` because the warning is whole-file). Files using only
    ``${...}`` / ``%{...}`` / ``{{...}}`` — and clean files — yield nothing.

    Dedup is by RESOLVED path, so the same file surfaced twice by the glob
    collapses to one record. Order is first-seen (an insertion-ordered
    ``dict``), mirroring the worker-side
    :func:`lhp.core.coordination._pool._merge_flowgroup_warnings` convention.

    A path that vanished or is unreadable between glob and read is skipped
    here — the parse pass is what surfaces real read/parse failures as fatal
    errors; this scan must never raise.

    Args:
        yaml_files: The flowgroup YAML paths to scan (typically the project's
            discovered ``pipelines/**/*.yaml`` set).

    Returns:
        One :class:`DeprecationWarningRecord` per offending file in
        first-seen order; empty if none offend.
    """
    deduped: Dict[Path, DeprecationWarningRecord] = {}
    message: str | None = None
    for yaml_file in yaml_files:
        resolved = yaml_file.resolve()
        if resolved in deduped:
            continue
        try:
            text = yaml_file.read_text(encoding="utf-8")
        except OSError:
            continue
        if not _BARE_TOKEN_PATTERN.search(text):
            continue
        if message is None:
            message = _render_deprecation_message()
        deduped[resolved] = DeprecationWarningRecord(
            code=codes.DEPR_001.code,
            message=message,
            file=yaml_file,
            flowgroup=None,
        )
    return tuple(deduped.values())
