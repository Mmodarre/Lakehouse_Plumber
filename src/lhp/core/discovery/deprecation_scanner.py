"""Main-thread bare-``{token}`` deprecation scan (LHP-DEPR-001).

Single source of truth for detecting the deprecated bare-``{token}``
substitution syntax (use ``${token}`` instead — ``%{local_var}`` stays
valid). Runs on the main thread before any worker pool is spawned: workers
attach a ``NullHandler`` only, so ``logger.warning`` from inside a worker
never reaches the user.

Emits structured :class:`DeprecationWarningRecord` data; the facade wrapper
merges these into the event stream as ``WarningEmitted`` events. See
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

    Routes through :meth:`ErrorFactory.deprecation_error` to keep the
    ``DEPR_001`` code and the factory path live. Composes only
    ``title`` + ``details`` — NOT the full ``str(LHPError)`` panel, which is
    inappropriate for a non-fatal warning surfaced as
    :class:`~lhp.api.WarningEmitted`. Text is file-agnostic; per-file
    identity lives in :attr:`DeprecationWarningRecord.file`.
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

    Scans EVERY file — does NOT stop at the first match. Multiple bare tokens
    in a single file collapse to a single per-file record (``flowgroup=None``).
    Dedup is by RESOLVED path (insertion-ordered). Unreadable paths are
    skipped — the parse pass surfaces real failures; this scan must never raise.
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
