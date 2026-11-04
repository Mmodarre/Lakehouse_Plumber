"""Worker-scoped collection of soft-deprecation and validation warnings.

Soft-deprecation sites run deep inside a spawn worker — during flowgroup
resolution (the ``database`` / ``database_suffix`` normalization) and during
codegen / validation (the schema-transform ``enforcement`` key). Worker-side
validators record coded, non-fatal validation warnings (e.g. ``LHP-VAL-073``)
there too. Workers run
under a ``NullHandler`` (their ``logger.warning`` calls are swallowed), so a
warning can only reach the user as structured data on
:attr:`FlowgroupOutcome.warnings`, which the main thread re-emits as a
:class:`~lhp.api.WarningEmitted` event that counts under ``--strict``.

Threading a collector parameter from the worker entry down through the
validator framework AND the per-action codegen chain AND the
layer-below ``parsers`` package would touch a large surface. Two of the three
sites also live in ``parsers`` / ``generators``, which sit in DIFFERENT layers
(``parsers`` is BELOW ``core``; ``generators`` is ABOVE it), so no shared
``core`` collaborator can reach all three. ``models`` is the one layer every
consumer sits above and may import, and it already owns the
:data:`RunWarningRecord` DTOs — so the worker-scoped collector lives
here too.

The mechanism mirrors the worker-scoped perf singleton
(:mod:`lhp.utils.performance_timer`): the worker wrapper opens a
:func:`collect_deprecations` scope around the single flowgroup it processes,
the nested sites call :func:`record_deprecation` or
:func:`record_validation_warning`, and the wrapper drains the
records onto the returned outcome. A :class:`contextvars.ContextVar` (rather
than a plain module global) scopes the active collector cleanly without a lock:
each spawn worker is its own process and handles ONE flowgroup at a time, so
there is no cross-flowgroup bleed.

The deprecation warning ``message`` is sourced from
:meth:`lhp.errors.ErrorFactory.deprecation_error` — the single, central
formatter for every ``LHP-DEPR-*`` code — so the wording is not duplicated
inline. The full ``LHPError`` panel (``Error [LHP-DEPR-00N]: ... ====``) is
error-framed and inappropriate as a non-fatal warning, so only the human-facing
``title`` + ``details`` are composed into the warning text (the code rides
separately on the record). A validation warning's ``message`` is the
producer's text verbatim.
"""

import logging
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, List, Optional, Sequence, Tuple

from .processing import (
    DeprecationWarningRecord,
    RunWarningRecord,
    ValidationWarningRecord,
)

if TYPE_CHECKING:
    from ..errors.codes import ErrorCode

logger = logging.getLogger(__name__)


@dataclass
class _RunWarningCollector:
    """Mutable per-flowgroup accumulator for :data:`RunWarningRecord`s.

    Holds the flowgroup identity (``file`` / ``flowgroup``) once so the nested
    call sites need only supply the warning's code + message material;
    every recorded record is stamped with this identity automatically.
    """

    file: Optional[Path]
    flowgroup: Optional[str]
    records: List[RunWarningRecord] = field(default_factory=list)


# The active collector for the flowgroup currently being processed in THIS
# worker, or ``None`` when no scope is open (e.g. unit tests that call a site
# directly, or the main thread). Every ``record_*`` function is a no-op when
# unset.
_active: ContextVar[Optional[_RunWarningCollector]] = ContextVar(
    "lhp_run_warning_collector", default=None
)


# REMOVE_AT_V1.0.0: delete with the last soft-deprecated field (``database`` /
# ``database_suffix`` / schema-transform ``enforcement``).
def _deprecation_message(code: "ErrorCode", title: str, details: str) -> str:
    """Render the warning text for a DEPR code via the central error factory.

    Reuses :meth:`ErrorFactory.deprecation_error` as the single source of the
    formatted wording (keeping ``deprecation_error`` a live, non-dead symbol),
    but composes only the human-facing ``title`` + ``details`` — NOT the full
    error-framed ``str(LHPError)`` panel, which is inappropriate for a
    non-fatal warning surfaced as :class:`~lhp.api.WarningEmitted`.
    """
    # Lazy import: ``models`` sits above ``errors`` in the layering, and
    # ``models/processing`` already imports ``lhp.errors`` lazily to avoid an
    # initialization cycle — match that pattern here.
    from ..errors import ErrorFactory

    error = ErrorFactory.deprecation_error(code, title=title, details=details)
    return f"{error.title} {error.details}"


# REMOVE_AT_V1.0.0: delete with the last soft-deprecated field, together with
# every call site.
def record_deprecation(
    code: "ErrorCode",
    *,
    title: str,
    details: str,
) -> None:
    """Record one soft-deprecation warning onto the active worker scope.

    A no-op when no :func:`collect_deprecations` scope is open (so call sites
    are safe to invoke from unit tests or the main thread). The record's
    ``file`` / ``flowgroup`` are taken from the active scope; the rendered
    ``code`` string and the composed ``message`` come from ``code`` / the
    central :meth:`ErrorFactory.deprecation_error` formatter.
    """
    collector = _active.get()
    if collector is None:
        return
    collector.records.append(
        DeprecationWarningRecord(
            code=code.code,
            message=_deprecation_message(code, title, details),
            file=collector.file,
            flowgroup=collector.flowgroup,
        )
    )


def record_validation_warning(code: "ErrorCode", *, message: str) -> None:
    """Record one coded validation warning onto the active worker scope.

    For worker-side validators whose finding must surface WITHOUT failing the
    flowgroup: the record rides back as a
    :class:`~lhp.models.processing.ValidationWarningRecord` and reaches the
    user as ``WarningEmitted(category="validation")``, which counts under
    ``--strict`` on both ``lhp validate`` and ``lhp generate``.

    The record's ``file`` / ``flowgroup`` are taken from the active scope and
    ``message`` is kept verbatim. Records dedup by ``(code, file)`` (first
    wins), so a producer MUST put every offending item for a flowgroup into ONE
    message rather than recording one warning per item.

    A no-op when no :func:`collect_deprecations` scope is open. NEVER raises
    (constitution §5.6): a malformed ``code`` drops the warning instead of
    taking the worker down.
    """
    collector = _active.get()
    if collector is None:
        return
    try:
        record = ValidationWarningRecord(
            code=code.code,
            message=message,
            file=collector.file,
            flowgroup=collector.flowgroup,
        )
    except Exception:  # §5.6: recording a warning must never fail the worker
        logger.exception(
            f"Dropped a validation warning for flowgroup {collector.flowgroup!r}: "
            f"malformed code of type {type(code).__name__}"
        )
        return
    collector.records.append(record)


@contextmanager
def collect_deprecations(
    *,
    file: Optional[Path],
    flowgroup: Optional[str],
) -> Iterator[_RunWarningCollector]:
    """Open a worker scope collecting run warnings for ONE flowgroup.

    Opened by the worker wrapper around the single flowgroup it resolves +
    generates. Records appended via :func:`record_deprecation` or
    :func:`record_validation_warning` while the scope is open are stamped with
    ``file`` / ``flowgroup`` and gathered on the yielded collector;
    :func:`drain_deprecations` reads them back. The previous active collector
    (normally ``None``) is restored on exit, so nested or repeated scopes never
    leak.
    """
    collector = _RunWarningCollector(file=file, flowgroup=flowgroup)
    token: Token = _active.set(collector)
    try:
        yield collector
    finally:
        _active.reset(token)


def drain_deprecations(
    collector: _RunWarningCollector,
) -> Tuple[RunWarningRecord, ...]:
    """Return the scope's records as a tuple, deduped by ``(code, file)``.

    The same deprecation can fire more than once for one flowgroup: the
    schema-transform ``enforcement`` site fires once in the validator AND once
    in codegen (generate mode), and the ``parse_inline_schema`` path also calls
    ``parse_file_data`` so a single inline schema trips the warning twice with
    SLIGHTLY different wording. Several actions in one flowgroup can each trip
    the ``database`` warning, too. All collapse to the FIRST-seen record per
    ``(code, file)`` — the same granularity the per-pipeline engine merge
    enforces (``merge_flowgroup_warnings``: one warning per affected file per
    code, NOT one per flowgroup or per call site). ``message`` / ``flowgroup``
    are NOT part of the key; the first record's full payload is kept verbatim.
    Order is deterministic first-seen.
    """
    return _dedup(collector.records)


def _dedup(
    records: Sequence[RunWarningRecord],
) -> Tuple[RunWarningRecord, ...]:
    seen: dict[Tuple[str, Optional[Path]], RunWarningRecord] = {}
    for record in records:
        seen.setdefault((record.code, record.file), record)
    return tuple(seen.values())
