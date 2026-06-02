"""Shared error <-> view conversion helpers for the public API surface.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Holds the cross-direction error-conversion helpers used by both the
generation and validation converter modules (and by the project
preflight): the :class:`LHPError` <-> :class:`ValidationIssueView`
projection pair, plus the shared deprecation-warning emission helper
used by the generate and validate streams.

:stability: internal
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Literal, Optional, Sequence, Set, Tuple

from lhp.api.events import WarningEmitted
from lhp.api.views import ValidationIssueView

if TYPE_CHECKING:
    from lhp.errors import LHPError
    from lhp.models.processing import DeprecationWarningRecord


def _lhp_error_to_issue_view(
    lhp_err: "LHPError",
    *,
    pipeline_name: Optional[str] = None,
    flowgroup_name: Optional[str] = None,
    file_path: Optional[Path] = None,
    severity: Literal["error", "warning"] = "error",
) -> ValidationIssueView:
    """Project an :class:`LHPError` onto a public :class:`ValidationIssueView`.

    Shared between the generation and validation paths. ``file_path`` is the
    originating flowgroup's source YAML on disk (the validation path threads
    it through from the per-issue
    :class:`~lhp.models.processing.ValidationIssueRecord`); it defaults to
    ``None`` so the generation / preflight callers are unaffected.
    """
    return ValidationIssueView(
        code=lhp_err.code,
        category=lhp_err.category.value,
        severity=severity,
        title=lhp_err.title,
        details=lhp_err.details or None,
        pipeline_name=pipeline_name,
        flowgroup_name=flowgroup_name,
        file_path=file_path,
        suggestions=tuple(lhp_err.suggestions or ()),
        context=dict(lhp_err.context or {}),
        doc_link=lhp_err.doc_link,
    )


def _issue_view_to_lhp_error(issue: ValidationIssueView) -> "LHPError":
    """Reconstruct an :class:`LHPError` from a public :class:`ValidationIssueView`.

    Reverse of :func:`_lhp_error_to_issue_view`. Used by the generate
    stream-protocol wrapper to SURFACE a preflight issue as a raised
    error (§9.24): the preflight logic is single-sourced and returns
    views; this rebuilds the minimal carrying ``LHPError`` so the
    ``ErrorEmitted`` → ``raise`` rendezvous can fire with the issue's
    code / title / details / suggestions / context / doc_link intact.

    ``ValidationIssueView`` is a flattened projection (it does not retain
    the original ``code_number``), so the number is recovered from the
    trailing segment of ``issue.code`` (e.g. ``"LHP-VAL-009"`` → ``"009"``).
    Unstructured views (empty ``code``) round-trip to ``code_number=""``.
    """
    from lhp.errors import LHPError
    from lhp.errors.categories import ErrorCategory

    try:
        category = ErrorCategory(issue.category)
    except ValueError:
        category = ErrorCategory.GENERAL

    code_number = issue.code.rsplit("-", 1)[-1] if "-" in issue.code else ""

    return LHPError(
        category=category,
        code_number=code_number,
        title=issue.title,
        details=issue.details or "",
        suggestions=list(issue.suggestions) or None,
        context=dict(issue.context) or None,
        doc_link=issue.doc_link,
    )


def _emit_deprecation_warnings(
    records: Sequence["DeprecationWarningRecord"],
    *,
    seen: Set[Tuple[str, Optional[Path]]],
) -> Iterator[WarningEmitted]:
    """Yield :class:`WarningEmitted` for each not-yet-seen deprecation record.

    Shared by the generate stream (:mod:`lhp.api._generate_stream`) and the
    validate facade (:mod:`lhp.api._validation_facade`) to surface the two
    deprecation-warning sources as public §5.7 stream events:

    * the MAIN-THREAD bare-``{token}`` scan
      (``orchestrator.discovery.scan_deprecation_warnings()`` →
      ``LHP-DEPR-001``), emitted in/after the ``discover`` phase, and
    * the WORKER warnings merged off the engine's per-pipeline results
      (``database`` / ``database_suffix`` / schema-transform ``enforcement`` →
      ``LHP-DEPR-002/003/004``), emitted in/after the ``generate`` /
      ``validate`` phase.

    ``seen`` is the SHARED ``(code, file)`` dedup set the caller threads across
    both emission points: a record whose key is already in ``seen`` is skipped,
    and every emitted record's key is added. This gives ONE deduped, ordered
    sequence across the union of both sources (the same ``(code, file)`` never
    surfaces twice — e.g. a file flagged by both a main-thread scan and a
    worker), emitted in caller-iteration order (discover-phase records first,
    then the deterministic engine-merge order). Each record's
    :meth:`~lhp.api.WarningEmitted` carries ``message`` positionally and
    ``code`` / ``file`` / ``flowgroup`` as structured context, with
    ``category="deprecation"`` (§13.4 locked field order).
    """
    for record in records:
        key = (record.code, record.file)
        if key in seen:
            continue
        seen.add(key)
        yield WarningEmitted(
            record.message,
            code=record.code,
            category="deprecation",
            file=record.file,
            flowgroup=record.flowgroup,
        )
