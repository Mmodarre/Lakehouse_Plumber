"""Shared error <-> view conversion helpers for the public API surface.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Holds the cross-direction error-conversion helpers used by both the
generation and validation converter modules (and by the project
preflight): the :class:`LHPError` <-> :class:`ValidationIssueView`
projection pair, plus the shared warning-record emission helper
(deprecation + sandbox) used by the generate and validate streams.

:stability: internal
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Literal, Optional, Sequence, Set, Tuple

from lhp.api.events import WarningEmitted
from lhp.api.views import ValidationIssueView

if TYPE_CHECKING:
    from lhp.errors import LHPError
    from lhp.models import FlowGroup
    from lhp.models.processing import RunWarningRecord


def _derive_worklist_fields(
    pipeline_filter: Optional[str],
    pipeline_fields: Sequence[str],
    resolved_flowgroups: Sequence["FlowGroup"],
) -> Sequence[str]:
    """Return the pipeline-name worklist, auto-deriving the all-pipelines set.

    Single canonical worklist-derivation seam shared by the generate, validate,
    and plan streams (§4.1: one helper, not three copy-pastes). The streams
    already resolve the project-wide flowgroup set inside their ``discover``
    phase; this folds the previously-eager CLI ``derive_pipeline_fields`` step
    into the facade so a caller that supplies NEITHER a ``pipeline_filter`` NOR
    a ``pipeline_fields`` batch processes the WHOLE project (rather than getting
    the orchestrator's empty-worklist no-op).

    Precedence (unchanged when a worklist is supplied):

    * ``pipeline_filter`` set — returned unchanged; the single-pipeline filter
      is keyed downstream via ``pipeline_filter``, so ``pipeline_fields`` stays
      whatever the caller passed (the orchestrator forwards ``None`` for the
      fields when a filter is present).
    * ``pipeline_fields`` non-empty — returned unchanged (explicit batch wins).
    * NEITHER supplied — derive ``tuple(sorted({fg.pipeline for fg
      in resolved_flowgroups}))``: the deduplicated, sorted pipeline names from
      the stream's already-discovered set. This is byte-for-byte the worklist
      the CLI used to pre-compute via ``list_flowgroups()`` (the same
      ``bootstrap.discover_all_flowgroups()`` source), so no new discovery
      mechanism is introduced and no extra filesystem scan occurs.
    """
    if pipeline_filter is not None or pipeline_fields:
        return pipeline_fields
    return tuple(sorted({fg.pipeline for fg in resolved_flowgroups}))


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


def _emit_warning_records(
    records: Sequence["RunWarningRecord"],
    *,
    seen: Set[Tuple[str, Optional[Path]]],
) -> Iterator[WarningEmitted]:
    """Yield :class:`WarningEmitted` for each not-yet-seen warning record.

    Shared by the generate stream (:mod:`lhp.api._generate_stream`) and the
    validate facade (:mod:`lhp.api._validation_facade`) to surface the
    warning-record sources as public §5.7 stream events:

    * the MAIN-THREAD bare-``{token}`` scan
      (``orchestrator.discovery.scan_deprecation_warnings()`` →
      ``LHP-DEPR-001``), emitted in/after the ``discover`` phase, and
    * the WORKER warnings merged off the engine's per-pipeline results —
      deprecations (``database`` / ``database_suffix`` / schema-transform
      ``enforcement`` → ``LHP-DEPR-002/003/004``) and sandbox rewrite
      warnings (mixed-producer sink ``LHP-VAL-065`` / unrewritable indirect
      read ``LHP-VAL-066``) — emitted in/after the ``generate`` /
      ``validate`` phase.

    The event ``category`` is derived from the record type:
    :class:`~lhp.models.processing.SandboxWarningRecord` → ``"sandbox"``,
    :class:`~lhp.models.processing.DeprecationWarningRecord` →
    ``"deprecation"``.

    ``seen`` is the SHARED ``(code, file)`` dedup set the caller threads across
    both emission points: a record whose key is already in ``seen`` is skipped,
    and every emitted record's key is added. This gives ONE deduped, ordered
    sequence across the union of both sources (the same ``(code, file)`` never
    surfaces twice — e.g. a file flagged by both a main-thread scan and a
    worker), emitted in caller-iteration order (discover-phase records first,
    then the deterministic engine-merge order). Each record's
    :meth:`~lhp.api.WarningEmitted` carries ``message`` positionally and
    ``code`` / ``file`` / ``flowgroup`` as structured context (§13.4 locked
    field order).
    """
    # Deferred: this module loads eagerly with ``import lhp.api``, which must
    # not pull ``lhp.models`` (and pydantic via its package init) at import
    # time. By the time records exist, their defining module is loaded.
    from lhp.models.processing import SandboxWarningRecord

    for record in records:
        key = (record.code, record.file)
        if key in seen:
            continue
        seen.add(key)
        category = (
            "sandbox" if isinstance(record, SandboxWarningRecord) else "deprecation"
        )
        yield WarningEmitted(
            record.message,
            code=record.code,
            category=category,
            file=record.file,
            flowgroup=record.flowgroup,
        )
