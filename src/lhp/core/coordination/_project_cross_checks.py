"""The project stage of the flat engine: checks that span every pipeline.

The per-pipeline cross-flowgroup barrier (:mod:`._pool`) sees one pipeline at a
time. A streaming read of a ``mode: replace_where`` table (``LHP-VAL-072``) is
wrong whichever pipeline reads it, so :func:`apply_project_checks` runs once,
after every pipeline's results are in and before either consumer reads them:
validate's report assembly and the generate gate. It joins the facts each
worker recorded on its :class:`~lhp.models.processing.FlowgroupOutcome`, asks
:meth:`ValidationService.validate_project_stream_reads` for the findings (§9.24:
detection lives there; the issues are built by
:func:`._cross_flowgroup_issues.build_cross_flowgroup_issues`), and attaches
them to each reading pipeline's ``cross_fg_issues``. Validate then reports them
with no owning flowgroup, and the gate stops generate before any file is
written.

A run over only some pipelines (``--pipeline``, a sandbox profile's scope)
sees only their facts, so a 2-part ``schema.table`` read cannot be resolved
against the project's producers: such a run leaves 2-part reads unmatched.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Sequence, Tuple

from ._cross_flowgroup_issues import build_cross_flowgroup_issues

if TYPE_CHECKING:
    from ...errors import LHPError
    from ._pool import _PipelinePoolResult
    from .validation_service import ValidationService

logger = logging.getLogger(__name__)


def apply_project_checks(
    pool_results: Sequence[_PipelinePoolResult],
    validation_service: ValidationService,
    partial_run: bool = False,
) -> List[_PipelinePoolResult]:
    """Return ``pool_results`` with the project-wide findings attached.

    ``partial_run`` is ``True`` when the run processed fewer pipelines than
    the project has; 2-part reads are then not matched. Each finding joins
    its reading pipeline's ``cross_fg_issues``. A run with
    no stream reads or no REPLACE WHERE producer is returned unchanged without
    consulting ``validation_service``. A raised :class:`~lhp.errors.LHPError`
    becomes an issue, and any other exception an error string, on every
    pipeline with a stream read, so the check never crashes the run and never
    passes silently.
    """
    from ...errors import LHPError

    producers = [
        fact
        for result in pool_results
        for outcome in result.outcomes_in_order
        for fact in outcome.producer_facts
    ]
    readers = [
        fact
        for result in pool_results
        for outcome in result.outcomes_in_order
        for fact in outcome.stream_read_facts
    ]
    if not readers or not any(fact.is_replace_where for fact in producers):
        return list(pool_results)

    issues: Dict[str, Tuple[LHPError, ...]] = {}
    errors: Dict[str, Tuple[str, ...]] = {}
    try:
        findings = validation_service.validate_project_stream_reads(
            producers, readers, match_two_part=not partial_run
        )
        issues = {
            pipeline: build_cross_flowgroup_issues(result, pipeline)
            for pipeline, result in findings.items()
        }
    except Exception as exc:
        reading_pipelines = {fact.pipeline for fact in readers}
        if isinstance(exc, LHPError):
            issues = dict.fromkeys(reading_pipelines, (exc,))
        else:
            logger.exception("REPLACE WHERE stream-read check raised")
            message = f"REPLACE WHERE stream-read check failed: {exc}"
            errors = dict.fromkeys(reading_pipelines, (message,))

    return [
        result._replace(
            cross_fg_issues=result.cross_fg_issues + issues.get(result.pipeline, ()),
            cross_fg_errors=result.cross_fg_errors + errors.get(result.pipeline, ()),
        )
        for result in pool_results
    ]
