"""Worker-warning folding for the flat engine's two merge points.

Two small, pure helpers that fold worker-attached
:data:`~lhp.models.processing.RunWarningRecord`s (deprecation AND sandbox
records), deduped by
``(code, file)`` in deterministic first-seen order:

* :func:`merge_flowgroup_warnings` — the PER-PIPELINE fold, run inside the
  engine's ``_finalize`` over one pipeline's sorted
  :class:`~lhp.models.processing.FlowgroupOutcome`s; its result rides on
  :attr:`_PipelinePoolResult.warnings`.
* :func:`merge_pool_warnings` — the WHOLE-BATCH fold, run by the executor over
  every :class:`_PipelinePoolResult`; its result is the single tuple the facade
  re-emits as :class:`~lhp.api.WarningEmitted` events.

Kept out of :mod:`._pool` so that module stays under the §3.3 size cap; this
leaf imports its collaborators only under ``TYPE_CHECKING`` (it merely reads
each carrier's ``.warnings`` attribute at runtime), so it adds NO runtime
import edge back to :mod:`._pool` — no cycle.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Sequence, Tuple

from ...models.processing import RunWarningRecord

if TYPE_CHECKING:
    from ...models.processing import FlowgroupOutcome
    from ._pool import _PipelinePoolResult


def _dedup_by_code_file(
    records: Sequence[RunWarningRecord],
) -> Tuple[RunWarningRecord, ...]:
    """Collapse records to the first-seen one per ``(code, file)`` key.

    A plain ``dict`` keyed by ``(code, file)`` preserves insertion order, so the
    result follows the caller's iteration order (never set-iteration order).
    ``message`` / ``flowgroup`` are NOT part of the key: the first record's full
    payload is kept verbatim.
    """
    deduped: Dict[Tuple[str, Optional[Path]], RunWarningRecord] = {}
    for record in records:
        deduped.setdefault((record.code, record.file), record)
    return tuple(deduped.values())


def merge_flowgroup_warnings(
    outcomes: Sequence["FlowgroupOutcome"],
) -> Tuple[RunWarningRecord, ...]:
    """Flatten + dedup a pipeline's per-flowgroup worker warnings.

    Each worker attaches any warnings it would have logged — deprecation AND
    sandbox records, the :data:`~lhp.models.processing.RunWarningRecord`
    union (it runs
    under a ``NullHandler``, so the logging channel is dead) — to its
    :attr:`FlowgroupOutcome.warnings`. This folds them across the pipeline's
    flowgroup outcomes into ONE collection (deduped by ``(code, file)``) for the
    main thread to re-emit as :class:`~lhp.api.WarningEmitted` events — the SAME
    deprecation surfaces once per affected file, not once per flowgroup. The
    caller iterates outcomes in ``flowgroup_name`` order, so first-seen order is
    deterministic.
    """
    flat = [warning for outcome in outcomes for warning in outcome.warnings]
    return _dedup_by_code_file(flat)


def merge_pool_warnings(
    pool_results: Sequence["_PipelinePoolResult"],
) -> Tuple[RunWarningRecord, ...]:
    """Merge + dedup the per-pipeline worker warnings across the whole batch.

    Each :attr:`_PipelinePoolResult.warnings` tuple is ALREADY deduped by
    ``(code, file)`` within its pipeline (:func:`merge_flowgroup_warnings`); this
    second fold collapses the same ``(code, file)`` across DIFFERENT pipelines to
    the first-seen record — the SAME deprecation surfaced once per affected file,
    never once per pipeline that touched it. ``pool_results`` arrives in input
    pipeline order with each pipeline's warnings already in ``flowgroup_name``
    order, so the merged sequence is deterministic. This is the SINGLE tuple the
    facade re-emits as the generate/validate-phase
    :class:`~lhp.api.WarningEmitted` events.
    """
    flat = [warning for result in pool_results for warning in result.warnings]
    return _dedup_by_code_file(flat)
